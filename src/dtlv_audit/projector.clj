(ns dtlv-audit.projector
  "Queryable audit projection built from a `dtlv-audit.journal`.

   The journal is the durable record; this is derived and disposable —
   `rebuild!` replays the journal from scratch, which is what makes the audit
   schema safe to iterate on.

   A projector is an audit DB following a journal, and holds the journal it
   follows. It reads through `entries-from` / `bounds` / `gaps` and never
   touches the KV encoding underneath.

   The projection is a pure function of journal entries plus its own offset.
   Crashes, restarts and long outages all resolve the same way: scan forward
   from the marker.

   Four ways to advance it, in increasing order of how much they do:

     drain!      one batch, then return
     catch-up!   batches until the journal is exhausted
     start!      catch-up! on a timer, forever, until the stop fn
     rebuild!    discard the audit DB and replay the journal whole

   Pick by who owns the schedule. `start!` if this library should own it;
   `catch-up!` if something else already does — a startup sequence that wants
   a warm audit DB before serving, a test that wants a projection without a
   sleep, a batch job with no background thread at all; `drain!` if that
   something else also wants to bound the work per run.

   Mixing them is safe, and deliberately so. Every drain takes the lock and
   re-reads the marker inside it, so a caller's `catch-up!` racing a running
   projector just means one of them finds less to do. `rebuild!` depends on
   this: it replays outside the lock precisely so a live projector can keep
   working through it."
  (:require [clojure.java.io :as io]
            [datalevin.core :as d]
            [dtlv-audit.journal :as journal]
            [taoensso.trove :as trove])
  (:import [java.util.concurrent Executors ThreadFactory TimeUnit]))

;;; ---------------------------------------------------------------------------
;;; Schema

(def audit-schema
  {;; Each journaled datom becomes an entity
   :datom/e     {:db/doc "entity id from the domain DB — NOT a ref"
                 :db/valueType :db.type/long}
   :datom/a     {:db/valueType :db.type/keyword}
   :datom/v     {:db/doc "edn value, equality-only matching"}
   :datom/tx    {:db/doc "denormalized with :tx/datom — enables range scans over
                          tx and ordering within a result set without joining
                          the tx entity, and lets datoms be built independent
                          of it (which some projections need)"
                 :db/valueType :db.type/long}
   :datom/added {:db/valueType :db.type/boolean}

   :tx/id    {:db/doc "Datalevin's own tx id, also the journal key"
              :db/valueType :db.type/long
              :db/unique :db.unique/identity}
   :tx/at    {:db/doc "epoch millis — for correlating with application logs
                       only. :tx/id is commit order and is authoritative for
                       sorting; wall clock is not."
              :db/valueType :db.type/long}
   :tx/commit {:db/doc "tx id the commit this belonged to began at. Datalevin
                        assigns an id per transact! and commits per
                        with-transaction, so one commit may hold several txs;
                        this is what says which. Equal to :tx/id for a write
                        that was its own commit, so grouping by it needs no
                        special case."
               :db/valueType :db.type/long}
   :tx/datom {:db/doc "datom entities are immutable and belong to this tx alone,
                       hence isComponent: pull inlines them, retractEntity
                       cascades. :datom/tx survives the cascade if that ever
                       needs to change."
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/isComponent true}

   :tx/new-attrs {:db/doc "attributes first seen in this tx. With schema-on-write
                           a non-empty value may be a typo'd keyword, not a
                           deliberate schema change — worth alerting on."
                  :db/valueType :db.type/keyword
                  :db/cardinality :db.cardinality/many}

   ;; Projector state. The offset lives here, not in KV, so it advances in the
   ;; same transaction as the entities it covers — no double-projection window.
   :marker/name    {:db/valueType :db.type/keyword :db/unique :db.unique/identity}
   :marker/last-tx {:db/valueType :db.type/long}})

(def ^:private reserved-nses
  "Namespaces the projection owns, derived from the schema rather than
   restated. An annotation in one of these would get merged over a projection
   attribute rather than beside it — :tx/id most damagingly, since ->tx-entity
   merges tx-meta on top of it and a caller passing that key would silently
   rewrite the journal key on the audit entity, taking the tx entity's
   identity, the marker relationship and every query with it.

   This is the projection's restriction, not the journal's: a journal with no
   projection attached has no reason to refuse :tx/id."
  (into #{} (map namespace) (keys audit-schema)))

(def ^:private batch
  "Journal entries per audit transaction. An entry count, not a tx-id window:
   see journal/entries-from."
  500)

;;; ---------------------------------------------------------------------------
;;; Open / close

(defn- same-dir?
  "Compares two store locations. Canonical paths rather than strings, so
   \"data/x\" and \"data/x/\" are caught; a location java.io.File cannot make
   sense of (a remote URI) falls back to comparing the text."
  [a b]
  (letfn [(canon [x] (try (.getCanonicalPath (io/file (str x)))
                          (catch Exception _ (str x))))]
    (= (canon a) (canon b))))

(defn open
  "Opens an audit projection over `journal`.

   :dir       audit DB directory — must not resolve to the journal's own
   :strict?   defaults to the journal's. Governs :closed-schema? on the audit
              DB, so it moves with the journal's own strictness unless you are
              mid-migration.

              Tightening later is loose-then-strict: declare everything the
              journal already contains, `rebuild!`, then flip the flag. Note
              that a value first written undeclared is stored under a different
              encoding than the same value written after the type is declared,
              so they will not compare equal — rebuilding after declaring
              reconciles them.

   :conn-opts extra options for the audit conn, as passed to d/get-conn.
              Merged *under* :closed-schema?, which :strict? owns: the two
              would otherwise be one setting with two sources, and a caller
              passing :closed-schema? here would silently disagree with the
              strictness the journal validates against. Everything else —
              :auto-entity-time?, :validate-data?, sizing — is the caller's.

   The annotation vocabulary comes from the journal, so it is declared once
   and used three times: checked by journal/tx!, merged into the audit schema
   here, and enforced there by :closed-schema?.

   Does not take ownership of the journal or its connection; `close!` closes
   only what this opened."
  [journal {:keys [dir strict? conn-opts]}]
  (let [{:keys [annotations]} journal
        strict? (if (some? strict?) strict? (:strict? journal))]
    (when-let [bad (seq (filter (comp reserved-nses namespace) (keys annotations)))]
      (throw (ex-info "annotation must be a keyword outside the reserved namespaces"
                      {:attributes (vec bad) :reserved reserved-nses})))
    ;; Two conns on one directory is a second LMDB env on one file, which LMDB
    ;; forbids. Caught here rather than at the composing layer so it holds for
    ;; anyone who assembles a journal and a projector by hand.
    (when (same-dir? (journal/dir journal) dir)
      (throw (ex-info "domain and audit DBs cannot share a directory"
                      {:dir (journal/dir journal) :audit-dir dir})))
    ;; rebuild! has to close and reopen the audit conn, so the projector holds
    ;; the recipe for opening it, and holds the conn itself by reference — a
    ;; projector already running keeps working across a rebuild.
    (let [open-audit #(d/get-conn dir
                                  (merge audit-schema annotations)
                                  (assoc conn-opts :closed-schema? strict?))]
      {:journal    journal
       :audit      (atom (open-audit))
       :open-audit open-audit
       ;; Serializes writers of the audit DB. Two concurrent drains would both
       ;; read the same marker and project the same entries twice: :tx/id upserts,
       ;; but datom entities have no identity, so the second pass duplicates every
       ;; datom under its tx. rebuild! takes it to swap the conn.
       :lock       (Object.)
       ;; Readable via `status` whether or not a projector thread is running —
       ;; a health check has the projector, not the stop fn.
       :state      (atom {:running? false})
       ;; The running thread's stop fn, so close! can stop it. It is handed to
       ;; the caller as well, but close! is given only the projector, and a
       ;; thread left ticking against a closed environment fails on every poll
       ;; forever. Doubles as the interlock against a second projector.
       :stopper    (atom nil)})))

(defn db
  "Audit db value. Reads don't need anything else — pass this to d/q, d/pull."
  [{:keys [audit]}]
  (d/db @audit))

;;; ---------------------------------------------------------------------------
;;; Projection

(defn- last-projected
  "Returns last projected tx or -1"
  [audit-db]
  (or (:marker/last-tx (d/entity audit-db [:marker/name :projector])) -1))

(defn- ->tx-entity [tx {:keys [commit tx-data tx-meta tx-timestamp new-attrs]}]
  ;; commit defaults to tx for entries written before the field existed.
  (cond-> (merge {:tx/id tx :tx/at tx-timestamp :tx/commit (or commit tx)} tx-meta)
    (seq new-attrs) (assoc :tx/new-attrs new-attrs)
    (seq tx-data)   (assoc :tx/datom
                           (mapv (fn [[e a v added]]
                                   {:datom/e     e
                                    :datom/a     a
                                    :datom/v     v
                                    :datom/added added
                                    :datom/tx    tx})
                                 tx-data))))

(defn- log-gaps!
  [last-tx entries]
  (doseq [[a b] (journal/gaps last-tx entries)]
    (trove/log! {:level :warn :id ::journal-gap
                 :msg  "journal gap — bypassed tx! or failed KV write"
                 :data {:after a :next b}})))

(defn- clear-failure-state!
  "Drops the stuck/backoff bookkeeping. Called on any successful drain, since
   a drain that returns at all is proof the projector is not stuck where it
   was — whoever ran it, and whether or not the projector is the one that
   fixed things. Otherwise `status` keeps reporting a resolved :stuck-at, and
   :retry-at keeps suppressing ticks, for up to max-backoff-ms after the
   problem is gone.

   Reads before it writes: the healthy case is every drain forever, and it
   should not pay for a swap. Two threads clearing at once is idempotent."
  [state]
  (when (:last-error @state)
    (swap! state assoc
           :failures 0 :retry-at nil :last-error nil
           :stuck-at nil :stuck-since nil)))

(defn drain!
  "Projects up to `batch` journal entries. Returns how many were projected.

   The marker moves in the same audit transaction as the entities it covers,
   so this is never partially applied and never needs to be idempotent. It
   moves to the last entry actually seen, so a gap is logged and stepped over
   rather than stranding everything above it."
  [{:keys [journal audit lock state]}]
  (locking lock
    (let [conn    @audit
          last-tx (last-projected (d/db conn))
          from-tx (inc last-tx)]
      (try
        (let [entries (journal/entries-from journal from-tx batch)
              n       (if-not (seq entries)
                        0
                        (do
                          (log-gaps! last-tx entries)
                          (d/transact! conn
                                       (conj (mapv (fn [[tx v]] (->tx-entity tx v))
                                                   entries)
                                             {:marker/name    :projector
                                              :marker/last-tx (first (last entries))}))
                          (count entries)))]
          ;; Reaching here at all — even with nothing to do, which means the
          ;; marker is at the head — settles that the projector is no longer
          ;; stuck where it was.
          (clear-failure-state! state)
          n)
        (catch Throwable e
          ;; The batch is atomic, so the marker is still where it was: whatever
          ;; went wrong, the projector cannot get past from-tx. Naming it is what
          ;; lets the caller tell "same failure again" from "a new one".
          (throw (ex-info "audit projection failed"
                          {:stuck-at from-tx :batch batch}
                          e)))))))

(defn catch-up!
  "Drains until the journal is exhausted. Batched, so a first run against an
   existing journal doesn't try to project everything in one transaction.

   Stops early if the calling thread has been interrupted. Nothing is lost —
   the marker moves per batch, so the next run resumes where this one left off
   — and it bounds shutdown at one batch rather than one journal.

   On a normal return, everything journal/tx! committed before the call is
   projected: the entry is already in the journal, and this only returns once
   a drain finds nothing left. That holds whoever does the draining, so it is
   a usable barrier even with a projector running.

   Not gated by the projector's backoff, and clears it on success — a stuck
   projector can be prodded from a REPL, and if the prod works, the recovery
   counts."
  [projector]
  (loop [total 0]
    (let [n (drain! projector)]
      (if (and (pos? n) (not (.isInterrupted (Thread/currentThread))))
        (recur (+ total n))
        total))))

(def ^:private max-backoff-ms (* 5 60 1000))

(defn- backoff-ms
  "Retry delay after `failures` consecutive failures: the interval, doubling,
   capped. A journal entry this code cannot project fails identically forever —
   recovery comes from a deploy and a rebuild!, never from the next poll — so
   retrying it at the poll rate is pure churn. Backing off also thins the
   still-stuck log line without suppressing it."
  [interval-ms failures]
  (min max-backoff-ms
       (* (long interval-ms) (bit-shift-left 1 (min 20 (dec (long failures)))))))

(defn status
  "What the projector is doing, for a health check or the REPL:

     :running?       is a projector thread attached
     :journal-head   newest tx id in the journal
     :last-projected newest tx id in the audit DB
     :lag            how many txs behind the audit DB is — the number to alarm
                     on, since a stuck projector shows up here long before
                     anyone reads the logs
     :failures       consecutive failed runs, 0 when healthy
     :stuck-at       tx id the projector cannot get past
     :stuck-since    when it first failed there
     :retry-at       when the projector will next attempt that tx
     :last-error     class, message and tx of the most recent failure

   The failure fields are cleared by the first successful drain from any
   caller, not only by a projector tick, so they do not go on reporting a
   problem someone already fixed by hand.

   Reads no locks, so during a rebuild! it can briefly see an empty audit DB."
  [{:keys [journal state] :as projector}]
  (let [[oldest head] (journal/bounds journal)
        done          (long (last-projected (db projector)))
        ;; Tx ids are dense, so lag is a subtraction — but only against a real
        ;; baseline. Before anything is projected there is none: the journal
        ;; starts at whatever id the domain store had reached, not at 0, and
        ;; last-projected's -1 is a sentinel, not a tx. Anchor on the oldest
        ;; entry instead, so a fresh store reports the entries it actually has.
        base          (if (neg? done) (some-> oldest long dec) done)]
    (assoc @state
           :journal-head   head
           :last-projected (when-not (neg? done) done)
           :lag            (when (and head base) (- (long head) (long base))))))

(defn start!
  "Polls the journal every interval-ms. The interval is the only latency bound:
   nothing here runs in the write path, so the cost of the projector being down
   is lag, never a failed domain write. Returns a stop fn.

   Fixed delay rather than fixed rate: a drain that overruns the interval pushes
   the next one out instead of stacking runs on a backlog.

   A failing drain does not stop the projector, and does not spam either. The
   first failure at a given tx is logged with its exception; further failures
   there are one thinning warn line, because the interesting fact — that the
   audit DB stopped advancing at tx N — is already in `status`, and is fixed
   by a deploy plus a rebuild!, not by the next poll.

   One thread per projector: a second would be correct, since the lock
   serializes drains, but the two would share the state atom and each stop fn
   would report the other stopped. Throws rather than allow it.

   :thread-name names that thread, defaulting to \"audit-projector\". Worth
   setting as soon as a process runs more than one — per tenant, per store —
   since the name is all a thread dump, a profiler or a stuck-thread log line
   has to say which projector is the one not moving. `status` answers that for
   a projector you already hold; a thread name answers it for one you are
   looking at from the outside.

   stop! returns true if the projector stopped, false if it was still draining
   after 10s. Only a stop that succeeded releases the slot — while a drain is
   still running, there is nothing safe to restart alongside."
  [{:keys [state stopper] :as projector}
   & {:keys [interval-ms thread-name]
      :or   {interval-ms 1000 thread-name "audit-projector"}}]
  (when-not (pos-int? interval-ms)
    (throw (ex-info "interval-ms must be a positive integer" {:interval-ms interval-ms})))
  (when-not (compare-and-set! stopper nil ::starting)
    (throw (ex-info "a projector is already running" {:projector @state})))
  (try
    (let [exec (Executors/newSingleThreadScheduledExecutor
                (reify ThreadFactory
                  (newThread [_ r]
                    (doto (Thread. ^Runnable r ^String thread-name)
                      (.setDaemon true)))))]
      (reset! state {:running? true :started-at (System/currentTimeMillis)
                     :failures 0})
      (.scheduleWithFixedDelay
       exec
       ;; Anything escaping this fn cancels every future run, and silently —
       ;; nobody holds the ScheduledFuture to see the exception. Catching here is
       ;; what keeps a single bad journal entry from stopping the projector for
       ;; the life of the process.
       (fn tick []
         (try
           (let [{:keys [retry-at]} @state]
             (when (or (nil? retry-at) (<= (long retry-at) (System/currentTimeMillis)))
               (catch-up! projector)
               ;; The clear is drain!'s: any successful drain does it, however
               ;; it was triggered. All this tick still owns is its heartbeat.
               (swap! state assoc
                      :last-run-at (System/currentTimeMillis))))
           (catch Throwable e
             ;; An interrupted drain is a stop!, not a projection failure. Logging
             ;; it would make every shutdown look like an incident.
             (when-not (or (.isShutdown exec)
                           (.isInterrupted (Thread/currentThread)))
               (let [now   (System/currentTimeMillis)
                     stuck (:stuck-at (ex-data e))
                     st    (swap! state
                                  (fn [{:keys [failures last-error stuck-since] :as s}]
                                    (let [same? (and last-error
                                                     (= stuck (:stuck-at last-error)))
                                          fails (if same? (inc (long failures)) 1)]
                                      (assoc s
                                             :failures    fails
                                             ;; Top level, not only inside
                                             ;; :last-error: this is the field
                                             ;; `status` promises and the one
                                             ;; worth alarming on.
                                             :stuck-at    stuck
                                             :stuck-since (if same? stuck-since now)
                                             :retry-at    (+ now (backoff-ms interval-ms fails))
                                             ;; drain! wraps, so the cause carries
                                             ;; what actually went wrong.
                                             :last-error  (let [root (or (ex-cause e) e)]
                                                            {:at       now
                                                             :stuck-at stuck
                                                             :class    (.getName (class root))
                                                             :message  (ex-message root)})))))]
                 (if (= 1 (long (:failures st)))
                   (trove/log! {:level :error :id ::projector-failed
                                :data {:stuck-at stuck} :error e})
                   (trove/log! {:level :warn :id ::projector-stuck
                                :msg  "audit projector still stuck"
                                :data {:stuck-at     stuck
                                       :failures     (:failures st)
                                       :stuck-for-ms (- now (long (:stuck-since st)))
                                       :retry-in-ms  (- (long (:retry-at st)) now)}})))))))
       0 interval-ms TimeUnit/MILLISECONDS)
      (let [stop (fn stop! []
                   ;; shutdownNow interrupts a drain in progress; catch-up! checks
                   ;; between batches, so this returns in batch time, not journal
                   ;; time.
                   (.shutdownNow exec)
                   (let [stopped (.awaitTermination exec 10 TimeUnit/SECONDS)]
                     (swap! state assoc :running? false)
                     (if stopped
                       ;; compare-and-set!, not reset!: releasing the slot
                       ;; unconditionally would let a stale stop fn unregister a
                       ;; projector started after it.
                       (do (compare-and-set! stopper stop! nil) true)
                       (do (trove/log! {:level :warn :id ::projector-slow-stop
                                        :msg "audit projector still draining after 10s"})
                           false))))]
        (reset! stopper stop)
        stop))
    (catch Throwable t
      ;; The slot was claimed above; hand it back rather than wedge things
      ;; into a state where no projector can ever start.
      (reset! stopper nil)
      (throw t))))

(defn close!
  "Stops the projector thread if one is attached, then closes the audit DB.

   Returns false if that thread was still draining after 10s — the audit DB is
   closed either way, since the caller asked, but a drain outliving the
   environment it writes to is worth surfacing rather than swallowing.

   Closes only what `open` opened. The journal and its connection belong to
   whoever opened them, and must be closed after this, not before: a drain
   still in flight reads the journal."
  [{:keys [audit stopper]}]
  (let [stopped (if-let [stop! @stopper] (stop!) true)]
    (d/close @audit)
    stopped))

;;; ---------------------------------------------------------------------------
;;; Rebuild

(defn rebuild!
  "Clears the audit DB and replays the entire journal. Safe whenever the audit
   schema changes — the journal is the source of truth, so getting the
   projection wrong costs a rebuild, not data. Worth exercising early; it is
   what makes the audit schema safe to iterate on.

   d/clear closes the conn and drops the schema along with the data. The schema
   wipe is the point — it is what lets an attribute change type — so the conn is
   reopened rather than reused, and the projector hands the new one to everyone
   holding it, including a running projector thread.

   Safe to run with the projector up: the clear and the swap happen under the
   same lock a drain takes, and a drain landing mid-replay only advances the
   marker this call would have advanced anyway. Reads are not locked, so a query
   racing the swap sees an empty audit DB, or — in a window of microseconds — a
   closed conn. A rebuild is a maintenance operation, not a live view.

   Clears the projector's stuck/backoff state on success, since a rebuild is
   the documented way out of it — as does any other successful drain."
  [{:keys [audit lock open-audit] :as projector}]
  (locking lock
    (d/clear @audit)
    (reset! audit (open-audit)))
  ;; The stuck/backoff state a rebuild resolves is cleared by the drains this
  ;; runs, not here — catch-up! always makes at least one.
  (catch-up! projector))
