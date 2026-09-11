(ns dtlv-audit.journal
  "Durable transaction journal for an embedded Datalevin store.

   Datalevin keeps no history: when data is retracted it is gone. The journal
   is the record of what was there. `tx!` writes the transaction report to a
   KV sub-database in the *same* LMDB transaction as the domain write, so a
   domain write and its journal entry commit together or not at all.

   Usable on its own. `dtlv-audit.projector` turns entries into a queryable
   audit DB and `dtlv-audit.core` wires the two together, but neither is
   required — a consumer that wants the durable record and nothing else needs
   only this namespace:

     (def conn (d/get-conn \"data/domain\" domain-schema))
     (def j    (open conn {:annotations {:audit/actor {:db/valueType :db.type/string}}}))

     (tx! j [{:quote/number \"Q-1042\"}] {:audit/actor \"geo\"})

     (let [[oldest _] (bounds j)]
       (doseq [[tx entry] (entries-from j oldest 500)]
         (ship! tx entry)))

     (d/close conn)

   There is no close for the journal, and that is not an omission. `open`
   acquires nothing to release: the KV handle is borrowed from `conn`, and the
   sub-database lives and dies with the store. Closing the domain connection is
   the whole of it. A journal is a value describing where to write, not a
   resource held open.

   Entry shape — this is the read API's return type, not an internal detail:

     [tx {:commit       2                    ; tx id this commit began at
          :tx-data      [[e a v added] ...]  ; resolved datoms, not the input
          :tx-meta      {...}                ; declared annotations
          :tx-timestamp 1757500000000
          :new-attrs    [...]}]

   Datalevin's `d/transact!` assigns a tx id; `d/with-transaction` commits, 
   and may contain several transacts. So a
   commit holds one or more txs, and an entry records both: `tx` is its own,
   `:commit` is the tx id the commit it belonged to began at. A write outside
   any transaction is its own commit, so the two are equal.

   Outside this journal, Datalevin retains no transaction identity at
   all.

   `tx` is the key. Tx ids are dense and assigned in commit order, which makes
   them authoritative for sorting and makes a missing one meaningful — see
   `gaps`. That density is why the key is the tx and not the commit.
   `:tx-timestamp` is wall clock, for correlating with application logs only;
   never sort by it.

   Embedded Datalevin only. The atomicity above comes from the journal living
   in the domain conn's own LMDB environment, reached through `d/datalog-kv`.
   In Datalevin's words, it \"only works for DBIs that live in the same local
   Datalevin store; it is not a cross-file transaction mechanism.\" A remote
   store (a dtlv:// URI) cannot provide the handle at all, and `open` rejects
   it rather than let a domain write commit without its journal entry."
  (:require [datalevin.core :as d]
            [taoensso.trove :as trove])
  (:import [java.lang AutoCloseable]))

(def journal-dbi "tx-journal")

;;; ---------------------------------------------------------------------------
;;; Annotations

(def ^:private type-preds
  {:db.type/string  string?
   :db.type/keyword keyword?
   :db.type/long    int?
   :db.type/boolean boolean?
   :db.type/instant inst?
   :db.type/uuid    uuid?})

(defn build-preds
  "Compiles an annotation declaration into predicates for `tx!` to check.

   An annotation declared with no :db/valueType is untyped: any value passes,
   and the projector stores it the way Datalevin stores an attribute with no
   declared type — as EDN, matchable by equality but not comparable or
   range-scannable. That is the same bargain :datom/v takes in the audit
   schema, and it is what lets an annotation carry a value with no scalar
   type: a command as [:entity/verb args], a request context map. Untyped is
   declared, not undeclared — the attribute is still in the vocabulary, still
   merged into the audit schema, and still accepted under :closed-schema?.
   The escape hatch for genuinely unknown keys remains :strict? false.

   Type checking only. Whether a given attribute *name* is acceptable depends
   on what reads the journal — `dtlv-audit.projector` rejects the namespaces
   its projection owns — so that check belongs to the reader, not here. A
   journal with no projection attached has no reserved names."
  [annotations]
  (into {}
        (for [[k decl] annotations
              :let [valueType (:db/valueType decl)]]
          (cond
            (not (keyword? k))
            (throw (ex-info "annotation attribute must be a keyword"
                            {:attribute k}))

            ;; Untyped: declared, with no type to check against.
            (nil? valueType)
            [k any?]

            (type-preds valueType)
            [k (type-preds valueType)]

            :else
            (throw (ex-info "unsupported annotation value type"
                            {:attribute k :type valueType
                             :supported (conj (set (keys type-preds)) nil)}))))))

;;; ---------------------------------------------------------------------------
;;; Opening
;;;
;;; `open` compiles the annotation declaration, creates the KV sub-database,
;;; and builds the journal map — of which only the middle one has any effect,
;;; and that one is idempotent. Opening a journal is not acquiring a resource:
;;; it is making sure the store has somewhere to put entries, and handing back
;;; a value that says how to write them. Nothing needs releasing afterwards;
;;; see the ns docstring.

(defn- kv-handle
  "The KV handle for `conn`'s own environment.

   MUST be the domain conn's own, or the transaction in `tx!` does not cover
   the journal put. `d/datalog-kv` is the supported way to get it — (d/open-kv
   dir) is a second environment on the same directory, which LMDB forbids and
   Datalevin rejects. Derived per call rather than stored, and it has to be:
   inside a transaction the conn to write through is the transaction-bound
   one, and only a handle taken from *that* writes into the transaction."
  [conn]
  (d/datalog-kv conn))

(defn open
  "A journal on an already-open domain connection, with its KV sub-database
   created.

   :annotations vocabulary callers may pass as tx-meta, in ordinary Datalevin
                schema shape:

                  {:audit/actor  {:db/valueType :db.type/string}
                   :audit/req-id {:db/valueType :db.type/string}}

                Kept on the journal so a reader can consult it — the projector
                merges it into the audit schema and enforces it there.

   :strict?     default true. `tx!` throws on an undeclared or mistyped
                annotation; when false it warns and passes the value through."
  [conn {:keys [annotations strict?] :or {annotations {}}}]
  ;; Compile first. A bad declaration is a config error, and throwing before
  ;; the DBI exists keeps it from leaving a half-set-up store behind.
  (let [preds (build-preds annotations)
        kv    (kv-handle conn)]
    (try
      ;; d/open-dbi is idempotent so this is safe on every store open
      ;;
      ;; A remote store fails with an AbstractMethodError: its store type implements
      ;; almost none of the KV interface. Rejected here rather than
      ;; left to surface at the first write.
      (d/open-dbi kv journal-dbi)
      (catch AbstractMethodError e
        (throw (ex-info (str "journal requires an embedded Datalevin store — "
                             "this connection cannot host a KV sub-database, "
                             "so a domain write could commit without its "
                             "journal entry")
                        {:store (.getName (class kv))}
                        e))))
    {:conn        conn
     :annotations annotations
     :preds       preds
     :strict?     (if (some? strict?) strict? true)}))

(defn conn
  "The connection to write and read through. Inside `with-tx` this is the
   transaction-bound one, which is the whole point of rebinding."
  [journal]
  (:conn journal))

(defn db
  "Database value for reads: `(d/q ... (db journal))`, `(d/pull (db journal) ...)`.

   Exists so an application can hold one thing per store. A journal is a conn
   plus what journaling needs, and a caller that reads through this never has
   to carry the conn separately or reach inside for it."
  [journal]
  (d/db (:conn journal)))

(defn dir
  "Where the journal lives, which is wherever its store does. For a caller
   that needs to tell two stores apart — the projector refuses to put an audit
   DB in the domain store's directory."
  [{:keys [conn]}]
  (d/dir (kv-handle conn)))

(defn clear!
  "DESTRUCTIVE: empties the journal, discarding every entry."
  [{:keys [conn]}]
  (d/clear-dbi (kv-handle conn) journal-dbi)
  true)

;;; ---------------------------------------------------------------------------
;;; Write

(defn- validate-meta!
  "Checked before the transaction opens, so a throw costs nothing and the stack
   trace points at the call site rather than at a reader, hours later."
  [preds strict? meta]
  (when-let [bad (seq (for [[k v] meta
                            :let  [p (get preds k)]
                            :when (or (nil? p) (not (p v)))]
                        [k v]))]
    (let [msg  "invalid tx annotation"
          data {:problems (vec bad) :declared (set (keys preds))}]
      (if strict?
        (throw (ex-info msg data))
        (trove/log! {:level :warn :id ::invalid-annotation
                     :msg msg :data data})))))

(defn tx!
  "Transacts into the domain DB, journaling the report to KV in the same LMDB
   transaction. Mirrors d/transact! but takes a journal, and checks tx-meta
   against the declared annotations first.

   One write function for both positions, because the journal says which one it
   is in. Given a journal from `open`, this opens a transaction on the
   store. Given one rebound by `with-tx`, whose conn is already transaction-
   bound, the same call nests on that bound conn — which Datalevin treats as
   joining the open transaction, not as a second writer. So a rollback out
   there takes these writes and their journal entries with it.

   `annotations` are handed to Datalevin as well as journaled, so they reach
   the tx-report and anything registered with `d/listen!` — a listener sees why
   a write happened, not only what changed. Datalevin does not persist them;
   the journal is still the only durable record.

   Returns the Datalevin tx-report without its two db handles, plus the two
   ids that place this write in the journal:

     :tx      this write's tx id, and the key of its journal entry
     :commit  the commit it belonged to — equal to :tx when it was the first
              write of one, so grouping by it needs no special case

   :db-before and :db-after are dropped rather than passed through. Both are
   dead by the time this returns: they were built inside the transaction, and
   this always runs in one — its own, or the caller's under `with-tx` — so
   reading either throws a NullPointerException from the LMDB binding about a
   null `rtx`. They would not help even alive; in Datalevin both handles wrap
   the same mutable store, so `db-before` answers with current state rather
   than prior state. `:tx-data` is the before/after that survives: the old
   value is there as a retraction, the new one as an assertion. For anything
   else, read `(db journal)`.

   :tx-data, :tempids, :tx-meta and :new-attributes are plain data and are
   fine. `(:db/current-tx (:tempids report))` is the same number as :tx.

   The journal records the resolved datoms, not what the caller passed. The
   prior value of anything that changed is already there as a retraction, and
   why it changed is in the annotations. A re-assertion of an existing value
   therefore leaves a tx with annotations and no datoms."
  ([journal tx-data] (tx! journal tx-data nil))
  ([{:keys [conn preds strict?] commit_ :commit} tx-data annotations]
   ;; Nil-valued annotations are dropped rather than declared-but-empty, so a
   ;; caller can splice an absent one in without it becoming a key with no
   ;; value — and without it failing validation as a mistyped annotation.
   (let [meta (into {} (remove (comp nil? val)) annotations)]
     (validate-meta! preds strict? meta)
     (d/with-transaction [c conn]
       ;; Two writes, which is why this always opens a transaction even for a
       ;; lone call: the domain write and the journal put have to commit
       ;; together or the record has a hole in it. The KV handle comes from
       ;; `c`, the transaction-bound conn, not from the one the journal was
       ;; built on — only a handle taken from `c` writes into this transaction.
       (let [c-kv   (kv-handle c)
             report (d/transact! c tx-data meta)
             ;; from db-after, not from a datom — :tx-data is empty on a no-op
             tx     (:max-tx (:db-after report))
             ;; `commit_` is the enclosing commit's id holder, or nil for a
             ;; write that is its own commit. The first write through a commit
             ;; claims it — `swap!` with `or` so later writes read what the
             ;; first set rather than overwriting it.
             commit (if commit_ (swap! commit_ #(or % tx)) tx)]
         (d/transact-kv c-kv
                        [[:put journal-dbi tx
                          {:commit       commit
                           :tx-data      (mapv (juxt :e :a :v :added) (:tx-data report))
                           :tx-meta      meta
                           :tx-timestamp (System/currentTimeMillis)
                           :new-attrs    (vec (:new-attributes report))}
                          :long :data]])
         ;; Not the Datalevin tx-report verbatim. The two db handles are
         ;; dropped: both are dead by the time this returns — see the docstring
         ;; — and a key whose only behaviour is to throw invites the use it
         ;; punishes, where an absent one is visible in the map. :tx and
         ;; :commit are added because they are what ties this write to its
         ;; journal entry, and :commit is otherwise unrecoverable: for the
         ;; first write of a commit it equals :tx, which makes it look
         ;; derivable right up until a second write proves it is not.
         (-> report
             (dissoc :db-before :db-after)
             (assoc :tx tx :commit commit)))))))

(defmacro with-tx
  "Runs `body` with `bound` set to `journal` rebound to one transaction, and
   every write through it joining that transaction. A throw rolls back the
   domain writes and their journal entries together.

   For read-then-write work — check an invariant, then write what it implies —
   which has to hold one transaction across both halves:

     (with-tx [j journal]
       (when-not (seq (d/q ... (db j)))
         (tx! j tx-data {:audit/cmd :quote/create})))

   Read through `(db j)`, not the journal you started with: only the rebound
   one sees the transaction's own uncommitted writes.

   Writes must go through `bound`. The journal you passed in still points at
   the store's own conn, and a write through it would open a second write
   transaction and wait forever on a lock this one holds.

   Several writes here each get their own tx id and their own entry — ids are
   assigned per d/transact!, not per commit — and every one records the commit
   they shared, so the grouping survives without the caller arranging it. A
   nested `with-tx` inherits the enclosing commit rather than starting one, so
   a transaction stays one unit however it is composed."
  [[bound journal] & body]
  `(let [j# ~journal]
     (d/with-transaction [c# (:conn j#)]
       ;; The holder the first write claims. Inherited when there is one, so a
       ;; nested with-tx joins the commit it is inside rather than opening a
       ;; rival — the same way its writes join the transaction.
       (let [~bound (assoc j# :conn c# :commit (or (:commit j#) (atom nil)))]
         ~@body))))

;;; ---------------------------------------------------------------------------
;;; Read

(defn entries-from
  "The next `limit` journal entries at or after `from-tx`, oldest first.

   Entries, not a tx-id window. A window would be simpler given dense ids, but
   then a gap wider than the window is a silent stall: every scan comes back
   empty while the reader's offset sits below the gap forever. Counting
   entries steps over a gap of any size.

   range-seq is the lazy counterpart to get-range and holds an open read
   transaction, so the result is realized and the transaction closed here,
   rather than staying open across whatever the caller does next.

   Go through `seq`, as its docstring says to: its IReduceInit steps over
   internal fetch batches, not over entries, so reducing it directly hands
   you vectors of kv pairs and a `take` would count batches."
  [{:keys [conn]} from-tx limit]
  (with-open [entries ^AutoCloseable (d/range-seq (kv-handle conn) journal-dbi
                                                  [:at-least from-tx]
                                                  :long :data)]
    (into [] (take limit) (seq entries))))

(defn bounds
  "[oldest newest] tx id in the journal, or nil when it is empty."
  [{:keys [conn]}]
  (let [kv (kv-handle conn)]
    (when-let [head (first (d/get-first kv journal-dbi [:all-back] :long :ignore))]
      [(first (d/get-first kv journal-dbi [:all] :long :ignore)) head])))

(defn gaps
  "Missing tx ids in `entries`, as [after next] pairs.

   Tx ids are dense — they increment even on no-ops — so a gap means a domain
   transaction committed without its journal entry: someone bypassed `tx!`, or
   the KV put failed. It is the one case where a missing entry carries
   information.

   `last-tx` anchors the run against whatever the caller has already seen.
   Pass a negative value when there is nothing to anchor to: the journal
   starts at whatever tx id the domain store had already reached opening its
   own schema, not at 0, so anchoring a first read against a sentinel would
   report every fresh store as a bypass.

   Returns data and logs nothing. A gap is a signal that something wrote
   outside `tx!`, never a reason to stop reading."
  [last-tx entries]
  (for [[a b] (partition 2 1 (cond->> (map first entries)
                               (nat-int? last-tx) (cons last-tx)))
        :when (not= b (inc a))]
    [a b]))
