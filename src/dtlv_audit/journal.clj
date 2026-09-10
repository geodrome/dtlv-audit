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

   Entry shape — this is the read API's return type, not an internal detail:

     [tx {:tx-data      [[e a v added] ...]  ; resolved datoms, not the input
          :tx-meta      {...}                ; declared annotations
          :tx-timestamp 1757500000000
          :new-attrs    [...]}]

   `tx` is Datalevin's own tx id and the journal key. Tx ids are dense and
   assigned in commit order, which makes them authoritative for sorting and
   makes a missing one meaningful — see `gaps`. `:tx-timestamp` is wall clock,
   for correlating with application logs only; never sort by it.

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

   Pure, and called before anything is opened: a bad declaration is a config
   error, throwing it here keeps a half-open journal from leaking.

   Type checking only. Whether a given attribute *name* is acceptable depends
   on what reads the journal — `dtlv-audit.projector` rejects the namespaces
   its projection owns — so that check belongs to the reader, not here. A
   journal with no projection attached has no reserved names."
  [annotations]
  (into {}
        (for [[k {:keys [db/valueType]}] annotations]
          (cond
            (not (keyword? k))
            (throw (ex-info "annotation attribute must be a keyword"
                            {:attribute k}))

            (type-preds valueType)
            [k (type-preds valueType)]

            :else
            (throw (ex-info "unsupported annotation value type"
                            {:attribute k :type valueType
                             :supported (set (keys type-preds))}))))))

;;; ---------------------------------------------------------------------------
;;; Open / close

(defn open
  "Opens a journal on an already-open domain connection.

   :annotations vocabulary callers may pass as tx-meta, in ordinary Datalevin
                schema shape:

                  {:audit/actor  {:db/valueType :db.type/string}
                   :audit/req-id {:db/valueType :db.type/string}}

                Kept on the journal so a reader can consult it — the projector
                merges it into the audit schema and enforces it there.

   :strict?     default true. `tx!` throws on an undeclared or mistyped
                annotation; when false it warns and passes the value through.

   Does not take ownership of `conn`: the KV handle is borrowed from it, and
   Datalevin is explicit that you close the Datalog connection, not the
   borrowed handle. `close!` honours that."
  [conn {:keys [annotations strict?] :or {annotations {}}}]
  ;; Pure work first, so a bad declaration throws before a DBI exists.
  (let [strict? (if (some? strict?) strict? true)
        preds   (build-preds annotations)
        ;; MUST be the domain conn's own environment, or the with-transaction
        ;; in tx! does not cover the journal put. d/datalog-kv is the supported
        ;; way to get that handle — (d/open-kv dir) is a second env on the same
        ;; directory, which LMDB forbids and Datalevin rejects.
        kv      (d/datalog-kv conn)]
    ;; Opening the DBI is idempotent and belongs at setup, outside any
    ;; transaction. It is also where a remote store gives itself away: its
    ;; store type implements almost none of the KV interface, so the call
    ;; fails with an AbstractMethodError that says nothing useful.
    (try
      (d/open-dbi kv journal-dbi)
      (catch AbstractMethodError e
        (throw (ex-info (str "journal requires an embedded Datalevin store — "
                             "this connection cannot host a KV sub-database, "
                             "so a domain write could commit without its "
                             "journal entry")
                        {:store (.getName (class kv))}
                        e))))
    {:conn        conn
     :kv          kv
     :dir         (d/dir kv)
     :annotations annotations
     :preds       preds
     :strict?     strict?
     ;; The handle is borrowed from conn, so close! must not close it. Carried
     ;; as data rather than assumed, because a backend that opened its own
     ;; handle would own it — see the namespace docstring.
     :owns-kv?    false}))

(defn close!
  "Releases what the journal owns, which for a borrowed handle is nothing.

   Exists so a journal-only consumer has one symmetric lifecycle call. Closing
   the domain connection remains the caller's job either way."
  [{:keys [kv owns-kv?]}]
  (when owns-kv? (d/close-kv kv))
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

   Returns the Datalevin tx-report unchanged — but read :db-after with care.
   It was built inside the transaction, and on commit the conn is reset to a
   different db value entirely. Query (d/db conn) afterwards, not the report.
   :tx-data and :tempids are plain data and are fine.

   The journal records the resolved datoms, not what the caller passed. The
   prior value of anything that changed is already there as a retraction, and
   why it changed is in the annotations — a declared, validated vocabulary,
   where the raw input would be a second, unbounded copy of the same intent.
   A re-assertion of an existing value therefore leaves a tx with annotations
   and no datoms."
  ([journal tx-data] (tx! journal tx-data nil))
  ([{:keys [conn preds strict?]} tx-data tx-meta]
   (let [tx-meta (into {} (remove (comp nil? val)) tx-meta)]
     (validate-meta! preds strict? tx-meta)
     (d/with-transaction [c conn]
       ;; The transaction-bound handle, taken from the transaction-bound conn,
       ;; is the shape Datalevin documents for writing KV inside a Datalog
       ;; transaction. The journal's long-lived handle happens to reach the
       ;; same write txn today, but that is an implementation detail of the
       ;; LMDB binding and not something to rest the guarantee on.
       (let [c-kv   (d/datalog-kv c)
             report (d/transact! c tx-data)
             ;; from db-after, not from a datom — :tx-data is empty on a no-op
             tx     (:max-tx (:db-after report))]
         (d/transact-kv c-kv
                        [[:put journal-dbi tx
                          {:tx-data      (mapv (juxt :e :a :v :added) (:tx-data report))
                           :tx-meta      tx-meta
                           :tx-timestamp (System/currentTimeMillis)
                           :new-attrs    (vec (:new-attributes report))}
                          :long :data]])
         report)))))

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
  [{:keys [kv]} from-tx limit]
  (with-open [entries ^AutoCloseable (d/range-seq kv journal-dbi
                                                  [:at-least from-tx]
                                                  :long :data)]
    (into [] (take limit) (seq entries))))

(defn bounds
  "[oldest newest] tx id in the journal, or nil when it is empty."
  [{:keys [kv]}]
  (when-let [head (first (d/get-first kv journal-dbi [:all-back] :long :ignore))]
    [(first (d/get-first kv journal-dbi [:all] :long :ignore)) head]))

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
