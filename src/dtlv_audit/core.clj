(ns dtlv-audit.core
  "Domain DB + durable transaction journal + queryable audit projection.

   Datalevin keeps no history: when data is retracted it is gone. That makes
   \"how did the DB get into this state?\" unanswerable. This library closes
   that gap.

   `tx!` writes the transaction report to a KV journal in the *same* LMDB
   transaction as the domain write. A polling projector then turns journal
   entries into a separate audit DB you can query with Datalog.

   The journal is the durable record. The audit DB is derived and disposable —
   `rebuild!` replays the journal from scratch.

   This namespace is the composed convenience: it opens both halves, holds
   them together, and adds the queries. The halves are independently usable
   and independently documented:

     dtlv-audit.journal    the durable record — write and read it, with no
                           audit DB, no projector and no background thread
     dtlv-audit.projector  the derived audit DB following a journal

   Either DB may be embedded or on a Datalevin server (a dtlv:// URI for :dir
   or :audit-dir). A server-hosted domain DB keeps the same atomicity; see
   `dtlv-audit.journal` for how, and for the Datalevin version it needs.

   Usage:

     (def store (open! {:dir \"data/domain\"
                        :audit-dir \"data/audit\"
                        :schema domain-schema
                        :conn-opts {:auto-entity-time? true}
                        :annotations {:audit/actor  {:db/valueType :db.type/string}
                                      :audit/req-id {:db/valueType :db.type/string}
                                      :audit/cmd    {:db/valueType :db.type/keyword}
                                      :audit/op     {}}}))
     (def stop! (start-projector! store))

     (tx! store [{:quote/number \"Q-1042\" :quote/total 4500}]
                 {:audit/actor \"geo\" :audit/req-id \"01J8X\" :audit/cmd :quote/create})

     (history store 42)
     (by-annotation store :audit/req-id \"01J8X\")

   An annotation declared with no :db/valueType — :audit/op above — is
   untyped: it takes any value, stored as EDN and matchable by equality. That
   is what carries a value with no scalar type, such as a command in the shape
   [:entity/verb args].

   Read-then-write needs one transaction across both halves. `with-tx` scopes
   a store to one, and writes through the scoped store join it:

     (with-tx [s store]
       (when (ok? (d/q ... (db s)))
         (tx! s tx-data {:audit/cmd :quote/create})))

   Reads take a plain conn — only writes need the store:

     (d/q '[...] (db store))"
  (:require [datalevin.core :as d]
            [dtlv-audit.journal :as journal]
            [dtlv-audit.projector :as projector]))

;;; ---------------------------------------------------------------------------
;;; Open / close

(defn open!
  "Opens a store: a domain conn, a journal on it, and an audit projection.

   :dir         domain DB directory, or a dtlv:// URI
   :audit-dir   audit DB directory or dtlv:// URI — must not resolve to :dir
   :schema      domain schema, as passed to d/get-conn
   :annotations vocabulary callers may pass as tx-meta, in ordinary Datalevin
                schema shape:

                  {:audit/actor  {:db/valueType :db.type/string}
                   :audit/req-id {:db/valueType :db.type/string}
                   :audit/cmd    {:db/valueType :db.type/keyword}}

                One declaration, three uses: checked by tx!, merged into the
                audit schema, and enforced there by :closed-schema?. Keys in
                the tx, datom and marker namespaces are rejected — those
                belong to the projection.

   :strict?     default true. Governs both layers together: tx! throws on an
                undeclared or mistyped annotation, and the audit DB is opened
                with :closed-schema?. When false, tx! warns and the audit DB
                accepts undeclared annotations as EDN blobs.

   :conn-opts   options for the *domain* conn, as passed to d/get-conn. These
                are the caller's own — :auto-entity-time?, :validate-data?,
                :closed-schema?, sizing — and this fn has no opinion on them.
                Not defaulted: a store opened here must behave exactly as the
                same store opened with d/get-conn directly, or journaling a
                write would change the write.

   :audit-conn-opts  the same for the audit conn, minus :closed-schema?, which
                :strict? owns. Separate from :conn-opts because the two DBs
                are shaped by different forces: the domain schema is the
                caller's, while the audit schema is this library's plus the
                declared annotations.

   Config validation lives downstream now — annotation types in
   `journal/open`, reserved namespaces and the shared-directory check in
   `projector/open` — so that it holds for anyone composing the two by hand,
   not only for callers of this fn. The consequence is that the domain conn
   opens before a config error is found, which is why everything after
   d/get-conn hands it back before rethrowing: a throw that stranded an
   environment nobody holds a handle to would, in-process, leave the directory
   locked for good."
  [{:keys [dir audit-dir schema annotations strict? conn-opts audit-conn-opts]
    :or   {annotations {}}}]
  (let [conn (d/get-conn dir schema conn-opts)
        j    (try
               (journal/open conn {:annotations annotations :strict? strict?})
               (catch Throwable t
                 (d/close conn)
                 (throw t)))
        p    (try
               (projector/open j {:dir audit-dir :conn-opts audit-conn-opts})
               (catch Throwable t
                 ;; The journal holds nothing to release — closing the domain
                 ;; conn is the whole unwind. See `dtlv-audit.journal`.
                 (d/close conn)
                 (throw t)))]
    {:conn conn :journal j :projector p}))

(defn close!
  "Closes the store, stopping the projector first if one is attached.

   Returns false if that projector was still draining after 10s — the store is
   closed either way, since the caller asked, but a drain outliving the
   environment it writes to is worth surfacing rather than swallowing.

   Two things close, not three: the journal holds nothing to release, its KV
   handle being borrowed from `conn`.

   Order matters: the projector goes down first because a drain in flight
   reads the journal, and the domain conn goes last because that handle is
   borrowed from it."
  [{:keys [conn projector]}]
  (let [stopped (projector/close! projector)]
    (d/close conn)
    stopped))

(defn db
  "Domain db value. Reads don't need the store — pass this to d/q, d/pull, etc."
  [store]
  (d/db (:conn store)))

(defn audit-db [store] (projector/db (:projector store)))

;;; ---------------------------------------------------------------------------
;;; Write path

(defn tx!
  "Transacts into the domain DB, journaling the report in the same LMDB
   transaction. See `dtlv-audit.journal/tx!`."
  ([store tx-data] (journal/tx! (:journal store) tx-data nil))
  ([store tx-data tx-meta] (journal/tx! (:journal store) tx-data tx-meta)))

(defmacro with-tx
  "Runs `body` with `bound` set to `store` scoped to one transaction: its
   journal rebound, and `db` reading the transaction's own uncommitted
   writes. See `dtlv-audit.journal/with-tx`.

     (with-tx [s store]
       (when (ok? (d/q ... (db s)))
         (tx! s tx-data {:audit/cmd :quote/create})))"
  [[bound store] & body]
  `(let [s# ~store]
     (journal/with-tx [j# (:journal s#)]
       (let [~bound (assoc s# :journal j# :conn (journal/conn j#))]
         ~@body))))

(defn clear-journal!
  "DESTRUCTIVE: empties the journal. See `dtlv-audit.journal/clear!`.

   Leaves the audit DB describing entries that no longer exist — `rebuild!`
   is what reconciles the two, and after this it empties the projection."
  [store]
  (journal/clear! (:journal store)))

;;; ---------------------------------------------------------------------------
;;; Projection
;;;
;;; Thin delegations. The four ways to advance the projection, and how they
;;; mix, are documented on `dtlv-audit.projector`.

(defn drain!
  "Projects up to one batch of journal entries. Returns how many."
  [store]
  (projector/drain! (:projector store)))

(defn catch-up!
  "Drains until the journal is exhausted. Returns how many were projected."
  [store]
  (projector/catch-up! (:projector store)))

(defn start-projector!
  "Polls the journal on a background thread. Returns a stop fn."
  [store & opts]
  (apply projector/start! (:projector store) opts))

(defn projector-status
  "What the projector is doing, for a health check or the REPL."
  [store]
  (projector/status (:projector store)))

(defn rebuild!
  "Clears the audit DB and replays the entire journal."
  [store]
  (projector/rebuild! (:projector store)))

;;; ---------------------------------------------------------------------------
;;; Queries
;;;
;;; Every query here returns rows in commit order, via Datalevin's :order-by
;;; rather than a sort in Clojure. Two consequences worth knowing: the engine
;;; sorts typed columns instead of Clojure sorting a set of boxed tuples, which
;;; measured ~18x faster on a 1600-row history; and an ordered result comes
;;; back as a seq, not the set an unordered :find relation returns. Rows are
;;; still deduplicated — it is the same relation, just arranged.
;;;
;;; Order keys never include a datom value. :datom/v holds arbitrary EDN, so
;;; comparing across rows would throw the moment two values differ in type.
;;;
;;; Reconstructing state at a point in time is a fold over changes ordered by
;;; :tx/id, not a query — Datalog is bad at "latest value before T", and
;;; cardinality-many attributes need the domain schema consulted during the
;;; fold. If that becomes common, project an interval model off the same
;;; journal instead of folding repeatedly.

(defn history
  "Every change to a domain entity, oldest first — and within a tx, by
   attribute, retraction before assertion, so an attribute's old value reads
   directly above its new one."
  [store eid]
  (d/q '[:find ?tx ?a ?v ?added ?at
         :in $ ?e
         :where [?d :datom/e ?e]
         [?d :datom/tx ?tx]
         [?d :datom/a ?a]
         [?d :datom/v ?v]
         [?d :datom/added ?added]
         [?t :tx/id ?tx]
         [?t :tx/at ?at]
         :order-by [?tx ?a ?added]]
       (audit-db store) eid))

(defn by-annotation
  "Every change made by transactions carrying k=v — e.g. one request id, which
   is usually how you enter the log when correlating with application logs.

   The attribute is a variable, so this cannot use an attribute index and
   scans the tx entities. Fine at the scale a single request id implies; if
   one annotation becomes a hot lookup path, query it with a literal
   attribute instead."
  [store k v]
  (d/q '[:find ?e ?a ?val ?added ?tx
         :in $ ?k ?v
         :where [?t ?k ?v]
         [?t :tx/id ?tx]
         [?t :tx/datom ?d]
         [?d :datom/e ?e]
         [?d :datom/a ?a]
         [?d :datom/v ?val]
         [?d :datom/added ?added]
         :order-by [?tx ?e ?a ?added]]
       (audit-db store) k v))

(defn changes-between
  "Every change in the tx range [from to) — `from` inclusive, `to` exclusive,
   so consecutive windows tile without overlapping. Uses :datom/tx directly
   rather than joining through tx entities; note the bounds are predicates
   over a scan of :datom/tx, not a range scan of it."
  [store from to]
  (d/q '[:find ?tx ?e ?a ?v ?added
         :in $ ?from ?to
         :where [?d :datom/tx ?tx]
         [(<= ?from ?tx)] [(< ?tx ?to)]
         [?d :datom/e ?e]
         [?d :datom/a ?a]
         [?d :datom/v ?v]
         [?d :datom/added ?added]
         :order-by [?tx ?e ?a ?added]]
       (audit-db store) from to))

(defn transaction
  "One transaction with its annotations and datoms inline. :tx/datom is a
   component, which a wildcard expands with a wildcard, so [*] already pulls
   the datoms whole. Use an explicit pattern with :limit for bulk
   transactions — otherwise every datom comes back."
  [store tx]
  (d/pull (audit-db store) '[*] [:tx/id tx]))

(defn no-ops
  "Transactions that committed but changed nothing — a re-assertion of an
   existing value, or an upsert that resolved to an existing entity. The
   annotations say which command did it, which is the operationally useful
   part: a sync whose writes are mostly no-ops is doing redundant work.

   Scans every tx entity and returns them all. Narrow with changes-between's
   bounds if the audit DB has grown past what you want to hold in memory."
  [store]
  (d/q '[:find ?tx ?at
         :where [?t :tx/id ?tx] [?t :tx/at ?at]
         [(missing? $ ?t :tx/datom)]
         :order-by [?tx]]
       (audit-db store)))

(defn schema-drift
  "Transactions that introduced a new attribute. Under schema-on-write these
   are usually typo'd keywords rather than deliberate changes.

   Unbounded like no-ops, but bounded in practice by how many distinct
   attributes have ever been written."
  [store]
  (d/q '[:find ?tx ?attr
         :where [?t :tx/new-attrs ?attr] [?t :tx/id ?tx]
         :order-by [?tx ?attr]]
       (audit-db store)))
