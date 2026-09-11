(ns dtlv-audit.journal-test
  "The journal on its own.

   Deliberately does NOT require dtlv-audit.core or dtlv-audit.projector: a
   consumer who wants the durable record and nothing else must be able to get
   it from this namespace alone. If this file ever needs another one of ours,
   the boundary has leaked."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [dtlv-audit.journal :as journal])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- tmpdir []
  (str (Files/createTempDirectory "dtlv-audit-journal"
                                  (make-array FileAttribute 0))))

(def ^:private domain-schema
  {:quote/number {:db/valueType :db.type/string
                  :db/unique :db.unique/identity}
   :quote/total  {:db/valueType :db.type/long}})

(defn- with-journal
  "Opens a domain conn and a journal on it, calls (f conn journal), then closes.
   The journal borrows its handle, so only the conn gets closed."
  ([f] (with-journal {} f))
  ([opts f]
   (let [conn (d/get-conn (tmpdir) domain-schema)
         j    (journal/open conn opts)]
     (try (f conn j)
          (finally (d/close conn))))))

(defn- all-entries [j] (journal/entries-from j 0 1000))

(def ^:private actor-only {:annotations {:audit/actor {:db/valueType :db.type/string}}})

(deftest standalone-usage-test
  (testing "write and read the journal with no projection anywhere"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1" :quote/total 100}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-2" :quote/total 200}] {:audit/actor "sam"})

        (let [entries (all-entries j)
              [tx {:keys [tx-data tx-meta tx-timestamp]}] (first entries)]
          (is (= 2 (count entries)))
          (is (pos-int? tx))
          (is (= {:audit/actor "geo"} tx-meta))
          (is (pos-int? tx-timestamp))
          (is (some (fn [[_ a v added]]
                      (and (= :quote/number a) (= "Q-1" v) (true? added)))
                    tx-data)
              "records the resolved datoms")

          (testing "bounds spans what was written"
            (let [[oldest newest] (journal/bounds j)]
              (is (= oldest (first (first entries))))
              (is (= newest (first (last entries))))))

          (testing "entries-from is bounded by limit and offset"
            (is (= 1 (count (journal/entries-from j 0 1))))
            (is (= 1 (count (journal/entries-from j (inc tx) 1000))))))))))

(deftest empty-journal-test
  (testing "bounds is nil and reads are empty before anything is written"
    (with-journal
      (fn [_conn j]
        (is (nil? (journal/bounds j)))
        (is (empty? (all-entries j)))))))

(deftest annotation-validation-test
  (testing "strict? rejects undeclared and mistyped annotations"
    (with-journal actor-only
      (fn [_conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/tx! j [{:quote/number "Q-1"}] {:audit/nope "x"})))
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor 42})))
        (is (empty? (all-entries j))
            "a rejected annotation costs nothing — the tx never opened"))))

  (testing "strict? false lets an undeclared annotation through"
    (with-journal {:annotations {} :strict? false}
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/whatever "x"})
        (is (= 1 (count (all-entries j)))))))

  (testing "a bad declaration is a config error, thrown before a DBI exists"
    (let [conn (d/get-conn (tmpdir) domain-schema)]
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/open conn {:annotations {:audit/x {:db/valueType :db.type/ratio}}})))
        (finally (d/close conn)))))

  (testing "nil-valued annotations are dropped, not rejected"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo" :audit/req-id nil})
        (is (= {:audit/actor "geo"} (-> (all-entries j) first second :tx-meta)))))))

(deftest no-op-tx-test
  (testing "a re-assertion journals a tx with annotations and no datoms"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (let [[_ entry] (last (all-entries j))]
          (is (empty? (:tx-data entry)))
          (is (= {:audit/actor "geo"} (:tx-meta entry))))))))

(deftest atomicity-test
  (testing "a failing tx! leaves neither the domain write nor a journal entry"
    (with-journal
      (fn [conn j]
        (is (thrown? Exception
                     ;; :quote/total is :db.type/long — this fails inside the
                     ;; transaction, after it has opened
                     (journal/tx! j [{:quote/number "Q-1" :quote/total "not-a-long"}])))
        (is (empty? (all-entries j)))
        (is (nil? (d/q '[:find ?e . :where [?e :quote/number "Q-1"]] (d/db conn)))))))

  (testing "a throw after the KV put rolls the put back with the domain write"
    ;; The library's central claim, at the level it actually rests on. tx!
    ;; cannot express a throw *after* its own journal write, so this drives the
    ;; same transaction shape directly.
    (with-journal
      (fn [conn j]
        (journal/tx! j [{:quote/number "Q-keep"}])
        (let [before (count (all-entries j))]
          (is (thrown? Exception
                       (d/with-transaction [c conn]
                         (let [c-kv (d/datalog-kv c)
                               r    (d/transact! c [{:quote/number "Q-doomed"}])]
                           (d/transact-kv c-kv journal/journal-dbi
                                          [[:put (:max-tx (:db-after r)) {:tx-data []}]]
                                          :long :data)
                           (throw (ex-info "boom" {}))))))
          (is (= before (count (all-entries j)))
              "the journal put aborted with the domain transaction")
          (is (nil? (d/q '[:find ?e . :where [?e :quote/number "Q-doomed"]] (d/db conn))))
          (is (some? (d/q '[:find ?e . :where [?e :quote/number "Q-keep"]] (d/db conn)))
              "and the earlier committed write survived"))))))

(deftest gaps-test
  (testing "dense ids report no gaps"
    (is (empty? (journal/gaps 4 [[5 {}] [6 {}] [7 {}]]))))

  (testing "a missing id between entries is reported"
    (is (= [[5 9]] (journal/gaps 4 [[5 {}] [9 {}]]))))

  (testing "a gap against the anchor is reported"
    (is (= [[4 7]] (journal/gaps 4 [[7 {}] [8 {}]]))))

  (testing "a negative anchor drops it — a fresh reader starts mid-journal"
    (is (empty? (journal/gaps -1 [[7 {}] [8 {}]])))))

;;; ---------------------------------------------------------------------------
;;; Untyped annotations

(def ^:private untyped
  {:annotations {:audit/op    {}
                 :audit/actor {:db/valueType :db.type/string}}})

(deftest untyped-annotation-test
  (testing "an annotation declared with no :db/valueType accepts any value"
    (with-journal untyped
      (fn [_conn j]
        ;; the case this exists for: a command as [:entity/verb args]
        (journal/tx! j [{:quote/number "Q-1"}]
                     {:audit/op [:quote/create {:number "Q-1" :total 100}]})
        (journal/tx! j [{:quote/number "Q-2"}] {:audit/op {:nested {:map true}}})
        (journal/tx! j [{:quote/number "Q-3"}] {:audit/op 42})

        (is (= [[:quote/create {:number "Q-1" :total 100}]
                {:nested {:map true}}
                42]
               (mapv (comp :audit/op :tx-meta second) (all-entries j)))
            "value round-trips as EDN, whatever its type"))))

  (testing "untyped is declared, not undeclared — an unknown key still throws"
    (with-journal untyped
      (fn [_conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/tx! j [{:quote/number "Q-4"}] {:audit/nope "x"}))))))

  (testing "a typed annotation alongside an untyped one is still checked"
    (with-journal untyped
      (fn [_conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/tx! j [{:quote/number "Q-5"}] {:audit/actor 99}))))))

  (testing "an unsupported value type is still a config error"
    (is (thrown? clojure.lang.ExceptionInfo
                 (journal/build-preds {:audit/x {:db/valueType :db.type/ref}})))))

;;; ---------------------------------------------------------------------------
;;; with-tx — one journal, both positions

(deftest with-tx-test
  (testing "read-then-write in one transaction, journaled"
    (with-journal actor-only
      (fn [_conn j]
        (journal/with-tx [j* j]
          ;; the shape a store-scoped journal cannot serve: the read has to see
          ;; the same transaction the write lands in
          (when-not (d/q '[:find ?e . :where [?e :quote/number "Q-1"]]
                         (journal/db j*))
            (journal/tx! j* [{:quote/number "Q-1" :quote/total 100}]
                         {:audit/actor "geo"})))
        (is (= 1 (count (all-entries j))))
        (is (= "geo" (-> (all-entries j) first second :tx-meta :audit/actor))))))

  (testing "several writes in one transaction each get their own entry"
    (with-journal actor-only
      (fn [_conn j]
        (journal/with-tx [j* j]
          (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor "geo"})
          (journal/tx! j* [{:quote/number "Q-2"}] {:audit/actor "geo"})
          (journal/tx! j* [{:quote/number "Q-3"}] {:audit/actor "geo"}))
        (let [txs (mapv first (all-entries j))]
          (is (= 3 (count txs)) "no entry overwrote another")
          (is (apply < txs) "distinct, ascending tx ids")))))

  (testing "a throw rolls back the domain writes and their journal entries"
    (with-journal actor-only
      (fn [conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/with-tx [j* j]
                       (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor "geo"})
                       (throw (ex-info "rollback" {})))))
        (is (empty? (all-entries j)) "journal entry rolled back with the write")
        (is (nil? (d/q '[:find ?e . :where [?e :quote/number "Q-1"]] (d/db conn)))))))

  (testing "annotations are validated the same in a transaction"
    (with-journal actor-only
      (fn [_conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/with-tx [j* j]
                       (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor 99})))))))

  (testing "the same tx! call works unscoped, opening its own transaction"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-9"}] {:audit/actor "geo"})
        (is (= 1 (count (all-entries j))))))))

;;; ---------------------------------------------------------------------------
;;; commits

(defn- commits [j]
  (mapv (fn [[tx entry]] [tx (:commit entry)]) (all-entries j)))

(deftest commit-test
  (testing "writes in one transaction record the commit they shared"
    (with-journal actor-only
      (fn [_conn j]
        (journal/with-tx [j* j]
          (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor "geo"})
          (journal/tx! j* [{:quote/number "Q-2"}] {:audit/actor "geo"})
          (journal/tx! j* [{:quote/number "Q-3"}] {:audit/actor "geo"}))
        (let [cs (commits j)
              [first-tx] (first cs)]
          (is (= 3 (count cs)))
          (is (apply = (map second cs)) "one commit across all three")
          (is (= first-tx (second (first cs)))
              "identified by the tx it began at")))))

  (testing "a write outside a transaction is its own commit, so a reader
            grouping by :commit needs no special case"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-2"}] {:audit/actor "geo"})
        (is (every? (fn [[tx commit]] (= tx commit)) (commits j))))))

  (testing "separate transactions get separate commits"
    (with-journal actor-only
      (fn [_conn j]
        (journal/with-tx [j* j]
          (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor "geo"})
          (journal/tx! j* [{:quote/number "Q-2"}] {:audit/actor "geo"}))
        (journal/with-tx [j* j]
          (journal/tx! j* [{:quote/number "Q-3"}] {:audit/actor "geo"}))
        (is (= 2 (count (distinct (map second (commits j)))))))))

  (testing "a nested with-tx joins the enclosing commit rather than opening a
            rival — the same way its writes join the transaction"
    (with-journal actor-only
      (fn [_conn j]
        (journal/with-tx [outer j]
          (journal/tx! outer [{:quote/number "Q-1"}] {:audit/actor "geo"})
          (journal/with-tx [inner outer]
            (journal/tx! inner [{:quote/number "Q-2"}] {:audit/actor "geo"})))
        (is (= 1 (count (distinct (map second (commits j)))))))))

  (testing "and a rolled-back transaction leaves no commit behind"
    (with-journal actor-only
      (fn [_conn j]
        (is (thrown? clojure.lang.ExceptionInfo
                     (journal/with-tx [j* j]
                       (journal/tx! j* [{:quote/number "Q-1"}] {:audit/actor "geo"})
                       (throw (ex-info "rollback" {})))))
        (is (empty? (commits j)))))))

;;; ---------------------------------------------------------------------------
;;; clear!

(deftest clear-test
  (testing "retracting the domain data leaves the journal intact — clear! is
            what empties it"
    (with-journal actor-only
      (fn [conn j]
        (journal/tx! j [{:quote/number "Q-1" :quote/total 100}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-2" :quote/total 200}] {:audit/actor "geo"})
        (let [eids (d/q '[:find [?e ...] :where [?e :quote/number _]] (d/db conn))]
          (journal/tx! j (mapv (fn [e] [:db.fn/retractEntity e]) eids) nil))

        (is (nil? (d/q '[:find ?e . :where [?e :quote/number _]] (d/db conn)))
            "domain data is gone")
        (is (= 3 (count (all-entries j)))
            "the record of it is not — including the retraction itself")

        (is (true? (journal/clear! j)))
        (is (empty? (all-entries j)))
        (is (nil? (journal/bounds j)) "reads as an empty journal, not a broken one"))))

  (testing "the journal is usable after clearing"
    (with-journal actor-only
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (journal/clear! j)
        (journal/tx! j [{:quote/number "Q-2"}] {:audit/actor "sam"})
        (is (= 1 (count (all-entries j))))
        (is (= "sam" (-> (all-entries j) first second :tx-meta :audit/actor)))))))

;;; ---------------------------------------------------------------------------
;;; Annotations reaching Datalevin

(deftest annotations-reach-listeners-test
  (testing "annotations are handed to Datalevin too, so a listener sees why a
            write happened and not only what changed"
    (with-journal untyped
      (fn [conn j]
        (let [heard (atom [])]
          (d/listen! conn :probe (fn [r] (swap! heard conj (:tx-meta r))))
          (try
            (journal/tx! j [{:quote/number "Q-1"}] {:audit/op [:quote/create]})
            (journal/with-tx [j* j]
              (journal/tx! j* [{:quote/number "Q-2"}] {:audit/op [:quote/rename]}))
            (is (= [{:audit/op [:quote/create]} {:audit/op [:quote/rename]}] @heard)
                "through both the standalone and the transaction-scoped path")
            (finally (d/unlisten! conn :probe)))))))

  (testing "and the journal still holds them — Datalevin does not persist
            tx-meta, so this is not a second copy of the record"
    (with-journal untyped
      (fn [_conn j]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/op [:quote/create]})
        (is (= [{:audit/op [:quote/create]}]
               (mapv (comp :tx-meta second) (all-entries j))))))))

;;; ---------------------------------------------------------------------------
;;; What tx! returns

(deftest tx-return-test
  (testing "the two ids that place a write in the journal come back with it"
    (with-journal untyped
      (fn [_conn j]
        (let [r  (journal/tx! j [{:quote/number "Q-1"}] {:audit/op [:a]})
              [[key entry]] (all-entries j)]
          (is (= key (:tx r)) ":tx is the journal key")
          (is (= (:commit entry) (:commit r)))
          (is (= (:tx r) (:commit r))
              "a standalone write is its own commit")))))

  (testing ":commit points back at the commit's first write, which nothing else
            in the report could tell you"
    (with-journal untyped
      (fn [_conn j]
        (journal/with-tx [j* j]
          (let [r1 (journal/tx! j* [{:quote/number "Q-1"}] {:audit/op [:a]})
                r2 (journal/tx! j* [{:quote/number "Q-2"}] {:audit/op [:b]})]
            (is (= (:tx r1) (:commit r1) (:commit r2)))
            (is (not= (:tx r2) (:commit r2))
                "the second write's own tx id is not the commit"))))))

  (testing "the dead db handles are gone, and what is left is plain data"
    (with-journal untyped
      (fn [_conn j]
        (let [r (journal/tx! j [{:db/id "e" :quote/number "Q-1"}] {:audit/op [:a]})]
          (is (not (contains? r :db-before)))
          (is (not (contains? r :db-after)))
          (is (= #{:tx :commit :tx-data :tempids :tx-meta} (set (keys r))))
          (is (some? (get (:tempids r) "e")) "tempids still resolve")
          (is (= {:audit/op [:a]} (:tx-meta r)) "annotations echo back")
          (is (= (:tx r) (:db/current-tx (:tempids r)))
              "and :tx is the same number Datalevin buried in :tempids"))))))
