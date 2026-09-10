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
          (finally (journal/close! j) (d/close conn))))))

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
