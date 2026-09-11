(ns dtlv-audit.projector-test
  "The projector driven directly, composed onto a journal by hand — the path a
   consumer takes when they own the connection lifecycle or the schedule."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [dtlv-audit.journal :as journal]
            [dtlv-audit.projector :as projector])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- tmpdir []
  (str (Files/createTempDirectory "dtlv-audit-projector"
                                  (make-array FileAttribute 0))))

(def ^:private domain-schema
  {:quote/number {:db/valueType :db.type/string
                  :db/unique :db.unique/identity}
   :quote/total  {:db/valueType :db.type/long}})

(def ^:private annotations
  {:audit/actor {:db/valueType :db.type/string}})

(defn- with-parts
  "Journal and projector assembled by hand; calls (f conn journal projector),
   then closes in the order the docstrings require: projector down first,
   domain conn last. The journal is not closed — it holds nothing to release."
  [f]
  (let [conn (d/get-conn (tmpdir) domain-schema)
        j    (journal/open conn {:annotations annotations})
        p    (projector/open j {:dir (tmpdir)})]
    (try (f conn j p)
         (finally (projector/close! p)
                  (d/close conn)))))

(deftest hand-composed-projection-test
  (testing "catch-up! projects what the journal holds, queryable via projector/db"
    (with-parts
      (fn [conn j p]
        (journal/tx! j [{:quote/number "Q-1" :quote/total 100}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-1" :quote/total 200}] {:audit/actor "sam"})

        (is (= 2 (projector/catch-up! p)))
        (is (zero? (projector/catch-up! p)) "second run finds nothing left")

        (let [adb (projector/db p)
              eid (d/q '[:find ?e . :where [?e :quote/number "Q-1"]] (d/db conn))]
          (is (= 2 (d/q '[:find (count ?t) . :where [?t :tx/id]] adb)))
          ;; :order-by vars must appear in :find, so tx comes back and is dropped
          (is (= ["geo" "sam"]
                 (mapv second
                       (d/q '[:find ?tx ?actor
                              :where [?t :audit/actor ?actor] [?t :tx/id ?tx]
                              :order-by [?tx]]
                            adb))))
          (testing "the whole history: the assertion, then its retraction beside the new value"
            (is (= [[100 true] [100 false] [200 true]]
                   (mapv (comp vec rest)
                         (d/q '[:find ?tx ?v ?added
                                :in $ ?e
                                :where [?d :datom/e ?e] [?d :datom/a :quote/total]
                                [?d :datom/v ?v] [?d :datom/added ?added] [?d :datom/tx ?tx]
                                :order-by [?tx ?added]]
                              adb eid))))))))))

(deftest status-test
  (testing "lag reflects unprojected entries and clears on catch-up"
    (with-parts
      (fn [_conn j p]
        (is (nil? (:journal-head (projector/status p))))
        (is (false? (:running? (projector/status p))))

        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (let [s (projector/status p)]
          (is (some? (:journal-head s)))
          (is (nil? (:last-projected s)))
          (is (= 1 (:lag s)) "a fresh projector anchors on the oldest entry"))

        (projector/catch-up! p)
        (let [s (projector/status p)]
          (is (zero? (:lag s)))
          (is (= (:journal-head s) (:last-projected s))))))))

(deftest drain-batches-test
  (testing "drain! advances the marker and stops when the journal is exhausted"
    (with-parts
      (fn [_conn j p]
        (dotimes [n 3]
          (journal/tx! j [{:quote/number (str "Q-" n)}] {:audit/actor "geo"}))
        (is (= 3 (projector/drain! p)) "one batch covers all three")
        (is (zero? (projector/drain! p)))))))

(deftest rebuild-test
  (testing "rebuild! replays the journal from scratch"
    (with-parts
      (fn [_conn j p]
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (journal/tx! j [{:quote/number "Q-2"}] {:audit/actor "sam"})
        (projector/catch-up! p)

        (let [before (d/q '[:find (count ?t) . :where [?t :tx/id]] (projector/db p))]
          (is (= 2 (projector/rebuild! p)) "replays every entry")
          (is (= before (d/q '[:find (count ?t) . :where [?t :tx/id]] (projector/db p)))
              "and lands on the same projection, not a doubled one"))))))

(deftest background-projector-test
  (testing "start! projects in the background and stop! releases the slot"
    (with-parts
      (fn [_conn j p]
        (let [stop! (projector/start! p {:interval-ms 20})]
          (try
            (is (true? (:running? (projector/status p))))
            (is (thrown? clojure.lang.ExceptionInfo (projector/start! p))
                "one projector thread at a time")
            (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
            (let [deadline (+ (System/currentTimeMillis) 5000)]
              (while (and (pos? (or (:lag (projector/status p)) 1))
                          (< (System/currentTimeMillis) deadline))
                (Thread/sleep 10)))
            (is (zero? (:lag (projector/status p))) "caught up on its own")
            (finally (is (true? (stop!))))))
        (is (false? (:running? (projector/status p))))
        (testing "and a projector can be started again after a clean stop"
          (let [stop2! (projector/start! p {:interval-ms 20})]
            (is (true? (stop2!)))))))))

(deftest config-guards-test
  (testing "annotations in the projection's own namespaces are rejected"
    (let [conn (d/get-conn (tmpdir) domain-schema)
          j    (journal/open conn {:annotations {:tx/id {:db/valueType :db.type/long}}})]
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reserved namespaces"
                              (projector/open j {:dir (tmpdir)})))
        (finally (d/close conn)))))

  (testing "the journal alone has no reserved namespaces"
    (let [conn (d/get-conn (tmpdir) domain-schema)
          j    (journal/open conn {:annotations {:tx/id {:db/valueType :db.type/long}}})]
      (try
        (journal/tx! j [{:quote/number "Q-1"}] {:tx/id 99})
        (is (= {:tx/id 99} (-> (journal/entries-from j 0 10) first second :tx-meta)))
        (finally (d/close conn)))))

  (testing "the audit DB cannot share the domain directory"
    (let [dir  (tmpdir)
          conn (d/get-conn dir domain-schema)
          j    (journal/open conn {:annotations annotations})]
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cannot share a directory"
                              (projector/open j {:dir dir})))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cannot share a directory"
                              (projector/open j {:dir (str dir "/")}))
            "trailing slash is the same directory")
        (finally (d/close conn))))))

;;; ---------------------------------------------------------------------------
;;; Thread naming and audit conn-opts

(defn- thread-named? [name]
  (some #(= name (.getName ^Thread %)) (keys (Thread/getAllStackTraces))))

(deftest thread-name-test
  (testing "the projector thread takes the name it is given"
    (with-parts
      (fn [_conn _j p]
        (let [stop! (projector/start! p {:interval-ms 20
                                         :thread-name "audit-projector:tenant-7"})]
          (try
            (is (thread-named? "audit-projector:tenant-7")
                "a process running one projector per store needs them told apart
                 from the outside — in a thread dump or a profiler")
            (finally (stop!)))))))

  (testing "and defaults to audit-projector when not given one"
    (with-parts
      (fn [_conn _j p]
        (let [stop! (projector/start! p {:interval-ms 20})]
          (try (is (thread-named? "audit-projector"))
               (finally (stop!))))))))

(deftest audit-conn-opts-test
  (testing ":conn-opts reach the audit conn"
    (let [conn (d/get-conn (tmpdir) domain-schema)
          j    (journal/open conn {:annotations annotations})
          p    (projector/open j {:dir (tmpdir) :conn-opts {:auto-entity-time? true}})]
      (try
        (journal/tx! j [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (projector/catch-up! p)
        (is (seq (d/q '[:find [?t ...] :where [?t :db/created-at _]]
                      (projector/db p)))
            "audit entities carry :db/created-at, so the option took effect")
        (finally (projector/close! p) (d/close conn)))))

  (testing ":closed-schema? stays :strict?'s to set, not the caller's"
    (let [conn (d/get-conn (tmpdir) domain-schema)
          j    (journal/open conn {:annotations annotations :strict? true})
          ;; a caller trying to loosen the audit DB behind strict?'s back
          p    (projector/open j {:dir (tmpdir) :conn-opts {:closed-schema? false}})]
      (try
        (is (thrown? Exception
                     (d/transact! @(:audit p) [{:not/declared "x"}]))
            "still closed — the two would otherwise be one setting with two
             sources, silently disagreeing")
        (finally (projector/close! p) (d/close conn))))))
