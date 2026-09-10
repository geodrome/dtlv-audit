(ns dtlv-audit.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [dtlv-audit.core :as core])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- tmpdir []
  (str (Files/createTempDirectory "dtlv-audit-test"
                                  (make-array FileAttribute 0))))

(deftest journal-and-project-test
  (testing "a tx! is journaled and projected into a queryable audit db"
    (let [store (core/open! {:dir         (tmpdir)
                             :audit-dir   (tmpdir)
                             :schema      {:quote/number {:db/valueType :db.type/string
                                                          :db/unique :db.unique/identity}}
                             :annotations {:audit/actor {:db/valueType :db.type/string}}})]
      (try
        (core/tx! store [{:quote/number "Q-1042"}] {:audit/actor "geo"})
        (let [eid (d/q '[:find ?e . :in $ ?n :where [?e :quote/number ?n]]
                       (core/db store) "Q-1042")]
          (is (pos? (core/catch-up! store)))
          (is (seq (core/history store eid)))
          (is (seq (core/by-annotation store :audit/actor "geo"))))
        (finally
          (core/close! store))))))
