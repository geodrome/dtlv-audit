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

(def ^:private schema
  {:quote/number {:db/valueType :db.type/string :db/unique :db.unique/identity}})

(defn- with-store [opts f]
  (let [store (core/open! (merge {:dir (tmpdir) :audit-dir (tmpdir) :schema schema}
                                 opts))]
    (try (f store) (finally (core/close! store)))))

(deftest conn-opts-test
  (testing ":conn-opts reach the domain conn"
    ;; The case this exists for: a caller whose invariants depend on a conn
    ;; option. :auto-entity-time? writes :db/created-at, which is how an
    ;; entity that was really created is told from a bare id written to — a
    ;; distinction :db/ensure preconditions can rest on. Dropping the option
    ;; would not fail; it would quietly make those checks pass on anything.
    (with-store {:conn-opts {:auto-entity-time? true}}
      (fn [store]
        (core/tx! store [{:quote/number "Q-1"}])
        (is (seq (d/q '[:find [?e ...] :where [?e :db/created-at _]] (core/db store)))))))

  (testing "a store opened with no :conn-opts behaves as d/get-conn's default"
    (with-store {}
      (fn [store]
        (core/tx! store [{:quote/number "Q-1"}])
        (is (empty? (d/q '[:find [?e ...] :where [?e :db/created-at _]] (core/db store))))))))

(deftest with-tx-test
  (testing "a store scoped to a transaction reads and writes inside it"
    (with-store {:annotations {:audit/op {}}}
      (fn [store]
        (core/with-tx [s store]
          (when-not (d/q '[:find ?e . :where [?e :quote/number "Q-1"]] (core/db s))
            (core/tx! s [{:quote/number "Q-1"}]
                      {:audit/op [:quote/create {:number "Q-1"}]})))
        (is (pos? (core/catch-up! store)))
        (is (seq (core/by-annotation store :audit/op [:quote/create {:number "Q-1"}]))
            "an untyped annotation is queryable by equality"))))

  (testing "a rollback takes the domain write and its journal entry together"
    (with-store {:annotations {:audit/op {}}}
      (fn [store]
        (is (thrown? clojure.lang.ExceptionInfo
                     (core/with-tx [s store]
                       (core/tx! s [{:quote/number "Q-1"}] {:audit/op [:x]})
                       (throw (ex-info "rollback" {})))))
        (is (zero? (core/catch-up! store)))
        (is (nil? (d/q '[:find ?e . :where [?e :quote/number "Q-1"]] (core/db store)))))))

  (testing "the writes a transaction produced share a commit, with no
            annotation and nothing for the caller to arrange"
    (with-store {:annotations {:audit/op {}}}
      (fn [store]
        (core/with-tx [s store]
          (core/tx! s [{:quote/number "Q-1"}] {:audit/op [:a]})
          (core/tx! s [{:quote/number "Q-2"}] {:audit/op [:b]}))
        (core/tx! store [{:quote/number "Q-3"}] {:audit/op [:c]})
        (core/catch-up! store)
        (let [by-commit (->> (d/q '[:find ?commit ?op
                                    :where [?t :tx/commit ?commit] [?t :audit/op ?op]]
                                  (core/audit-db store))
                             (group-by first)
                             vals
                             (map #(sort (map (comp first second) %)))
                             set)]
          (is (contains? by-commit '(:a :b)) "the two in one transaction")
          (is (contains? by-commit '(:c)) "and the standalone write on its own"))))))

(deftest clear-journal-test
  (testing "clear-journal! empties the record; rebuild! then empties the projection"
    (with-store {:annotations {:audit/actor {:db/valueType :db.type/string}}}
      (fn [store]
        (core/tx! store [{:quote/number "Q-1"}] {:audit/actor "geo"})
        (core/catch-up! store)
        (is (seq (core/by-annotation store :audit/actor "geo")))

        (is (true? (core/clear-journal! store)))
        (is (seq (core/by-annotation store :audit/actor "geo"))
            "the projection is derived, so clearing the journal does not touch it")

        (core/rebuild! store)
        (is (empty? (core/by-annotation store :audit/actor "geo"))
            "rebuild! is what reconciles them")))))
