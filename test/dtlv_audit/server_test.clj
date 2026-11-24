(ns dtlv-audit.server-test
  "The domain DB on a Datalevin server.

   The journal's atomicity claim, rerun against a dtlv:// domain conn. On a
   server the shared LMDB transaction lives on the server side, and the KV
   handle is a remote view that joins it — see `dtlv-audit.journal`. These
   tests check that the claim survives that indirection, not just that the
   calls succeed.

   One in-process server for the namespace, one DB name per test."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.server :as srv]
            [dtlv-audit.core :as core]
            [dtlv-audit.journal :as journal]
            [taoensso.timbre :as timbre])
  (:import [java.net ServerSocket]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- tmpdir [prefix]
  (str (Files/createTempDirectory prefix (make-array FileAttribute 0))))

(defn- free-port []
  (with-open [s (ServerSocket. 0)] (.getLocalPort s)))

(def ^:private port nil)

(defn- with-server
  "Runs the namespace against a fresh server. Datalevin's server logs every
   message at DEBUG through Timbre, so its level is raised for the duration
   and put back afterwards."
  [f]
  (let [p         (free-port)
        s         (srv/create {:port p :root (tmpdir "dtlv-audit-server-root")})
        old-level (:min-level timbre/*config*)]
    (timbre/set-min-level! :warn)
    (srv/start s)
    (try
      (with-redefs [port p] (f))
      (finally
        (srv/stop s)
        (timbre/set-min-level! old-level)))))

(use-fixtures :once with-server)

(defmacro ^:private quietly
  "Runs `body` with the server's logging off. For a write that is meant to
   fail: the server logs its own stack trace for it, which would otherwise
   bury the test output. Global rather than a binding, because the server
   logs from its own threads."
  [& body]
  `(let [level# (:min-level timbre/*config*)]
     (timbre/set-min-level! :report)
     (try ~@body (finally (timbre/set-min-level! level#)))))

(def ^:private db-counter (atom 0))

(defn- uri
  "A dtlv:// URI for a DB name no other test has used. Bound to 127.0.0.1, so
   the server's built-in default credentials apply."
  [prefix]
  (str "dtlv://datalevin:datalevin@127.0.0.1:" port "/"
       prefix "-" (swap! db-counter inc)))

(def ^:private schema
  {:quote/number {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :quote/total  {:db/valueType :db.type/long}})

(def ^:private annotations {:audit/actor {:db/valueType :db.type/string}})

(defn- with-remote-journal
  "Opens a remote domain conn and a journal on it, calls (f conn journal uri),
   then closes the conn."
  [f]
  (let [u    (uri "domain")
        conn (d/get-conn u schema)]
    (try (f conn (journal/open conn {:annotations annotations}) u)
         (finally (d/close conn)))))

(defn- all-entries [j] (vec (journal/entries-from j 0 1000)))

(defn- quote-totals [conn]
  (into {} (d/q '[:find ?n ?t :where [?e :quote/number ?n] [?e :quote/total ?t]]
                (d/db conn))))

(deftest remote-open-test
  (testing "journal/open accepts a dtlv:// domain conn"
    (with-remote-journal
      (fn [conn j _]
        (is (= "datalevin.remote.KVStore"
               (.getName (class (d/datalog-kv conn))))
            "the KV handle is a remote view, not the Datalog store itself")
        (is (= conn (journal/conn j)))))))

(deftest remote-tx-test
  (testing "tx! writes the domain datoms and the journal entry"
    (with-remote-journal
      (fn [conn j _]
        (let [{:keys [tx commit]} (journal/tx! j [{:quote/number "Q-1" :quote/total 10}]
                                               {:audit/actor "geo"})
              [[etx entry]]       (all-entries j)]
          (is (= {"Q-1" 10} (quote-totals conn)))
          (is (= tx etx commit (:commit entry)))
          (is (= {:audit/actor "geo"} (:tx-meta entry)))
          (is (= #{[:quote/number "Q-1" true] [:quote/total 10 true]}
                 (set (map (fn [[_ a v added]] [a v added]) (:tx-data entry))))))))))

(deftest remote-atomicity-test
  (testing "a failing tx! leaves neither the domain write nor a journal entry"
    (with-remote-journal
      (fn [conn j _]
        (is (thrown? Exception
                     (quietly
                       (journal/tx! j [{:quote/number "Q-1" :quote/total "not-a-long"}]))))
        (is (empty? (all-entries j)))
        (is (empty? (quote-totals conn))))))

  (testing "a throw after the KV put rolls the put back with the domain write"
    ;; The same shape as journal-test/atomicity-test, against the server. The
    ;; put has been sent and acknowledged before the throw, so only a shared
    ;; server-side transaction can undo it.
    (with-remote-journal
      (fn [conn j _]
        (journal/tx! j [{:quote/number "Q-keep" :quote/total 1}])
        (let [before (all-entries j)]
          (is (thrown-with-msg? Exception #"boom"
                (d/with-transaction [c conn]
                  (let [r (d/transact! c [{:quote/number "Q-doomed" :quote/total 2}])]
                    (d/transact-kv (d/datalog-kv c) journal/journal-dbi
                                   [[:put (:max-tx (:db-after r)) {:tx-data []}]]
                                   :long :data)
                    (throw (ex-info "boom" {}))))))
          (is (= before (all-entries j))
              "the journal put aborted with the domain transaction")
          (is (= {"Q-keep" 1} (quote-totals conn))
              "and only the earlier committed write survived")))))

  (testing "a throw in with-tx rolls back every write and entry in it"
    (with-remote-journal
      (fn [conn j _]
        (journal/tx! j [{:quote/number "Q-keep" :quote/total 1}])
        (let [before (all-entries j)]
          (is (thrown-with-msg? Exception #"boom"
                (journal/with-tx [jj j]
                  (journal/tx! jj [{:quote/number "Q-2" :quote/total 2}])
                  (journal/tx! jj [{:quote/number "Q-3" :quote/total 3}])
                  (throw (ex-info "boom" {})))))
          (is (= before (all-entries j)))
          (is (= {"Q-keep" 1} (quote-totals conn))))))))

(deftest remote-with-tx-test
  (testing "with-tx commits several writes as one commit, with dense tx ids"
    (with-remote-journal
      (fn [conn j u]
        (journal/tx! j [{:quote/number "Q-1" :quote/total 10}])
        (journal/with-tx [jj j]
          (journal/tx! jj [{:quote/number "Q-2" :quote/total 20}])
          (journal/tx! jj [{:quote/number "Q-2" :quote/total 21}]))
        (let [[e1 e2 e3 :as entries] (all-entries j)]
          (is (= 3 (count entries)))
          (is (empty? (journal/gaps -1 entries)))
          (is (= (first e2) (:commit (second e2)) (:commit (second e3)))
              "both writes record the commit the first one began")
          (is (not= (:commit (second e1)) (:commit (second e2))))
          (is (some (fn [[_ a v added]] (and (= :quote/total a) (= 20 v) (false? added)))
                    (:tx-data (second e3)))
              "the replaced value is journaled as a retraction")
          (is (= {"Q-1" 10 "Q-2" 21} (quote-totals conn)))

          (testing "a separate client sees the same committed state"
            (let [conn2 (d/get-conn u schema)]
              (try
                (let [j2 (journal/open conn2 {:annotations annotations})]
                  (is (= entries (all-entries j2)))
                  (is (= {"Q-1" 10 "Q-2" 21} (quote-totals conn2))))
                (finally (d/close conn2))))))))))

(deftest remote-store-test
  (testing "core/open! with the domain and audit DBs both on the server"
    (let [store (core/open! {:dir         (uri "domain")
                             :audit-dir   (uri "audit")
                             :schema      schema
                             :annotations annotations})]
      (try
        (let [r   (core/tx! store [{:quote/number "Q-1" :quote/total 100}]
                            {:audit/actor "geo"})
              eid (-> r :tx-data first :e)]
          (core/tx! store [{:quote/number "Q-1" :quote/total 150}] {:audit/actor "sam"})
          (is (= 2 (core/catch-up! store)))
          (is (= #{[:quote/total 100 false] [:quote/total 150 true]}
                 (->> (core/history store eid)
                      (filter (fn [[_ a]] (= :quote/total a)))
                      (filter (fn [[tx]] (not= tx (:tx r))))
                      (map (fn [[_ a v added]] [a v added]))
                      set))
              "the projection carries the retraction of the old value")
          (is (seq (core/by-annotation store :audit/actor "sam"))))
        (finally
          (core/close! store))))))
