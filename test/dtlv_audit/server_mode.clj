(ns dtlv-audit.server-mode
  "REPL walkthrough of dtlv-audit against a Datalevin *server*.

   Not a test — nothing here runs under `clojure -X:test`. Start a REPL with
   `clojure -M:dev`, load this namespace, and evaluate the comment blocks top
   to bottom. Each block is self-contained apart from the server started in
   the first one.

   What server mode means for this library, and what the blocks show:

     1. The domain DB can live on a server. The journal shares the domain
        write's LMDB transaction, and on a server that transaction is held
        server-side; `d/datalog-kv` of the transaction-bound conn joins it.
        A throw rolls back the domain write and its journal entry together,
        exactly as embedded. Needs a Datalevin with remote `d/datalog-kv`
        (1.2.11 is tested); older ones are rejected by `journal/open`.
        `dtlv-audit.server-test` asserts all of this; block 1 is the tour.

     2. So can the audit DB. It is a separate conn opened from :audit-dir, and
        a dtlv:// URI there works end to end: projection, queries, rebuild!,
        strict/non-strict schema, restart. Blocks 2-4 pair it with an
        embedded domain DB, which keeps the write path off the network.

     3. What happens when the audit server goes away: with an embedded domain
        DB, domain writes carry on and the projector backs off — but
        `projector-status` throws, and the Datalevin client never reconnects,
        so recovery is `rebuild!` or a close!/open! of the store. (With the
        domain DB on that server too, domain writes stop with it.)"
  (:require [datalevin.core :as d]
            [datalevin.server :as srv]
            [dtlv-audit.core :as core]
            [dtlv-audit.journal :as journal]
            [taoensso.trove :as trove])
  (:import [java.net ServerSocket]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;;; ---------------------------------------------------------------------------
;;; Helpers

(defn tmpdir [prefix]
  (str (Files/createTempDirectory prefix (make-array FileAttribute 0))))

(defn free-port []
  (with-open [s (ServerSocket. 0)] (.getLocalPort s)))

(def schema
  {:quote/number {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :quote/total  {:db/valueType :db.type/long}})

(def annotations
  {:audit/actor {:db/valueType :db.type/string}
   :audit/op    {}})

(defonce ^{:doc "In-process server: {:server srv :port n :root dir}. :server is
                 nil while stopped; :port and :root survive so a restart lands
                 on the same address, which is what a real restart looks like
                 to a client."}
  server (atom nil))

(defn start-server!
  "Starts a Datalevin server. First call: a free port and a fresh root dir.
   After `stop-server!`: the same port and root, i.e. a restart. Bound to
   127.0.0.1, so the built-in default password applies. Server debug logging
   is loud; it is written by Datalevin's own logger (Timbre), not Trove."
  []
  (let [{:keys [port root]} @server
        port (or port (free-port))
        root (or root (tmpdir "dtlv-audit-server-root"))
        s    (srv/create {:port port :root root})]
    (srv/start s)
    (reset! server {:server s :port port :root root})))

(defn stop-server!
  "Stops the server, keeping :port and :root so `start-server!` restarts it."
  []
  (some-> @server :server srv/stop)
  (swap! server assoc :server nil))

(defn uri
  "dtlv:// URI for a DB named `db-name` on the running server."
  [db-name]
  (str "dtlv://datalevin:datalevin@127.0.0.1:" (:port @server) "/" db-name))

(defn remote-conn?
  "Is the audit conn of `store` talking to a server?"
  [store]
  (= "datalevin.remote.DatalogStore"
     (.getName (class (.-store ^datalevin.db.DB @(-> store :projector :audit deref))))))

;;; ---------------------------------------------------------------------------
;;; 0. Start the server

(comment
  (start-server!)
  ;; => {:server ..., :port 5xxxx, :root "/var/.../dtlv-audit-server-root..."}
  ;; `stop-server!` / `start-server!` later on restart it at the same address.

  ;; Datalevin's server logs every message at DEBUG. Quieten it if you like:
  (require '[taoensso.timbre :as timbre])
  (timbre/set-min-level! :warn))

;;; ---------------------------------------------------------------------------
;;; 1. Domain DB on the server

(comment
  (def conn (d/get-conn (uri "domain-1") schema))

  ;; The KV handle is a remote view onto the same server-side environment,
  ;; not the Datalog store itself (which is what Datalevin 1.1.0 returned).
  (class (d/datalog-kv conn))
  ;; => datalevin.remote.KVStore

  (def j (journal/open conn {:annotations annotations}))

  (journal/tx! j [{:quote/number "Q-1" :quote/total 10}] {:audit/actor "geo"})
  ;; => {... :tx 2 :commit 2}

  ;; Two writes, then a throw. Both journal puts reach the server before the
  ;; throw; only a shared server-side transaction can take them back.
  (try (journal/with-tx [jj j]
         (journal/tx! jj [{:quote/number "Q-2" :quote/total 20}] {:audit/actor "geo"})
         (journal/tx! jj [{:quote/number "Q-3" :quote/total 30}] {:audit/actor "geo"})
         (throw (ex-info "boom" {})))
       (catch Exception e (ex-message e)))
  ;; => "boom"

  (d/q '[:find ?n :where [_ :quote/number ?n]] (d/db conn))
  ;; => #{["Q-1"]}
  (map first (journal/entries-from j 0 100))
  ;; => (2) — and the rolled-back transaction used no tx ids

  (d/close conn)

  ;; The whole store on the server: domain and audit DBs both dtlv:// URIs.
  (def store (core/open! {:dir         (uri "domain-1b")
                          :audit-dir   (uri "audit-1b")
                          :schema      schema
                          :annotations annotations}))
  (core/tx! store [{:quote/number "Q-1" :quote/total 100}] {:audit/actor "geo"})
  (core/catch-up! store)
  ;; => 1
  (core/by-annotation store :audit/actor "geo")
  (core/close! store))

;;; ---------------------------------------------------------------------------
;;; 2. Audit DB on the server, domain DB embedded

(comment
  (def store
    (core/open! {:dir         (tmpdir "domain-2")
                 :audit-dir   (uri "audit-2")
                 :schema      schema
                 :annotations annotations}))

  (remote-conn? store)
  ;; => true

  ;; Write path is embedded and never touches the server.
  (def r1 (core/tx! store [{:quote/number "Q-1" :quote/total 100}]
                    {:audit/actor "geo" :audit/op [:quote/create {:n 1}]}))
  (def eid (-> r1 :tx-data first :e))
  (core/tx! store [{:quote/number "Q-1" :quote/total 150}] {:audit/actor "sam"})
  (core/with-tx [s store]
    (core/tx! s [{:quote/number "Q-2" :quote/total 7}] {:audit/actor "geo"})
    (core/tx! s [{:quote/number "Q-3" :quote/total 8}] {:audit/actor "geo"}))

  ;; Projection goes over the wire.
  (core/catch-up! store)
  ;; => 4

  (core/history store eid)
  ;; => ([tx :quote/number "Q-1" true at]
  ;;     [tx :quote/total 100 true at]
  ;;     [tx' :quote/total 100 false at']   ; old value retracted
  ;;     [tx' :quote/total 150 true at'])   ; new value asserted

  (core/by-annotation store :audit/actor "geo")
  (core/by-annotation store :audit/op [:quote/create {:n 1}])   ; untyped, by equality
  (core/transaction store (:tx r1))

  ;; The with-tx pair shares one :tx/commit.
  (d/q '[:find ?commit (count ?t)
         :where [?t :tx/commit ?commit]
         :order-by [?commit]]
       (core/audit-db store))

  (core/projector-status store)
  ;; => {:running? false :journal-head n :last-projected n :lag 0}

  ;; Background projector against the remote audit DB.
  (def stop! (core/start-projector! store :interval-ms 100))
  (core/tx! store [{:quote/number "Q-4" :quote/total 9}] {:audit/actor "geo"})
  (Thread/sleep 300)
  (core/projector-status store)
  ;; => lag 0, :running? true

  ;; rebuild! really clears the remote DB: plant garbage, rebuild, it is gone.
  (d/transact! @(-> store :projector :audit)
               [{:tx/id 9999 :tx/at 0 :tx/commit 9999 :audit/actor "bogus"}])
  (core/rebuild! store)
  ;; => the number of journal entries replayed. With the projector running
  ;;    this can be 0: its tick may do the replay before rebuild!'s own
  ;;    catch-up! runs. Either way the result below is the same.
  (d/entity (core/audit-db store) [:tx/id 9999])
  ;; => nil
  (count (d/q '[:find ?t :where [?t :tx/id]] (core/audit-db store)))
  ;; => 5 tx entities, none duplicated
  (remote-conn? store)
  ;; => true — the reopened audit conn is still the remote one

  (stop!)
  (core/close! store)

  ;; The projection persisted on the server: a new store against the same
  ;; audit DB resumes from the stored marker rather than starting empty.
  (def store' (core/open! {:dir         (tmpdir "domain-2b")
                           :audit-dir   (uri "audit-2")
                           :schema      schema
                           :annotations annotations}))
  (count (core/changes-between store' 0 Long/MAX_VALUE))
  (core/close! store'))

;;; ---------------------------------------------------------------------------
;;; 3. Schema strictness reaches the server

(comment
  ;; :strict? true sends :closed-schema? true in the open request. Watch for
  ;; `:opts {:closed-schema? true, ...}` in the server's DEBUG log.
  ;; :strict? false lets an undeclared annotation through as an EDN blob.
  (def loose (core/open! {:dir         (tmpdir "domain-3")
                          :audit-dir   (uri "audit-3")
                          :schema      schema
                          :annotations annotations
                          :strict?     false}))
  (core/tx! loose [{:quote/number "Q-x"}]
            {:audit/actor "geo" :audit/undeclared {:k [1 2]}})
  ;; logs a ::journal/invalid-annotation warn, then commits
  (core/catch-up! loose)
  (core/by-annotation loose :audit/undeclared {:k [1 2]})
  ;; => one row
  (core/close! loose))

;;; ---------------------------------------------------------------------------
;;; 4. The audit server goes away

(comment
  (def store (core/open! {:dir         (tmpdir "domain-4")
                          :audit-dir   (uri "audit-4")
                          :schema      schema
                          :annotations annotations}))
  (def stop! (core/start-projector! store :interval-ms 100))
  (core/tx! store [{:quote/number "Q-1"}] {:audit/actor "geo"})
  (Thread/sleep 300)
  (core/projector-status store)
  ;; => lag 0

  ;; Kill the audit server.
  (stop-server!)

  ;; Domain writes are unaffected — the write path is embedded.
  (core/tx! store [{:quote/number "Q-2"}] {:audit/actor "geo"})

  ;; The projector thread survives, logs ::projector/projector-failed once,
  ;; then ::projector/projector-stuck at a backoff. But the health check
  ;; itself throws, because it reads the (dead) remote audit db:
  (Thread/sleep 1500)
  (core/projector-status store)
  ;; => throws "This client is closed"

  ;; Bring the server back on the same port.
  (start-server!)

  ;; The Datalevin client does not reconnect: the audit conn is dead for good.
  (core/catch-up! store)
  ;; => throws "This client is closed"

  ;; rebuild! is the way back — it closes and reopens the audit conn, at the
  ;; cost of replaying the whole journal. close!/open! of the store is the other.
  (core/rebuild! store)
  ;; => 2
  (core/projector-status store)
  ;; => lag 0, :failures 0, :running? true

  (stop!)
  (core/close! store))

;;; ---------------------------------------------------------------------------
;;; 5. Tear down

(comment
  (stop-server!))
