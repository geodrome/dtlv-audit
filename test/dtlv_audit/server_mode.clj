(ns dtlv-audit.server-mode
  "REPL walkthrough of dtlv-audit against a Datalevin *server*.

   Not a test — nothing here runs under `clojure -X:test`. Start a REPL with
   `clojure -M:dev`, load this namespace, and evaluate the comment blocks top
   to bottom. Each block is self-contained apart from the server started in
   the first one.

   What server mode means for this library, and what the blocks show:

     1. The domain DB cannot live on a server. The journal shares the domain
        write's LMDB transaction, and a remote conn has no KV handle to share —
        `journal/open` and `core/open!` reject a dtlv:// domain :dir.

     2. The audit DB can. It is a separate conn opened from :audit-dir, and a
        dtlv:// URI there works end to end: projection, queries, rebuild!,
        strict/non-strict schema, restart.

     3. What happens when the audit server goes away: domain writes carry on,
        the projector backs off — but `projector-status` throws, and the
        Datalevin client never reconnects, so recovery is `rebuild!` or a
        close!/open! of the store."
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
;;; 1. Domain DB on the server is rejected

(comment
  ;; journal/open: the remote store cannot host the journal's KV sub-database.
  (let [conn (d/get-conn (uri "domain-1") schema)]
    (try (journal/open conn {:annotations annotations})
         (catch Exception e
           [(ex-message e) (ex-data e) (class (ex-cause e))])
         (finally (d/close conn))))
  ;; => ["journal requires an embedded Datalevin store — ..."
  ;;     {:store "datalevin.remote.DatalogStore"}
  ;;     java.lang.AbstractMethodError]

  ;; core/open!: same rejection, and the remote conn is closed on the way out.
  (try (core/open! {:dir         (uri "domain-1")
                    :audit-dir   (tmpdir "audit-1")
                    :schema      schema
                    :annotations annotations})
       (catch Exception e (ex-message e)))

  ;; Proof nothing was stranded: the same remote DB opens and closes cleanly.
  (let [c (d/get-conn (uri "domain-1") schema)]
    (d/close c)))

;;; ---------------------------------------------------------------------------
;;; 2. Audit DB on the server — the supported server-mode layout

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
