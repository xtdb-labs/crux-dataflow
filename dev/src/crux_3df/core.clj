(ns crux-3df.core
  (:require
   [clojure.tools.logging :as log]
   [clj-3df.core :as df]
   [clj-3df.attribute :as attribute]
   [crux.api :as api]
   [crux.bootstrap :as b]
   [crux.dataflow :as dataflow]
   [crux.io :as cio])
  (:import java.io.Closeable))

(def schema
  {:user/name (merge
               (attribute/of-type :String)
               (attribute/input-semantics :db.semantics.cardinality/one)
               (attribute/tx-time))

   :user/email (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics.cardinality/one)
                (attribute/tx-time))

   :user/knows (merge
                (attribute/of-type :Eid)
                (attribute/input-semantics :db.semantics.cardinality/many)
                (attribute/tx-time))

   :user/likes (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics.cardinality/many)
                (attribute/tx-time))})

(defn -main [& args]
  (println "doing core")
  (with-open [crux (api/start-standalone-node
                    {:kv-backend "crux.kv.rocksdb.RocksKv"
                     :event-log-dir "data/eventlog"
                     :db-dir "data/db-dir"})
              crux-3df (dataflow/start-dataflow-tx-listener
                        crux
                        {:crux.dataflow/schema schema
                         :crux.dataflow/debug-connection? true
                         :crux.dataflow/embed-server? true})]

    (api/submit-tx
     crux
     [[:crux.tx/put
       {:crux.db/id :patrik
        :user/name "Patrik"
        :user/likes ["apples" "bananas"]
        :user/email "p@p.com"}]])

    (api/submit-tx
     crux
     [[:crux.tx/put
       {:crux.db/id :patrik
        :user/likes ["something new" "change this"]
        :user/name "Patrik"
        :user/knows [:henrik]}]])

    (api/submit-tx
     crux
     [[:crux.tx/put
       {:crux.db/id :lars
        :user/name "lars"
        :user/knows [:henrik]}]
      [:crux.tx/put
       {:crux.db/id :henrik
        :user/name "henrik"
        :user/knows [:robert]}]])

    (dataflow/subscribe-query!
     crux-3df
     "patrik-email"
     '[:find ?email
       :where
       [?patrik :user/name "Patrik"]
       [?patrik :user/email ?email]])

    (dataflow/subscribe-query!
     crux-3df
     "patrik-likes"
     '[:find ?likes
       :where
       [?patrik :user/name "Patrik"]
       [?patrik :user/likes ?likes]])

    (dataflow/subscribe-query!
     crux-3df
     "patrik-knows-1"
     '[:find ?knows
       :where
       [?patrik :user/name "Patrik"]
       [?patrik :user/knows ?knows]])

    #_(let [{:keys [conn db]} crux-3df]

        (df/exec! conn
                  (df/query
                   db
                   '[:find ?email
                     :where
                     [?patrik :user/name "Patrik"]
                     [?patrik :user/email ?email]]))

        (df/exec! conn
                  (df/query
                   db "patrik-likes"
                   '[:find ?likes
                     :where
                     [?patrik :user/name "Patrik"]
                     [?patrik :user/likes ?likes]]))

        (df/exec! conn
                  (df/query
                   db "patrik-knows-1"
                   '[:find ?knows
                     :where
                     [?patrik :user/name "Patrik"]
                     [?patrik :user/knows ?knows]]))

        ;; ;; TODO: Does not seem to work.
        (df/exec! conn
                  (df/query
                   db "patrik-knows"
                   '[:find ?user-name
                     :where
                     [?patrik :user/name "Patrik"]
                     (trans-knows ?patrik ?knows)
                     [?knows :user/name ?user-name]]
                   '[[(trans-knows ?user ?knows)
                      [?user :user/knows ?knows]]
                     [(trans-knows ?user ?knows)
                      [?user :user/knows ?knows-between]
                      (trans-knows ?knows-between ?knows)]]))

        (df/listen!
         conn
         :key
         (fn [& data] (log/info "DATA: " data)))

        (df/listen-query!
         conn
         "patrik-knows"
         (fn [& message]
           (log/info "QUERY BACK: " message))))

    @(promise)))
