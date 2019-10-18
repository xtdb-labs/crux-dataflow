(ns crux-3df.repl-friendly
  (:require
    [clojure.tools.logging :as log]
    [clj-3df.core :as df]
    [clj-3df.attribute :as attribute]
    [clojure.pprint :as pp]
    [crux.api :as api]
    [crux.node :as node]
    [crux.dataflow-2 :as dataflow]
    [crux.io :as cio]
    [manifold.deferred :as d])
  (:import java.io.Closeable
           (java.util.concurrent LinkedBlockingQueue)))

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

(defonce node
  (api/start-node
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store "crux.kv.rocksdb/kv"
     :crux.standalone/event-log-dir "data/eventlog"
     :crux.kv/db-dir "data/db-dir"}))

(def crux-3df
  (dataflow/start-dataflow-tx-listener
    node
    {:crux.dataflow/schema schema
     :crux.dataflow/debug-connection? true
     :crux.dataflow/embed-server? false}))


(api/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :katrik
     :user/name "katrik"
     :user/likes ["apples" "daples"]
     :user/email "ool@g2ei.com"}]])

(def sub1
  ^LinkedBlockingQueue
  (dataflow/subscribe-query!
    crux-3df
    "patrik-email3"
    '[:find ?email
      :where
      [?patrik :user/name "Patrik"]
      [?patrik :user/email ?email]]))


(.poll sub1)

; when subscribing consecutively queue stops working
; with (dataflow/subscribe-query! crux-3df "patrik-email")
; so I need to check subscription map

; (dataflow/unsubscribe-query! crux-3df "patrik-email")


(let [{:keys [conn db]} crux-3df]
  (df/exec! conn
    (df/query
      db "patrik-email"
      '[:find ?email
        :where
        [?patrik :user/name "Patrik"]
        [?patrik :user/email ?email]])))

(let [{:keys [conn db]} crux-3df]
  (df/listen!
    conn
    :key
    (fn [& data] (log/info "DATA: " data))))

(let [{:keys [conn db]} crux-3df
  (df/listen-query!
    conn
    "patrik-email"
    (fn [& message]
      (log/info "QUERY BACK: " message))))

(comment

  (.close node)
  (.close crux-3df)

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


  (let [{:keys [conn db]} crux-3df]
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

  @(promise))
