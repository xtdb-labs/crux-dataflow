(ns crux-dataflow.dev
  (:require
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [clojure.pprint :as pp]
    [crux-dataflow.schema :as schema])
  (:import (java.util.concurrent LinkedBlockingQueue)
           (java.io Closeable)))

(def task-schema
  (schema/inflate
    {:task/owner [:Eid]
     :task/title [:String]
     :task/content [:String]
     :task/followers [:Eid ::schema/set]}))

(def user-schema
  (schema/inflate
    {:user/name [:String]
     :user/email [:String]
     :user/knows [:Eid ::schema/set]
     :user/likes [:String ::schema/list]}))

(defonce node
  (api/start-node
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-dir "data/eventlog"
     :crux.kv/db-dir "data/db-dir"}))

(def ^Closeable crux-3df
  (do
    (if (bound? #'crux-3df)
      (.close crux-3df))
    (dataflow/start-dataflow-tx-listener
      node
      {:crux.dataflow/schema            (merge user-schema task-schema)
       :crux.dataflow/debug-connection? true
       :crux.dataflow/embed-server?     false})))

(def ^LinkedBlockingQueue sub1
  (dataflow/subscribe-query! crux-3df ; seems like ingests trigger updates
    {:crux.dataflow/sub-id ::one
     :crux.dataflow/query-name "two"
     :crux.dataflow/query
     '{:find [?name ?email]
       :where
       [[?user :user/name ?name]
        [?user :user/email ?email]]}}))

(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :patrik
     :user/name  "4"
     :user/email "4"}]])

(.poll sub1)


(comment
  (pp/pprint crux-3df)

  (api/submit-tx node
     [[:crux.tx/put
       {:crux.db/id :patrik
        :user/name  "Patrik"
        ; :user/knows [:ids/bart] ; fixme may not index properly
        ; :user/likes ["apples" "daples"] ; fixme fails to accept seqs
        :user/email "eiowefojhhhh"}]])

  (pp/pprint @(:query-listeners (.-conn crux-3df)))

  (.close node)
  (.close crux-3df))
