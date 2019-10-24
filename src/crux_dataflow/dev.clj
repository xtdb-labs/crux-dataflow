(ns crux-dataflow.dev
  (:require
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [clojure.pprint :as pp]
    [crux-dataflow.schema :as schema])
  (:import (java.util.concurrent LinkedBlockingQueue)))

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

(def crux-3df
  (dataflow/start-dataflow-tx-listener ; todo restart connection
    node
    {:crux.dataflow/schema            (merge user-schema task-schema)
     :crux.dataflow/debug-connection? true
     :crux.dataflow/embed-server?     false}))

(def ^LinkedBlockingQueue sub1
  (dataflow/subscribe-query! crux-3df
    {:crux.dataflow/sub-id ::one
     :crux.dataflow/query-name "one"
     :crux.dataflow/query
     '{:find [?email]
       :where
       [[?user :user/name "Patrik"]
        [?user :user/email ?email]]}}))

(.poll sub1)

; poll worked just once

(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :patrik
     :user/name  "Patrik"
     ; :user/knows [:ids/bart] ; fixme may not index properly
     ; :user/likes ["apples" "daples"] ; fixme fails to accept seqs
     :user/email "ojelji"}]])


(comment
  (pp/pprint crux-3df)

  (pp/pprint @(:query-listeners (.-conn crux-3df)))

  (.close node)
  (.close crux-3df))
