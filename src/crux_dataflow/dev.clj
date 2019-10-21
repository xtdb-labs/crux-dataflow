(ns crux-dataflow.dev
  (:require
    [clj-3df.attribute :as attribute]
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [crux-dataflow.df-upload :as ingest]
    [clojure.pprint :as pp])
  (:import (java.util.concurrent LinkedBlockingQueue)))


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


(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :katrik
     :user/name "katrik"
     :user/likes ["apples" "daples"]
     :user/email "imea"}]])

(def ^LinkedBlockingQueue sub1
  (dataflow/subscribe-query! crux-3df
    {:crux.dataflow/sub-id ::one
     :crux.dataflow/query-name "one"
     :crux.dataflow/query
    '{:find [?email]
      :where
      [[?patrik :user/name "Patrik"]
       [?patrik :user/email ?email]]}}))

(ingest/upload-crux-query-results
  crux-3df
  [{:crux.db/id :katrik
    :user/name "katrik"
    :user/likes ["apples" "daples"]
    :user/email "imea"}
   {:crux.db/id :batrik
    :user/name "katrik"
    :user/likes ["apples" "daples"]
    :user/email "imea"}])

(pp/pprint crux-3df)

(pp/pprint @(:query-listeners (.-conn crux-3df)))

(.poll sub1)


(comment
  (.close node)
  (.close crux-3df))
