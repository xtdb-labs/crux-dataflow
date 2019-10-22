(ns crux-dataflow.dev
  (:require
    [clj-3df.attribute :as attribute]
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [crux-dataflow.df-upload :as ingest]
    [clojure.pprint :as pp]
    [clj-3df.core :as df])
  (:import (java.util.concurrent LinkedBlockingQueue)))

; db semantics doesn't mean what you think it means
; see https://github.com/sixthnormal/clj-3df/issues/45
(def schema
  {:user/name (merge
               (attribute/of-type :String)
               (attribute/input-semantics :db.semantics.cardinality/many)
               (attribute/tx-time))
   :user/email (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics.cardinality/many)
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
     :crux.node/kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-kv-store :crux.kv.rocksdb/kv
     :crux.standalone/event-log-dir "data/eventlog"
     :crux.kv/db-dir "data/db-dir"}))

(def crux-3df
  (dataflow/start-dataflow-tx-listener
    node
    {:crux.dataflow/schema schema
     :crux.dataflow/debug-connection? true
     :crux.dataflow/embed-server? false}))

(def ^LinkedBlockingQueue sub1
  (dataflow/subscribe-query! crux-3df
    {:crux.dataflow/sub-id ::one
     :crux.dataflow/query-name "one"
     :crux.dataflow/query
     '{:find [?user]
       :where
       [[?user :user/name "Patrik"]
        [?user :user/email ?email]]}}))

(.poll sub1)

(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :patrik
     :user/name "Patrik"
   ; :user/knows [:ids/bart] ; fixme may not index properly
     :user/likes ["apples" "daples"]
     :user/email "ojifiwjfoweijfweofijwi"}]])

(dataflow/transact-data-for-query!
  crux-3df
 '{:find [?user]
   :where
   [[?user :user/name "Patrik"]
    [?user :user/email ?email]]})


(df/exec!
  (.-conn crux-3df)
  (df/register-query (.-df-db crux-3df) "email"
    '[:find ?email
      :where
      [?user :user/name "Patrik"]
      [?user :user/email ?email]]))

(df/exec!
  (.-conn crux-3df)
  (df/query
    (.-df-db crux-3df)
    "email2"
    '[:find ?email
      :where
      [?user :user/name "Patrik"]
      [?user :user/email ?email]]))

(df/listen-query!
  (.-conn crux-3df) "email" ::one
  #(println "eauau" %))

(df/listen!
  (.-conn crux-3df) "on-all"
  #(println "eauau" %))

(df/listen-query!
  (.-conn crux-3df) "email2" ::one
  #(println "eauau" %))

(comment
  (pp/pprint crux-3df)

  (pp/pprint @(:query-listeners (.-conn crux-3df)))

  (.close node)
  (.close crux-3df))
