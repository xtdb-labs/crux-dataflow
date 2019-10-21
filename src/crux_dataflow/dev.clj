(ns crux-dataflow.dev
  (:require
    [clj-3df.attribute :as attribute]
    [crux.api :as api]
    [crux-dataflow.api-2 :as dataflow]
    [crux-dataflow.df-upload :as ingest]
    [clojure.pprint :as pp])
  (:import (java.util.concurrent LinkedBlockingQueue)))

; db semantics doesn't mean what you think it means
; see https://github.com/sixthnormal/clj-3df/issues/45
(def schema
  {:user/name (merge
               (attribute/of-type :String)
               (attribute/input-semantics :db.semantics/raw)
               (attribute/tx-time))
   :user/email (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics/raw)
                (attribute/tx-time))
   :user/knows (merge
                (attribute/of-type :Eid)
                (attribute/input-semantics :db.semantics/raw)
                (attribute/tx-time))
   :user/likes (merge
                (attribute/of-type :String)
                (attribute/input-semantics :db.semantics/raw)
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

(def ^LinkedBlockingQueue sub1
  (dataflow/subscribe-query! crux-3df
    {:crux.dataflow/sub-id ::one
     :crux.dataflow/query-name "one"
     :crux.dataflow/query
     '{:find [?email]
       :where
       [[?user :user/name "Patrik"]
        [?user :user/email ?email]]}}))

(api/q (api/db node)
  '{:find [?email]
    :full-results? true
    :where
     [[?user :user/name "Patrik"]
      [?user :user/email ?email]]})

(api/submit-tx node
  [[:crux.tx/put
    {:crux.db/id :katrik
     :user/name "Patrik"
     :user/knows [:ids/bart]
     :user/likes ["apples" "daples"]
     :user/email "i00fiojooiiioo"}]])

(.poll sub1)


(comment
  (pp/pprint crux-3df)

  (pp/pprint @(:query-listeners (.-conn crux-3df)))

  (.close node)
  (.close crux-3df))
