(ns crux-dataflow.api-2
  (:require
    [clojure.tools.logging :as log]
    [clj-3df.core :as df]
    [manifold.stream]
    [crux.api :as api]
    [crux-dataflow.results-processing :as res-process]
    [crux-dataflow.query-altering :as q-alt]
    [crux-dataflow.schema :as schema]
    [crux-dataflow.df-relay :as df-consumer]
    [crux-dataflow.misc-helpers :as fm]
    [crux-dataflow.df-upload :as ingest]
    [crux-dataflow.query-analysis :as qa])
  (:import [java.util.concurrent LinkedBlockingQueue BlockingQueue]))


(def ^:private query->name (atom {}))

(defn- map-query-to-id! [q]
  (if-let [qname (@query->name q)]
    qname
    (let [qname (fm/uuid)]
      (swap! query->name assoc q qname)
      qname)))

(defn- submit-query! [{:keys [conn db] :as df-tx-listener} query-name query-prepared]
  (let [q-core (select-keys query-prepared [:find :where])
        rules  (get query-prepared :rules [])]
    (log/debug "SUBMITTING QUERY" q-core)
    (df/exec! conn (df/query db query-name q-core rules))))

(defn- query-entities [crux-node query]
  (let [fr-query (q-alt/entities-grabbing-alteration query)]
    (mapv first (api/q (api/db crux-node) fr-query))))

(defn- transact-data-for-query!
  [{:keys [crux-node] :as df-listener} query]
  (let [results (query-entities crux-node query)]
    (ingest/submit-crux-query-results df-listener results)))


; ----- API -----
(defn subscribe-query!
  ^BlockingQueue
  [{:keys [conn schema flat-schema] :as df-listener}
   {:crux.dataflow/keys [sub-id query query-name] :as opts}]
  (let [query--prepared (schema/prepare-query flat-schema query)
        opts (assoc opts :crux.dataflow/query-analysis (qa/analyse-query query--prepared))
        query-name (or query-name (map-query-to-id! query--prepared))
        queue (LinkedBlockingQueue.)
        listener (res-process/mk-listener--shaping flat-schema schema query-name queue opts)]
    (transact-data-for-query! df-listener query)
    (submit-query! df-listener query-name query--prepared)
    (df/listen-query! conn query-name sub-id listener)
    queue))

(defn unsubscribe-query! [{:keys [conn] :as dataflow-tx-listener} query-name]
  (df/unlisten-query! conn query-name query-name))

(def start-dataflow-tx-listener df-consumer/start-dataflow-tx-listener)

