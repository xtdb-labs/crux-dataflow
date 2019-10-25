(ns crux-dataflow.api-2
  (:require
    [clojure.tools.logging :as log]
    [clj-3df.core :as df]
    [clj-3df.encode :as dfe]
    [manifold.stream]
    [crux.api :as api]
    [crux-dataflow.query-altering :as q-alt]
    [crux-dataflow.schema :as schema]
    [crux-dataflow.df-relay :as df-consumer]
    [crux-dataflow.misc-helpers :as fm]
    [crux-dataflow.df-upload :as ingest])
  (:import [java.util.concurrent LinkedBlockingQueue BlockingQueue]))


(def ^:private query->name (atom {}))

(defn- map-query-to-id! [q]
  (if-let [qname (@query->name q)]
    qname
    (let [qname (fm/uuid)]
      (swap! query->name assoc q qname)
      qname)))

(def start-dataflow-tx-listener df-consumer/start-dataflow-tx-listener)

(defn- submit-query! [{:keys [conn db] :as df-tx-listener} query-name query-prepared]
  (df/exec! conn
            (df/query
              db
              query-name
              (select-keys query-prepared [:find :where])
              (get query-prepared :rules []))))

(defn- mk-listener--raw
  [query-name
   {:crux.dataflow/keys [query]}
   queue]
  (fn on-3df-message [results]
    (log/debug "RESULTS" query-name results)
    (let [tuples (->> (for [[tx tx-results] (->> (group-by second (schema/decode-result-ids results))
                                                 (sort-by (comp :TxId key)))
                            :let [tuples (for [[t tx add-delete] tx-results
                                               :when (= 1 add-delete)]
                                           (mapv dfe/decode-value t))]
                            :when (seq tuples)]
                        [(:TxId tx) (vec tuples)])
                      (into (sorted-map)))]
      (log/debug query-name "updated:" (pr-str results) "tuples:" (pr-str tuples))
      (.put queue tuples))))

(comment
  ["two"
   (([{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}]
     {:TxId 19}
     1)
    ([{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}]
     {:TxId 19}
     3)
    ([{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1)
    ([{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3))])

(def raw-res
  [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}]
    {:TxId 19}
    1]
   [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}]
    {:TxId 19}
    3]
   [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
   [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]])

(->> (group-by (comp :TxId second) raw-res)
     (sort-by key))
; (1) yields
(def df-triplet-batches
  [[19
    [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
     [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3] ; addition triplet
     [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1] ; retraction triplet
     [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]]]])

(defn- df-triplet-type [[_ _ ^Long cardinality-delta :as df-diff-tuple]]
  (if (> cardinality-delta 0)
    :crux.df/added
    :crux.df/deleted))

(defn- decode-tuple-values [tuple-values]
  (mapv dfe/decode-value tuple-values))

(defn- shape-batch--vector [schema query tx-id triplets-batch]
  (->> triplets-batch
       (group-by df-triplet-type)
       (fm/map-values #(mapv (comp decode-tuple-values first) %))))

(defn- shape-batch--map [schema query tx-id triplets-batch]
  (->> triplets-batch
       (group-by df-triplet-type)
       (fm/map-values #(mapv (comp decode-tuple-values first) %))))

(assert
  (= {:crux.df/added [["pafoewijfewoijfwehhhhh" "hofiewjoiwef"]
                      ["pafoewijfewoijfwehhhhh" "hofijoiwef"]],
      :crux.df/deleted [["pak" "hofiewjoiwef"] ["pak" "hofijoiwef"]]}
     (shape-batch--vector
       schema/test-schema
       '{:find [?name ?email]
         :where
         [[?user :user/name ?name]
          [?user :user/email ?email]]}
       19
       [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
        [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3]
        [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
        [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]])))

(defn- mk-listener--shaping
  [schema
   query-name
   {:crux.dataflow/keys [query results-shape]}
   queue]
  (fn on-3df-message [results]
    (log/debug "RESULTS" query-name (fm/pp-str results))
    (let [shape-fn (case results-shape
                     :crux.dataflow.results-shape/maps shape-batch--map
                     shape-batch--vector)
          ordered-result-triplets ; (1)
          (->> (group-by (comp :TxId second) results)
               (sort-by key))
          shaped-results-by-tx
          (for [[tx-id triplets-batch] ordered-result-triplets]
            (shape-fn schema query, tx-id triplets-batch))]
      (doseq [diff-op shaped-results-by-tx]
        (.put queue diff-op)))))

(defn query-entities [crux-node query]
  (let [fr-query (q-alt/entities-grabbing-alteration query)]
    (mapv first (api/q (api/db crux-node) fr-query))))

(defn- transact-data-for-query!
  [{:keys [crux-node] :as df-listener} query]
  (let [results (query-entities crux-node query)]
    (ingest/submit-crux-query-results df-listener results)))

(defn subscribe-query!
  ^BlockingQueue
  [{:keys [conn schema flat-schema] :as df-listener}
   {:crux.dataflow/keys [sub-id query query-name] :as opts}]
  (let [query--prepared (schema/prepare-query flat-schema query)
        query-name (or query-name (map-query-to-id! query--prepared))
        queue (LinkedBlockingQueue.)]
    (transact-data-for-query! df-listener query)
    (submit-query! df-listener query-name query--prepared)
    (df/listen-query! conn query-name sub-id (mk-listener--shaping schema query-name opts queue))
    queue))

(defn unsubscribe-query! [{:keys [conn] :as dataflow-tx-listener} query-name]
  (df/unlisten-query! conn query-name query-name))
