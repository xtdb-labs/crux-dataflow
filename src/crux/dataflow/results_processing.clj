(ns crux.dataflow.results-processing
  (:require [clojure.tools.logging :as log]
            [clj-3df.encode :as dfe]
            [crux.dataflow.misc-helpers :as fm]
            [crux.dataflow.schema :as schema]
            [crux.dataflow.query-analysis :as qa]))

(defn- df-triplet-type [[_ _ ^Long cardinality-delta :as df-diff-tuple]]
  (if (> cardinality-delta 0)
    :updated-props
    :deleted-props))

(defn- decode-tuple-value [tuple-value]
  (if-let [eid (:Eid tuple-value)]
    (schema/maybe-decode-id eid)
    (dfe/decode-value tuple-value)))

(defn- decode-tuple-values [tuple-values]
  (mapv decode-tuple-value tuple-values))

(defn- shape-batch--vector
  [flat-schema full-schema triplets-batch opts]
  (->> triplets-batch
       (group-by df-triplet-type)
       (fm/map-values #(mapv (comp decode-tuple-values first) %))))

(defn index-by [key coll]
  (persistent!
    (reduce (fn [m v] (assoc! m (get v key) v))
            (transient {})
            coll)))

(defn- shape-batch--map
  [flat-schema full-schema triplets-batch
   {:crux.dataflow/keys [query query-analysis results-shape results-root-symbol]}]
  (let [attr-vec (:query/attr-vec query-analysis)
        tuple->map (comp (partial zipmap attr-vec) decode-tuple-values first)
        triplet-group-map
        (fn [tuples]
          (index-by :crux.db/id (mapv tuple->map tuples)))]
    (->> triplets-batch
         (group-by df-triplet-type)
         (fm/map-values triplet-group-map))))

(defn mk-listener--raw
  [query-name queue]
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

(defn mk-listener--shaping
  [flat-schema schema query-name dst-queue
   {:crux.dataflow/keys [results-shape] :as opts}]
  (let [shape-fn
        (case results-shape
          :crux.dataflow.results-shape/maps shape-batch--map
          shape-batch--vector)]
    (fn on-3df-message [results]
      (log/debug "RESULTS" query-name (fm/pp-str results))
      (let [ordered-result-triplets (->> results (group-by (comp :TxId second)) (sort-by key))
            shaped-results-by-tx
            (for [[tx-id triplets-batch] ordered-result-triplets]
              (shape-fn flat-schema schema triplets-batch opts))]
        (doseq [diff-op shaped-results-by-tx]
          (.put dst-queue diff-op))))))


; ----- basic tests -----
(assert
  (= {:updated-props   [["pafoewijfewoijfwehhhhh" "hofiewjoiwef"]
                        ["pafoewijfewoijfwehhhhh" "hofijoiwef"]],
      :deleted-props [["pak" "hofiewjoiwef"] ["pak" "hofijoiwef"]]}
     (shape-batch--vector
       schema/test-schema
       {}
       ; that's what 3df gives you back except lists not vectors
       [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
        [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3]
        [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
        [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]]
       {:crux.dataflow/query
        '{:find [?name ?email]
          :where
          [[?user :user/name ?name]
           [?user :user/email ?email]]}})))

(assert
  (= {:updated-props
      {#crux/id":ids/pat"
       {:crux.db/id #crux/id":ids/pat"
        :user/name "pafoewijfewoijfwehhhhh"
        :user/email "hofiewjoiwef"}
       #crux/id":ids/mat"
       {:crux.db/id #crux/id":ids/mat"
        :user/name "pafoewijfewoijfwehhhhh"
        :user/email "hofijoiwef"}}
      :deleted-props
      {#crux/id":ids/mat"
       {:crux.db/id #crux/id":ids/mat"
        :user/name "pak"
        :user/email "hofijoiwef"}}}
     (let [user-schema
             {:user/name [:String]
              :user/email [:String]
              :user/knows [:Eid ::schema/set]
              :user/likes [:String ::schema/list]}
           full-schema (schema/calc-full-schema {:user user-schema})
           flat-schema (schema/calc-flat-schema full-schema)
           q '{:find [?user ?name ?email]
               :where [[?user :user/name ?name]
                       [?user :user/email ?email]]}]
       (shape-batch--map
         flat-schema
         full-schema
         [[[{:Eid "#crux/id :ids/pat"} {:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
          [[{:Eid "#crux/id :ids/mat"} {:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3]
          [[{:Eid "#crux/id :ids/mat"} {:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
          [[{:Eid "#crux/id :ids/mat"} {:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]]
         {:crux.dataflow/results-root-symbol '?user
          :crux.dataflow/query-analysis    (qa/analyse-query q)
          :crux.dataflow/query               q}))))
