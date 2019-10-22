(ns crux-dataflow.schema
  (:require [clojure.tools.logging :as log]
            [crux.codec :as c]
            [clojure.walk :as w]
            [crux.query :as q]
            [crux-dataflow.misc-helpers :as fm])
  (:import (java.util Date)))

(def test-schema
  #:user{:name {:db/valueType :String,
                :query_support "AdaptiveWCO",
                :index_direction "Both",
                :input_semantics "CardinalityOne",
                :trace_slack {:TxId 1}},
         :email {:db/valueType :String,
                 :query_support "AdaptiveWCO",
                 :index_direction "Both",
                 :input_semantics "CardinalityOne",
                 :trace_slack {:TxId 1}},
         :knows {:db/valueType :Eid,
                 :query_support "AdaptiveWCO",
                 :index_direction "Both",
                 :input_semantics "CardinalityMany",
                 :trace_slack {:TxId 1}},
         :likes {:db/valueType :String,
                 :query_support "AdaptiveWCO",
                 :index_direction "Both",
                 :input_semantics "CardinalityMany",
                 :trace_slack {:TxId 1}}})

(defn- validate-value-type! [value-type v]
  (assert
    (case value-type
      :String (string? v)
      :Number (number? v)
      :Bool (boolean? v)
      :Eid (c/valid-id? v)
      :Aid (keyword? v)
      :Instant (instance? Date v)
      :Uuid (uuid? v)
      :Real (float? v))
    (str "Invalid value type: " value-type " " (pr-str v))))

; db input_semantics doesn't mean what you think it means
; they are more like write semantics
; see https://github.com/comnik/declarative-dataflow/issues/85
; see https://github.com/sixthnormal/clj-3df/issues/45
(defn- validate-schema! [schema {:keys [crux.db/id] :as doc}]
  (assert (c/valid-id? id))
  (assert (map? doc))
  (doseq [[k v] (dissoc doc :crux.db/id)
          :let [{:keys [db/valueType input_semantics]} (get schema k)]]
    (assert (contains? schema k))
    (if (coll? v)
      (doseq [item v]
        (validate-value-type! valueType item))
      (validate-value-type! valueType v))))

(defn matches-schema? [schema doc]
  (try
    (validate-schema! schema doc)
    true
    (catch AssertionError e
      (log/debug e "Does not match schema:")
      false)))

(defn encode-id [v]
  (assert (c/valid-id? v))
  (if (or (string? v) (keyword? v))
    (str "#crux/id "(pr-str v))
    v)) ; todo case type

(defn maybe-encode-id [schema attr-name v]
  (if (and (or (= :crux.db/id attr-name)
               (= :Eid (get-in schema [attr-name :db/valueType]))))
    (if (coll? v) ; todo check on maps
      (into (empty v) (map encode-id v))
      (encode-id v))
    v))

(defn- maybe-decode-id [v]
  (if (string? v)
    (try
      (read-string v)
      (catch Exception e v))
    v))

(defn encode-query-ids [schema clauses]
  (w/postwalk
    (fn [x]
      (if (and (vector? x) (= 3 (count x)))
        (let [[e a v] x]
          [e a (maybe-encode-id schema a v)])
        x))
    clauses))

(defn decode-result-ids [results]
  (w/postwalk
    (fn [x]
      (if (and (map? x) (= [:Eid] (keys x)))
        (update x :Eid maybe-decode-id)
        x))
    results))

(defn- auto-encode-eids [doc schema]
  (reduce-kv
    (fn [m k v] (assoc m k (maybe-encode-id schema k v)))
    {}
    doc))

(defn prepare-map-for-3df [schema {:keys [crux.db/id] :as crux-query-result-map}]
  (assert (map? crux-query-result-map))
  (-> crux-query-result-map
      (assoc :db/id (encode-id id))
      (dissoc :crux.db/id)
      (auto-encode-eids schema)))

(assert
  (= {:user/name "Patrik",
      :user/knows ["#crux/id :ids/bart"],
      :user/likes ["apples" "daples"],
      :user/email "iifojweiwei",
      :db/id "#crux/id :patrik"}
     (let [args [test-schema
                 {:crux.db/id :patrik,
                  :user/name "Patrik",
                  :user/knows [:ids/bart],
                  :user/likes ["apples" "daples"],
                  :user/email "iifojweiwei"}]]
       (apply prepare-map-for-3df args))))

(defn prepare-query [schema query]
  (-> (q/normalize-query query)
      (update :where #(encode-query-ids schema %))
      (update :rules #(encode-query-ids schema %))))
