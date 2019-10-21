(ns crux-dataflow.schema
  (:require [clojure.tools.logging :as log]
            [crux.codec :as c]
            [clojure.walk :as w]
            [crux.query :as q])
  (:import (java.util Date)))

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

(defn- validate-schema! [schema {:keys [crux.db/id] :as doc}]
  (assert (c/valid-id? id))
  (assert (map? doc))
  (doseq [[k v] (dissoc doc :crux.db/id)
          :let [{:keys [db/valueType input_semantics]} (get schema k)]]
    (assert (contains? schema k))
    (case input_semantics
      "CardinalityMany"
      (do (do (assert (coll? v))
              (doseq [i v]
                (validate-value-type! valueType i))))
      "CardinalityOne"
      (validate-value-type! valueType v))))

(defn matches-schema? [schema doc]
  (try
    (validate-schema! schema doc)
    true
    (catch AssertionError e
      (log/debug e "Does not match schema:")
      false)))

(defn encode-id [v]
  (if (or (string? v) (keyword? v))
    (str "#crux/id "(pr-str v))
    v)) ; todo case type

(defn maybe-encode-id [schema a v]
  (if (and (or (= :crux.db/id a)
               (= :Eid (get-in schema [a :db/valueType])))
           (c/valid-id? v))
    (if (coll? v)
      (->> (for [v v] ; todo check on maps
             (encode-id v))
           (into (empty v)))
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

(defn prepare-map-for-3df [{:keys [crux.db/id] :as crux-query-result-map}]
  (-> crux-query-result-map
      (assoc :db/id (encode-id id))
      (dissoc :crux.db/id)))

(defn prepare-query [schema query]
  (-> (q/normalize-query query)
      (update :where #(encode-query-ids schema %))
      (update :rules #(encode-query-ids schema %))))
