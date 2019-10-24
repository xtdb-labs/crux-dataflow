(ns crux-dataflow.misc-helpers
  (:require [clojure.pprint :as pp])
  (:import (java.util UUID Map)
           (clojure.lang ITransientAssociative)))

(defn uuid []
  (UUID/randomUUID))

(defn maps->tx-ops [maps]
  (mapv #(vector :crux.tx/put %) maps))

(defn map-values [f ^Map m]
  (persistent!
    (reduce-kv
      (fn [^ITransientAssociative acc k v] (assoc! acc k (f v)))
      (transient {})
      m)))

(defn pp-str [v]
  (with-out-str (pp/pprint v)))
