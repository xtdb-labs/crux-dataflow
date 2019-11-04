(ns crux.dataflow.misc-helpers
  (:require [clojure.pprint :as pp])
  (:import (java.util UUID Map)
           (clojure.lang ITransientAssociative)))

(defn uuid []
  (UUID/randomUUID))

(defn maps->tx-ops [maps]
  (mapv #(vector :crux.tx/put %) maps))

(defn map-values
  ([f ^Map m]
   (map-values f m false))
  ([f ^Map m with-keys?]
   (persistent!
     (reduce-kv
       (if with-keys?
         (fn [^ITransientAssociative acc k v] (assoc! acc k (f k v)))
         (fn [^ITransientAssociative acc k v] (assoc! acc k (f v))))
       (transient {})
       m))))

(defn pp-str [v]
  (with-out-str (pp/pprint v)))
