(ns crux-dataflow.misc-helpers
  (:require [clojure.pprint :as pp])
  (:import (java.util UUID)))

(defn uuid []
  (UUID/randomUUID))

(defn maps->tx-ops [maps]
  (mapv #(vector :crux.tx/put %) maps))

(defn pp-str [v]
  (with-out-str (pp/pprint v)))
