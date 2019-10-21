(ns crux-dataflow.misc-helpers
  (:import (java.util UUID)))

(defn uuid []
  (UUID/randomUUID))

(defn maps->tx-ops [maps]
  (mapv #(vector :crux.tx/put %) maps))
