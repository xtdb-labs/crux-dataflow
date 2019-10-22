(ns crux-dataflow.crux-helpers
  (:require [crux.api :as api]))


(defn tx-log [node & [from-tx-id]]
  (api/tx-log node (api/new-tx-log-context node) from-tx-id true))

(defn latest-tx-id [crux-node]
  (or (-> crux-node tx-log last :crux.tx/tx-id) 0))
