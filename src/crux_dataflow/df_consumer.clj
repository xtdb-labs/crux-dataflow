(ns crux-dataflow.df-consumer
  (:require [clojure.tools.logging :as log]
            [crux-dataflow.crux-helpers :as f]
            [crux-dataflow.df-upload :as ingest]
            [crux.api :as api])
  (:import (java.lang Thread)
           (clojure.lang Atom)))

(defn- dataflow-consumer
  [crux-node conn df-db from-tx-id
   {:crux.dataflow/keys [schema
                         poll-interval
                         batch-size]
    :or {poll-interval 100
         batch-size 1000}
    :as options}]
  (loop [tx-id from-tx-id]
    (let [last-tx-id (with-open [tx-log-context (api/new-tx-log-context crux-node)]
                       (->> (api/tx-log crux-node tx-log-context (inc tx-id) true)
                            (take batch-size)
                            (reduce
                              (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                                (ingest/upload-single-crux-tx-to-3df crux-node conn df-db schema tx-log-entry)
                                tx-id)
                              tx-id)))]
      (when (= last-tx-id tx-id)
        (Thread/sleep poll-interval))
      (recur (long last-tx-id)))))

(defn mk-worker [crux-node conn df-db options ^Atom on-thread-death]
  (let [from-tx-id (inc (f/latest-tx-id crux-node))
        payload
        (fn []
          (try
            (dataflow-consumer crux-node conn df-db from-tx-id options)
            (catch InterruptedException ignore)
            (catch Throwable t
              (log/fatal t "Polling failed:")
              (if-let [f (and on-thread-death @on-thread-death)]
                (f)))
            (finally
              (log/info "Dataflow consumer thread exiting"))))]
    (doto
      (Thread. payload)
      (.setName "crux-dataflow.worker-thread"))))
