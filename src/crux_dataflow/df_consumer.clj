(ns crux-dataflow.df-consumer
  (:require [clojure.tools.logging :as log]
            [crux-dataflow.crux-helpers :as f]
            [crux-dataflow.df-upload :as ingest]
            [crux.api :as api]
            [clojure.spec.alpha :as s]
            [crux-dataflow.server-connect :as srv-conn]
            [clj-3df.core :as df])
  (:import (java.lang Thread)
           (clojure.lang Atom)
           (java.io Closeable)))

(defn- dataflow-consumer
  [crux-node conn df-db from-tx-id
   {:crux.dataflow/keys [flat-schema
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
                                (ingest/submit-tx-log-entry crux-node conn df-db flat-schema tx-log-entry)
                                tx-id)
                              tx-id)))]
      (when (= last-tx-id tx-id)
        (Thread/sleep poll-interval))
      (recur (long last-tx-id)))))

(defn- mk-worker [crux-node conn df-db options ^Atom on-thread-death]
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


(defrecord CruxDataflowTxListener
  [conn df-db crux-node
   schema  ; map with per-entity sub-schemas
   flat-schema ; flat map with all attribute definitions
   ^Atom worker-thread ^Process server-process]
  Closeable
  (close [_]
    (manifold.stream/close! (:ws conn))
    (doto @worker-thread
      (.interrupt)
      (.join))
    (when server-process
      (doto server-process
        (.destroy)
        (.waitFor)))))

(s/def :crux.dataflow/url string?)
(s/def :crux.dataflow/schema map?)
(s/def :crux.dataflow/poll-interval nat-int?)
(s/def :crux.dataflow/batch-size nat-int?)
(s/def :crux.dataflow/debug-connection? boolean?)
(s/def :crux.dataflow/embed-server? boolean?)

(s/def :crux.dataflow/tx-listener-options
  (s/keys :req [:crux.dataflow/schema]
          :opt [:crux.dataflow/url
                :crux.dataflow/poll-interval
                :crux.dataflow/batch-size
                :crux.dataflow/debug-connection?
                :crux.dataflow/embed-server?]))

(defn start-dataflow-tx-listener
  ^Closeable
  [crux-node
   {:crux.dataflow/keys
        [schema ; map with per entity sub-schemas
         restart-on-death?
         url debug-connection? embed-server?]
    :or {url               srv-conn/default-dataflow-server-url
         restart-on-death? true
         debug-connection? false
         embed-server?     false}
    :as options}]

  (s/assert :crux.dataflow/tx-listener-options options)
  (let [server-process (when embed-server?
                         (srv-conn/start-dataflow-server))
        conn ((if debug-connection?
                df/create-debug-conn!
                df/create-conn!) url)
        flat-schema (apply merge (vals schema))
        df-db (df/create-db flat-schema)
        on-thread-death (atom nil)
        _ (df/exec! conn (df/create-db-inputs df-db))
        init-worker
        (fn []
          (let [wt (mk-worker crux-node conn df-db options on-thread-death)]
            (.start wt)
            wt))
        worker-thread (atom (init-worker))
        listener
        (->CruxDataflowTxListener
          conn df-db crux-node
          schema flat-schema
          worker-thread server-process)]
    (if restart-on-death?
      (reset! on-thread-death
              #(do (log/info "Polling thread blew up. Perhaps #364? Restarting...")
                   (reset! worker-thread (init-worker)))))
    listener))
