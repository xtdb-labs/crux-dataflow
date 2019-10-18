(ns crux-dataflow.3df-ingest
  (:require [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux-dataflow.schema :as schema]
            [clj-3df.core :as df]))

(defn- tx-put
  [schema crux-db snapshot acc doc-or-id
   {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id] :as tx-log-entry-w-doc}]
  ;
  (if-not (schema/matches-schema? schema doc-or-id)
    (do (log/debug "SKIPPING-DOC:" doc-or-id)
        acc)
    (let [new-doc doc-or-id
          _ (log/debug "NEW-DOC:" (pr-str new-doc))
          eid (:crux.db/id new-doc)
          eid-3df (schema/encode-id eid)
          old-doc (some->> (api/history-descending crux-db snapshot eid)
                           ;; NOTE: This comment seems like a potential bug?
                           ;; history-descending inconsistently includes the current document
                           ;; sometimes (on first transaction attleast
                           (filter
                            (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                           first :crux.db/doc)
          _ (log/debug "OLD-DOC:" (pr-str old-doc))]
      (into
       acc
       (apply
        concat
        (for [k (set (concat (keys new-doc) (keys old-doc)))
              :when (not= k :crux.db/id)]
          (let [old-val (get old-doc k)
                new-val (get new-doc k)
                old-set (when (not (nil? old-val)) (if (coll? old-val) (set old-val) #{old-val}))
                new-set (when (not (nil? new-val)) (if (coll? new-val) (set new-val) #{new-val}))]
            (log/debug "KEY:" k (pr-str old-set) (pr-str new-set))
            (concat
             (for [new new-set
                   :when (not (nil? new))
                   :when (not (contains? old-set new))]
               [:db/add eid-3df k (schema/maybe-encode-id schema k new)])
             (for [old old-set
                   :when (not (nil? old))
                   :when (not (contains? new-set old))]
               [:db/retract eid-3df k (schema/maybe-encode-id schema k old)])))))))))


(defn index-to-3df
  [crux-node conn db schema {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id] :as tx}]
  (let [crux-db (api/db crux-node tx-time tx-time)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-transaction
            (reduce
             (fn [acc [op-key doc-or-id]]
               (case op-key
                 :crux.tx/put (tx-put schema db snapshot acc doc-or-id tx)))
             []
             tx-ops)]
        (log/debug "3DF Tx:" (pr-str new-transaction))
        @(df/exec! conn (df/transact db new-transaction))))))

