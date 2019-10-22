(ns crux-dataflow.df-upload
  (:require [clojure.tools.logging :as log]
            [crux.api :as api]
            [clojure.test]
            [crux-dataflow.schema :as schema]
            [clj-3df.core :as df]))

(defn calc-changed-triplets [eid-3df schema old-doc new-doc]
  (vec
    (apply
      concat
      (for [k (set (concat (keys new-doc) (keys old-doc)))
            :when (not= k :crux.db/id)]
        (let [old-val (get old-doc k)
              new-val (get new-doc k)
              old-set (when (not (nil? old-val)) (if (coll? old-val) (set old-val) #{old-val}))
              new-set (when (not (nil? new-val)) (if (coll? new-val) (set new-val) #{new-val}))]
          (concat
            (for [old old-set
                  :when (not (nil? old))
                  :when (not (contains? new-set old))]
              [:db/retract eid-3df k (schema/maybe-encode-id schema k old)])
            (for [new new-set
                  :when (not (nil? new))
                  :when (not (contains? old-set new))]
              [:db/add eid-3df k (schema/maybe-encode-id schema k new)])))))))

(defn- process-put-doc
  "submits docs in put tx"
  ; todo possibly select part of the doc matching schema
  [schema crux-db snapshot acc new-doc
   {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id] :as tx-log-entry-w-doc}]
  (let [_ (log/debug "NEW-DOC:" (pr-str new-doc))
        eid (:crux.db/id new-doc)
        eid-3df (schema/encode-id eid)
        old-doc (some->> (api/history-descending crux-db snapshot eid)
                         ;; NOTE: This comment seems like a potential bug?
                         ;; history-descending inconsistently includes the current document
                         ;; sometimes (on first transaction attleast
                         (filter
                          (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                         first :crux.db/doc)
        _ (log/debug "OLD-DOC:" (pr-str old-doc))
        doc-changed-triplets (calc-changed-triplets eid-3df schema old-doc new-doc)]
    (into acc doc-changed-triplets)))

(defn- sanitise-tx-ops [schema tx-ops]
  (reduce
    (fn [acc [tx-type doc-or-id :as tx-op]]
      (case tx-type
        :crux.tx/put
        (if (and (map? doc-or-id) (schema/matches-schema? schema doc-or-id))
          (conj acc tx-op)
          acc)
        acc))
    []
    tx-ops))

(defn upload-single-crux-tx-to-3df
  [crux-node conn df-db schema {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id] :as tx}]
  (log/debug "CRUX_TX:" (pr-str tx))
  (let [crux-db (api/db crux-node tx-time tx-time)
        clean-tx-ops (sanitise-tx-ops schema tx-ops)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-df-txes
            (reduce
              (fn [acc [tx-type doc-or-id]]
                (case tx-type
                  :crux.tx/put (process-put-doc schema crux-db snapshot acc doc-or-id tx)
                  acc))
              []
              clean-tx-ops)]
        (when (not-empty new-df-txes)
          (log/debug "3DF Tx:" (pr-str new-df-txes))
          @(df/exec! conn (df/transact df-db new-df-txes)))))))

(defn upload-crux-query-results
  [{:keys [conn df-db crux-node schema] :as df-listener}
   crux-query-results]
  (let [df-compatible-maps (mapv (partial schema/prepare-map-for-3df schema) crux-query-results)]
    @(df/exec! conn (df/transact df-db df-compatible-maps))))



(assert
  (schema/matches-schema?
    schema/test-schema
    {:crux.db/id :ids/pat :user/name "pat"}))

(assert
  (= [[:crux.tx/put {:crux.db/id :ids/pat :user/name "pat"}]]
     (sanitise-tx-ops schema/test-schema
                      [[:crux.tx/put {:crux.db/id :ids/pat :user/name "pat"}]
                       [:crux.tx/put #crux/id "7cd3351653ab62a4c69c7fa7e45837d092d3e4de"]
                       [:crux.tx/evict :ids/pat]])))

(assert
  (let [args1
        ["#crux/id :katrik"
         schema/test-schema
         {:crux.db/id :katrik,
          :user/name "katrik",
          :user/likes ["apples" "daples"],
          :user/email "iwefoiiejfoiewfj"}
         {:crux.db/id :katrik,
          :user/name "katrik",
          :user/likes ["apples" "daples"],
          :user/email "iwefoiiejfewfj"}]]
    (= [[:db/retract "#crux/id :katrik" :user/email "iwefoiiejfoiewfj"]
        [:db/add "#crux/id :katrik" :user/email "iwefoiiejfewfj"]]
       (apply calc-changed-triplets args1))))
