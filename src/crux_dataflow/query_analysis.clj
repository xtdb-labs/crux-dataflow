(ns crux-dataflow.query-analysis
  "Mostly copied from crux-console, perhaps a unified query-analysis.cljc
  could be a better answer facilitating growth."
  (:require [crux-dataflow.misc-helpers :as fm]
            [clojure.set :as cset]
            [clojure.tools.reader.edn :as reader]
            [crux.query :as q])
  (:import (clojure.lang IPersistentSet)))

(defn calc-vector-headers [query-vector]
  (->> query-vector
       (drop-while #(not= :find %))
       (rest)
       (take-while #(not= (keyword? %)))))

(defn analyse-full-results-headers [query-results-seq]
  (let [res-count (count query-results-seq)
        sample (if (> res-count 50)
                 (random-sample (/ 50 res-count) query-results-seq)
                 query-results-seq)]
    (set (flatten (map (comp keys first) sample)))))

(defn single-tx-vec->map [[type doc-id doc vt tt]]
  {:crux.ui/query-type :crux.ui.query-type/tx
   :crux.tx/type type
   :crux.db/id   doc-id
   :crux.db/doc  doc
   :crux.ui/vt   vt
   :crux.ui/tt   tt})

(defn multi-tx-vec->map [txes-vector]
  (let [tx-infos (map single-tx-vec->map txes-vector)]
    {:crux.ui/query-type :crux.ui.query-type/tx
     :tx-count           (count tx-infos)
     :tx-infos           tx-infos}))

(defn try-parse-edn-string-or-nil [^String str]
  (try
    (reader/read-string str)
    (catch Exception e
      nil)))

(defn try-parse-edn-string [^String str]
  (try
    (reader/read-string str)
    (catch Exception e
      {:error e})))

(defn query-vector? [edn]
  (and (vector? edn) (= :find (first edn))))

(def crux-tx-types-set
  #{:crux.tx/put :crux.tx/cas :crux.tx/delete :crux.tx/evict})

(defn- single-tx-vector? [edn]
  (and (vector? edn) (crux-tx-types-set (first edn))))

(defn- multi-tx-vector? [edn]
  (and (vector? edn) (not-empty edn) (every? single-tx-vector? edn)))

(defn- query-map? [edn]
  (and (map? edn) (every? edn [:find :where])))

(defn- third [coll]
  (nth coll 2 nil))

(defn- collect-eid-triplets [where-vec]
  (filter (comp symbol? first) where-vec))

(defn ^IPersistentSet collect-where-vector-eids [where-vec]
  (set (map first (collect-eid-triplets where-vec))))

(defn- infer-symbol-attr-map
  "Given a simple datalog query map returns a map symbol -> attribute"
  [qmap]
  (let [symbols-queried   (set (:find qmap))
        where-vec         (:where qmap)
        attr-triplets     (filter (comp keyword? second) where-vec)
        retained-triplets (filter #(symbols-queried (third %)) attr-triplets)
        eid-symbols       (collect-where-vector-eids where-vec)
        retained-eids     (set (filter symbols-queried eid-symbols))
        lookup-symbol-attr (fn [symbol]
                             (second (first (filter #(= (third %) symbol) retained-triplets))))
        symbol-attr-pair  (fn [symbol]
                            [symbol
                              (if (retained-eids symbol)
                                :crux.db/id
                                (lookup-symbol-attr symbol))])
        symbol->attr      (into {} (map symbol-attr-pair symbols-queried))]
    symbol->attr))

(assert
  (= {'?name :user/name}
     (infer-symbol-attr-map
       '{:find [?name]
         :where [[?e :user/name ?name]]})))

(defn analyse-query [input-edn]
  (let [qmap (q/normalize-query input-edn)
        s->a (infer-symbol-attr-map qmap)
        a->s (cset/map-invert s->a)
        attr-seq (vals s->a)
        attr-set (set attr-seq)
        find-vec (:find qmap)
        symbol-positions (into {} (map vector find-vec (range)))
        attr-positions (zipmap attr-seq (map (comp symbol-positions a->s) attr-seq))]
    (assoc qmap :crux.ui/query-type :crux.ui.query-type/query
                :query/style (if (map? input-edn) :query.style/map :query.style/vector)
                :query/ids-queried? (attr-set :crux.db/id)
                :query/symbol-positions symbol-positions
                :query/original-edn input-edn
                :query/all-eids (collect-where-vector-eids (:where qmap))
                :query/normalized-edn qmap
                :query/attr-set attr-set
                :query/attr-vec (mapv s->a find-vec)
                :query/attr-positions attr-positions
                :query/pos->attr (cset/map-invert attr-positions)
                :query/attributes s->a)))

(assert
  (= #{'e 'p 'f}
     (:query/all-eids
       (analyse-query
         '{:find [e p f]
           :where
           [[e :user/name "Matt"]
            [p :user/name "Pat"]
            [f :task/followers p]]}))))

(defn analyse-any-query [input-edn]
  (try
    (cond
      (query-vector? input-edn)     (analyse-query input-edn)
      (multi-tx-vector?  input-edn) (multi-tx-vec->map input-edn)
      (query-map? input-edn)        (analyse-query input-edn)
      :else                         false)
    (catch Exception e
      false)))

(defn- calc-numeric-keys [result-map]
  (map first (filter (comp number? second) result-map)))

(defn analyse-results
  [{:query/keys
    [attr-set
     attr-vec]
    :as query-info}
   results]
  (if (and (= (:crux.ui/query-type query-info) :crux.ui.query-type/query)
           (not-empty results))
    (let [r-count (count results)
          ids-received? (or (attr-set :crux.db/id) (:full-results? query-info))
          full-results? (:full-results? query-info)
          first-res (if full-results?
                      (-> results first first)
                      (-> results first))
          first-res-map (if full-results?
                          first-res
                          (zipmap attr-vec first-res))
          ids-pluck
          (cond
            full-results? (comp :crux.db/id first)
            ids-received? first
            :else identity)
          numeric-attrs (disj (set (calc-numeric-keys first-res-map)) :crux.db/id)
          discrete-attrs (cset/difference (disj attr-set :crux.db/id)  numeric-attrs)
          ids (if ids-received? (map ids-pluck results))]
      {:ra/single-entity?     (= 1 r-count)
       :ra/results-count      r-count
       :ra/has-results?       (> r-count 0)
       :ra/numeric-attrs      numeric-attrs
       :ra/has-numeric-attrs? (> (count numeric-attrs) 0)
       :ra/discrete-attrs     discrete-attrs
       :ra/entity-ids         ids
       :ra/entity-id          (first ids)
       :ra/first-res-map      first-res-map
       :ra/first-res          first-res})
    (println :bailing-out query-info results)))
