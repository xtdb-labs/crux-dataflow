(ns crux.dataflow.query-analysis
  "Mostly copied from crux-console, perhaps a unified query-analysis.cljc
  could be a better answer facilitating growth."
  (:require [crux.dataflow.misc-helpers :as fm]
            [clojure.set :as cset]
            [clojure.tools.reader.edn :as reader]
            [crux.query :as q])
  (:import (clojure.lang IPersistentSet)))

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

(analyse-query
  '{:find [e n m]
    :where
    [[e :user/name n]
     [p :user/email m]]})

(assert
  (= {'?name :user/name}
     (infer-symbol-attr-map
       '{:find [?name]
         :where [[?e :user/name ?name]]})))

(assert
  (= #{'e 'p 'f}
     (:query/all-eids
       (analyse-query
         '{:find [e p f]
           :where
           [[e :user/name "Matt"]
            [p :user/name "Pat"]
            [f :task/followers p]]}))))


