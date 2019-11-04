(ns crux.dataflow.query-altering
  (:require [crux.dataflow.query-analysis :as qa])
  (:import (clojure.lang IPersistentMap)))

(defn entities-grabbing-alteration [^IPersistentMap query]
  (assoc query :find (vec (qa/collect-where-vector-eids (:where query)))
               :full-results? true))

(assert
  (= '{:find [p e f]
       :full-results? true
       :where
       [[e :user/name "Matt"]
        [p :user/name "Pat"]
        [f :task/followers p]
        [f :task/followers e]
        [f :task/title tt]]}
     (entities-grabbing-alteration
       '{:find [tt]
         :where
         [[e :user/name "Matt"]
          [p :user/name "Pat"]
          [f :task/followers p]
          [f :task/followers e]
          [f :task/title tt]]})))
