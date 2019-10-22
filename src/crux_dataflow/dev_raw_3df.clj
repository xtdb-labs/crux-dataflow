(ns crux-dataflow.dev-raw-3df
  (:require [clj-3df.core :as df]))

(df/exec!
  (.-conn crux-3df)
  (df/transact (.-df-db crux-3df)
               [{:db/id "patrik"
                 :user/name "Patrik"
                 :user/email "ojeijwi"}]))


(df/exec!
  (.-conn crux-3df)
  (df/register-query (.-df-db crux-3df) "email"
                     '[:find ?email
                       :where
                       [?user :user/name "Patrik"]
                       [?user :user/email ?email]]))

(df/exec!
  (.-conn crux-3df)
  (df/query
    (.-df-db crux-3df)
    "email2"
    '[:find ?email
      :where
      [?user :user/name "Patrik"]
      [?user :user/email ?email]]))

(df/listen-query!
  (.-conn crux-3df) "email" ::one
  #(println "eauau" %))

(df/listen!
  (.-conn crux-3df) "on-all"
  #(println "eauau" %))

(df/listen-query!
  (.-conn crux-3df) "email2" ::one
  #(println "eauau" %))
