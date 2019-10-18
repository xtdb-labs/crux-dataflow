(ns crux-dataflow.misc-helpers
  (:import (java.util UUID)))

(defn uuid []
  (UUID/randomUUID))
