(ns clamq.helpers
 (:use [clamq.producer])
 )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )


