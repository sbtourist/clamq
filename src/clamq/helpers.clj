(ns clamq.helpers
 (:use [clamq.producer])
 )

(defn discard [message]
  nil
  )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )


