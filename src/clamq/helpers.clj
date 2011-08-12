(ns clamq.helpers)

(defn discard [message]
  nil
  )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )