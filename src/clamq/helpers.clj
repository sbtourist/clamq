(ns clamq.helpers
 (:use [clamq.producer])
 )

(defn reroute-on-failure [producer destination failure]
  (send-to producer destination (:message failure) {})
  )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )


