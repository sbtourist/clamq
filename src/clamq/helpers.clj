(ns clamq.helpers)

(defn discard [message]
  nil
  )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )

(defn receiver-seq [request-queue timeout] 
  (lazy-seq 
    (if-let [m (.poll request-queue timeout java.util.concurrent.TimeUnit/MILLISECONDS)] 
      (cons m (receiver-seq request-queue timeout)) 
      nil
      )
    )
  )