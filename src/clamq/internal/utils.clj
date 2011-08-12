(ns clamq.internal.utils)

(defn receiver-seq [request-queue timeout] 
  (lazy-seq 
    (if-let [m (.poll request-queue timeout java.util.concurrent.TimeUnit/MILLISECONDS)] 
      (cons m (receiver-seq request-queue timeout)) 
      nil
      )
    )
  )