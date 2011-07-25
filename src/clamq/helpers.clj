(ns clamq.helpers)

(defn discard [message]
  nil
  )

(defn process [message container handler-fn failure-fn limit counter]
  (swap! counter inc)
  (try
    (handler-fn message)
    (catch Exception ex
      (failure-fn {:message message :exception ex})
      )
    (finally
      (if (= limit @counter) (do (.stop container) (future (.shutdown container))))
      )
    )
  )

(defn rethrow-on-failure [failure]
  (throw (:exception failure))
  )