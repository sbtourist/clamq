(ns clamq.consumer
 (:import
   [javax.jms BytesMessage ObjectMessage TextMessage ExceptionListener MessageListener]
   [org.springframework.jms.listener DefaultMessageListenerContainer]
   )
 (:use
   [clamq.helpers]
   )
 )

(defprotocol Consumer
  (start [self])
  (stop [self])
  )

(defn- convert-message [message]
  (cond
    (instance? TextMessage message)
    (.getText message)
    (instance? ObjectMessage message)
    (.getObject message)
    (instance? BytesMessage message)
    (let [byteArray (byte-array (.getBodyLength message))] (.readBytes message byteArray) byteArray)
    :else
    (throw (IllegalStateException. (str "Unknown message format: " (class message))))
    )
  )

(defn- proxy-message-listener [handler-fn failure-fn]
  (proxy [MessageListener] []
    (onMessage [message]
      (let [converted (convert-message message)]
        (try
          (handler-fn converted)
          (catch Exception ex (failure-fn {:message converted :exception ex}))
          )
        )
      )
    )
  )

(defn consumer [connection destination transacted handler-fn & {consumers :consumers failure-fn :on-failure :or {consumers 1 failure-fn rethrow-on-failure}}]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (if (nil? destination) (throw (IllegalArgumentException. "No value specified for destination!")))
  (if (nil? transacted) (throw (IllegalArgumentException. "No value specified for transacted!")))
  (if (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for handler function!")))
  (let [container (DefaultMessageListenerContainer.) listener (proxy-message-listener handler-fn failure-fn)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName destination)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      (.setConcurrentConsumers consumers)
      )
    (reify Consumer
      (start [self] (doto container (.start) (.initialize)))
      (stop [self] (.shutdown container))
      )
    )
  )


