(ns clamq.consumer
 (:import
   [javax.jms BytesMessage ObjectMessage TextMessage MessageListener]
   [org.springframework.jms.listener DefaultMessageListenerContainer]
   )
 )

(defprotocol Consumer
  (start [self])
  (stop [self])
  )

(defn- proxy-message-listener [handler]
  (proxy [MessageListener] []
    (onMessage [message]
      (cond
          (instance? TextMessage message)
          (handler (.getText message))
          (instance? ObjectMessage message)
          (handler (.getObject message))
          (instance? BytesMessage message)
          (let [byteArray (byte-array (.getBodyLength message))] (.readBytes message byteArray) (handler byteArray))
          :else
          (throw (IllegalStateException. (str "Unknown message format: " (class message))))
          )
      )
    )
  )

(defn consumer [connection destination handler {transacted :transacted consumers :consumers :or {consumers 1}}]
  (if (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (let [container (DefaultMessageListenerContainer.) listener (proxy-message-listener handler)]
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


