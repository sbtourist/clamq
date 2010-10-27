(ns clamq.consumer
 (:import
   [javax.jms MessageListener]
   [org.springframework.jms.listener DefaultMessageListenerContainer]
   )
 )

(defprotocol Consumer
  (start [self])
  (stop [self])
  )

(defn- proxy-message-listener [handler]
  (proxy [MessageListener] []
    (onMessage [message] (handler message)))
  )

(defn make-consumer [connection destination handler transacted]
  (let [container (DefaultMessageListenerContainer.) listener (proxy-message-listener handler)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName destination)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      )
    (reify Consumer
      (start [self] (doto container (.start) (.initialize)))
      (stop [self] (.shutdown container))
      )
    )
  )


