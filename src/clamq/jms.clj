(ns clamq.jms
 (:use
   [clamq.helpers] [clamq.protocol]
   )
 (:import
   [javax.jms BytesMessage ObjectMessage TextMessage ExceptionListener MessageListener]
   [org.springframework.jms.core JmsTemplate MessagePostProcessor]
   [org.springframework.jms.support.converter SimpleMessageConverter]
   [org.springframework.jms.listener DefaultMessageListenerContainer]
   )
 )

(defn- proxy-message-post-processor [attributes]
  (proxy [MessagePostProcessor] []
    (postProcessMessage [message]
      (doseq [attribute attributes] (.setStringProperty message (attribute 0) (attribute 1)))
      message
      )
    )
  )

(defn- proxy-message-listener [handler-fn failure-fn limit container]
  (let [counter (atom 0) converter (SimpleMessageConverter.)]
    (proxy [MessageListener] []
      (onMessage [message]
        (process (.fromMessage converter message) container handler-fn failure-fn limit counter)
        )
      )
    )
  )

(defn- jms-producer [connection {pubSub :pubSub :or {pubSub false}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (let [template (JmsTemplate. connection)]
    (doto template (.setMessageConverter (SimpleMessageConverter.)) (.setPubSubDomain pubSub))
    (reify Producer
      (send-to [self destination message attributes]
        (.convertAndSend template destination message (proxy-message-post-processor attributes))
        )
    (send-to [self destination message] (send-to self destination message {}))
    )
  )
)

(defn- jms-consumer [connection {endpoint :endpoint handler-fn :on-message transacted :transacted pubSub :pubSub limit :limit failure-fn :on-failure :or {pubSub false limit 0 failure-fn rethrow-on-failure}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (when (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (when (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (when (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for :on-message!")))
  (let [container (DefaultMessageListenerContainer.) listener (proxy-message-listener handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName endpoint)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      (.setPubSubDomain pubSub)
      (.setConcurrentConsumers 1)
      )
    (reify Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (stop [self] (do (.shutdown container) nil))
      )
    )
  )

(defn jms-connection [connectionFactory]
  "Returns a JMS Connection from the given javax.jms.ConnectionFactory object."
  (reify Connection
    (producer [self]
      (jms-producer connectionFactory {})
      )
    (producer [self conf]
      (jms-producer connectionFactory conf)
      )
    (consumer [self conf]
      (jms-consumer connectionFactory conf)
      )
    )
  )
