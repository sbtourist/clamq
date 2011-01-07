(ns clamq.jms
 (:use
   [clamq.helpers] [clamq.protocol]
   )
 (:import
   [javax.jms BytesMessage ObjectMessage TextMessage ExceptionListener MessageListener]
   [org.springframework.jms.core JmsTemplate MessageCreator]
   [org.springframework.jms.listener DefaultMessageListenerContainer]
   )
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

(defn- proxy-message-creator [creator-fn obj attributes]
  (proxy [MessageCreator] []
    (createMessage [session]
      (let [message (creator-fn session obj)]
        (doseq [attribute attributes] (.setStringProperty message (attribute 0) (attribute 1)))
        message
        )
      )
    )
  )

(defn- proxy-message-listener [handler-fn failure-fn limit container]
  (let [counter (atom 0)]
    (proxy [MessageListener] []
      (onMessage [message]
        (swap! counter inc)
        (let [converted (convert-message message)]
          (try
            (handler-fn converted)
            (catch Exception ex (failure-fn {:message converted :exception ex}))
            (finally (if (= limit @counter) (do (.stop container) (future (.shutdown container)))))
            )
          )
        )
      )
    )
  )

(defn- jms-producer [connection {pubSub :pubSub :or {pubSub false}}]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (let [template (JmsTemplate. connection)]
    (doto template
      (.setPubSubDomain pubSub)
      )
    (reify Producer
      (send-to [self destination message attributes]
        (cond
          (string? message)
          (.send template destination (proxy-message-creator #(.createTextMessage %1 %2) message attributes))
          (instance? java.io.Serializable message)
          (.send template destination (proxy-message-creator #(.createObjectMessage %1 %2) message attributes))
          (instance? (Class/forName "[B") message)
          (.send template destination (proxy-message-creator #(doto (.createBytesMessage %1) (.writeBytes %2)) message attributes))
          :else
          (throw (IllegalStateException. (str "Unknown message format: " (class message))))
          )
        )
      )
    )
  )

(defn- jms-consumer [connection {endpoint :endpoint handler-fn :on-message transacted :transacted pubSub :pubSub limit :limit failure-fn :on-failure :or {pubSub false limit 0 failure-fn rethrow-on-failure}}]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (if (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (if (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (if (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for :on-message!")))
  (let [container (DefaultMessageListenerContainer.) listener (proxy-message-listener handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName endpoint)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      (.setPubSubDomain pubSub)
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
