(ns clamq.producer
 (:import
   [org.springframework.jms.core JmsTemplate MessageCreator]
   )
 )

(defprotocol Producer
  (send-to [self destination message attributes])
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

(defn producer [connection transacted]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (if (nil? transacted) (throw (IllegalArgumentException. "No value specified for transacted!")))
  (let [template (JmsTemplate. connection)]
    (doto template
      (.setSessionTransacted transacted)
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


