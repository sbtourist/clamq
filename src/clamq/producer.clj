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

(defn make-producer [connection transacted]
  (let [template (JmsTemplate. connection)]
    (doto template
      (.setSessionTransacted transacted)
      )
    (reify Producer
      (send-to [self destination message attributes]
        (if (string? message)
          (.send template destination (proxy-message-creator #(.createTextMessage %1 %2) message attributes))
          (.send template destination (proxy-message-creator #(.createObjectMessage %1 %2) message attributes))
          )
        )
      )
    )
  )


