(ns clamq.converters.body-and-header-converter
  (:import [org.springframework.jms.support.converter MessageConverter SimpleMessageConverter]
           [javax.jms JMSException]))

(defn- message-headers [message]
  {:correlation-id (.getJMSCorrelationID message)
   :correlation-id-as-bytes (.getJMSCorrelationIDAsBytes message)
   :delivery-mode (.getJMSDeliveryMode message)
   :destination (.getJMSDestination message)
   :expiration (.getJMSExpiration message)
   :message-id (.getJMSMessageID message)
   :priority (.getJMSPriority message)
   :redelivered (.getJMSRedelivered message)
   :reply-to (.getJMSReplyTo message)
   :time-stamp (.getJMSTimestamp message)
   :type (.getJMSType message)
   })

(defn- message-properties [message]
  (when-let [properties (.getProperties message)]
    (zipmap
     (map #(keyword %) (keys properties))
     (vals properties))))

(defn body-and-header-converter []
  (reify
    MessageConverter
    (fromMessage [this message]
      (let [body (.fromMessage (SimpleMessageConverter.) message)
            hdrs (message-headers message)
            props (message-properties message)]
        {:headers (merge hdrs props) :body body}))

    (toMessage [this message session]
      (.toMessage (SimpleMessageConverter.) message session))))
