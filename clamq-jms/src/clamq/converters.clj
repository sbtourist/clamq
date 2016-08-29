(ns clamq.converters
  (:require [camel-snake-kebab.core :refer [->PascalCase]])
  (:import [org.springframework.jms.support.converter MessageConverter SimpleMessageConverter]
           [org.springframework.jms.core JmsTemplate MessageCreator] 
           [javax.jms Session TextMessage DeliveryMode]))

(defn- jms-message->headers [jms-message]
  {:correlation-id (.getJMSCorrelationID jms-message)
   :correlation-id-as-bytes (.getJMSCorrelationIDAsBytes jms-message)
   :delivery-mode (.getJMSDeliveryMode jms-message)
   :destination (.getJMSDestination jms-message)
   :expiration (.getJMSExpiration jms-message)
   :message-id (.getJMSMessageID jms-message)
   :priority (.getJMSPriority jms-message)
   :redelivered (.getJMSRedelivered jms-message)
   :reply-to (.getJMSReplyTo jms-message)
   :timestamp (.getJMSTimestamp jms-message)
   :type (.getJMSType jms-message)})

(defn uuid [] 
  (.toString (java.util.UUID/randomUUID)))

(defn- headers->jms-message [jms-message headers]
  (.setJMSCorrelationID jms-message (:correlation-id headers (uuid))) 
  ;; (.setJMSCorrelationIDAsBytes jms-message (:correlation-id-as-bytes headers))
  (.setJMSDeliveryMode jms-message (:delivery-mode headers (DeliveryMode/PERSISTENT)))
  (.setJMSExpiration jms-message (:expiration headers 0))
  (.setJMSPriority jms-message (:priority headers 4))
  (.setJMSRedelivered jms-message (:redelivered headers false))
  (.setJMSType jms-message (:jms-type headers))
  ;; (.setJMSReplyTo jms-message (:reply-to headers))
  jms-message)

(defn- jms-message->properties [jms-message]
  (when-let [properties (.getProperties jms-message)]
    (zipmap
     (map #(keyword %) (keys properties))
     (vals properties))))

(defn- properties->jms-message [jms-message properties {:keys [key-fn]
                                                        :or   {key-fn ->PascalCase}}]
  (doseq [[k v] properties] 
    (.setStringProperty jms-message (key-fn (name k)) v))
  jms-message)

(defn body-and-header-converter []
  (reify
    MessageConverter
    (fromMessage [this message]
      (let [body (.fromMessage (SimpleMessageConverter.) message)
	    hdrs (jms-message->headers message)
	    props (jms-message->properties message)]
	{:headers (merge hdrs props) :body body}))

    (toMessage [this message session]
      (.toMessage (SimpleMessageConverter.) message session))))

(defn- text-message [session message headers opts]
  (let [txt-msg (-> (.createTextMessage session message)
                    (headers->jms-message headers)
                    (properties->jms-message (:properties headers) opts))]
    txt-msg))

(defn message-creator 
  "Creates an implementation of a Spring MessageCreator. This implementation creates a
   TextMessage and sets any provided headers and properties on that message." 
  [message headers & [opts]]
  (reify MessageCreator
    (createMessage [_ session]
      (text-message session message headers opts))))
