(ns clamq.jms
 (:require
   [clamq.helpers :as helpers] 
   [clamq.internal.macros :as macros]
   [clamq.internal.utils :as utils] 
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer])
 (:import
   [java.util.concurrent SynchronousQueue]
   [javax.jms BytesMessage ObjectMessage TextMessage ExceptionListener MessageListener]
   [org.springframework.jms.core JmsTemplate MessagePostProcessor]
   [org.springframework.jms.support.converter SimpleMessageConverter]
   [org.springframework.jms.listener DefaultMessageListenerContainer]))

(defn- proxy-message-post-processor [attributes]
  (proxy [MessagePostProcessor] []
    (postProcessMessage [message]
      (doseq [attribute attributes] (.setStringProperty message (attribute 0) (attribute 1)))
      message)))

(defn- jms-producer [connection {pubSub :pubSub timeToLive :timeToLive :or {pubSub false}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (let [template (JmsTemplate. connection)]
    (doto template (.setMessageConverter (SimpleMessageConverter.)) (.setPubSubDomain pubSub))
    (when timeToLive
      (doto template
        (.setExplicitQosEnabled true)
        (.setTimeToLive timeToLive)))
    (reify producer/Producer
      (publish [self destination message attributes]
        (.convertAndSend template destination message (proxy-message-post-processor attributes)))
      (publish [self destination message] (producer/publish self destination message {})))))

(defn- jms-consumer [connection {endpoint :endpoint handler-fn :on-message transacted :transacted pubSub :pubSub limit :limit failure-fn :on-failure :or {pubSub false limit 0 failure-fn helpers/rethrow-on-failure}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (when (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (when (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (when (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for :on-message!")))
  (let [container (DefaultMessageListenerContainer.) 
        listener (macros/non-blocking-listener MessageListener onMessage (SimpleMessageConverter.) handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName endpoint)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      (.setPubSubDomain pubSub)
      (.setConcurrentConsumers 1))
    (reify consumer/Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (close [self] (do (.shutdown container) nil)))))

(defn- jms-seqable-consumer [connection {endpoint :endpoint timeout :timeout :or {timeout 0}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (when (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (let [request-queue (SynchronousQueue.) reply-queue (SynchronousQueue.)
        container (DefaultMessageListenerContainer.) 
        listener (macros/blocking-listener MessageListener onMessage (SimpleMessageConverter.) request-queue reply-queue container)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName endpoint)
      (.setMessageListener listener)
      (.setSessionTransacted true)
      (.setConcurrentConsumers 1)
      (.start) 
      (.initialize))
    (reify seqable/Seqable
      (mseq [self]
        (utils/receiver-seq request-queue timeout))
      (ack [self]
        (when-not (.offer reply-queue :commit 10 java.util.concurrent.TimeUnit/SECONDS)
          (.shutdown container)
          (throw (IllegalStateException. "Unable to ack message, failing fast by shutting down consumer."))))
      (close [self]
        (.offer reply-queue :rollback 5 java.util.concurrent.TimeUnit/SECONDS)
        (.shutdown container)))))

(defn jms-connection [connectionFactory close-fn]
"Returns a JMS Connection from the given javax.jms.ConnectionFactory object."
  (reify connection/Connection
    (producer [self]
      (jms-producer connectionFactory {}))
    (producer [self conf]
      (jms-producer connectionFactory conf))
    (consumer [self conf]
      (jms-consumer connectionFactory conf))
    (seqable [self conf]
      (jms-seqable-consumer connectionFactory conf))
    (close [self]
      (close-fn))))
