(ns clamq.jms
 (:require
   [clamq.helpers :as helpers]
   [clamq.internal.macros :as macros]
   [clamq.internal.utils :as utils]
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   [clamq.converters :as conv])
 (:import
   [java.util.concurrent SynchronousQueue]
   [javax.jms BytesMessage ObjectMessage TextMessage ExceptionListener MessageListener Session]
   [org.springframework.jms.core JmsTemplate MessagePostProcessor MessageCreator]
   [org.springframework.jms.support.converter SimpleMessageConverter]
   [org.springframework.jms.listener DefaultMessageListenerContainer]))

(defn- jms-producer [connection {:keys [pubsub time-to-live receive-timeout]
                                 :or   {pubsub false
                                        receive-timeout 10000}}]
  (macros/validate connection "No value specified for connection!")
  (let [template (JmsTemplate. connection)]
    (doto template 
      (.setMessageConverter (conv/body-and-header-converter))
      (.setPubSubDomain pubsub) 
      (.setReceiveTimeout receive-timeout)) 
    (when time-to-live
      (doto template
        (.setExplicitQosEnabled true)
        (.setTimeToLive time-to-live)))

    (reify producer/Producer
      (publish [_ destination message headers]
        (.send template destination (conv/message-creator message headers)))
      (publish [self destination message] (producer/publish self destination message {}))
      (request-reply [_ destination message headers]
        (.sendAndReceive template destination (conv/message-creator message headers))))))

(defn- jms-consumer [connection {:keys [endpoint transacted pubsub limit convert-with-headers] 
                                 handler-fn :on-message 
                                 failure-fn :on-failure
                                 :or {pubsub false 
                                      limit 0 
                                      failure-fn helpers/rethrow-on-failure 
                                      convert-with-headers false}}]
  (macros/validate connection "No value specified for connection!")
  (macros/validate endpoint "No value specified for :endpoint!")
  (macros/validate transacted "No value specified for :transacted!")
  (macros/validate handler-fn "No value specified for :on-message!")
  (let [container (DefaultMessageListenerContainer.)
        msg-converter (if convert-with-headers (conv/body-and-header-converter) (SimpleMessageConverter.))
        listener (macros/non-blocking-listener MessageListener onMessage msg-converter handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setDestinationName endpoint)
      (.setMessageListener listener)
      (.setSessionTransacted transacted)
      (.setPubSubDomain pubsub)
      (.setConcurrentConsumers 1))
    (reify consumer/Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (close [self] (do (.shutdown container) nil)))))

(defn- jms-seqable-consumer [connection {:keys [endpoint timeout] :or {timeout 0}}]
  (macros/validate connection "No value specified for connection!")
  (macros/validate endpoint "No value specified for :endpoint!")
  (let [request-queue (SynchronousQueue.) 
        reply-queue (SynchronousQueue.)
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

(defn jms-connection 
  "Returns a JMS Connection from the given javax.jms.ConnectionFactory object."
  [connectionFactory close-fn]
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
