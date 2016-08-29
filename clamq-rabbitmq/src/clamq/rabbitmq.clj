(ns clamq.rabbitmq
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
   [org.springframework.amqp.core MessageListener]
   [org.springframework.amqp.support.converter SimpleMessageConverter]
   [org.springframework.amqp.rabbit.connection CachingConnectionFactory]
   [org.springframework.amqp.rabbit.core RabbitTemplate]
   [org.springframework.amqp.rabbit.listener SimpleMessageListenerContainer]))

(defn- rabbitmq-producer [connection]
  (macros/validate connection "No value specified for connection!")
  (let [template (RabbitTemplate. connection)]
    (doto template (.setMessageConverter (SimpleMessageConverter.)))
    (reify producer/Producer
      (publish [_ destination message headers]
        (let [exchange    (or (destination :exchange) "")
              routing-key (or (destination :routing-key) "")]
          (.convertAndSend template exchange routing-key message)))
      (publish [self destination message] (producer/publish self destination message {})) 
      (request-reply [_ destination message headers]
        (let [exchange    (or (destination :exchange) "")
              routing-key (or (destination :routing-key) "")]
          (.sendAndReceive exchange routing-key message))))))

(defn- rabbitmq-consumer [connection {:keys [endpoint transacted limit] 
                                      handler-fn :on-message 
                                      failure-fn :on-failure
                                      :or   {limit 0 
                                             failure-fn helpers/rethrow-on-failure}}]
  (macros/validate connection "No value specified for connection!")
  (macros/validate endpoint "No value specified for :endpoint!")
  (macros/validate transacted "No value specified for :transacted!")
  (macros/validate handler-fn "No value specified for :on-message!")
  (let [container (SimpleMessageListenerContainer.) 
        listener (macros/non-blocking-listener MessageListener onMessage (SimpleMessageConverter.) handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setQueueNames (into-array String (vector endpoint)))
      (.setMessageListener listener)
      (.setChannelTransacted transacted)
      (.setConcurrentConsumers 1))
    (reify consumer/Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (close [self] (do (.stop container) nil)))))

(defn- rabbitmq-seqable-consumer [connection {:keys [endpoint timeout] :or {timeout 0}}]
  (macros/validate connection "No value specified for connection!")
  (macros/validate endpoint "No value specified for :endpoint!")
  (let [request-queue (SynchronousQueue.) reply-queue (SynchronousQueue.)
        container (SimpleMessageListenerContainer.) 
        listener (macros/blocking-listener MessageListener onMessage (SimpleMessageConverter.) request-queue reply-queue container)]
    (doto container
      (.setConnectionFactory connection)
      (.setQueueNames (into-array String (vector endpoint)))
      (.setMessageListener listener)
      (.setChannelTransacted true)
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

(defn rabbitmq-connection 
  "Returns a RabbitMQ connection pointing to the given broker url.
   It currently supports the following optional named arguments (refer to RabbitMQ docs for more details about them):
   :username, :password."
  [broker & {:keys [username password]
             :or   {username "guest" password "guest"}}]
  (macros/validate broker "No value specified for broker URL!")
  (let [factory (CachingConnectionFactory. broker)]
    (doto factory
      (.setUsername username)
      (.setPassword password))
    (reify connection/Connection
      (producer [self]
        (rabbitmq-producer factory))
      (producer [self conf]
        (rabbitmq-producer factory))
      (consumer [self conf]
        (rabbitmq-consumer factory conf))
      (seqable [self conf]
        (rabbitmq-seqable-consumer factory conf))
      (close [self]
        (.destroy factory)))))
