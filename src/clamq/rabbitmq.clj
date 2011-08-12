(ns clamq.rabbitmq
 (:require
   [clamq.helpers :as helpers] 
   [clamq.internal.macros :as macros]
   [clamq.internal.utils :as utils] 
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   )
 (:import
   [java.util.concurrent SynchronousQueue]
   [org.springframework.amqp.core MessageListener]
   [org.springframework.amqp.support.converter SimpleMessageConverter]
   [org.springframework.amqp.rabbit.connection SingleConnectionFactory]
   [org.springframework.amqp.rabbit.core RabbitTemplate]
   [org.springframework.amqp.rabbit.listener SimpleMessageListenerContainer]
   )
 )

(defn- rabbitmq-producer [connection]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (let [template (RabbitTemplate. connection)]
    (doto template (.setMessageConverter (SimpleMessageConverter.)))
    (reify producer/Producer
      (publish [self destination message attributes]
        (let [exchange (or (destination :exchange) "") routing-key (or (destination :routing-key) "")]
          (.convertAndSend template exchange routing-key message)
          )
        )
      (publish [self destination message] (producer/publish self destination message {}))
      )
    )
  )

(defn- rabbitmq-consumer [connection {endpoint :endpoint handler-fn :on-message transacted :transacted limit :limit failure-fn :on-failure :or {limit 0 failure-fn helpers/rethrow-on-failure}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (when (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (when (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (when (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for :on-message!")))
  (let [container (SimpleMessageListenerContainer.) 
        listener (macros/non-blocking-listener MessageListener onMessage (SimpleMessageConverter.) handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setQueueNames (into-array String (vector endpoint)))
      (.setMessageListener listener)
      (.setChannelTransacted transacted)
      (.setConcurrentConsumers 1)
      )
    (reify consumer/Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (close [self] (do (.stop container) nil))
      )
    )
  )

(defn- rabbitmq-seqable-consumer [connection {endpoint :endpoint timeout :timeout :or {timeout 0}}]
  (when (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (when (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (let [request-queue (SynchronousQueue.) reply-queue (SynchronousQueue.)
        container (SimpleMessageListenerContainer.) 
        listener (macros/blocking-listener MessageListener onMessage (SimpleMessageConverter.) request-queue reply-queue container)
        ]
    (doto container
      (.setConnectionFactory connection)
      (.setQueueNames (into-array String (vector endpoint)))
      (.setMessageListener listener)
      (.setChannelTransacted true)
      (.setConcurrentConsumers 1)
      (.start) 
      (.initialize)
      )
    (reify seqable/Seqable
      (seqc [self]
        (utils/receiver-seq request-queue timeout)
        )
      (ack [self]
        (.offer reply-queue :commit timeout java.util.concurrent.TimeUnit/MILLISECONDS)
        )
      (close [self]
        (.offer reply-queue :rollback timeout java.util.concurrent.TimeUnit/MILLISECONDS)
        (.shutdown container)
        )
      )
    )
  )

(defn rabbitmq-connection [broker & {username :username password :password :or {username "guest" password "guest"}}]
  "Returns a RabbitMQ connection pointing to the given broker url.
It currently supports the following optional named arguments (refer to RabbitMQ docs for more details about them):
:username, :password."
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (SingleConnectionFactory. broker)]
    (doto factory
      (.setUsername username)
      (.setPassword password)
      )
    (reify connection/Connection
      (producer [self]
        (rabbitmq-producer factory)
        )
      (producer [self conf]
        (rabbitmq-producer factory)
        )
      (consumer [self conf]
        (rabbitmq-consumer factory conf)
        )
      (seqable [self conf]
        (rabbitmq-seqable-consumer factory conf)
        )
      (close [self]
        (.destroy factory)
        )
      )
    )
  )