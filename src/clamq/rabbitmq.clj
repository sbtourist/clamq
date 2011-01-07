(ns clamq.rabbitmq
 (:use
   [clamq.helpers] [clamq.protocol]
   )
 (:import
   [org.springframework.amqp.core MessageListener]
   [org.springframework.amqp.support.converter SimpleMessageConverter]
   [org.springframework.amqp.rabbit.connection SingleConnectionFactory]
   [org.springframework.amqp.rabbit.core RabbitTemplate]
   [org.springframework.amqp.rabbit.listener SimpleMessageListenerContainer]
   )
 )

(defn- proxy-message-listener [handler-fn failure-fn limit container]
  (let [counter (atom 0) converter (SimpleMessageConverter.)]
    (proxy [MessageListener] []
      (onMessage [message]
        (swap! counter inc)
        (let [converted (.fromMessage converter message)]
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

(defn- rabbitmq-producer [connection]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (let [template (RabbitTemplate. connection)]
    (reify Producer
      (send-to [self destination message attributes]
        (let [exchange (get destination :exchange "") routing-key (get destination :routing-key "")]
          (.convertAndSend template exchange routing-key message)
          )
        )
      )
    )
  )

(defn- rabbitmq-consumer [connection {endpoint :endpoint handler-fn :on-message transacted :transacted limit :limit failure-fn :on-failure :or {limit 0 failure-fn rethrow-on-failure}}]
  (if (nil? connection) (throw (IllegalArgumentException. "No value specified for connection!")))
  (if (nil? endpoint) (throw (IllegalArgumentException. "No value specified for :endpoint!")))
  (if (nil? transacted) (throw (IllegalArgumentException. "No value specified for :transacted!")))
  (if (nil? handler-fn) (throw (IllegalArgumentException. "No value specified for :on-message!")))
  (let [container (SimpleMessageListenerContainer.) listener (proxy-message-listener handler-fn failure-fn limit container)]
    (doto container
      (.setConnectionFactory connection)
      (.setQueueName endpoint)
      (.setMessageListener listener)
      (.setChannelTransacted transacted)
      )
    (reify Consumer
      (start [self] (do (doto container (.start) (.initialize)) nil))
      (stop [self] (do (.shutdown container) nil))
      )
    )
  )

(defn rabbitmq-connection [broker & {username :username password :password :or {username "guest" password "guest"}}]
"Returns a RabbitMQ connection pointing to the given broker url. 
It currently supports the following optional named arguments (refer to RabbitMQ docs for more details about them):
:username, :password."
  (if (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (SingleConnectionFactory. broker)]
    (reify Connection
      (producer [self]
        (rabbitmq-producer factory)
        )
      (producer [self conf]
        (rabbitmq-producer factory)
        )
      (consumer [self conf]
        (rabbitmq-consumer factory conf)
        )
      )
    )
  )