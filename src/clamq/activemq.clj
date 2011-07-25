(ns clamq.activemq
 (:import
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.pool PooledConnectionFactory]
   )
 )

(defn activemq [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
  "Returns an ActiveMQ javax.jms.ConnectionFactory pointing to the given broker url.
It currently supports the following optional named arguments (refer to ActiveMQ docs for more details about them):
:username, :password, :max-connections."
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (doto (ActiveMQConnectionFactory. broker) (.setUserName username) (.setPassword password))]
    (doto (PooledConnectionFactory. factory)
      (.setMaxConnections max-connections)
      )
    )
  )
