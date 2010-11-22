(ns clamq.connection.activemq
 (:import
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.pool PooledConnectionFactory]
   )
 )

(defn- configuration-map [factory]
  {:username #(.setUserName factory %1)
   :password #(.setPassword factory %1)
   }
  )

(defn activemq [broker & {max-connections :max-connections :or {max-connections 1} :as parameters}]
  "Returns an ActiveMQ connection pointing to the given broker url.
   It currently accepts a map of optional parameters (refer to ActiveMQ docs for more details about them):
   :username, :password, :max-connections."
  (if (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (ActiveMQConnectionFactory. broker) configuration (configuration-map factory) settings (dissoc parameters :max-connections)]
    (doseq [param (select-keys settings (keys configuration))]
      ((configuration param) (settings param))
      )
    (doto (PooledConnectionFactory. factory)
      (.setMaxConnections max-connections)
      )
    )
  )
