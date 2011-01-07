(ns clamq.activemq
 (:use
   [clamq.jms]
   )
 (:import
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.pool PooledConnectionFactory]
   )
 )

(defn- make-factory-configuration [factory]
  {:username #(.setUserName factory %1)
   :password #(.setPassword factory %1)
   }
  )

(defn activemq-connection [broker & {max-connections :max-connections :or {max-connections 1} :as parameters}]
  "Returns an ActiveMQ javax.jms.ConnectionFactory pointing to the given broker url.
It currently supports the following optional named arguments (refer to ActiveMQ docs for more details about them):
:username, :password, :max-connections."
  (if (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (ActiveMQConnectionFactory. broker) configuration (make-factory-configuration factory) settings (dissoc parameters :max-connections)]
    (doseq [param (select-keys settings (keys configuration))]
      ((configuration param) (settings param))
      )
    (doto (PooledConnectionFactory. factory)
      (.setMaxConnections max-connections)
      )
    )
  )
