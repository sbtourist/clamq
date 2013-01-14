(ns clamq.activemq
 (:require [clamq.jms :as jms])
 (:import
   [java.io Closeable]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.pool PooledConnectionFactory]))

(defn activemq-connection [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
"Returns an ActiveMQ javax.jms.ConnectionFactory pointing to the given broker url.
It currently supports the following optional named arguments (refer to ActiveMQ docs for more details about them):
:username, :password"
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [pool (doto 
               (PooledConnectionFactory. (ActiveMQConnectionFactory. username password broker)) 
               (.setMaxConnections max-connections))]    
    (jms/jms-connection pool #(.stop pool))))
