(ns clamq.activemq
 (:require [clamq.jms :as jms])
 (:import
   [java.io Closeable]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.springframework.jms.connection CachingConnectionFactory]))

(defn activemq-connection 
  "Returns an ActiveMQ javax.jms.ConnectionFactory pointing to the given broker url.
   It currently supports the following optional named arguments (refer to ActiveMQ docs for more details about them):
   :username, :password"
  [broker & {:keys [username password max-connections]
             :or   {max-connections 1}}]
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (doto (ActiveMQConnectionFactory. broker) 
                  (.setUserName username) 
                  (.setPassword password))
        pool (CachingConnectionFactory. factory)]
    (jms/jms-connection pool #(.destroy pool))))
