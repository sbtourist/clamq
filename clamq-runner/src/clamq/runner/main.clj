(ns clamq.runner.main
 (:require 
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   [clamq.pipes :as pipes]
   [clamq.activemq :as activemq]
   [clamq.rabbitmq :as rabbitmq])
 (:gen-class))

(defn -main [& args]
  (if-let [command (first args)]
    (binding [*ns* (the-ns 'clamq.runner.main)]
      (load-file command))))