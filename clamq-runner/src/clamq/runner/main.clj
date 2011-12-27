(ns clamq.runner.main
 (:require 
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   [clamq.protocol.pipe :as pipe])
 (:gen-class))

(defn -main [& args]
  (if-let [command (first args)]
    (binding [*ns* (the-ns 'clamq.runner.main)]
      (load-file command))))