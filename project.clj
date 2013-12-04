(defproject clamq "0.5-SNAPSHOT"
 :description "Clojure APIs for Message Queues"
 :dev-dependencies
    [[lein-sub "0.1.1"]]
  :sub
    ["clamq-core"
     "clamq-jms"
     "clamq-activemq"
     "clamq-rabbitmq"
     "clamq-runner"]

:plugins [[lein-sub "0.3.0"]])
