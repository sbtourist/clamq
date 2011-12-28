(defproject clamq/clamq-activemq "0.5-SNAPSHOT"
 :description "Clojure APIs for Message Queues"
 :dependencies [[clamq/clamq-jms "0.5-SNAPSHOT"]
                [org.slf4j/slf4j-api "1.6.1"]
                [org.apache.activemq/activemq-core "5.5.0"]]
 :dev-dependencies [[org.slf4j/slf4j-simple "1.6.1"]])