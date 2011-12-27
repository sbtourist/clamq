(defproject clamq/clamq-rabbitmq "0.4-SNAPSHOT"
 :description "Clojure APIs for Message Queues"
 :dependencies [[clamq/clamq-core "0.4-SNAPSHOT"]
                [org.slf4j/slf4j-api "1.6.1"]
                [org.springframework.amqp/spring-rabbit "1.0.0.RELEASE"]]
 :dev-dependencies [
                [org.slf4j/slf4j-simple "1.6.1"]])