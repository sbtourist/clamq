(defproject clamq "0.1"
  :description "Clojure Adapter for JMS Message Queues"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.apache.activemq/activemq-core "5.4.0" 
                  :exclusions
                  [org.apache.activemq/kahadb
                   org.apache.activemq.protobuf/activemq-protobuf
                   org.osgi/org.osgi.core
                   org.springframework.osgi/spring-osgi-core
                   commons-logging/commons-logging-api
                   commons-logging/commons-logging]
                  ]
                 [org.apache.activemq/activemq-pool "5.4.0"]
                 [org.springframework/spring-context "3.0.5.RELEASE"]
                 [org.springframework/spring-jms "3.0.5.RELEASE"]
                 [org.springframework.amqp/spring-amqp "1.0.0.M2"]
                 [org.springframework.amqp/spring-rabbit "1.0.0.M2"]]
   :repositories {"springsource-milestones" "http://maven.springframework.org/milestone"}
)