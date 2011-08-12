(defproject clamq "0.3-SNAPSHOT"
 :description "Clojure APIs for Message Queues"
 :dependencies [
                [org.clojure/clojure "1.2.1"]
                [org.clojure/clojure-contrib "1.2.0"]

                [org.springframework/spring-context "3.0.5.RELEASE"]
                [org.springframework/spring-jms "3.0.5.RELEASE"]
                [org.springframework.amqp/spring-amqp "1.0.0.RC3"
                 :exclusions
                 [commons-logging/commons-logging-api
                  commons-logging/commons-logging
                  org.slf4j/slf4j-api
                  org.slf4j/slf4j-log4j12
                  org.slf4j/jcl-over-slf4j
                  log4j/log4j
                  ]
                 ]

                [org.apache.activemq/activemq-core "5.5.0"
                 :exclusions
                 [org.apache.activemq/kahadb
                  org.apache.activemq.protobuf/activemq-protobuf
                  org.osgi/org.osgi.core
                  org.springframework.osgi/spring-osgi-core
                  commons-logging/commons-logging-api
                  commons-logging/commons-logging]
                 ]
                [org.apache.activemq/activemq-pool "5.5.0"]
                [org.springframework.amqp/spring-rabbit "1.0.0.RC3"
                 :exclusions
                 [commons-logging/commons-logging-api
                  commons-logging/commons-logging
                  org.slf4j/slf4j-api
                  org.slf4j/slf4j-log4j12
                  org.slf4j/jcl-over-slf4j
                  log4j/log4j
                  ]
                 ]

                [org.slf4j/slf4j-api "1.6.1"]
                [org.slf4j/jcl-over-slf4j "1.6.1"]
                [org.slf4j/log4j-over-slf4j "1.6.1"]
                ]
 :repositories {"springsource-milestones" "http://maven.springframework.org/milestone"}
 )