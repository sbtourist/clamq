(defproject clamq "0.3-SNAPSHOT"
 :description "Clojure APIs for Message Queues"
 :dependencies [
                [org.clojure/clojure "1.2.1"]
                [org.clojure/clojure-contrib "1.2.0"]

                [org.springframework/spring-context "3.0.5.RELEASE"]
                [org.springframework/spring-jms "3.0.5.RELEASE"]
                [org.springframework.amqp/spring-amqp "1.0.0.RC3"]

                [org.slf4j/slf4j-api "1.6.1"]
                ]
 :dev-dependencies [
                [org.apache.activemq/activemq-core "5.5.0"]
                                        
                [org.springframework.amqp/spring-rabbit "1.0.0.RC3"]
                ]
 :repositories {"springsource-milestones" "http://maven.springframework.org/milestone"}
 )