(defproject clamq/clamq-activemq "0.4.dm.1"
 :description "Clojure APIs for Message Queues"
 :dependencies [[clamq/clamq-jms "0.4.dm.1"]
                [org.slf4j/slf4j-api "1.6.1"]
                [org.apache.activemq/activemq-core "5.5.0"]]
 :dev-dependencies [[org.slf4j/slf4j-simple "1.6.1"]]
 :repositories {"snapshots" {:url "http://10.251.76.73:8081/nexus/content/repositories/snapshots"
                             :username "admin" :password "admin123"}
                "releases" {:url "http://10.251.76.73:8081/nexus/content/repositories/releases"
                            :username "admin" :password "admin123" }
                "thirdparty" {:url "http://10.251.76.73:8081/nexus/content/repositories/thirdparty"}
                "sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"})
