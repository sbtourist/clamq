(defproject clamq "0.4.dm.1"
 :description "Clojure APIs for Message Queues"
 :dev-dependencies
    [[lein-sub "0.1.1"]]
  :sub
    ["clamq-core"
     "clamq-jms"
     "clamq-activemq"
     "clamq-rabbitmq"
     "clamq-runner"]

  :plugins [[lein-sub "0.3.0"]]
  :repositories {"snapshots" {:url "http://10.251.76.73:8081/nexus/content/repositories/snapshots"
                              :username "admin" :password "admin123"}
                 "releases" {:url "http://10.251.76.73:8081/nexus/content/repositories/releases"
                             :username "admin" :password "admin123" }
                 "thirdparty" {:url "http://10.251.76.73:8081/nexus/content/repositories/thirdparty"}
                 "sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"})
