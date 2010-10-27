(ns clamq.test.activemq-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer])
 )

(def broker-uri "tcp://localhost:61616")

(deftest producer-consumer-test
  (let [broker (activemq broker-uri {:ignored "ignored"})
        queue "clamq-test"
        consumer (make-consumer broker queue #(println (str "Message: " (.toString %1))) true)
        producer (make-producer broker true)]
    (send-to producer queue "Text Message" {"JMSXGroupID" "text"})
    (send-to producer queue (proxy [Serializable] [] (toString [] (str "Object Message"))) {"JMSXGroupID" "obj"})
    (start consumer)
    (Thread/sleep 3000)
    (stop consumer)
    )
  )
