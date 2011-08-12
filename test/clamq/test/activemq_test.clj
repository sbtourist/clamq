(ns clamq.test.activemq-test
 (:use [clojure.test] [clamq.test.base-jms-test] [clamq.jms] [clamq.activemq])
 )

(defn setup-connection-and-test [test-fn]
  (with-open [connection (activemq-connection "tcp://localhost:61616")]
    (test-fn connection)
    )
  )

(deftest activemq-test-suite []
  (setup-connection-and-test producer-consumer-test)
  (setup-connection-and-test producer-consumer-topic-test)
  (setup-connection-and-test producer-consumer-limit-test)
  (setup-connection-and-test on-failure-test)
  (setup-connection-and-test transacted-test)
  (setup-connection-and-test seqable-consumer-test)
  (setup-connection-and-test seqable-consumer-close-test)
  (setup-connection-and-test pipe-test)
  (setup-connection-and-test pipe-topic-test)
  (setup-connection-and-test pipe-limit-test)
  (setup-connection-and-test multi-pipe-test)
  (setup-connection-and-test multi-pipe-topic-test)
  (setup-connection-and-test multi-pipe-limit-test)
  (setup-connection-and-test router-pipe-test)
  (setup-connection-and-test router-pipe-topic-test)
  (setup-connection-and-test router-pipe-limit-test)
  )