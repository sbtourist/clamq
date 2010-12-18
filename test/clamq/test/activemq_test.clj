(ns clamq.test.activemq-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer] [clamq.pipes] [clamq.helpers] [clamq.test.base-test])
 )

(defn setup-broker-fixture [test-fn]
  (let [broker (activemq "tcp://localhost:61616")]
    (test-fn broker)
    )
  )

(deftest test-suite
  (setup-broker-fixture producer-consumer-test)
  (setup-broker-fixture producer-consumer-topic-test)
  (setup-broker-fixture producer-consumer-limit-test)
  (setup-broker-fixture on-failure-test)
  (setup-broker-fixture transacted-test)
  (setup-broker-fixture pipe-test)
  (setup-broker-fixture pipe-topic-test)
  (setup-broker-fixture pipe-limit-test)
  (setup-broker-fixture on-failure-pipe-test)
  (setup-broker-fixture multi-pipe-test)
  (setup-broker-fixture multi-pipe-topic-test)
  (setup-broker-fixture multi-pipe-limit-test)
  (setup-broker-fixture multi-pipe-with-discard-test)
  (setup-broker-fixture on-failure-multi-pipe-test)
  )

(defn test-ns-hook []
  (test-suite)
  )