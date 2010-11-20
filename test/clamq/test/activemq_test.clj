(ns clamq.test.activemq-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer] [clamq.pipes])
 )

(def broker-uri "tcp://localhost:61616")

(deftest producer-consumer-test
  (let [broker (activemq broker-uri {})
        received (atom "")
        queue "clamq-test"
        consumer (consumer broker queue #(reset! received %1) {:transacted false})
        producer (producer broker {:transacted false})
        test-message "producer-consumer-test"]
    (send-to producer queue test-message {})
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest transacted-test
  (let [broker (activemq broker-uri {})
        received (atom "")
        queue "clamq-test"
        failing-consumer (consumer broker queue #(throw (RuntimeException. %1)) {:transacted true})
        working-consumer (consumer broker queue #(reset! received %1) {:transacted true})
        producer (producer broker {:transacted true})
        test-message "transacted-test"]
    (send-to producer queue test-message {})
    (start failing-consumer)
    (Thread/sleep 1000)
    (stop failing-consumer)
    (start working-consumer)
    (Thread/sleep 1000)
    (stop working-consumer)
    (is (= test-message @received))
    )
  )

(deftest pipe-test
  (let [broker (activemq broker-uri {})
        received (atom "")
        consumer (consumer broker "pipe2" #(reset! received %1) {:transacted true})
        producer (producer broker {:transacted true})
        test-pipe (pipe {:from {:connection broker :source "pipe1"} :to {:connection broker :destination "pipe2"} :transacted true})
        test-message "pipe-test"]
    (start consumer)
    (send-to producer "pipe1" test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest multi-pipe-test
  (let [broker (activemq broker-uri {})
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker "pipe2" #(reset! received1 %1) {:transacted true})
        consumer2 (consumer broker "pipe3" #(reset! received2 %1) {:transacted true})
        producer (producer broker {:transacted true})
        test-pipe (multi-pipe {:from {:connection broker :source "pipe1"} :to [{:connection broker :destination "pipe2"} {:connection broker :destination "pipe3"}] :transacted true})
        test-message "multi-pipe-test"]
    (start consumer1)
    (start consumer2)
    (send-to producer "pipe1" test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )
