(ns clamq.test.activemq-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer] [clamq.pipes])
 )

(def broker-uri "tcp://localhost:61616")

(deftest producer-consumer-test
  (let [broker (activemq broker-uri)
        received (atom "")
        queue "clamq-test"
        consumer (consumer broker queue false #(reset! received %1))
        producer (producer broker false)
        test-message "producer-consumer-test"]
    (send-to producer queue test-message {})
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest on-failure-test
  (let [broker (activemq broker-uri)
        received (atom "")
        queue "clamq-test"
        dlq "clamq-dlq"
        producer (producer broker true)
        failing-consumer (consumer broker queue true #(throw (RuntimeException. %1)) :on-failure #(send-to producer dlq (:message %1) {}))
        working-consumer (consumer broker dlq true #(reset! received %1))
        test-message "on-failure-test"]
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

(deftest transacted-test
  (let [broker (activemq broker-uri)
        received (atom "")
        queue "clamq-test"
        failing-consumer (consumer broker queue true #(throw (RuntimeException. %1)))
        working-consumer (consumer broker queue true #(reset! received %1))
        producer (producer broker true)
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
  (let [broker (activemq broker-uri)
        received (atom "")
        consumer (consumer broker "pipe2" true #(reset! received %1))
        producer (producer broker true)
        test-pipe (pipe {:from {:connection broker :endpoint "pipe1"} :to {:connection broker :endpoint "pipe2"} :transacted true})
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

(deftest on-failure-pipe-test
  (let [broker (activemq broker-uri)
        dlq "clamq-dlq"
        received (atom "")
        consumer (consumer broker dlq true #(reset! received %1))
        producer (producer broker true)
        test-pipe (pipe {:from {:connection broker :endpoint "pipe1"} :to {:connection broker :endpoint "pipe2"} :transacted true :filter-by #(throw (RuntimeException. %1)) :on-failure #(send-to producer dlq (:message %1) {})})
        test-message "on-failure-pipe-test"]
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
  (let [broker (activemq broker-uri)
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker "pipe2" true #(reset! received1 %1))
        consumer2 (consumer broker "pipe3" true #(reset! received2 %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint "pipe1"} :to [{:connection broker :endpoint "pipe2"} {:connection broker :endpoint "pipe3"}] :transacted true})
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

(deftest on-failure-multi-pipe-test
  (let [broker (activemq broker-uri)
        dlq "clamq-dlq"
        dlq-received (atom "")
        pipe2-received (atom "")
        dlq-consumer (consumer broker dlq true #(reset! dlq-received %1))
        pipe2-consumer (consumer broker "pipe2" true #(reset! pipe2-received %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint "pipe1"} :to [{:connection broker :endpoint "pipe2" :on-failure #(send-to producer dlq (:message %1) {})} {:connection broker :endpoint "pipe3" :filter-by #(throw (RuntimeException. %1)) :on-failure #(send-to producer dlq (:message %1) {})}] :transacted true})
        test-message "on-failure-multi-pipe-test"]
    (start dlq-consumer)
    (start pipe2-consumer)
    (send-to producer "pipe1" test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop pipe2-consumer)
    (stop dlq-consumer)
    (is (= test-message @dlq-received))
    (is (= test-message @pipe2-received))
    )
  )