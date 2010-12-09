(ns clamq.test.activemq-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer] [clamq.pipes] [clamq.helpers])
 )

(def broker-uri "tcp://localhost:61616")

(deftest producer-consumer-test
  (let [broker (activemq broker-uri)
        received (atom "")
        queue "producer-consumer-test-queue"
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

(deftest producer-consumer-limit-test
  (let [broker (activemq broker-uri)
        received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 3
        limit 2
        consumer (consumer broker queue false #(do (swap! received inc) %1) :limit limit)
        producer (producer broker false)
        test-message "producer-consumer-limit-test"]
    (loop [i 1] (send-to producer queue test-message {}) (if (< i messages) (recur (inc i))))
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= limit @received))
    )
  )

(deftest on-failure-test
  (let [broker (activemq broker-uri)
        received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
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
        queue "transacted-test-queue"
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
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (consumer broker queue2 true #(reset! received %1))
        producer (producer broker true)
        test-pipe (pipe {:from {:connection broker :endpoint queue1} :to {:connection broker :endpoint queue2} :transacted true})
        test-message "pipe-test"]
    (start consumer)
    (send-to producer queue1 test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest pipe-limit-test
  (let [broker (activemq broker-uri)
        received (atom 0)
        queue1 "pipe-limit-test-queue1"
        queue2 "pipe-limit-test-queue2"
        messages 3
        limit 2
        consumer (consumer broker queue2 true #(do (swap! received inc) %1))
        producer (producer broker true)
        test-pipe (pipe {:from {:connection broker :endpoint queue1} :to {:connection broker :endpoint queue2} :transacted true :limit limit})
        test-message "pipe-limit-test"]
    (start consumer)
    (loop [i 1] (send-to producer queue1 test-message {}) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= limit @received))
    )
  )

(deftest on-failure-pipe-test
  (let [broker (activemq broker-uri)
        queue1 "on-failure-pipe-test-queue1"
        queue2 "on-failure-pipe-test-queue2"
        dlq "on-failure-pipe-test-dlq"
        received (atom "")
        consumer (consumer broker dlq true #(reset! received %1))
        producer (producer broker true)
        test-pipe (pipe {:from {:connection broker :endpoint queue1} :to {:connection broker :endpoint queue2} :transacted true :filter-by #(throw (RuntimeException. %1)) :on-failure #(send-to producer dlq (:message %1) {})})
        test-message "on-failure-pipe-test"]
    (start consumer)
    (send-to producer queue1 test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest multi-pipe-test
  (let [broker (activemq broker-uri)
        queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker queue2 true #(reset! received1 %1))
        consumer2 (consumer broker queue3 true #(reset! received2 %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint queue1} :to [{:connection broker :endpoint queue2} {:connection broker :endpoint queue3}] :transacted true})
        test-message "multi-pipe-test"]
    (start consumer1)
    (start consumer2)
    (send-to producer queue1 test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(deftest multi-pipe-limit-test
  (let [broker (activemq broker-uri)
        queue1 "multi-pipe-limit-test-queue1"
        queue2 "multi-pipe-limit-test-queue2"
        queue3 "multi-pipe-limit-test-queue3"
        received1 (atom 0)
        received2 (atom 0)
        messages 3
        limit 2
        consumer1 (consumer broker queue2 true #(do (swap! received1 inc) %1))
        consumer2 (consumer broker queue3 true #(do (swap! received2 inc) %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint queue1} :to [{:connection broker :endpoint queue2} {:connection broker :endpoint queue3}] :transacted true :limit limit})
        test-message "multi-pipe-limit-test"]
    (start consumer1)
    (start consumer2)
    (loop [i 1] (send-to producer queue1 test-message {}) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= limit @received1))
    (is (= limit @received2))
    )
  )

(deftest multi-pipe-with-discard-test
  (let [broker (activemq broker-uri)
        queue1 "multi-pipe-with-discard-test-queue1"
        queue2 "multi-pipe-with-discard-test-queue2"
        queue3 "multi-pipe-with-discard-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker queue2 true #(reset! received1 %1))
        consumer2 (consumer broker queue3 true #(reset! received2 %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint queue1} :to [{:connection broker :endpoint queue2} {:connection broker :endpoint queue3 :filter-by discard}] :transacted true})
        test-message "multi-pipe-test"]
    (start consumer1)
    (start consumer2)
    (send-to producer queue1 test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= "" @received2))
    )
  )

(deftest on-failure-multi-pipe-test
  (let [broker (activemq broker-uri)
        queue1 "on-failure-multi-pipe-test-queue1"
        queue2 "on-failure-multi-pipe-test-queue2"
        queue3 "on-failure-multi-pipe-test-queue3"
        dlq "on-failure-multi-pipe-test-dlq"
        pipe2-received (atom "")
        dlq-received (atom "")
        pipe2-consumer (consumer broker queue2 true #(reset! pipe2-received %1))
        dlq-consumer (consumer broker dlq true #(reset! dlq-received %1))
        producer (producer broker true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint queue1} :to [{:connection broker :endpoint queue2} {:connection broker :endpoint queue3 :filter-by #(throw (RuntimeException. %1)) :on-failure #(send-to producer dlq (:message %1) {})}] :transacted true})
        test-message "on-failure-multi-pipe-test"]
    (start dlq-consumer)
    (start pipe2-consumer)
    (send-to producer queue1 test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop pipe2-consumer)
    (stop dlq-consumer)
    (is (= test-message @pipe2-received))
    (is (= test-message @dlq-received))
    )
  )