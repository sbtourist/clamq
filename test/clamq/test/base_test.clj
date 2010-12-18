(ns clamq.test.base-test
 (:import [java.io Serializable])
 (:use [clojure.test] [clamq.connection.activemq] [clamq.consumer] [clamq.producer] [clamq.pipes] [clamq.helpers])
 )

(defn producer-consumer-test [broker]
  (let [received (atom "")
        queue "producer-consumer-test-queue"
        consumer (consumer broker queue #(reset! received %1) :transacted false)
        producer (producer broker :transacted false)
        test-message "producer-consumer-test"]
    (send-to producer queue test-message {})
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-topic-test [broker]
  (let [received (atom "")
        topic "producer-consumer-topic-test-topic"
        consumer (consumer broker topic #(reset! received %1) :transacted false :pubSub true)
        producer (producer broker :transacted false :pubSub true)
        test-message "producer-consumer-topic-test"]
    (start consumer)
    (Thread/sleep 1000)
    (send-to producer topic test-message {})
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-limit-test [broker]
  (let [received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 3
        limit 2
        consumer (consumer broker queue #(do (swap! received inc) %1) :transacted false :limit limit)
        producer (producer broker :transacted false)
        test-message "producer-consumer-limit-test"]
    (loop [i 1] (send-to producer queue test-message {}) (if (< i messages) (recur (inc i))))
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= limit @received))
    )
  )

(defn on-failure-test [broker]
  (let [received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
        producer (producer broker :transacted true)
        failing-consumer (consumer broker queue #(throw (RuntimeException. %1)) :transacted true :on-failure #(send-to producer dlq (:message %1) {}))
        working-consumer (consumer broker dlq #(reset! received %1) :transacted true)
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

(defn transacted-test [broker]
  (let [received (atom "")
        queue "transacted-test-queue"
        failing-consumer (consumer broker queue #(throw (RuntimeException. %1)) :transacted true)
        working-consumer (consumer broker queue #(reset! received %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn pipe-test [broker]
  (let [received (atom "")
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (consumer broker queue2 #(reset! received %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn pipe-topic-test [broker]
  (let [received (atom "")
        topic1 "pipe-topic-test-topic1"
        topic2 "pipe-topic-test-topic2"
        consumer (consumer broker topic2 #(reset! received %1) :transacted true :pubSub true)
        producer (producer broker :transacted true :pubSub true)
        test-pipe (pipe {:from {:connection broker :endpoint topic1 :pubSub true} :to {:connection broker :endpoint topic2 :pubSub true} :transacted true})
        test-message "pipe-topic-test"]
    (start consumer)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer topic1 test-message {})
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-limit-test [broker]
  (let [received (atom 0)
        queue1 "pipe-limit-test-queue1"
        queue2 "pipe-limit-test-queue2"
        messages 3
        limit 2
        consumer (consumer broker queue2 #(do (swap! received inc) %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn on-failure-pipe-test [broker]
  (let [queue1 "on-failure-pipe-test-queue1"
        queue2 "on-failure-pipe-test-queue2"
        dlq "on-failure-pipe-test-dlq"
        received (atom "")
        consumer (consumer broker dlq #(reset! received %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn multi-pipe-test [broker]
  (let [queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker queue2 #(reset! received1 %1) :transacted true)
        consumer2 (consumer broker queue3 #(reset! received2 %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn multi-pipe-topic-test [broker]
  (let [topic1 "multi-pipe-topic-test-topic1"
        topic2 "multi-pipe-topic-test-topic2"
        topic3 "multi-pipe-topic-test-topic3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker topic2 #(reset! received1 %1) :transacted true :pubSub true)
        consumer2 (consumer broker topic3 #(reset! received2 %1) :transacted true :pubSub true)
        producer (producer broker :transacted true :pubSub true)
        test-pipe (multi-pipe {:from {:connection broker :endpoint topic1 :pubSub true} :to [{:connection broker :endpoint topic2 :pubSub true} {:connection broker :endpoint topic3 :pubSub true}] :transacted true})
        test-message "multi-pipe-topic-test"]
    (start consumer1)
    (start consumer2)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer topic1 test-message {})
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(defn multi-pipe-limit-test [broker]
  (let [queue1 "multi-pipe-limit-test-queue1"
        queue2 "multi-pipe-limit-test-queue2"
        queue3 "multi-pipe-limit-test-queue3"
        received1 (atom 0)
        received2 (atom 0)
        messages 3
        limit 2
        consumer1 (consumer broker queue2 #(do (swap! received1 inc) %1) :transacted true)
        consumer2 (consumer broker queue3 #(do (swap! received2 inc) %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn multi-pipe-with-discard-test [broker]
  (let [queue1 "multi-pipe-with-discard-test-queue1"
        queue2 "multi-pipe-with-discard-test-queue2"
        queue3 "multi-pipe-with-discard-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer broker queue2 #(reset! received1 %1) :transacted true)
        consumer2 (consumer broker queue3 #(reset! received2 %1) :transacted true)
        producer (producer broker :transacted true)
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

(defn on-failure-multi-pipe-test [broker]
  (let [queue1 "on-failure-multi-pipe-test-queue1"
        queue2 "on-failure-multi-pipe-test-queue2"
        queue3 "on-failure-multi-pipe-test-queue3"
        dlq "on-failure-multi-pipe-test-dlq"
        pipe2-received (atom "")
        dlq-received (atom "")
        pipe2-consumer (consumer broker queue2 #(reset! pipe2-received %1) :transacted true)
        dlq-consumer (consumer broker dlq #(reset! dlq-received %1) :transacted true)
        producer (producer broker :transacted true)
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