(ns clamq.test.base-jms-test
 (:use [clojure.test] [clamq.protocol] [clamq.jms] [clamq.pipes])
 )

(defn producer-consumer-test [connection]
  (let [received (atom "")
        queue "producer-consumer-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-test"]
    (send-to producer queue test-message)
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-topic-test [connection]
  (let [received (atom "")
        topic "producer-consumer-topic-test-topic"
        consumer (consumer connection {:endpoint topic :on-message #(reset! received %1) :transacted false :pubSub true})
        producer (producer connection {:pubSub true})
        test-message "producer-consumer-topic-test"]
    (start consumer)
    (Thread/sleep 1000)
    (send-to producer topic test-message)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-limit-test [connection]
  (let [received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 5
        limit 2
        consumer (consumer connection {:endpoint queue :on-message #(do (swap! received inc) %1) :transacted true :limit limit})
        producer (producer connection)
        test-message "producer-consumer-limit-test"]
    (loop [i 1] (send-to producer queue test-message) (if (< i messages) (recur (inc i))))
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= limit @received))
    )
  )

(defn on-failure-test [connection]
  (let [received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
        producer (producer connection)
        failing-consumer (consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true :on-failure #(send-to producer dlq (:message %1) {})})
        working-consumer (consumer connection {:endpoint dlq :on-message #(reset! received %1) :transacted true})
        test-message "on-failure-test"]
    (send-to producer queue test-message)
    (start failing-consumer)
    (Thread/sleep 1000)
    (stop failing-consumer)
    (start working-consumer)
    (Thread/sleep 1000)
    (stop working-consumer)
    (is (= test-message @received))
    )
  )

(defn transacted-test [connection]
  (let [received (atom "")
        queue "transacted-test-queue"
        failing-consumer (consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true})
        working-consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted true})
        producer (producer connection)
        test-message "transacted-test"]
    (send-to producer queue test-message)
    (start failing-consumer)
    (Thread/sleep 1000)
    (stop failing-consumer)
    (start working-consumer)
    (Thread/sleep 1000)
    (stop working-consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-test [connection]
  (let [received (atom "")
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (consumer connection {:endpoint queue2 :on-message #(reset! received %1) :transacted true})
        producer (producer connection)
        test-pipe (pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint queue2} :transacted true})
        test-message "pipe-test"]
    (start consumer)
    (send-to producer queue1 test-message)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-topic-test [connection]
  (let [received (atom "")
        topic1 "pipe-topic-test-topic1"
        topic2 "pipe-topic-test-topic2"
        consumer (consumer connection {:endpoint topic2 :on-message #(reset! received %1) :transacted true :pubSub true})
        producer (producer connection {:pubSub true})
        test-pipe (pipe {:from {:connection connection :endpoint topic1 :pubSub true} :to {:connection connection :endpoint topic2 :pubSub true} :transacted true})
        test-message "pipe-topic-test"]
    (start consumer)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer topic1 test-message)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-limit-test [connection]
  (let [received (atom 0)
        queue1 "pipe-limit-test-queue1"
        queue2 "pipe-limit-test-queue2"
        messages 5
        limit 2
        consumer (consumer connection {:endpoint queue2 :on-message #(do (swap! received inc) %1) :transacted true})
        producer (producer connection)
        test-pipe (pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint queue2} :transacted true :limit limit})
        test-message "pipe-limit-test"]
    (start consumer)
    (loop [i 1] (send-to producer queue1 test-message) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= limit @received))
    )
  )

(defn multi-pipe-test [connection]
  (let [queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (producer connection)
        test-pipe (multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint queue2} {:connection connection :endpoint queue3}] :transacted true})
        test-message "multi-pipe-test"]
    (start consumer1)
    (start consumer2)
    (send-to producer queue1 test-message)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(defn multi-pipe-topic-test [connection]
  (let [topic1 "multi-pipe-topic-test-topic1"
        topic2 "multi-pipe-topic-test-topic2"
        topic3 "multi-pipe-topic-test-topic3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint topic2 :on-message #(reset! received1 %1) :transacted true :pubSub true})
        consumer2 (consumer connection {:endpoint topic3 :on-message #(reset! received2 %1) :transacted true :pubSub true})
        producer (producer connection {:pubSub true})
        test-pipe (multi-pipe {:from {:connection connection :endpoint topic1 :pubSub true} :to [{:connection connection :endpoint topic2 :pubSub true} {:connection connection :endpoint topic3 :pubSub true}] :transacted true})
        test-message "multi-pipe-topic-test"]
    (start consumer1)
    (start consumer2)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer topic1 test-message)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(defn multi-pipe-limit-test [connection]
  (let [queue1 "multi-pipe-limit-test-queue1"
        queue2 "multi-pipe-limit-test-queue2"
        queue3 "multi-pipe-limit-test-queue3"
        received1 (atom 0)
        received2 (atom 0)
        messages 5
        limit 2
        consumer1 (consumer connection {:endpoint queue2 :on-message #(do (swap! received1 inc) %1) :transacted true})
        consumer2 (consumer connection {:endpoint queue3 :on-message #(do (swap! received2 inc) %1) :transacted true})
        producer (producer connection)
        test-pipe (multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint queue2} {:connection connection :endpoint queue3}] :transacted true :limit limit})
        test-message "multi-pipe-limit-test"]
    (start consumer1)
    (start consumer2)
    (loop [i 1] (send-to producer queue1 test-message) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= limit @received1))
    (is (= limit @received2))
    )
  )

(defn router-pipe-test [connection]
  (let [queue1 "router-pipe-test-queue1"
        queue2 "router-pipe-test-queue2"
        queue3 "router-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (producer connection)
        router-fn #(if (= "router-pipe-test2" %1) [{:connection connection :endpoint queue2 :message %1}] [{:connection connection :endpoint queue3 :message %1}])
        test-pipe (router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true})
        test-message1 "router-pipe-test2"
        test-message2 "router-pipe-test3"]
    (start consumer1)
    (start consumer2)
    (send-to producer queue1 test-message1)
    (send-to producer queue1 test-message2)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message1 @received1))
    (is (= test-message2 @received2))
    )
  )

(defn router-pipe-topic-test [connection]
  (let [topic1 "router-pipe-topic-test-topic1"
        topic2 "router-pipe-topic-test-topic2"
        topic3 "router-pipe-topic-test-topic3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint topic2 :on-message #(reset! received1 %1) :transacted true :pubSub true})
        consumer2 (consumer connection {:endpoint topic3 :on-message #(reset! received2 %1) :transacted true :pubSub true})
        producer (producer connection {:pubSub true})
        router-fn #(if (= "router-pipe-topic-test-topic2" %1) [{:connection connection :endpoint topic2 :message %1 :pubSub true}] [{:connection connection :endpoint topic3 :message %1 :pubSub true}])
        test-pipe (router-pipe {:from {:connection connection :endpoint topic1 :pubSub true} :route-with router-fn :transacted true})
        test-message1 "router-pipe-topic-test-topic2"
        test-message2 "router-pipe-topic-test-topic3"]
    (start consumer1)
    (start consumer2)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer topic1 test-message1)
    (send-to producer topic1 test-message2)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message1 @received1))
    (is (= test-message2 @received2))
    )
  )

(defn router-pipe-limit-test [connection]
  (let [queue1 "router-pipe-limit-test-queue1"
        queue2 "router-pipe-limit-test-queue2"
        queue3 "router-pipe-limit-test-queue3"
        received1 (atom 0)
        received2 (atom 0)
        consumer1 (consumer connection {:endpoint queue2 :on-message #(do (swap! received1 inc) %1) :transacted true})
        consumer2 (consumer connection {:endpoint queue3 :on-message #(do (swap! received2 inc) %1) :transacted true})
        producer (producer connection)
        limit 2
        router-fn #(if (= "router-pipe-limit-test2" %1) [{:connection connection :endpoint queue2 :message %1}] [{:connection connection :endpoint queue3 :message %1}])
        test-pipe (router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true :limit limit})
        test-message1 "router-pipe-limit-test2"
        test-message2 "router-pipe-limit-test3"]
    (start consumer1)
    (start consumer2)
    (send-to producer queue1 test-message1)
    (send-to producer queue1 test-message2)
    (send-to producer queue1 test-message1)
    (send-to producer queue1 test-message2)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= 1 @received1))
    (is (= 1 @received2))
    )
  )