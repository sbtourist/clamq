(ns clamq.test.base-jms-test
 (:require
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   [clamq.protocol.pipe :as pipe]
   [clamq.pipes :as pipes]
   )
 (:use 
   [clojure.test]
   )
 )

(defn producer-consumer-test [connection]
  (let [received (atom "")
        queue "producer-consumer-test-queue"
        consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (connection/producer connection)
        test-message "producer-consumer-test"]
    (producer/publish producer queue test-message)
    (consumer/start consumer)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-topic-test [connection]
  (let [received (atom "")
        topic "producer-consumer-topic-test-topic"
        consumer (connection/consumer connection {:endpoint topic :on-message #(reset! received %1) :transacted false :pubSub true})
        producer (connection/producer connection {:pubSub true})
        test-message "producer-consumer-topic-test"]
    (consumer/start consumer)
    (Thread/sleep 1000)
    (producer/publish producer topic test-message)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= test-message @received))
    )
  )

(defn producer-consumer-limit-test [connection]
  (let [received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 5
        limit 2
        consumer (connection/consumer connection {:endpoint queue :on-message #(do (swap! received inc) %1) :transacted true :limit limit})
        producer (connection/producer connection)
        test-message "producer-consumer-limit-test"]
    (loop [i 1] (producer/publish producer queue test-message) (if (< i messages) (recur (inc i))))
    (consumer/start consumer)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= limit @received))
    )
  )

(defn on-failure-test [connection]
  (let [received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
        producer (connection/producer connection)
        failing-consumer (connection/consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true :on-failure #(producer/publish producer dlq (:message %1) {})})
        working-consumer (connection/consumer connection {:endpoint dlq :on-message #(reset! received %1) :transacted true})
        test-message "on-failure-test"]
    (producer/publish producer queue test-message)
    (consumer/start failing-consumer)
    (Thread/sleep 1000)
    (consumer/close failing-consumer)
    (consumer/start working-consumer)
    (Thread/sleep 1000)
    (consumer/close working-consumer)
    (is (= test-message @received))
    )
  )

(defn transacted-test [connection]
  (let [received (atom "")
        queue "transacted-test-queue"
        failing-consumer (connection/consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true})
        working-consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted true})
        producer (connection/producer connection)
        test-message "transacted-test"]
    (producer/publish producer queue test-message)
    (consumer/start failing-consumer)
    (Thread/sleep 1000)
    (consumer/close failing-consumer)
    (consumer/start working-consumer)
    (Thread/sleep 1000)
    (consumer/close working-consumer)
    (is (= test-message @received))
    )
  )

(defn seqable-consumer-test [connection]
  (let [queue "seqable-consumer-test-queue"
        consumer (connection/seqable connection {:endpoint queue :timeout 1000})
        producer (connection/producer connection)
        test-message1 "seqable-consumer-test1"
        test-message2 "seqable-consumer-test2"]
    (producer/publish producer queue test-message1)
    (producer/publish producer queue test-message2)
    (let [result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
      (is (= test-message1 (result 0)))
      (is (= test-message2 (result 1)))
      )
    (let [result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
      (is (empty? result))
      )
    (seqable/close consumer)
    )
  )

(defn seqable-consumer-close-test [connection]
  (let [queue "seqable-consumer-close-test-queue"
        consumer (connection/seqable connection {:endpoint queue :timeout 1000})
        producer (connection/producer connection)
        test-message1 "seqable-consumer-close-test1"
        test-message2 "seqable-consumer-close-test2"]
    (producer/publish producer queue test-message1)
    (producer/publish producer queue test-message2)
    (reduce into [] (map #(do [%1]) (seqable/mseq consumer)))
    (seqable/close consumer)
    (let [consumer (connection/seqable connection {:endpoint queue :timeout 1000})
          result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
      (is (= test-message1 (result 0)))
      (is (= test-message2 (result 1)))
      (seqable/close consumer)
      )
    )
  )

(defn pipe-test [connection]
  (let [received (atom "")
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (connection/consumer connection {:endpoint queue2 :on-message #(reset! received %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint queue2} :transacted true})
        test-message "pipe-test"]
    (consumer/start consumer)
    (producer/publish producer queue1 test-message)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-topic-test [connection]
  (let [received (atom "")
        topic1 "pipe-topic-test-topic1"
        topic2 "pipe-topic-test-topic2"
        consumer (connection/consumer connection {:endpoint topic2 :on-message #(reset! received %1) :transacted true :pubSub true})
        producer (connection/producer connection {:pubSub true})
        test-pipe (pipes/pipe {:from {:connection connection :endpoint topic1 :pubSub true} :to {:connection connection :endpoint topic2 :pubSub true} :transacted true})
        test-message "pipe-topic-test"]
    (consumer/start consumer)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (producer/publish producer topic1 test-message)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer)
    (is (= test-message @received))
    )
  )

(defn pipe-limit-test [connection]
  (let [received (atom 0)
        queue1 "pipe-limit-test-queue1"
        queue2 "pipe-limit-test-queue2"
        messages 5
        limit 2
        consumer (connection/consumer connection {:endpoint queue2 :on-message #(do (swap! received inc) %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint queue2} :transacted true :limit limit})
        test-message "pipe-limit-test"]
    (consumer/start consumer)
    (loop [i 1] (producer/publish producer queue1 test-message) (if (< i messages) (recur (inc i))))
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer)
    (is (= limit @received))
    )
  )

(defn multi-pipe-test [connection]
  (let [queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint queue2} {:connection connection :endpoint queue3}] :transacted true})
        test-message "multi-pipe-test"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (producer/publish producer queue1 test-message)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
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
        consumer1 (connection/consumer connection {:endpoint topic2 :on-message #(reset! received1 %1) :transacted true :pubSub true})
        consumer2 (connection/consumer connection {:endpoint topic3 :on-message #(reset! received2 %1) :transacted true :pubSub true})
        producer (connection/producer connection {:pubSub true})
        test-pipe (pipes/multi-pipe {:from {:connection connection :endpoint topic1 :pubSub true} :to [{:connection connection :endpoint topic2 :pubSub true} {:connection connection :endpoint topic3 :pubSub true}] :transacted true})
        test-message "multi-pipe-topic-test"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (producer/publish producer topic1 test-message)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
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
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(do (swap! received1 inc) %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(do (swap! received2 inc) %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint queue2} {:connection connection :endpoint queue3}] :transacted true :limit limit})
        test-message "multi-pipe-limit-test"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (loop [i 1] (producer/publish producer queue1 test-message) (if (< i messages) (recur (inc i))))
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
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
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (connection/producer connection)
        router-fn #(if (= "router-pipe-test2" %1) [{:connection connection :endpoint queue2 :message %1}] [{:connection connection :endpoint queue3 :message %1}])
        test-pipe (pipes/router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true})
        test-message1 "router-pipe-test2"
        test-message2 "router-pipe-test3"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (producer/publish producer queue1 test-message1)
    (producer/publish producer queue1 test-message2)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
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
        consumer1 (connection/consumer connection {:endpoint topic2 :on-message #(reset! received1 %1) :transacted true :pubSub true})
        consumer2 (connection/consumer connection {:endpoint topic3 :on-message #(reset! received2 %1) :transacted true :pubSub true})
        producer (connection/producer connection {:pubSub true})
        router-fn #(if (= "router-pipe-topic-test-topic2" %1) [{:connection connection :endpoint topic2 :message %1 :pubSub true}] [{:connection connection :endpoint topic3 :message %1 :pubSub true}])
        test-pipe (pipes/router-pipe {:from {:connection connection :endpoint topic1 :pubSub true} :route-with router-fn :transacted true})
        test-message1 "router-pipe-topic-test-topic2"
        test-message2 "router-pipe-topic-test-topic3"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (producer/publish producer topic1 test-message1)
    (producer/publish producer topic1 test-message2)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
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
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(do (swap! received1 inc) %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(do (swap! received2 inc) %1) :transacted true})
        producer (connection/producer connection)
        limit 2
        router-fn #(if (= "router-pipe-limit-test2" %1) [{:connection connection :endpoint queue2 :message %1}] [{:connection connection :endpoint queue3 :message %1}])
        test-pipe (pipes/router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true :limit limit})
        test-message1 "router-pipe-limit-test2"
        test-message2 "router-pipe-limit-test3"]
    (consumer/start consumer1)
    (consumer/start consumer2)
    (producer/publish producer queue1 test-message1)
    (producer/publish producer queue1 test-message2)
    (producer/publish producer queue1 test-message1)
    (producer/publish producer queue1 test-message2)
    (pipe/open test-pipe)
    (Thread/sleep 1000)
    (pipe/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
    (is (= 1 @received1))
    (is (= 1 @received2))
    )
  )