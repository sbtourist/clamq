(ns clamq.test.rabbitmq-test
 (:require
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.seqable :as seqable]
   [clamq.protocol.producer :as producer]
   [clamq.pipes :as pipes])
 (:use [clojure.test] 
   [clamq.rabbitmq])
 (:import [org.springframework.amqp.core BindingBuilder Exchange Queue DirectExchange FanoutExchange TopicExchange] 
   [org.springframework.amqp.rabbit.connection CachingConnectionFactory] 
   [org.springframework.amqp.rabbit.core RabbitAdmin]))

(defonce admin (RabbitAdmin. (CachingConnectionFactory. "localhost")))

(defn- declareQueue [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (DirectExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (DirectExchange. queue)) (with queue))))

(defn- declareTopic [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (TopicExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (TopicExchange. queue)) (with queue))))

(defn- declareFanout [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (FanoutExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (FanoutExchange. queue)))))

(defn- producer-consumer-direct-test [connection]
  (let [received (atom "")
        queue "producer-consumer-direct-test-queue"
        consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (connection/producer connection)
        test-message "producer-consumer-test"]
    (declareQueue queue)
    (producer/publish producer {:exchange queue :routing-key queue} test-message)
    (consumer/start consumer)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= test-message @received))))

(defn- producer-consumer-topic-test [connection]
  (let [received (atom "")
        queue "producer-consumer-topic-test-queue"
        consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (connection/producer connection)
        test-message "producer-consumer-topic-test"]
    (declareTopic queue)
    (consumer/start consumer)
    (Thread/sleep 1000)
    (producer/publish producer {:exchange queue :routing-key queue} test-message)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= test-message @received))))

(defn- producer-consumer-fanout-test [connection]
  (let [received (atom "")
        queue "producer-consumer-fanout-test-queue"
        consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (connection/producer connection)
        test-message "producer-consumer-fanout-test"]
    (declareFanout queue)
    (consumer/start consumer)
    (Thread/sleep 1000)
    (producer/publish producer {:exchange queue} test-message)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= test-message @received))))

(defn- producer-consumer-limit-test [connection]
  (let [received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 5
        limit 2
        consumer (connection/consumer connection {:endpoint queue :on-message #(do (swap! received inc) %1) :transacted true :limit limit})
        producer (connection/producer connection)
        test-message "producer-consumer-limit-test"]
    (declareQueue queue)
    (loop [i 1] (producer/publish producer {:routing-key queue} test-message) (if (< i messages) (recur (inc i))))
    (consumer/start consumer)
    (Thread/sleep 1000)
    (consumer/close consumer)
    (is (= limit @received))))

(defn- on-failure-test [connection]
  (let [received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
        producer (connection/producer connection)
        failing-consumer (connection/consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted false :on-failure #(producer/publish producer {:exchange dlq :routing-key dlq} (:message %1) {})})
        working-consumer (connection/consumer connection {:endpoint dlq :on-message #(reset! received %1) :transacted true})
        test-message "on-failure-test"]
    (declareQueue queue)
    (declareQueue dlq)
    (producer/publish producer {:exchange queue :routing-key queue} test-message)
    (consumer/start failing-consumer)
    (Thread/sleep 1000)
    (consumer/close failing-consumer)
    (Thread/sleep 1000)
    (consumer/start working-consumer)
    (Thread/sleep 1000)
    (consumer/close working-consumer)
    (is (= test-message @received))))

(defn- transacted-test [connection]
  (let [received (atom "")
        queue "transacted-test-queue"
        failing-consumer (connection/consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true})
        working-consumer (connection/consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted true})
        producer (connection/producer connection)
        test-message "transacted-test"]
    (declareQueue queue)
    (producer/publish producer {:exchange queue :routing-key queue} test-message)
    (consumer/start failing-consumer)
    (Thread/sleep 1000)
    (consumer/close failing-consumer)
    (Thread/sleep 1000)
    (consumer/start working-consumer)
    (Thread/sleep 1000)
    (consumer/close working-consumer)
    (is (= test-message @received))))

(defn- seqable-consumer-test [connection]
  (let [queue "seqable-consumer-test-queue"
        producer (connection/producer connection)
        test-message1 "seqable-consumer-test1"
        test-message2 "seqable-consumer-test2"]
    (declareQueue queue)
    (producer/publish producer {:exchange queue :routing-key queue} test-message1)
    (producer/publish producer {:exchange queue :routing-key queue} test-message2)
    (let [consumer (connection/seqable connection {:endpoint queue :timeout 1000})
          result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
      (is (= test-message1 (result 0)))
      (is (= test-message2 (result 1)))
      (let [result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
        (is (empty? result)))
      (seqable/close consumer))))

(defn- seqable-consumer-close-test [connection]
  (let [queue "seqable-consumer-close-test-queue"
        producer (connection/producer connection)
        test-message1 "seqable-consumer-close-test1"
        test-message2 "seqable-consumer-close-test2"]
    (declareQueue queue)
    (producer/publish producer {:exchange queue :routing-key queue} test-message1)
    (producer/publish producer {:exchange queue :routing-key queue} test-message2)
    (let [consumer (connection/seqable connection {:endpoint queue :timeout 1000})]
      (reduce into [] (map #(do [%1]) (seqable/mseq consumer)))
      (seqable/close consumer)
      (let [consumer (connection/seqable connection {:endpoint queue :timeout 1000})
            result (reduce into [] (map #(do (seqable/ack consumer) [%1]) (seqable/mseq consumer)))]
        (is (= test-message1 (result 0)))
        (is (= test-message2 (result 1)))
        (seqable/close consumer)))))

(defn- pipe-test [connection]
  (let [received (atom "")
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (connection/consumer connection {:endpoint queue2 :on-message #(reset! received %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/single-pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint {:exchange queue2 :routing-key queue2}} :transacted true})
        test-message "pipe-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (consumer/start consumer)
    (producer/publish producer {:exchange queue1 :routing-key queue1} test-message)
    (pipes/open test-pipe)
    (Thread/sleep 1000)
    (pipes/close test-pipe)
    (consumer/close consumer)
    (is (= test-message @received))))

(defn- multi-pipe-test [connection]
  (let [queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (connection/producer connection)
        test-pipe (pipes/multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint {:exchange queue2 :routing-key queue2}} {:connection connection :endpoint {:exchange queue3 :routing-key queue3}}] :transacted true})
        test-message "multi-pipe-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (declareQueue queue3)
    (consumer/start consumer1)
    (consumer/start consumer2)
    (producer/publish producer {:exchange queue1 :routing-key queue1} test-message)
    (pipes/open test-pipe)
    (Thread/sleep 1000)
    (pipes/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))))

(defn- router-pipe-test [connection]
  (let [queue1 "router-pipe-test-queue1"
        queue2 "router-pipe-test-queue2"
        queue3 "router-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (connection/consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (connection/consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (connection/producer connection)
        router-fn #(if (= "router-pipe-test2" %1) [{:connection connection :endpoint {:exchange queue2 :routing-key queue2} :message %1}] [{:connection connection :endpoint {:exchange queue3 :routing-key queue3} :message %1}])
        test-pipe (pipes/router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true})
        test-message1 "router-pipe-test2"
        test-message2 "router-pipe-test3"]
    (declareQueue queue1)
    (declareQueue queue2)
    (declareQueue queue3)
    (consumer/start consumer1)
    (consumer/start consumer2)
    (producer/publish producer {:exchange queue1 :routing-key queue1} test-message1)
    (producer/publish producer {:exchange queue1 :routing-key queue1} test-message2)
    (pipes/open test-pipe)
    (Thread/sleep 1000)
    (pipes/close test-pipe)
    (consumer/close consumer2)
    (consumer/close consumer1)
    (is (= test-message1 @received1))
    (is (= test-message2 @received2))))

(defn setup-connection-and-test [test-fn]
  (with-open [connection (rabbitmq-connection "localhost")]
    (test-fn connection)))

(deftest rabbitmq-test-suite []
  (setup-connection-and-test producer-consumer-direct-test)
  (setup-connection-and-test producer-consumer-topic-test)
  (setup-connection-and-test producer-consumer-fanout-test)
  (setup-connection-and-test producer-consumer-limit-test)
  (setup-connection-and-test on-failure-test)
  (setup-connection-and-test transacted-test)
  (setup-connection-and-test seqable-consumer-test)
  (setup-connection-and-test seqable-consumer-close-test)
  (setup-connection-and-test pipe-test)
  (setup-connection-and-test multi-pipe-test)
  (setup-connection-and-test router-pipe-test))