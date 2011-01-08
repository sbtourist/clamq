(ns clamq.test.rabbitmq-test
 (:use [clojure.test] [clamq.protocol] [clamq.rabbitmq] [clamq.pipes])
 (:import [org.springframework.amqp.core Binding Exchange Queue DirectExchange FanoutExchange TopicExchange] [org.springframework.amqp.rabbit.connection SingleConnectionFactory] [org.springframework.amqp.rabbit.core RabbitAdmin])
 )

(def admin (RabbitAdmin. (SingleConnectionFactory. "localhost")))
(def connection (rabbitmq-connection "localhost"))

(defn- declareQueue [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (DirectExchange. queue))
  (.declareBinding admin (Binding. (Queue. queue) (DirectExchange. queue) queue))
  )

(defn- declareTopic [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (TopicExchange. queue))
  (.declareBinding admin (Binding. (Queue. queue) (TopicExchange. queue) queue))
  )

(defn- declareFanout [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (FanoutExchange. queue))
  (.declareBinding admin (Binding. (Queue. queue) (FanoutExchange. queue)))
  )

(deftest producer-consumer-direct-test
  (let [received (atom "")
        queue "producer-consumer-direct-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-test"]
    (declareQueue queue)
    (send-to producer {:exchange queue :routing-key queue} test-message {})
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest producer-consumer-topic-test
  (let [received (atom "")
        queue "producer-consumer-topic-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-topic-test"]
    (declareTopic queue)
    (start consumer)
    (Thread/sleep 1000)
    (send-to producer {:exchange queue :routing-key queue} test-message {})
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest producer-consumer-fanout-test
  (let [received (atom "")
        queue "producer-consumer-fanout-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-fanout-test"]
    (declareFanout queue)
    (start consumer)
    (Thread/sleep 1000)
    (send-to producer {:exchange queue} test-message {})
    (Thread/sleep 1000)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest producer-consumer-limit-test
  (let [received (atom 0)
        queue "producer-consumer-limit-test-queue"
        messages 5
        limit 2
        consumer (consumer connection {:endpoint queue :on-message #(do (swap! received inc) %1) :transacted true :limit limit})
        producer (producer connection)
        test-message "producer-consumer-limit-test"]
    (declareQueue queue)
    (loop [i 1] (send-to producer {:routing-key queue} test-message {}) (if (< i messages) (recur (inc i))))
    (start consumer)
    (Thread/sleep 1000)
    (stop consumer)
    (is (= limit @received))
    )
  )

(deftest on-failure-test
  (let [received (atom "")
        queue "on-failure-test-queue"
        dlq "on-failure-test-dlq"
        producer (producer connection)
        failing-consumer (consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted false :on-failure #(send-to producer {:exchange dlq :routing-key dlq} (:message %1) {})})
        working-consumer (consumer connection {:endpoint dlq :on-message #(reset! received %1) :transacted true})
        test-message "on-failure-test"]
    (declareQueue queue)
    (declareQueue dlq)
    (send-to producer {:exchange queue :routing-key queue} test-message {})
    (start failing-consumer)
    (Thread/sleep 1000)
    (stop failing-consumer)
    (Thread/sleep 1000)
    (start working-consumer)
    (Thread/sleep 1000)
    (stop working-consumer)
    (is (= test-message @received))
    )
  )

(deftest transacted-test
  (let [received (atom "")
        queue "transacted-test-queue"
        failing-consumer (consumer connection {:endpoint queue :on-message #(throw (RuntimeException. %1)) :transacted true})
        working-consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted true})
        producer (producer connection)
        test-message "transacted-test"]
    (declareQueue queue)
    (send-to producer {:exchange queue :routing-key queue} test-message {})
    (start failing-consumer)
    (Thread/sleep 1000)
    (stop failing-consumer)
    (Thread/sleep 1000)
    (start working-consumer)
    (Thread/sleep 1000)
    (stop working-consumer)
    (is (= test-message @received))
    )
  )

(deftest pipe-test
  (let [received (atom "")
        queue1 "pipe-test-queue1"
        queue2 "pipe-test-queue2"
        consumer (consumer connection {:endpoint queue2 :on-message #(reset! received %1) :transacted true})
        producer (producer connection)
        test-pipe (pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint {:exchange queue2 :routing-key queue2}} :transacted true})
        test-message "pipe-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (start consumer)
    (send-to producer {:exchange queue1 :routing-key queue1} test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest pipe-topic-test
  (let [received (atom "")
        topic1 "pipe-topic-test-topic1"
        topic2 "pipe-topic-test-topic2"
        consumer (consumer connection {:endpoint topic2 :on-message #(reset! received %1) :transacted true})
        producer (producer connection)
        test-pipe (pipe {:from {:connection connection :endpoint topic1} :to {:connection connection :endpoint {:exchange topic2 :routing-key topic2}} :transacted true})
        test-message "pipe-topic-test"]
    (declareTopic topic1)
    (declareTopic topic2)
    (start consumer)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer {:exchange topic1 :routing-key topic1} test-message {})
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
    )
  )

(deftest pipe-limit-test
  (let [received (atom 0)
        queue1 "pipe-limit-test-queue1"
        queue2 "pipe-limit-test-queue2"
        messages 5
        limit 2
        consumer (consumer connection {:endpoint queue2 :on-message #(do (swap! received inc) %1) :transacted true})
        producer (producer connection)
        test-pipe (pipe {:from {:connection connection :endpoint queue1} :to {:connection connection :endpoint {:exchange queue2 :routing-key queue2}} :transacted true :limit limit})
        test-message "pipe-limit-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (start consumer)
    (loop [i 1] (send-to producer {:exchange queue1 :routing-key queue1} test-message {}) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= limit @received))
    )
  )

(deftest multi-pipe-test
  (let [queue1 "multi-pipe-test-queue1"
        queue2 "multi-pipe-test-queue2"
        queue3 "multi-pipe-test-queue3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint queue2 :on-message #(reset! received1 %1) :transacted true})
        consumer2 (consumer connection {:endpoint queue3 :on-message #(reset! received2 %1) :transacted true})
        producer (producer connection)
        test-pipe (multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint {:exchange queue2 :routing-key queue2}} {:connection connection :endpoint {:exchange queue3 :routing-key queue3}}] :transacted true})
        test-message "multi-pipe-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (declareQueue queue3)
    (start consumer1)
    (start consumer2)
    (send-to producer {:exchange queue1 :routing-key queue1} test-message {})
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(deftest multi-pipe-topic-test
  (let [topic1 "multi-pipe-topic-test-topic1"
        topic2 "multi-pipe-topic-test-topic2"
        topic3 "multi-pipe-topic-test-topic3"
        received1 (atom "")
        received2 (atom "")
        consumer1 (consumer connection {:endpoint topic2 :on-message #(reset! received1 %1) :transacted true :pubSub true})
        consumer2 (consumer connection {:endpoint topic3 :on-message #(reset! received2 %1) :transacted true :pubSub true})
        producer (producer connection)
        test-pipe (multi-pipe {:from {:connection connection :endpoint topic1} :to [{:connection connection :endpoint {:exchange topic2 :routing-key topic2}} {:connection connection :endpoint {:exchange topic3 :routing-key topic3}}] :transacted true})
        test-message "multi-pipe-topic-test"]
    (declareTopic topic1)
    (declareTopic topic2)
    (declareTopic topic3)
    (start consumer1)
    (start consumer2)
    (open test-pipe)
    (Thread/sleep 1000)
    (send-to producer {:exchange topic1 :routing-key topic1} test-message {})
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
    )
  )

(deftest multi-pipe-limit-test
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
        test-pipe (multi-pipe {:from {:connection connection :endpoint queue1} :to [{:connection connection :endpoint {:exchange queue2 :routing-key queue2}} {:connection connection :endpoint {:exchange queue3 :routing-key queue3}}] :transacted true :limit limit})
        test-message "multi-pipe-limit-test"]
    (declareQueue queue1)
    (declareQueue queue2)
    (declareQueue queue3)
    (start consumer1)
    (start consumer2)
    (loop [i 1] (send-to producer {:exchange queue1 :routing-key queue1} test-message {}) (if (< i messages) (recur (inc i))))
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= limit @received1))
    (is (= limit @received2))
    )
  )