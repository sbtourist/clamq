(ns clamq.test.rabbitmq-test
 (:use [clojure.test] [clamq.protocol] [clamq.rabbitmq] [clamq.pipes])
 (:import [org.springframework.amqp.core BindingBuilder Exchange Queue DirectExchange FanoutExchange TopicExchange] [org.springframework.amqp.rabbit.connection SingleConnectionFactory] [org.springframework.amqp.rabbit.core RabbitAdmin])
 )

(def admin (RabbitAdmin. (SingleConnectionFactory. "localhost")))
(def connection (rabbitmq-connection "localhost"))

(defn- declareQueue [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (DirectExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (DirectExchange. queue)) (with queue)))
  )

(defn- declareTopic [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (TopicExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (TopicExchange. queue)) (with queue)))
  )

(defn- declareFanout [queue]
  (.declareQueue admin (Queue. queue))
  (.declareExchange admin (FanoutExchange. queue))
  (.declareBinding admin (.. BindingBuilder (bind (Queue. queue)) (to (FanoutExchange. queue))))
  )

(deftest producer-consumer-direct-test
  (let [received (atom "")
        queue "producer-consumer-direct-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-test"]
    (declareQueue queue)
    (send-to producer {:exchange queue :routing-key queue} test-message)
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
    (send-to producer {:exchange queue :routing-key queue} test-message)
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
    (send-to producer {:exchange queue} test-message)
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
    (loop [i 1] (send-to producer {:routing-key queue} test-message) (if (< i messages) (recur (inc i))))
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
    (send-to producer {:exchange queue :routing-key queue} test-message)
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
    (send-to producer {:exchange queue :routing-key queue} test-message)
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

(deftest seqable-consumer-test
  (let [queue "seqable-consumer-test-queue"
        producer (producer connection)
        test-message1 "seqable-consumer-test1"
        test-message2 "seqable-consumer-test2"]
    (declareQueue queue)
    (send-to producer {:exchange queue :routing-key queue} test-message1)
    (send-to producer {:exchange queue :routing-key queue} test-message2)
    (let [consumer (seqable-consumer connection {:endpoint queue :timeout 1000})
          result (reduce into [] (map #(do (ack consumer) [%1]) (seqable consumer)))]
      (is (= test-message1 (result 0)))
      (is (= test-message2 (result 1)))
      (let [result (reduce into [] (map #(do (ack consumer) [%1]) (seqable consumer)))]
        (is (empty? result))
        )
      (abort consumer)
      )
    )
  )

(deftest seqable-consumer-abort-test
  (let [queue "seqable-consumer-abort-test-queue"
        producer (producer connection)
        test-message1 "seqable-consumer-abort-test1"
        test-message2 "seqable-consumer-abort-test2"]
    (declareQueue queue)
    (send-to producer {:exchange queue :routing-key queue} test-message1)
    (send-to producer {:exchange queue :routing-key queue} test-message2)
    (let [consumer (seqable-consumer connection {:endpoint queue :timeout 1000})]
      (reduce into [] (map #(do [%1]) (seqable consumer)))
      (abort consumer)
      (let [consumer (seqable-consumer connection {:endpoint queue :timeout 1000})
            result (reduce into [] (map #(do (ack consumer) [%1]) (seqable consumer)))]
        (is (= test-message1 (result 0)))
        (is (= test-message2 (result 1)))
        (abort consumer)
        )
      )
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
    (send-to producer {:exchange queue1 :routing-key queue1} test-message)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer)
    (is (= test-message @received))
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
    (send-to producer {:exchange queue1 :routing-key queue1} test-message)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message @received1))
    (is (= test-message @received2))
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
        router-fn #(if (= "router-pipe-test2" %1) [{:connection connection :endpoint {:exchange queue2 :routing-key queue2} :message %1}] [{:connection connection :endpoint {:exchange queue3 :routing-key queue3} :message %1}])
        test-pipe (router-pipe {:from {:connection connection :endpoint queue1} :route-with router-fn :transacted true})
        test-message1 "router-pipe-test2"
        test-message2 "router-pipe-test3"]
    (declareQueue queue1)
    (declareQueue queue2)
    (declareQueue queue3)
    (start consumer1)
    (start consumer2)
    (send-to producer {:exchange queue1 :routing-key queue1} test-message1)
    (send-to producer {:exchange queue1 :routing-key queue1} test-message2)
    (open test-pipe)
    (Thread/sleep 1000)
    (close test-pipe)
    (stop consumer2)
    (stop consumer1)
    (is (= test-message1 @received1))
    (is (= test-message2 @received2))
    )
  )