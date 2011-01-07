(ns clamq.test.rabbitmq-test
 (:use [clojure.test] [clamq.protocol] [clamq.rabbitmq])
 (:import [org.springframework.amqp.core Binding Exchange Queue DirectExchange FanoutExchange TopicExchange] [org.springframework.amqp.rabbit.connection SingleConnectionFactory] [org.springframework.amqp.rabbit.core RabbitAdmin])
 )

(def admin (RabbitAdmin. (SingleConnectionFactory. "localhost")))
(def connection (rabbitmq-connection "localhost"))

(deftest producer-consumer-direct-test
  (let [received (atom "")
        queue "producer-consumer-direct-test-queue"
        consumer (consumer connection {:endpoint queue :on-message #(reset! received %1) :transacted false})
        producer (producer connection)
        test-message "producer-consumer-test"]
    (.declareQueue admin (Queue. queue))
    (.declareExchange admin (DirectExchange. queue))
    (.declareBinding admin (Binding. (Queue. queue) (DirectExchange. queue) queue))
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
    (.declareQueue admin (Queue. queue))
    (.declareExchange admin (TopicExchange. queue))
    (.declareBinding admin (Binding. (Queue. queue) (TopicExchange. queue) queue))
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
    (.declareQueue admin (Queue. queue))
    (.declareExchange admin (FanoutExchange. queue))
    (.declareBinding admin (Binding. (Queue. queue) (FanoutExchange. queue)))
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
    (.declareQueue admin (Queue. queue))
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
    (.declareQueue admin (Queue. queue))
    (.declareExchange admin (DirectExchange. queue))
    (.declareBinding admin (Binding. (Queue. queue) (DirectExchange. queue) queue))
    (.declareQueue admin (Queue. dlq))
    (.declareExchange admin (DirectExchange. dlq))
    (.declareBinding admin (Binding. (Queue. dlq) (DirectExchange. dlq) dlq))
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
    (.declareQueue admin (Queue. queue))
    (.declareExchange admin (DirectExchange. queue))
    (.declareBinding admin (Binding. (Queue. queue) (DirectExchange. queue) queue))
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

