# Clamq - Version 0.1 (in progress)

Clamq is a Clojure adapter for interacting with JMS brokers, providing simple APIs to connect to brokers and sending/consuming messages to/from message queues and topics.

Currently supported JMS brokers:

* [ActiveMQ](http://activemq.apache.org)

## Connecting to brokers

### ActiveMQ

Here is how to define an ActiveMQ connection:

    (ns clamq.test (:use [clamq.connection.activemq]))
    (def connection (activemq broker-uri))

Where:

* **broker-uri** is the ActiveMQ broker URI.

You can also pass an optional sequence of keyed parameters as follows:

    (ns clamq.test (:use [clamq.connection.activemq]))
    (def connection (activemq :username username :password password :max-connection max-connections))

## Producer/Consumer APIs

### Producing messages

First, define a producer:

    (ns clamq.test (:use [clamq.producer]))
    (def producer (producer connection :transacted transacted :pubSub pubSub))

Where:

* **connection** is a broker connection obtained as previously described.
* **transacted** defines (true/false) if the message sending must be transactional.
* **pubSub** defines if produced messages are for publish/subscribe, that is, must be sent to a topic (optional: default to false).

Then, send messages through it:

    (send-to producer destination message attributes)

Where:

* **producer** is a producer obtained as previously described.
* **destination** is the name of the JMS destination.
* **message** is the message to be sent, of type text or object.
* **attributes** is the map of string attributes to set into the message.

### Consuming messages

First, define a consumer:

    (ns clamq.test (:use [clamq.consumer]))
    (def consumer (consumer connection destination handler-fn :transacted transacted :pubSub pubSub :limit limit :on-failure on-failure))

Where:

* **connection** is a broker connection obtained as previously described.
* **destination** is the name of the JMS destination to consume from.
* **handler-fn** is the handler function to call at each consumed message, accepting the message itself as unique argument.
* **transacted** defines (true/false) if the message consumption must be transactional.
* **pubSub** defines if consumer messages are from publish/subscribe, that is, must be consumed from a topic (optional: default to false).
* **limit** defines the max number of consumed messages, after which the consumer stops itself (optional: default to 0, unlimited).
* **on-failure** defines a function called in case of exception during message handling (optional: by default the exception is just rethrown).

Then, start/stop consuming:

    (start consumer)
    (stop consumer)

## Pipes and Filters APIs

## Creating Pipes

Pipes define a conduit between source and destination endpoints, which can be different queues/topics belonging to different brokers.
Each message flowing between endpoints in a pipe is filtered by a filter function, and eventually processed by a failure function in case of errors.

Clamq provides two different kind of pipes: unicast pipes, connecting two single endpoints, and multicast pipes, connecting a source endpoint with multiple destination endpoints.

Unicast pipes are defined as follows:

    (ns clamq.test (:use [clamq.pipes]))
    (def pipe (pipe {
      :from {:connection broker :endpoint source :pubSub pubSub}
      :to {:connection broker :endpoint destination} :transacted true :pubSub pubSub :limit limit :filter-by filter-fn :on-failure failure-fn}))

Multicast pipes are pretty similar, except they get an array of destinations:

    (ns clamq.test (:use [clamq.pipes]))
    (def pipe (multi-pipe {
      :from {:connection broker :endpoint source :pubSub pubSub}
      :to [{:connection broker :endpoint destination1 :pubSub pubSub :limit limit :filter-by filter-fn :on-failure failure-fn}] :transacted true}))

Where:

* **connection** is a broker connection obtained as previously described.
* **endpoint** is the name of a JMS queue/topic.
* **transacted** defines (true/false) if the message consumption must be transactional.
* **pubSub** defines if messages are from/to a topic (optional: default to false).
* **limit** defines the max number of flowing messages, after which the pipe stops consuming messages (optional: default to 0, unlimited).
* **filter-by** defines the name of the filter function (optional, default to identity function).
* **on-failure** defines a function called in case of exception during message handling (optional: by default the exception is just rethrown).

Once defined, you can open/close pipes to let messages flow:

    (open pipe)
    (close pipe)

## Examples

* clamq/test/activemq_test.clj
