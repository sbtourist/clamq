# Clamq - Version 0.1 (in progress)

Clamq is a Clojure adapter for JMS brokers, providing simple APIs to connect to JMS brokers and sending/consuming messages to/from message queues.

Currently supported JMS brokers:

* [ActiveMQ](http://activemq.apache.org)

## Connecting to brokers

### ActiveMQ

Here is how to define an ActiveMQ connection:

    (ns clamq.test (:use [clamq.connection.activemq]))
    (def activemq-connection (activemq broker-uri parameters))

Where:

* **broker-uri** is the ActiveMQ broker URI.
* **parameters** is an optional map of connection parameters, currently supported ones are: :username, :password, :max-connections.

## Producing messages

First, define a producer:

    (ns clamq.test (:use [clamq.producer]))
    (def producer (make-producer connection transacted))

Where:

* **connection** is a broker connection obtained as previously described.
* **transacted** defines if the message sending must be transactional (true, false otherwise).

Then, send messages through it:

    (send-to producer destination message attributes)

Where:

* **producer** is a producer obtained as previously described.
* **destination** is the name of the JMS destination.
* **message** is the message to be sent, of type text or object.
* **attributes** is the map of string attributes to set into the message.

## Consuming messages

First, define a consumer:

    (ns clamq.test (:use [clamq.consumer]))
    (def consumer (make-consumer connection destination handler-fn transacted))

Where:

* **connection** is a broker connection obtained as previously described.
* **destination** is the name of the JMS destination to consume from.
* **handler-fn** is the handler function to call at each consumed message, accepting the message itself as unique argument.
* **transacted** defines if the message consuming must be transactional (true, false otherwise).

Then, start/stop consuming:

    (start consumer)
    (stop consumer)

## Examples

* clamq/test/activemq_test.clj

