# Clamq - Clojure APIs for Message Queues - Version 0.3 (WORK IN PROGRESS)

Clamq is a Clojure adpater for interacting with message queues, providing simple APIs to connect to brokers and sending/consuming messages to/from message queues and topics.

Clamq supports **JMS** and **AMQP** brokers, more specifically:

* JMS
    * Any JMS broker for which you're able to provide a *javax.jmsConnectionFactory* object.
    * [ActiveMQ](http://activemq.apache.org)
* AMQP
    * [RabbitMQ](http://www.rabbitmq.com/)

## Connecting to brokers

### Generic JMS

Here is how to connect to a generic JMS broker:

    (ns clamq.test (:use [clamq.jms] [clamq.protocol]))
    (def connection (jms-connection connection-factory))

Where:

* **connection-factory** is a *javax.jmsConnectionFactory* object.

### ActiveMQ

Here is how to connect to an ActiveMQ broker:

    (ns clamq.test (:use [clamq.activemq] [clamq.jms] [clamq.protocol]))
    (def connection (jms-connection (activemq broker-uri)))

Where:

* **broker-uri** is the ActiveMQ broker URI.

It also supports the following optional keyed parameters:

* **:username** : the username for broker authentication.
* **:password** : the password for broker authentication.
* **:max-connection** : the max number of allowed connections.

### RabbitMQ

Here is how to connect to a RabbitMQ broker:

    (ns clamq.test (:use [clamq.rabbitmq] [clamq.jms] [clamq.protocol]))
    (def connection (rabbitmq-connection broker-host))

Where:

* **broker-host** is the host where the RabbitMQ broker is running.

It also supports the following optional keyed parameters:

* **:username** : the username for broker authentication (defaults to "guest").
* **:password** : the password for broker authentication (defaults to "guest").

## Producer/Consumer APIs

### Producing messages

After the connection definition as previously described, define a producer as follows:

    (ns clamq.test (:use [clamq.protocol]))
    (def producer (producer connection configuration))

Where:

* **connection** is a broker connection obtained as previously described.
* **configuration** is an optional map of configuration parameters.

The configuration map currently supports the following keys:

* **:pubSub** : valid only for JMS brokers, defines (true/false) if produced messages are for publish/subscribe, that is, must be sent to a topic.

Once defined, the producer can be used to send messages as follows:

    (send-to producer endpoint message attributes)

Where:

* **producer** is a producer obtained as previously described.
* **endpoint** is the definition of a message queue endpoint.
* **message** is the message to be sent, of type text or object.
* **attributes** is only for JMS brokers, and defines an optional map of attributes to set into the message.

The **endpoint** definition depends on the actual broker:
for JMS brokers, it is just the queue/topic name, for AMQP brokers it is a map containing two entries, *:exchange* and *:routing-key*.

### Consuming messages

After the connection definition as previously described, define a consumer as follows:

    (ns clamq.test (:use [clamq.protocol]))
    (def consumer (consumer connection configuration))

Where:

* **connection** is a broker connection obtained as previously described.
* **configuration** is a mandatory map of configuration parameters.

The configuration map currently supports the following keys (mandatory, except where differently noted):

* **:endpoint** is the name of a message queue endpoint to consume from.
* **:on-message** is the handler function to call at each consumed message, accepting the message itself as unique argument.
* **:transacted** defines (true/false) if the message consumption must be locally transactional.
* **:pubSub** in only valid for JMS brokers, and defines if consumer messages are from publish/subscribe, that is, must be consumed from a topic (optional).
* **:limit** defines the max number of consumed messages, after which the consumer stops itself (optional: defaults to 0, unlimited).
* **:on-failure** defines a function called in case of exception during message handling (optional: by default the exception is just rethrown).

When setting an explicit **:limit**, you're strongly suggested to also set **:transacted** to true, in order to avoid losing messages when the consumption
is interrupted due to the reached limit.

Once defined, start/stop consuming as follows:

    (start consumer)
    (stop consumer)

## Pipes and Filters APIs

## Pipes

Pipes define a conduit between source and destination endpoints, which can be different queues/topics belonging to different brokers.
Each message flowing between endpoints in a pipe is filtered by a filter function, and eventually processed by a failure function in case of errors.

Clamq provides different kind of pipes:

**Unicast** pipes, connecting two single endpoints:

    (ns clamq.test (:use [clamq.protocol] [clamq.pipes]))
    (def pipe (pipe {
      :from {:connection connection :endpoint source :pubSub pubSub}
      :to {:connection connection :endpoint destination} :transacted true :pubSub pubSub :limit limit :filter-by filter-fn :on-failure failure-fn}))

**Multicast** pipes, connecting a source endpoint with multiple destination endpoints:

    (ns clamq.test (:use [clamq.protocol] [clamq.pipes]))
    (def pipe (multi-pipe {
      :from {:connection connection :endpoint source :pubSub pubSub}
      :to [{:connection connection :endpoint destination1 :pubSub pubSub :filter-by filter-fn }] :transacted true :limit limit :on-failure failure-fn}))

**Router** pipes, connecting a source endpoint with one or more destination endpoints dynamically defined by a router function:

    (ns clamq.test (:use [clamq.protocol] [clamq.pipes]))
    (def pipe (router-pipe {
      :from {:connection connection :endpoint source :pubSub pubSub}
      :route-with router-fn :transacted true :limit limit :on-failure failure-fn}))

Where:

* **:connection** is a broker connection obtained as previously described.
* **:endpoint** is the definition of a JMS or AMQP message queue endpoint as previously described.
* **:transacted** defines (true/false) if the message consumption must be transactional.
* **:limit** defines the max number of flowing messages, after which the pipe stops consuming messages (optional: defaults to 0, unlimited).
* **:pubSub** defines if messages are from/to a topic (valid only for JMS brokers, defaults to false).
* **:filter-by** defines the name of the filter function (optional, defaults to identity function).
* **:on-failure** defines a function called in case of exceptions during message handling (optional: by default the exception is just rethrown).
* **:route-with** defines the name of the router function (only for router pipes, and mandatory).

Once defined, you can open/close pipes to let messages flow:

    (open pipe)
    (close pipe)

## Filters

Pipes come with three types of filter functions, as previously cited:

Standard filter functions, configured under the **:filter-by** key, which takes the consumed message and returns an eventually processed message:

    (defn my-identity-filter [message] message)

Router filters, configured under the **:route-with** key, which takes the consumed message and returns a vector of maps,
each containing the message to route, the destination endpoint and the destination connection:

    (defn my-router [message] {:message message :endpoint destination :connection connection-object})

Finally, failure filters, configured under the :on-failure key, fired in case of exceptions during message handling and taking a map containing the
handled message (:message) and the raised exception (:exception):

    (defn my-failure-handler [failure] (println "Message: " (failure :message)) (println "Exception: " (failure :exception)))

## Examples

* clamq/test/base\_jms\_test.clj
* clamq/test/activemq\_test.clj
* clamq/test/rabbitmq\_test.clj

## Feedback

Feel free to open issues on the project tracker, and/or contact me on twitter: [sbtourist](http://twitter.com/sbtourist)