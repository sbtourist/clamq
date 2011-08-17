# Clamq - Clojure APIs for Message Queues - Version 0.3 (WORK IN PROGRESS)

Clamq is a Clojure adpater for interacting with message queues, providing simple APIs to connect to brokers and sending/consuming messages to/from message queues and topics.

Clamq supports **JMS** and **AMQP** brokers, more specifically:

* JMS
    * Any JMS broker for which you're able to provide a *javax.jmsConnectionFactory* object.
    * [ActiveMQ](http://activemq.apache.org)
* AMQP
    * [RabbitMQ](http://www.rabbitmq.com/)

## Configuring dependencies

Clamq can be configured in your Leiningen project file as follows:

    [clamq "0.3-SNAPSHOT"]

Also, depending on the broker you want to connect to, you need to declare the related dependency.

For ActiveMQ:

    [org.apache.activemq/activemq-core "5.5.0"]

For RabbitMQ:

    [org.springframework.amqp/spring-rabbit "1.0.0.RC3"]

## Connecting to brokers

Connection to your preferred broker is the first mandatory step in order to interact with your message queues.

### Generic JMS

In order to connect to generic JMS brokers you need the following namespaces:

    (use 'clamq.protocol.connection 'clamq.jms)

Now you can connect to a generic JMS broker as follows:

    (jms-connection connection-factory close-fn)

Where:

* **connection-factory** is a *javax.jmsConnectionFactory* object.
* **close-fn** is a no-arg function invoked when closing the connection, whose implementation must be provided in order to properly close the connection factory and its connection.

### ActiveMQ

In order to connect to an ActiveMQ broker you need the following namespaces:

    (use 'clamq.protocol.connection 'clamq.activemq)

Now you can connect as follows:

    (activemq-connection broker-uri)

Where:

* **broker-uri** is the ActiveMQ broker URI.

It also supports the following optional keyed parameters:

* **:username** : the username for broker authentication.
* **:password** : the password for broker authentication.

### RabbitMQ

In order to connect to a RabbitMQ broker you need the following namespaces:

    (use 'clamq.protocol.connection 'clamq.rabbitmq)

Now you can connect as follows:

    (rabbitmq-connection broker-host)

Where:

* **broker-host** is the host where the RabbitMQ broker is running.

It also supports the following optional keyed parameters:

* **:username** : the username for broker authentication (defaults to "guest").
* **:password** : the password for broker authentication (defaults to "guest").

## Producer/Consumer APIs

### Message producers

In order to start producing messages, you need the following namespaces:

    (use 'clamq.protocol.producer)

Now you can define a producer as follows:

    (producer connection configuration)

Where:

* **connection** is a broker connection obtained as previously described.
* **configuration** is an optional map of configuration parameters.

The configuration map currently supports the following keys:

* **:pubSub** : valid only for JMS brokers, defines (true/false) if produced messages are for publish/subscribe, that is, must be sent to a topic.

Once defined, the producer can be used to send messages as follows:

    (publish producer endpoint message attributes)

Where:

* **producer** is a producer obtained as previously described.
* **endpoint** is the definition of a message queue endpoint.
* **message** is the message to be sent, of type text or object.
* **attributes** is only for JMS brokers, and defines an optional map of attributes to set into the message.

The **endpoint** definition depends on the actual broker:
for JMS brokers, it is just the queue/topic name, for AMQP brokers it is a map containing two entries, *:exchange* and *:routing-key*.

### Asynchronous message consumers

In order to use asynchronous message consumers, you need the following namespaces:

    (use 'clamq.protocol.consumer)

Now you can define an asynchronous consumer as follows:

    (consumer connection configuration)

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

Once defined, asynchronous consumers can be started and closed as follows:

    (start consumer)
    (close consumer)

### Seqable (synchronous) message consumers

In order to use synchronous message consumers, you need the following namespaces:

    (use 'clamq.protocol.seqable)

Synchronous message consumers are based on Clojure lazy sequences, consuming messages only when actually requested to do so.

First you need to define a synchronous consumer as follows:

    (def consumer (seqable connection configuration))

Where:

* **connection** is a broker connection obtained as previously described.
* **configuration** is a mandatory map of configuration parameters.

The configuration map currently supports the following keys (mandatory, except where differently noted):

* **:endpoint** is the name of a message queue endpoint to consume from.
* **:timeout** is the maximum time in milliseconds to wait for available message; if no new messages are found, the sequence returns nil.

Now you can obtain the message sequence:

    (mseq consumer)

In order to advance with message consumption, you need to acknowledge the current message to the consumer, otherwise if trying to access another message without
first acknowledging, the consumer will simply time out and return nil:

    (ack consumer)

So here's a simple example about how to move all available messages from your sequable consumer to a vector:

    (with-open [consumer (seqable connection {:endpoint "queue" :timeout 1000})]
        (reduce into [] 
            (map #(do (ack consumer) [%1]) (mseq consumer))))

Finally, synchronous consumers must be closed as follows:

    (close consumer)

## Pipes and Filters APIs

### Pipes

Pipes define a conduit between source and destination endpoints, which can be different queues/topics belonging to different brokers.
Each message flowing between endpoints in a pipe is filtered by a filter function, and eventually processed by a failure function in case of errors.

Clamq provides different kind of pipes:

**Unicast** pipes, connecting two single endpoints:

    (use 'clamq.protocol.pipe 'clamq.pipes)
    (def pipe (pipe {
      :from {:connection connection :endpoint source :pubSub pubSub}
      :to {:connection connection :endpoint destination} :transacted true :pubSub pubSub :limit limit :filter-by filter-fn :on-failure failure-fn}))

**Multicast** pipes, connecting a source endpoint with multiple destination endpoints:

    (use 'clamq.protocol.pipe 'clamq.pipes)
    (def pipe (multi-pipe {
      :from {:connection connection :endpoint source :pubSub pubSub}
      :to [{:connection connection :endpoint destination1 :pubSub pubSub :filter-by filter-fn }] :transacted true :limit limit :on-failure failure-fn}))

**Router** pipes, connecting a source endpoint with one or more destination endpoints dynamically defined by a router function:

    (use 'clamq.protocol.pipe 'clamq.pipes)
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

### Filters

Pipes come with three types of filter functions, as previously cited:

Standard filter functions, configured under the **:filter-by** key, which takes the consumed message and returns an eventually processed message:

    (defn my-identity-filter [message] message)

Router filters, configured under the **:route-with** key, which takes the consumed message and returns a vector of maps,
each containing the message to route, the destination endpoint and the destination connection:

    (defn my-router [message] {:message message :endpoint destination :connection connection-object})

Finally, failure filters, configured under the :on-failure key, fired in case of exceptions during message handling and taking a map containing the
handled message (:message) and the raised exception (:exception):

    (defn my-failure-handler [failure] (println "Message: " (failure :message)) (println "Exception: " (failure :exception)))

## A note about resource management

As you may have noted, connections, consumers and pipes provide a close function to properly free resources. So, you can use all of them in idiomatic 
"with-open" blocks, as follows:

    (with-open [connection (activemq-connection "tcp://localhost:61616")]
        (do-something-with connection)
        )

## Examples

* clamq/test/base\_jms\_test.clj
* clamq/test/activemq\_test.clj
* clamq/test/rabbitmq\_test.clj

## Feedback

Feel free to open issues on the project tracker, and/or contact me on twitter: [sbtourist](http://twitter.com/sbtourist)