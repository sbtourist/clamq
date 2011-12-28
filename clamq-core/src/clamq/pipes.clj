(ns clamq.pipes
 (:require
   [clamq.helpers :as helpers] 
   [clamq.protocol.connection :as connection]
   [clamq.protocol.consumer :as consumer]
   [clamq.protocol.producer :as producer]))

(defn- make-producer [connection pubSub]
  (connection/producer connection {:pubSub pubSub}))

(defprotocol Pipe
  (open [self])
  (close [self]))

(defn single-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
             {destination :endpoint d-connection :connection d-pubSub :pubSub :or {d-pubSub false}} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             limit :limit
             :or {filter-fn identity failure-fn helpers/rethrow-on-failure limit 0}}]
  (let [memoized-producer
        (memoize make-producer)
        filtered-unicast
        #(producer/publish (memoized-producer d-connection d-pubSub) destination (filter-fn %1) {})
        head-consumer
        (connection/consumer s-connection {:endpoint source :on-message filtered-unicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})]
    (reify Pipe
      (open [self] (consumer/start head-consumer))
      (close [self] (consumer/close head-consumer)))))

(defn multi-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
                   destinations :to
                   failure-fn :on-failure
                   transacted :transacted
                   limit :limit
                   :or {failure-fn helpers/rethrow-on-failure limit 0}}]
  (let [memoized-producer
        (memoize make-producer)
        filtered-multicast
        #(doseq [d destinations]
           (let [filtered-message ((or (d :filter-by) identity) %1)]
             (when (not (nil? filtered-message)) (producer/publish (memoized-producer (d :connection) (or (d :pubSub) false)) (d :endpoint) filtered-message {}))))
        head-consumer
        (connection/consumer s-connection {:endpoint source :on-message filtered-multicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})]
    (reify Pipe
      (open [self] (consumer/start head-consumer))
      (close [self] (consumer/close head-consumer)))))

(defn router-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
                    router-fn :route-with
                    failure-fn :on-failure
                    transacted :transacted
                    limit :limit
                    :or {failure-fn helpers/rethrow-on-failure limit 0}}]
  (when (nil? router-fn) (throw (IllegalArgumentException. "No value specified for :route-with router function!")))
  (let [memoized-producer
        (memoize make-producer)
        routed-multicast
        #(doseq [d (router-fn %1)]
           (let [connection (d :connection)
                 endpoint (d :endpoint)
                 pubSub (or (d :pubSub) false)
                 message (d :message)
                 producer (memoized-producer connection pubSub)]
             (when (not (nil? message)) (producer/publish producer endpoint message {}))))
        head-consumer
        (connection/consumer s-connection {:endpoint source :on-message routed-multicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})]
    (reify Pipe
      (open [self] (consumer/start head-consumer))
      (close [self] (consumer/close head-consumer)))))