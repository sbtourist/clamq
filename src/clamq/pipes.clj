(ns clamq.pipes
 (:use
   [clamq.helpers]
   [clamq.protocol]
   )
 )

(defn- make-producer [connection pubSub]
  (producer connection {:pubSub pubSub})
  )

(defn pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
             {destination :endpoint d-connection :connection d-pubSub :pubSub :or {d-pubSub false}} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             limit :limit
             :or {filter-fn identity failure-fn rethrow-on-failure limit 0}
             }]
  (let [memoized-producer
        (memoize make-producer)
        filtered-unicast
        #(send-to (memoized-producer d-connection d-pubSub) destination (filter-fn %1) {})
        head-consumer
        (consumer s-connection {:endpoint source :on-message filtered-unicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})]
    (reify Pipe
      (open [self] (start head-consumer))
      (close [self] (stop head-consumer))
      )
    )
  )

(defn multi-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
                   destinations :to
                   failure-fn :on-failure
                   transacted :transacted
                   limit :limit
                   :or {failure-fn rethrow-on-failure limit 0}
                   }]
  (let [memoized-producer
        (memoize make-producer)
        filtered-multicast
        #(doseq [d destinations]
           (let [filtered-message ((or (d :filter-by) identity) %1)]
             (when (not (nil? filtered-message)) (send-to (memoized-producer (d :connection) (or (d :pubSub) false)) (d :endpoint) filtered-message {}))
             )
           )
        head-consumer
        (consumer s-connection {:endpoint source :on-message filtered-multicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})
        ]
    (reify Pipe
      (open [self] (start head-consumer))
      (close [self] (stop head-consumer))
      )
    )
  )

(defn router-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
                    router-fn :route-with
                    failure-fn :on-failure
                    transacted :transacted
                    limit :limit
                    :or {failure-fn rethrow-on-failure limit 0}
                    }]
  (when (nil? router-fn) (throw (IllegalArgumentException. "No value specified for :route-with router function!")))
  (let [memoized-producer
        (memoize make-producer)
        routed-multicast
        #(doseq [d (router-fn %1)]
           (let [connection (d :connection)
                 endpoint (d :endpoint)
                 pubSub (or (d :pubSub) false)
                 message (d :message)
                 producer (memoized-producer connection pubSub)
                 ]
             (when (not (nil? message)) (send-to producer endpoint message {}))
             )
           )
        head-consumer
        (consumer s-connection {:endpoint source :on-message routed-multicast :on-failure failure-fn :transacted transacted :pubSub s-pubSub :limit limit})
        ]
    (reify Pipe
      (open [self] (start head-consumer))
      (close [self] (stop head-consumer))
      )
    )
  )