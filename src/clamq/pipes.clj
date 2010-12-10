(ns clamq.pipes
 (:use
   [clamq.connection.activemq]
   [clamq.helpers]
   [clamq.consumer]
   [clamq.producer]
   )
 )

(defprotocol Pipe
  (open [self])
  (close [self])
  )

(defn pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
             {destination :endpoint d-connection :connection d-pubSub :pubSub :or {d-pubSub false}} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             limit :limit
             :or {filter-fn identity failure-fn rethrow-on-failure limit 0}
             }]
  (let [tail (producer d-connection :transacted transacted :pubSub d-pubSub)
        filtered-handoff #(send-to tail destination (filter-fn %1) {})
        head (consumer s-connection source filtered-handoff :transacted transacted :pubSub s-pubSub :limit limit :on-failure failure-fn)]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )

(defn multi-pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
                   destinations :to
                   transacted :transacted
                   limit :limit
                   :or {limit 0}
                   }]
  (let [filtered-multicast
        #(doseq [d destinations]
             (try
               (let [message ((get d :filter-by identity) %1)]
                 (if (not (nil? message)) (send-to (producer (:connection d) :transacted transacted :pubSub (get d :pubSub false)) (:endpoint d) message {}))
                 )
               (catch Exception ex ((get d :on-failure rethrow-on-failure) {:exception ex :message %1}))
               )
             )
        head
        (consumer s-connection source filtered-multicast :transacted transacted :pubSub s-pubSub :limit limit)
        ]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )