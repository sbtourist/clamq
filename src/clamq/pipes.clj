(ns clamq.pipes
 (:use
   [clamq.helpers]
   [clamq.protocol]
   )
 )

(defn pipe [{{source :endpoint s-connection :connection s-pubSub :pubSub :or {s-pubSub false}} :from
             {destination :endpoint d-connection :connection d-pubSub :pubSub :or {d-pubSub false}} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             limit :limit
             :or {filter-fn identity failure-fn rethrow-on-failure limit 0}
             }]
  (let [tail
        (producer d-connection {:pubSub d-pubSub})
        filtered-unicast
        #(send-to tail destination (filter-fn %1) {})
        head
        (consumer s-connection {:endpoint source :on-message filtered-unicast :transacted transacted :pubSub s-pubSub :limit limit :on-failure failure-fn})]
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
  (let [tails
        (into {} (map #(conj [] (:endpoint %1) (producer (:connection %1) {:pubSub (get %1 :pubSub false)})) destinations))
        filtered-multicast
        #(doseq [d destinations]
           (try
             (let [filtered-message ((get d :filter-by identity) %1)]
               (if (not (nil? filtered-message)) (send-to (get tails (:endpoint d)) (:endpoint d) filtered-message {}))
               )
             (catch Exception ex ((get d :on-failure rethrow-on-failure) {:exception ex :message %1}))
             )
           )
        head
        (consumer s-connection {:endpoint source :on-message filtered-multicast :transacted transacted :pubSub s-pubSub :limit limit})
        ]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )