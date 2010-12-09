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

(defn pipe [{{source :endpoint s-connection :connection} :from
             {destination :endpoint d-connection :connection} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             limit :limit
             :or {filter-fn identity failure-fn rethrow-on-failure limit 0}
             }]
  (let [tail (producer d-connection transacted)
        filtered-handoff #(send-to tail destination (filter-fn %1) {})
        head (consumer s-connection source transacted filtered-handoff :limit limit :on-failure failure-fn)]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )

(defn multi-pipe [{{source :endpoint s-connection :connection} :from
                   destinations :to
                   transacted :transacted
                   limit :limit
                   :or {limit 0}
                   }]
  (let [filtered-multicast
        #(doseq [d destinations]
             (try
               (let [message ((get d :filter-by identity) %1)]
                 (if (not (nil? message)) (send-to (producer (:connection d) transacted) (:endpoint d) message {}))
                 )
               (catch Exception ex ((get d :on-failure rethrow-on-failure) {:exception ex :message %1}))
               )
             )
        head
        (consumer s-connection source transacted filtered-multicast :limit limit)
        ]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )