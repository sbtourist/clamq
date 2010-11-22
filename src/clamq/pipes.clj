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

(defn pipe [{{source :source s-connection :connection} :from
             {destination :destination d-connection :connection} :to
             filter-fn :filter-by
             failure-fn :on-failure
             transacted :transacted
             :or {filter-fn identity failure-fn rethrow-on-failure}
             }]
  (let [tail (producer d-connection {:transacted transacted})
        filtered-handoff #(send-to tail destination (filter-fn %1) {})
        head (consumer s-connection source filtered-handoff {:transacted transacted :on-failure failure-fn})]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )

(defn multi-pipe [{{source :source s-connection :connection} :from
                   destinations :to
                   transacted :transacted
                   }]
  (let [filtered-multicast
          #(doseq [d destinations]
             (try
               (send-to (producer (:connection d) {:transacted transacted}) (:destination d) ((get d :filter-by identity) %1) {})
               (catch Exception ex ((get d :on-failure rethrow-on-failure) {:exception ex :message %1}))))
        head
          (consumer s-connection source filtered-multicast {:transacted transacted})
        ]
    (reify Pipe
      (open [self] (start head))
      (close [self] (stop head))
      )
    )
  )