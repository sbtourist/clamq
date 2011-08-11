(ns clamq.protocol)

(defprotocol Connection
  (producer [self] [self conf])
  (consumer [self conf])
  (seqable-consumer [self conf])
  )

(defprotocol Producer
  (send-to [self destination message] [self destination message attributes])
  )

(defprotocol Consumer
  (start [self])
  (stop [self])
  )

(defprotocol Seqable-Consumer
  (seqable [self])
  (ack [self])
  (abort [self])
  )

(defprotocol Pipe
  (open [self])
  (close [self])
  )