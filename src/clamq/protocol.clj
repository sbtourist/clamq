(ns clamq.protocol)

(defprotocol Connection
  (producer [self] [self conf])
  (consumer [self conf])
  )

(defprotocol Producer
  (send-to [self destination message attributes])
  )

(defprotocol Consumer
  (start [self])
  (stop [self])
  )

(defprotocol Pipe
  (open [self])
  (close [self])
  )