(ns clamq.protocol.connection)

(defprotocol Connection
  (producer [self] [self conf])
  (consumer [self conf])
  (seqable [self conf])
  (close [self])
  )