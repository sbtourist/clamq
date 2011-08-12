(ns clamq.protocol.seqable)

(defprotocol Seqable
  (seqc [self])
  (ack [self])
  (close [self])
  )