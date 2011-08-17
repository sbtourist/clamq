(ns clamq.protocol.seqable)

(defprotocol Seqable
  (mseq [self])
  (ack [self])
  (close [self])
  )