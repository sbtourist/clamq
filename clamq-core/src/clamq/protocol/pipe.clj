(ns clamq.protocol.pipe)

(defprotocol Pipe
  (open [self])
  (close [self]))