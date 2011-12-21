(ns clamq.protocol.consumer)

(defprotocol Consumer
  (start [self])
  (close [self]))