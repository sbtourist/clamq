(ns clamq.protocol.producer)

(defprotocol Producer
  (publish [self destination message] [self destination message attributes]))