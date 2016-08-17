(ns clamq.protocol.producer)

(defprotocol Producer
  (publish [self destination message] [self destination message attributes]) 
  (request-reply [self destination message][self destination message attributes]))
