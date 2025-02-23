#lang racket/base

(require racket/class
         db/private/cassandra/message)

; Modify the protocol version and exports
(provide (all-from-out db/private/cassandra/message)
         VERSION)

(define VERSION #x04)  ; Changed from #x03 for v3.4.6