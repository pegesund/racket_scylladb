#lang racket/base

(require racket/class
         db/private/cassandra/message
         racket/bytes)

(provide (all-from-out db/private/cassandra/message)
         VERSION)

(define VERSION #x03)  ; Use v3 protocol
