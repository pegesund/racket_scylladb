#lang racket/base

(require racket/class
         db/private/cassandra/message
         racket/bytes)

(provide (all-from-out db/private/cassandra/message)
         read-response-body
         VERSION)

(define VERSION #x03)  ; Use v3 protocol

(define (read-response-body in opcode)
  (define (read-bytes-safe n)
    (define bytes (read-bytes n in))
    (when (or (eof-object? bytes) (< (bytes-length bytes) n))
      (error 'read-response-body "connection closed by peer while reading ~a bytes, got ~a" 
             n (if (eof-object? bytes) "EOF" (bytes-length bytes))))
    bytes)
  
  (define (read-int)
    (integer-bytes->integer (read-bytes-safe 4) #f #t))

  (define (read-string)
    (define len (read-int))
    (bytes->string/utf-8 (read-bytes-safe len)))

  (define (read-string-map)
    (define count (read-int))
    (for/hash ([i (in-range count)])
      (values (read-string) (read-string))))

  (case opcode
    [(ERROR)
     (Error (read-int)
            (read-string))]
    [(READY)
     (Ready)]
    [(AUTHENTICATE)
     (Authenticate (read-string))]
    [(SUPPORTED)
     (Supported (read-string-map))]
    [(RESULT)
     (case (read-int)
       [(#x0001)
        (Result:Void 'VOID)]
       [(#x0002)
        (define-values (pagestate column-count info) (read-Metadata in #t))
        (define rows-count (read-int))
        (define rows (for/list ([i (in-range rows-count)]) (read-Row in column-count)))
        (for-each (make-decode-row! info) rows)
        (Result:Rows 'ROWS pagestate info rows)])]
    [else
     (error 'read-response-body "unknown opcode ~a" opcode)]))