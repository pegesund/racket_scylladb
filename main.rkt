#lang racket

(require "./scylla.rkt")  ; Use our new implementation
(require racket/format)

(displayln "Starting...")

; Create connection using our new implementation
(define conn
  (scylla-connect #:server "localhost"
                  #:port 9042
                  #:username "cassandra"  ; Note: changed from user to username
                  #:password "cassandra"))

(displayln "Connected")

; Try a query
(displayln "Attempting query...")
(displayln "Switching to toldyou keyspace...")
(query conn "use toldyou")
(displayln "Querying users...")
(define users (query conn "select * from users"))
(displayln "Users:")
(for ([user users])
  (printf "~a: ~a (~a)~n" 
          (vector-ref user 6)  ; username
          (vector-ref user 2)  ; email
          (vector-ref user 0))) ; user_id

; Clean up
(displayln "Disconnecting...")
(send conn disconnect)
(displayln "Done")