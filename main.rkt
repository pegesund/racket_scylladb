#lang racket

(require "./scylla.rkt")  ; Use our new implementation

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
(display "Query result: ")
(display (query conn "use toldyou"))
(displayln (query conn"select * from users"))

; Clean up
(displayln "Disconnecting...")
(send conn disconnect)
(displayln "Done")