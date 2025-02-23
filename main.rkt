#lang racket

(require racket/match
         racket/format
         (only-in "./scylla.rkt" scylla-connect query disconnect prepare bind-params))

(displayln "Starting...")

; Create connection using our new implementation
(define conn
  (scylla-connect #:server "localhost"
                  #:port 9042
                  #:username "cassandra"  
                  #:password "cassandra"))

(displayln "Connected")
(displayln "Attempting query...")

; Switch to keyspace
(query conn "USE toldyou")
(displayln "Switching to toldyou keyspace...")

; Prepare and execute statement
(displayln "Preparing statement...")
(define stmt (prepare conn "SELECT email FROM users WHERE email = ? ALLOW FILTERING"))
(displayln "Querying users...")
(define users (bind-params stmt conn "john.doe@example.com"))
(displayln "Users:")
(displayln users)

(displayln (query conn "select * from users"))

; Clean up
(displayln "Disconnecting...")
(disconnect conn)
(displayln "Done")
