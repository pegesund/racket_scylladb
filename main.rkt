#lang racket

(require racket/match
         racket/format
         (only-in "./scylla.rkt" scylla-connect query disconnect prepare))

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

; Prepare statement
(displayln "Preparing statement...")
(define stmt (prepare conn "SELECT email FROM users WHERE email = ? ALLOW FILTERING"))

; Execute prepared statement
(displayln "Querying users...")
(define bound-stmt (send stmt bind 'query (list "john.doe@example.com")))
(define users (query conn bound-stmt))
(displayln "Users:")
(displayln users)

(displayln (query conn "select * from users"))

; Clean up
(displayln "Disconnecting...")
(disconnect conn)
(displayln "Done")


