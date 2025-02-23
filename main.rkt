#lang racket

(require racket/match
         racket/format
         (only-in "./scylla.rkt" scylla-connect query disconnect prepare query-params query-result))

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
(define result (query-params stmt conn (list "john.doe@example.com")))
(displayln "Users:")
(match result
  [(query-result metadata rows)
   (displayln "Metadata:")
   (displayln metadata)
   (displayln "Rows:")
   (displayln rows)])

(displayln "All users:")
(match (query conn "select * from users")
  [(query-result metadata rows)
   (displayln "Metadata:")
   (displayln metadata)
   (displayln "Rows:")
   (displayln rows)])

(time 
    (for ([i (in-range 100000)])
        (query-params stmt conn (list "john.doe@example.com"))))

; Clean up
(displayln "Disconnecting...")
(disconnect conn)
(displayln "Done")
(displayln result)
