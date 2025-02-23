#lang racket/base

(require racket/class
         racket/match
         racket/tcp
         racket/port
         db/private/cassandra/connection 
         db/private/generic/sql-data
         db/private/generic/interfaces     
         db/private/generic/prepared      
         (prefix-in msg: db/private/cassandra/message)
         "scylla-message.rkt")

(provide scylla-connect
         query
         disconnect
         prepare
         query-params
         query-result
         (struct-out query-result))

(struct response-header (version flags streamid opcode length))
(struct query-result (metadata rows) #:transparent)

(define (query-params stmt conn params)
  (if conn
      (query conn (send stmt bind 'query params))
      (send stmt bind 'query params)))

(define (query connection stmt . args)
  (if (null? args)
      (send connection query 'query stmt #f)
      (send connection query 'query (send stmt bind 'query args) #f)))

(define (disconnect connection)
  (send connection disconnect))

(define (prepare connection query-text)
  (send connection prepare 'prepare query-text))

(define (raise-backend-error who msg)
  (match msg
    [(msg:Error code msg)
     (error who "error ~s: ~s" code msg)]))

(define scylla-connection%
  (class connection%
    (inherit call-with-lock
             dprintf
             disconnect)

    (field [recv-thread-go (make-semaphore 0)]
           [next-streamid 1]
           [recv-map (make-hash)]
           [consistency 'one])

    (super-new)
    (inherit-field inport outport)

    (define/public (get-recv-thread-go)
      recv-thread-go)

    (define (write-stream-id out n)
      (write-bytes (integer->integer-bytes n 2 #t #t) out))  ; signed=true for v3.4

    (define (read-stream-id in)
      (integer-bytes->integer (read-bytes 2 in) #t #t))  ; signed=true for v3.4

    (define/private (start-recv-thread)
      (define thread-ready (make-semaphore 0))
      (define t 
        (thread
         (lambda ()
           (semaphore-post thread-ready)
           (semaphore-wait recv-thread-go)
           (with-handlers ([exn? (lambda (e)
                                  (log-error "Receive thread error: ~a" (exn-message e))
                                  (disconnect))])
             (let loop ()
               (define header (read-response-header))
               (define streamid (response-header-streamid header))
               (define opcode (response-header-opcode header))
               (define body-bytes (read-bytes (response-header-length header) inport))
               (when (or (eof-object? body-bytes) 
                        (< (bytes-length body-bytes) (response-header-length header)))
                 (error 'read-response "connection closed by peer while reading body"))
               (define msg (call-with-input-bytes body-bytes
                           (lambda (in) (msg:read-response-body in opcode))))
               (let ([lwac (hash-ref recv-map streamid #f)])
                 (when lwac
                   (set-box! (cdr lwac) msg)
                   (semaphore-post (car lwac))
                   (hash-remove! recv-map streamid)))
               (loop))))))
      (semaphore-wait thread-ready)
      t)

    (define/private (new-streamid)
      (begin0 next-streamid
        (set! next-streamid
              (if (< next-streamid 32767)  ; Maximum value for signed 16-bit int
                  (add1 next-streamid)
                  1))))

    (define/private (read-response-header)
      (with-handlers ([exn? (lambda (e)
                             (raise e))])
        (define version (msg:io:read-byte inport))
        (when (eof-object? version)
          (error 'read-response-header "connection closed by peer"))
        (define flags (msg:io:read-byte inport))
        (when (eof-object? flags)
          (error 'read-response-header "connection closed by peer"))
        (define streamid (read-stream-id inport))  ; Use signed stream ID
        (define opcode-byte (msg:io:read-byte inport))
        (when (eof-object? opcode-byte)
          (error 'read-response-header "connection closed by peer"))
        (define length (integer-bytes->integer (read-bytes 4 inport) #f #t))
        (when (eof-object? length)
          (error 'read-response-header "connection closed by peer"))
        (define opcode (hash-ref msg:int=>opcode opcode-byte))
        (response-header version flags streamid opcode length)))

    (define/private (send-message streamid msg)
      (call-with-lock 'send-message
        (lambda ()
          (dprintf "  >> #~s ~s\n" streamid msg)
          (define body-out (open-output-bytes))
          (msg:write-request-body body-out msg)
          (define body-bytes (get-output-bytes body-out))
          (define body-length (bytes-length body-bytes))
          (msg:io:write-byte outport VERSION)
          (msg:io:write-byte outport OUT-FLAGS)
          (write-stream-id outport streamid)  ; Use signed stream ID
          (msg:io:write-byte outport (hash-ref msg:opcode=>int (msg:message->opcode msg)))
          (write-bytes (integer->integer-bytes body-length 4 #f #t) outport)
          (write-bytes body-bytes outport)
          (flush-output outport))))

    (define/private (add-recv-lwachan streamid)
      (define lwac (cons (make-semaphore 0) (box #f)))
      (hash-set! recv-map streamid lwac)
      lwac)

    (define/private (check-statement who stmt cursor?)
      (cond [(statement-binding? stmt)
             (let ([pst (statement-binding-pst stmt)])
               (send pst check-owner who this stmt)
               stmt)]
            [(prepared-statement? stmt)
             (send stmt check-owner who this stmt)
             stmt]
            [(string? stmt) stmt]
            [else (error who "not a statement: ~e" stmt)]))

    (define/private (read-response)
      (define lwac (hash-ref recv-map 1 #f))
      (unless lwac
        (set! lwac (add-recv-lwachan 1)))
      (lwac-ref lwac))

    (define/private (recv-message who)
      (match (read-response)
        [(? msg:Error? r)
         (raise-backend-error who r)]
        [r r]))

    (define/private (connect:expect-ready username password)
      (match (recv-message 'cassandra-connect)
        [(msg:Ready) (void)]
        [(msg:Authenticate auth)
         (unless (and username password)
           (error 'cassandra-connect "username and password required\n  authenticator: ~s" auth))
         (send-message 1 (msg:AuthResponse 
                         (bytes-append (string->bytes/utf-8 username)
                                     (bytes 0)
                                     (string->bytes/utf-8 username)
                                     (bytes 0)
                                     (string->bytes/utf-8 password))))
         (define auth-response (recv-message 'cassandra-connect))
         (unless (msg:AuthSuccess? auth-response)
           (error 'cassandra-connect "authentication failed"))]))

    (define/override (start-connection-protocol username password)
      (define recv-thread (start-recv-thread))
      (define lwac (add-recv-lwachan 1))
      (semaphore-post recv-thread-go)
      (sleep 0.1)
      (send-message 1 (msg:Startup '(("CQL_VERSION" . "3.4.6"))))
      (define startup-response (lwac-ref lwac))
      (match startup-response
        [(msg:Ready) (void)]
        [(msg:Authenticate auth)
         (unless (and username password)
           (error 'cassandra-connect "username and password required\n  authenticator: ~s" auth))
         (define auth-lwac (add-recv-lwachan 1))
         (send-message 1 (msg:AuthResponse 
                         (bytes-append (string->bytes/utf-8 username)
                                     (bytes 0)
                                     (string->bytes/utf-8 username)
                                     (bytes 0)
                                     (string->bytes/utf-8 password))))
         (define auth-response (lwac-ref auth-lwac))
         (unless (msg:AuthSuccess? auth-response)
           (error 'cassandra-connect "authentication failed"))]))

    (define/private (call/lock/streamid who proc [k values])
      (define streamid (new-streamid))
      (define lwac (add-recv-lwachan streamid))
      (call-with-lock who 
        (lambda () 
          (proc streamid)
          (void)))
      (k lwac))

    (define/private (query1:send who stmt consistency cursor?)
      (let ([stmt (check-statement who stmt cursor?)])
        (define streamid (new-streamid))
        (define lwac (add-recv-lwachan streamid))
        (match stmt
          [(? prepared-statement? pst)
           (send-message streamid
                        (msg:Execute (send pst get-handle) consistency null))]
          [(struct statement-binding (pst params))
           (send-message streamid
                        (msg:Execute (send pst get-handle) consistency params))]
          [(? string? stmt)
           (send-message streamid
                        (msg:Query stmt consistency cursor?))])
        lwac))

    (define/private (query1:collect who msg)
      (match msg
        [(msg:Result:Rows _ pagestate info rows)
         (query-result info rows)]
        [(msg:Result:Void _)
         (query-result null null)]
        [(msg:Result:SetKeyspace _ keyspace)
         (query-result null null)]
        [(msg:Error code msg)
         (error 'scylla-query "query error ~s: ~s" code msg)]))

    (define/override (query who stmt cursor?)
      (let ([stmt (check-statement who stmt cursor?)])
        (define streamid (new-streamid))
        (define lwac (add-recv-lwachan streamid))
        (match stmt
          [(? prepared-statement? pst)
           (send-message streamid
                        (msg:Execute (send pst get-handle) consistency null))]
          [(struct statement-binding (pst params))
           (send-message streamid
                        (msg:Execute (send pst get-handle) consistency params))]
          [(? string? stmt)
           (send-message streamid
                        (msg:Query stmt consistency cursor?))])
        (define result (lwac-ref lwac))
        (query1:collect who result)))

    (define/override (prepare who query-text)
      (define streamid (new-streamid))
      (define lwac (add-recv-lwachan streamid))
      (send-message streamid (msg:Prepare query-text))
      (match (lwac-ref lwac)
        [(msg:Result:Prepared _ id metadata result-metadata)
         (new prepared-statement%
              [owner this]
              [handle id]
              [close-on-exec? #t]
              [param-typeids (map (lambda (x) (vector-ref x 3)) metadata)]  ; Get the type from metadata vector
              [result-dvecs result-metadata]
              [stmt query-text])]
        [(msg:Error code msg)
         (error 'scylla-prepare "prepare error ~s: ~s" code msg)]))

    (define/private (lwac-ref lwac)
      (semaphore-wait (car lwac))
      (unbox (cdr lwac)))))

(define (scylla-connect #:server [server "localhost"]
                       #:port [port 9042]
                       #:username [username #f]
                       #:password [password #f])
  (define-values (in out) (tcp-connect server port))
  (define c (new scylla-connection% 
                 [inport in]
                 [outport out]))
  (with-handlers ([exn? (lambda (e)
                         (send c disconnect)
                         (raise e))])
    (send c start-connection-protocol username password))
  c)