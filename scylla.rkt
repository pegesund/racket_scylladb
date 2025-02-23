#lang racket/base

(require racket/class
         racket/match
         racket/tcp
         db/private/cassandra/connection 
         db/private/generic/sql-data
         db/private/generic/interfaces     ; Add this for statement-binding
         db/private/generic/prepared      ; Add this for prepared-statement%
         (prefix-in msg: db/private/cassandra/message)
         "scylla-message.rkt")

(provide scylla-connect
         query)

(define (query connection stmt . args)
  (send connection query 'query stmt #f))

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

    (define/private (new-streamid)
      (begin0 next-streamid
        (set! next-streamid
              (if (< next-streamid 16000) 
                  (add1 next-streamid)
                  1))))

    (define/private (add-recv-lwachan streamid)
      (define lwac (cons (make-semaphore 0) (box #f)))
      (hash-set! recv-map streamid lwac)
      lwac)

    (define/private (check-statement who stmt cursor?)
      (display "Checking statement ") (newline)
      (cond [(statement-binding? stmt)
             (let ([pst (statement-binding-pst stmt)])
               (display "  ** statement binding") (newline)
               (send pst check-owner who this stmt)
               stmt)]
            [(string? stmt) stmt]))

    (define/private (send-message streamid msg)
      (display "send-message ") (newline)
      (dprintf "  >> #~s ~s\n" streamid msg)
      (display "send-message2 ") (newline)
      (msg:write-message outport msg streamid)
      (display "send-message3 ") (newline)
      (flush-output outport)
      (display "send-message4 ") (newline))

    (define/private (read-response)
      (let ([r (msg:read-response inport)])
        (match r
          [(cons resp streamid)
           (dprintf "  << #~s ~s\n" streamid resp)
           (cond [(msg:Error? resp)
                  (raise-backend-error 'scylla-query resp)]
                 [else r])])))

    (define/private (recv-message who)
      (match (read-response)
        [(cons r streamid)
         (dprintf "  << #~s ~s\n" streamid r)
         r]))

    (define/private (connect:expect-ready username password)
      (let loop ()
        (let ([r (recv-message 'cassandra-connect)])
          (match r
            [(or (msg:Ready) (msg:AuthSuccess _))
             (semaphore-post recv-thread-go)]
            [(msg:Authenticate auth)
             (unless (and username password)
               (error 'cassandra-connect "username and password required\n  authenticator: ~s" auth))
             (send-message 1 (msg:AuthResponse 
                            (bytes-append (string->bytes/utf-8 username)
                                        (bytes 0)
                                        (string->bytes/utf-8 username)
                                        (bytes 0)
                                        (string->bytes/utf-8 password))))
             (loop)]))))

    (define/override (start-connection-protocol username password)
      (call-with-lock 'scylla-connect
        (lambda ()
          (send-message 1 (msg:Startup '(("CQL_VERSION" . "3.4.6"))))
          (connect:expect-ready username password))))

    (define/private (call/lock/streamid who proc [k values])
      (define streamid (new-streamid))
      (define lwac (add-recv-lwachan streamid))
      ; Only lock during send
      (call-with-lock who 
        (lambda () 
          (proc streamid)
          (void)))
      ; Wait for response outside the lock
      (k lwac))

    (define/private (query1:send who stmt cursor?)
      (let ([stmt (check-statement who stmt cursor?)])
        (call/lock/streamid who
          (lambda (streamid)
            (match stmt
              [(struct statement-binding (pst params))
               (send-message streamid
                           (msg:Execute (send pst get-handle) consistency params))]
              [(? string? stmt)
               (send-message streamid
                           (msg:Query stmt consistency #f))])))))

    (define/private (query1:collect who msg)
      (match msg
        [(msg:Result:Void _) (void)]
        [(msg:Result:Rows metadata flags rows _) rows]
        [(msg:Result:SetKeyspace _ keyspace) (void)]
        [(msg:Result:Prepared _ id _ _)
         (new prepared-statement%
              [-owner this]
              [handle id]
              [close-on-exec? #t]
              [param-typeids null]
              [result-dvecs null])]
        [(msg:Error code msg)
         (error 'scylla-query "query error ~s: ~s" code msg)]))

    (define/override (query who stmt cursor?)
      (display "Petter was here") (newline)
      (when cursor? (error who "cursors not supported yet"))
      (display "No cursors") (newline)
      (let ([lw (query1:send who stmt cursor?)])
        (display "  ** done querying") 
        (display lw) (newline)
        (query1:collect who (lwac-ref lw))))

    (define/private (lwac-ref lwac)
      (display "waiting for semaphore ") (newline)
      (semaphore-wait (car lwac))
      (display "got it") (newline)
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
  (semaphore-post (send c get-recv-thread-go))
  c)