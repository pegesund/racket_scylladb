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
         query)

(struct response-header (version flags streamid opcode length))

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

    (define (write-stream-id out n)
      (write-bytes (integer->integer-bytes n 2 #t #t) out))  ; signed=true for v3.4

    (define (read-stream-id in)
      (integer-bytes->integer (read-bytes 2 in) #t #t))  ; signed=true for v3.4

    (define/private (start-recv-thread)
      (display "Starting receive thread...") (newline)
      (define thread-ready (make-semaphore 0))
      (define t 
        (thread
         (lambda ()
           (display "Receive thread waiting for go signal...") (newline)
           (semaphore-post thread-ready)  ; Signal that thread is ready to wait
           (semaphore-wait recv-thread-go)
           (display "Receive thread starting...") (newline)
           (with-handlers ([exn? (lambda (e)
                                  (log-error "Receive thread error: ~a" (exn-message e))
                                  (disconnect))])
             (let loop ()
               (display "Receive thread waiting for message...") (newline)
               (define header (read-response-header))
               (display "Got header, streamid: ") 
               (display (response-header-streamid header))
               (display " opcode: ")
               (display (response-header-opcode header))
               (newline)
               (define streamid (response-header-streamid header))
               (define opcode (response-header-opcode header))
               (display "Reading body...") (newline)
               (define body-bytes (read-bytes (response-header-length header) inport))
               (when (or (eof-object? body-bytes) 
                        (< (bytes-length body-bytes) (response-header-length header)))
                 (error 'read-response "connection closed by peer while reading body"))
               (display "Body read, part 1") (newline)
               (display "Body length: ") (display (bytes-length body-bytes)) (newline)
               (display "Body: ") (display body-bytes) (newline)
               (define msg (call-with-input-bytes body-bytes
                           (lambda (in) (msg:read-response-body in opcode))))
               (display "Got message: ") (display msg) (newline)
               (let ([lwac (hash-ref recv-map streamid #f)])
                 (when lwac
                   (display "Found lwac for streamid ") (display streamid) (newline)
                   (set-box! (cdr lwac) msg)
                   (semaphore-post (car lwac))
                   (hash-remove! recv-map streamid)))
               (loop))))))
      (semaphore-wait thread-ready)  ; Wait for thread to be ready
      t)

    (define/private (new-streamid)
      (begin0 next-streamid
        (set! next-streamid
              (if (< next-streamid 32767)  ; Maximum value for signed 16-bit int
                  (add1 next-streamid)
                  1))))

    (define/private (read-response-header)
      (display "Reading response header...") (newline)
      (with-handlers ([exn? (lambda (e)
                             (display "Error reading response header: ") 
                             (display (exn-message e))
                             (newline)
                             (raise e))])
        (define version (msg:io:read-byte inport))
        (when (eof-object? version)
          (error 'read-response-header "connection closed by peer"))
        (display "Got version: ") (display version) (newline)
        (define flags (msg:io:read-byte inport))
        (when (eof-object? flags)
          (error 'read-response-header "connection closed by peer"))
        (display "Got flags: ") (display flags) (newline)
        (define streamid (read-stream-id inport))  ; Use signed stream ID
        (display "Got streamid: ") (display streamid) (newline)
        (define opcode-byte (msg:io:read-byte inport))
        (when (eof-object? opcode-byte)
          (error 'read-response-header "connection closed by peer"))
        (display "Got opcode byte: ") (display opcode-byte) (newline)
        (define length (integer-bytes->integer (read-bytes 4 inport) #f #t))
        (when (eof-object? length)
          (error 'read-response-header "connection closed by peer"))
        (display "Got length: ") (display length) (newline)
        (define opcode (hash-ref msg:int=>opcode opcode-byte))
        (display "Got opcode: ") (display opcode) (newline)
        (response-header version flags streamid opcode length)))

    (define/private (send-message streamid msg)
      (call-with-lock 'send-message
        (lambda ()
          (display "send-message ") (newline)
          (dprintf "  >> #~s ~s\n" streamid msg)
          (display "send-message2 ") (newline)
          (define body-out (open-output-bytes))
          (msg:write-request-body body-out msg)
          (define body-bytes (get-output-bytes body-out))
          (define body-length (bytes-length body-bytes))
          (display "Writing version: ") (display VERSION) (newline)
          (msg:io:write-byte outport VERSION)
          (display "Writing flags: ") (display OUT-FLAGS) (newline)
          (msg:io:write-byte outport OUT-FLAGS)
          (display "Writing streamid: ") (display streamid) (newline)
          (write-stream-id outport streamid)  ; Use signed stream ID
          (display "Writing opcode: ") 
          (display (hash-ref msg:opcode=>int (msg:message->opcode msg)))
          (newline)
          (msg:io:write-byte outport (hash-ref msg:opcode=>int (msg:message->opcode msg)))
          (display "Writing length: ") (display body-length) (newline)
          (write-bytes (integer->integer-bytes body-length 4 #f #t) outport)
          (display "Writing body") (newline)
          (write-bytes body-bytes outport)
          (display "send-message3 ") (newline)
          (flush-output outport)
          (display "send-message4 ") (newline))))

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

    (define/private (read-response)
      (display "Reading response...") (newline)
      (define lwac (hash-ref recv-map 1 #f))
      (unless lwac
        (set! lwac (add-recv-lwachan 1)))
      (lwac-ref lwac))

    (define/private (recv-message who)
      (display "Receiving message...") (newline)
      (match (read-response)
        [(? msg:Error? r)
         (raise-backend-error who r)]
        [r r]))

    (define/private (connect:expect-ready username password)
      (match (recv-message 'cassandra-connect)
        [(msg:Ready)
         (display "Got ready directly") (newline)]
        [(msg:Authenticate auth)
         (display "Got auth request") (newline)
         (unless (and username password)
           (error 'cassandra-connect "username and password required\n  authenticator: ~s" auth))
         (display "Sending auth response") (newline)
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
      (display "Starting connection protocol...") (newline)
      (define recv-thread (start-recv-thread))  ; Start and store the receive thread
      (display "Starting receive loop...") (newline)
      ; Add lwac before starting receive thread
      (define lwac (add-recv-lwachan 1))
      (semaphore-post recv-thread-go)  ; Start the receive loop
      (sleep 0.1)  ; Give the receive thread time to start
      (display "Sending startup message...") (newline)
      (send-message 1 (msg:Startup '(("CQL_VERSION" . "3.4.6"))))  ; Use CQL 3.4.6 with protocol v3
      (display "Waiting for ready...") (newline)
      (define startup-response (lwac-ref lwac))
      (display "Got startup response: ") (display startup-response) (newline)
      (match startup-response
        [(msg:Ready)
         (display "Got ready directly") (newline)]
        [(msg:Authenticate auth)
         (display "Got auth request") (newline)
         (unless (and username password)
           (error 'cassandra-connect "username and password required\n  authenticator: ~s" auth))
         (display "Sending auth response") (newline)
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
      ; Only lock during send
      (call-with-lock who 
        (lambda () 
          (proc streamid)
          (void)))
      ; Wait for response outside the lock
      (k lwac))

    (define/private (query1:send who stmt cursor?)
      (let ([stmt (check-statement who stmt cursor?)])
        (define streamid (new-streamid))
        (define lwac (add-recv-lwachan streamid))
        ; Send message without extra lock
        (match stmt
          [(struct statement-binding (pst params))
           (send-message streamid
                        (msg:Execute (send pst get-handle) consistency params))]
          [(? string? stmt)
           (send-message streamid
                        (msg:Query stmt consistency #f))])
        lwac))

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
      (display "Starting query...") (newline)
      (when cursor? (error who "cursors not supported yet"))
      (display "No cursors") (newline)
      (let ([lw (query1:send who stmt cursor?)])
        (display "  ** done sending query") (newline)
        (display lw) (newline)
        (display "  ** waiting for response") (newline)
        (define result (query1:collect who (lwac-ref lw)))
        (display "  ** got response") (newline)
        result))

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
  c)