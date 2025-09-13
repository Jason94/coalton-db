(cl:in-package :cl-user)
(defpackage :coalton-db/sqlite
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/db)
  (:local-nicknames
   (:sl #:sqlite)
   (:ev #:coalton-library/monad/environment)
   (:io #:simple-io/io)
   )
  (:export
   #:SqlLiteConnection
   #:connect-sqlite!
   #:disconnect-sqlite!
   #:run-sqlite!
   #:run-with-sqlite-connection!
   ))
(in-package :coalton-db/sqlite)

;;;
;;; SQLite Wrapper
;;;

(coalton-toplevel
  (repr :native sl:sqlite-handle)
  (define-type SqlLiteConnection)

  (declare connect-sqlite! (String -> SqlLiteConnection))
  (define (connect-sqlite! connection-spec)
    (lisp :a (connection-spec)
      (sl:connect connection-spec)))

  (declare disconnect-sqlite! (SqlLiteConnection -> Unit))
  (define (disconnect-sqlite! connection)
    (lisp :a (connection)
      (sl:disconnect connection))
    Unit)

  (define-instance (MonadDatabase (ev:EnvT SqlLiteConnection io:IO))
    (define (query-none (Query sql bound-vals))
      (do
       (connection <- ev:ask)
       (lift (io:wrap-io
               (lisp :x (connection sql bound-vals)
                 (cl:let ((unwrapped-bound-vals (cl:mapcar #'coalton-db/db::unwrap-sql-value bound-vals)))
                   (cl:handler-case
                       (cl:progn
                         (cl:apply #'sl:execute-non-query (cl:cons connection (cl:cons sql unwrapped-bound-vals)))
                         (Ok Unit))
                     (cl:error (e)
                       (Err (QueryError (cl:format cl:nil "~a" e)))))))))))
    (define (query-rows (Query sql bound-vals) cols)
      (do
       (connection <- ev:ask)
       (let types = (map .type cols))
       (lift (io:wrap-io
               (lisp :x (connection sql bound-vals types)
                 (cl:handler-case
                     (cl:progn
                       (cl:let* ((unwrapped-bound-vals (cl:mapcar #'coalton-db/db::unwrap-sql-value bound-vals))
                                 (rows (cl:apply
                                        #'sl:execute-to-list
                                        (cl:cons connection (cl:cons sql unwrapped-bound-vals)))))
                         (Ok (cl:mapcar
                              (cl:lambda (row)
                                (cl:mapcar #'coalton-db/db::wrap-raw-sql-value types row))
                              rows))))
                   (cl:error (e)
                     (Err (QueryError (cl:format cl:nil "~a" e)))))))))))

  (declare run-sqlite!_ (ev:EnvT SqlLiteConnection io:IO :a -> SqlLiteConnection -> :a))
  (define (run-sqlite!_ op connection)
    (io:run! (ev:run-envT op connection)))

  (declare run-sqlite! (DbOp (ev:EnvT SqlLiteConnection io:IO) (QueryResult :a) -> SqlLiteConnection -> QueryResult :a))
  (define run-sqlite! (compose run-sqlite!_ run-dbop))

  (declare run-with-sqlite-connection! (SqlLiteConnection -> DbOp (ev:EnvT SqlLiteConnection io:IO) (QueryResult :a) -> QueryResult :a))
  (define (run-with-sqlite-connection! connection op)
    (run-sqlite!_ (run-dbop op) connection)))
