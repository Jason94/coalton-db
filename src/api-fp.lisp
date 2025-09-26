(cl:in-package :cl-user)
(defpackage :coalton-db/api-fp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/db-m
   #:coalton-db/api-helpers
   )
  (:local-nicknames
   (:f  #:coalton-library/monad/free))
  (:export
   #:query-rows
   #:execute-query
   ))
(cl:in-package :coalton-db/api-fp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare query-rows ((Monad :m) (Queryable :q) => :q -> DBM :m (DbResult (List Row))))
  (define (query-rows qry)
    (f:liftF (QueryRows (to-query qry) id)))

  (declare execute-query ((Monad :m) (Queryable :q) => :q -> DBM :m (DBResult Unit)))
  (define (execute-query qry)
    (f:liftF (ExecuteQuery (to-query qry) id))))
