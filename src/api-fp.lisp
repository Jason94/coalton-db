(cl:in-package :cl-user)
(defpackage :coalton-db/api-fp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/db-m
   )
  (:local-nicknames
   (:f  #:coalton-library/monad/free))
  (:export
   #:query-rows
   ))
(cl:in-package :coalton-db/api-fp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare query-rows ((Monad :m) (Queryable :q) => :q -> DBM :m (DbResult (List Row))))
  (define (query-rows qry)
    (f:liftF (QueryRows (to-query qry) id))))
