(cl:in-package :cl-user)
(defpackage :coalton-db/api-helpers
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   )
  (:local-nicknames
   (:ty #:coalton-library/types))
  (:export
   ;;; Library Private
   #:QueryContainer
   #:Queryable
   #:to-query
   #:unwrap-query-container
   ))
(cl:in-package :coalton-db/api-helpers)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Queryable Typeclass
;;;

(coalton-toplevel
  (define-type QueryContainer
    (StringQuery String)
    (SqlQueryQuery SqlQuery)
    (QueryQuery Query))

  (declare unwrap-query-container (DatabaseAdapter :a => ty:Proxy :a -> QueryContainer -> SqlQuery))
  (define (unwrap-query-container db-ty qry)
    (match qry
      ((StringQuery sql)
       (SqlQuery sql (make-list)))
      ((SqlQueryQuery sql-qry)
       sql-qry)
      ((QueryQuery qry)
       (to-sql db-ty qry))))

  (define-class (Queryable :a)
    "A type that can be converted to a `SqlQuery` in the context of a connection
type, such that it could be run on an instance of that connection. The main purpose
of the typeclass is to provide syntactic sugar to make it easier to run plain-string
queries in some places."
    (to-query (:a -> QueryContainer)))

  (define-instance (Queryable SqlQuery)
    (inline)
    (define to-query SqlQueryQuery))

  (define-instance (Queryable String)
    (inline)
    (define to-query StringQuery))

  (define-instance (Queryable Query)
    (inline)
    (define to-query QueryQuery)))
