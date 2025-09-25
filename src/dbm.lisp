(cl:in-package :cl-user)
(defpackage :coalton-db/db-m
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   )
  (:local-nicknames
   (:i #:coalton-library/monad/identity)
   (:f  #:coalton-library/monad/free)
   (:ft #:coalton-library/monad/freet)
   (:ty #:coalton-library/types))
  (:export
   #:DBM
   #:DB

   #:query-rows

   #:run-dbM!
   #:run-db!))
(cl:in-package :coalton-db/db-m)

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

;;;
;;; Monad Interface
;;;

(coalton-toplevel
  (define-type (DbF :next)
    (QueryRows QueryContainer (DbResult (List Row) -> :next)))

  (define-instance (Functor DbF)
    (define (map f db-f)
      (match db-f
        ((QueryRows qry cont) (QueryRows qry (map f cont))))))

  (define-type-alias DBM (ft:FReeT DbF))
  (define-type-alias DB (DBM i:Identity))

  (declare query-rows ((Monad :m) (Queryable :q) => :q -> DBM :m (DbResult (List Row))))
  (define (query-rows qry)
    (f:liftF (QueryRows (to-query qry) id))))

;;;
;;; Interpreter
;;;

(coalton-toplevel
  (declare run-dbM! ((DatabaseAdapter :d) (Monad :m) => :d -> DBM :m (DbResult :a) -> :m (DbResult :a)))
  (define (run-dbM! cnxn op)
    (do
     (step <- (ft:run-freeT op))
     (match step
       ((ft:Val a) (pure a))
       ((ft:FreeF op)
        (match op
          ((QueryRows qry next)
           (let result = (run-query! cnxn (unwrap-query-container (ty:proxy-of cnxn) qry)))
           (run-dbM! cnxn (next result))))))))

  (declare run-db! (DatabaseAdapter :d => :d -> DB (DbResult :a) -> DbResult :a))
  (define (run-db! cnxn op)
    (i:run-identity (run-dbM! cnxn op))))
