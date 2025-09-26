(cl:in-package :cl-user)
(defpackage :coalton-db/db-m
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/api-helpers
   )
  (:local-nicknames
   (:i #:coalton-library/monad/identity)
   (:ft #:coalton-library/monad/freet)
   (:ty #:coalton-library/types)
   (:io-t #:simple-io/term)
   (:io-u #:simple-io/unique))
  (:export
   ;;; Library Public
   #:DBM
   #:DB

   #:run-dbM!
   #:run-db!
   ;;; Library Private
   #:QueryRows
   #:ExecuteQuery
   ))
(cl:in-package :coalton-db/db-m)

(named-readtables:in-readtable coalton:coalton)


;;;
;;; Monad Interface
;;;

(coalton-toplevel
  (define-type (DbF :next)
    (QueryRows QueryContainer (DbResult (List Row) -> :next))
    (ExecuteQuery QueryContainer (DbResult Unit -> :next)))

  (define-instance (Functor DbF)
    (define (map f db-f)
      (match db-f
        ((QueryRows qry cont) (QueryRows qry (map f cont)))
        ((ExecuteQuery qry cont) (ExecuteQuery qry (map f cont))))))

  (define-type-alias DBM (ft:FReeT DbF))
  (define-type-alias DB (DBM i:Identity)))

;;;
;;; Interpreter
;;;

(coalton-toplevel
  (declare run-dbM! ((DatabaseAdapter :d) (Monad :m) => :d -> DBM :m :a -> :m :a))
  (define (run-dbM! cnxn op)
    (do
     (step <- (ft:run-freeT op))
     (match step
       ((ft:Val a) (pure a))
       ((ft:FreeF op)
        (match op
          ((QueryRows qry next)
           (let result = (run-query! cnxn (unwrap-query-container (ty:proxy-of cnxn) qry)))
           (run-dbM! cnxn (next result)))
          ((ExecuteQuery qry next)
           (let result = (execute-query!_ cnxn (unwrap-query-container (ty:proxy-of cnxn) qry)))
           (run-dbM! cnxn (next result))))))))

  (declare run-db! (DatabaseAdapter :d => :d -> DB :a -> :a))
  (define (run-db! cnxn op)
    (i:run-identity (run-dbM! cnxn op))))

;;;
;;; Other DBM Instances
;;;

(coalton-toplevel
  (define-instance (io-u:MonadIoUnique :m => io-u:MonadIoUnique (DBM :m))
    (define io-u:new-unique (lift io-u:new-unique)))

  (define-instance (io-t:MonadIoTerm :m => io-t:MonadIoTerm (DBM :m))
    (define io-t:write-line (compose lift io-t:write-line))
    (define io-t:read-line (lift io-t:read-line))))
