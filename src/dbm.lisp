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
   (:env #:coalton-library/monad/environment)
   (:st #:coalton-library/monad/stateT)
   (:ty #:coalton-library/types)
   (:io #:simple-io/io)
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
   #:QueryVals
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

  (declare run-db! (DatabaseAdapter :d => :d * DB :a -> :a))
  (define (run-db! cnxn op)
    (i:run-identity (run-dbM! cnxn op))))

;;;
;;; Std. Lib. Transformer Instances
;;;

(coalton-toplevel
  (define-instance (st:MonadState :s :m => (st:MonadState :s (DbM :m)))
    (define st:get (lift st:get))
    (define st:put (compose lift st:put))
    (define st:modify (compose lift st:modify))))

;;;
;;; Other DBM Instances
;;;

(coalton-toplevel
  (define-instance (io-u:MonadIoUnique :m => io-u:MonadIoUnique (DBM :m))
    (define io-u:new-unique (lift io-u:new-unique)))

  (io:derive-monad-io :m (DBM :m))
  (io-t:derive-monad-io-term (DBM :m)))
