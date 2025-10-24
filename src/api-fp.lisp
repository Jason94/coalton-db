(cl:in-package :cl-user)
(defpackage :coalton-db/api-fp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/from-row
   #:coalton-db/persistable
   #:coalton-db/queries
   #:coalton-db/db-m
   #:coalton-db/api-helpers
   )
  (:local-nicknames
   (:f  #:coalton-library/monad/free)
   (:ty #:coalton-library/types)
   )
  (:export
   #:query-sql-rows
   #:query-sql-row
   #:query-rows
   #:query-row
   #:execute-query
   #:select-objs
   #:select-obj
   #:delete-obj
   #:insert-obj
   #:insert-objs
   #:insert-obj-returning
   #:insert-objs-returning
   #:update-obj
   #:begin-transaction
   #:commit-transaction
   #:rollback-transaction
   #:with-transaction
   #:do-transaction
   ))
(cl:in-package :coalton-db/api-fp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  ;;;
  ;;; Low Level Query Ops (Return SQL Values)
  ;;;

  (declare query-sql-rows ((Monad :m) (Queryable :q) => :q -> DBM :m (DbResult (List Row))))
  (define (query-sql-rows qry)
    (f:liftF (QueryRows (to-query qry) id)))

  (declare query-sql-row ((Monad :m) (Queryable :q) => :q -> DBM :m (DbResult Row)))
  (define (query-sql-row qry)
    (f:liftF (QueryRows (to-query qry)
                        (fn (input?)
                          (>>= input?
                               (fn (input)
                                 (match input
                                   ((Nil) (err-out-of-vals))
                                   ((Cons row _) (Ok row)))))))))

  (declare query-rows ((Monad :m) (Queryable :q) (ParseSqlRow :p) => :q -> DBM :m (DbResult (List :p))))
  (define (query-rows qry)
    (f:liftF (QueryRows (to-query qry)
                        (fn (input)
                          (>>= input parse-rows)))))

  (declare query-row ((Monad :m) (Queryable :q) (ParseSqlRow :p) => :q -> DBM :m (DbResult :p)))
  (define (query-row qry)
    (f:liftF (QueryRows (to-query qry)
                        (fn (input?)
                          (>>= input?
                               (fn (input)
                                 (match input
                                   ((Nil) (err-out-of-vals))
                                   ((Cons row _)
                                    (parse-row row)))))))))

  (declare execute-query ((Monad :m) (Queryable :q) => :q -> DBM :m (DBResult Unit)))
  (define (execute-query qry)
    (f:liftF (ExecuteQuery (to-query qry) id)))

  ;;;
  ;;; FRM Query Ops
  ;;;

  (declare select-objs_ ((Monad :m) (Persistable :p) => Optional QueryOption -> DBM :m (DbResult (List :p))))
  (define (select-objs_ opt?)
    (let prx-rst = ty:Proxy)
    (let prx-obj = (ty:proxy-inner (ty:proxy-inner (ty:proxy-inner prx-rst))))
    (let tbl-name = (.tbl-name (schema-for prx-obj)))
    (let qry =
      (match opt?
        ((None)
         (Select AllCols (From tbl-name)))
        ((Some opt)
         (Select AllCols (From tbl-name) opt))))
    (ty:as-proxy-of
     (query-rows qry)
     prx-rst))

  (declare select-obj_ ((Monad :m) (Persistable :p) => Optional QueryOption -> DBM :m (DbResult :p)))
  (define (select-obj_ opt?)
    (let prx-rst = ty:Proxy)
    (let prx-obj = (ty:proxy-inner (ty:proxy-inner prx-rst)))
    (let tbl-name = (.tbl-name (schema-for prx-obj)))
    (let qry =
      (match opt?
        ((None)
         (Select AllCols (From tbl-name)))
        ((Some opt)
         (Select AllCols (From tbl-name) opt))))
    (ty:as-proxy-of
     (query-row qry)
     prx-rst))

  (declare delete-obj ((Monad :m) (Persistable :p) => :p -> DBM :m (DbResult Unit)))
  (define (delete-obj obj)
    (f:liftF (ExecuteQuery (to-query (delete-obj-query obj))
                           id)))

  (declare insert-obj ((Monad :m) (Persistable :p) => :p -> DBM :m (DbResult Unit)))
  (define (insert-obj obj)
    (execute-query (insert-obj-query obj None)))

  (declare insert-objs ((Monad :m) (Persistable :p) => List :p -> DBM :m (DbResult Unit)))
  (define (insert-objs objs)
    (match (insert-objs-query objs None)
      ((None) (pure (Ok Unit)))
      ((Some qry)
       (execute-query qry))))

  (declare insert-obj-returning ((Monad :m) (Persistable :p) (ParseSqlRow :r) =>
                                 :p -> DBM :m (DbResult :r)))
  (define (insert-obj-returning obj)
    (let qry = (insert-obj-query obj (Some (Returning AllCols))))
    (query-row qry))

  (declare insert-objs-returning ((Monad :m) (Persistable :p) (ParseSqlRow :r) =>
                                  List :p -> DBM :m (DbResult (List :r))))
  (define (insert-objs-returning objs)
    (match (insert-objs-query objs (Some (Returning AllCols)))
      ((None) (pure (Ok Nil)))
      ((Some qry)
       (query-rows qry))))

  (declare update-obj_ ((Monad :m) (Persistable :p) => :p -> Optional (List String) -> DBM :m (DbResult Unit)))
  (define (update-obj_ obj cols)
    (match (update-obj-query obj cols)
      ((None) (pure (Ok Unit)))
      ((Some qry)
       (execute-query qry))))

  (declare begin-transaction (Monad :m => DBM :m (DbResult Unit)))
  (define begin-transaction
    (execute-query begin-tx-query))

  (declare commit-transaction (Monad :m => DBM :m (DbResult Unit)))
  (define commit-transaction
    (execute-query commit-tx-query))

  (declare rollback-transaction (Monad :m => DBM :m (DbResult Unit)))
  (define rollback-transaction
    (execute-query rollback-tx-query))

  (declare with-transaction (Monad :m => DBM :m (DbResult :a) -> DBM :m (DbResult :a)))
  (define (with-transaction op)
    "Execute the given database operation inside of a transaction. If the operation returns an Err value,
rollback the transaction and bubble the error. Otherwise, commit the transaction and return the Ok value of OP.
If an intermediate query fails but the entire transaction returns an Ok value, it will commit!"
    (do
     begin-transaction
     (result <- op)
     (match result
       ((Err _)
        rollback-transaction)
       ((Ok _)
        commit-transaction))
     (pure result)))
  )

(cl:defmacro select-objs (cl:&optional where?)
  `(select-objs_ ,(optional-clause where?)))

(cl:defmacro select-obj (cl:&optional where?)
  `(select-obj_ ,(optional-clause where?)))

(cl:defmacro update-obj (obj cl:&optional where-cols)
  (cl:let ((where-cols-clause (cl:if where-cols
                                     `(Some (make-list ,@where-cols))
                                     `None)))
    `(update-obj_ ,obj ,where-cols-clause)))

(cl:defmacro do-transaction (cl:&body body)
  `(with-transaction
       (do
        ,@body)))
