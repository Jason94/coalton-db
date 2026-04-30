(cl:in-package :cl-user)
(defpackage :coalton-db/api-imp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/from-row
   #:coalton-db/persistable
   #:coalton-db/queries
   #:coalton-db/api-helpers)
  (:local-nicknames
   (:r #:coalton-library/result)
   (:op #:coalton-library/optional)
   (:ty #:coalton-library/types))
  (:export
   ;;; Library Public
   #:query-sql-rows!
   #:query-sql-rows!#
   #:query-sql-row!
   #:query-sql-row!#
   #:execute-query!
   #:execute-query!#
   #:query-rows!
   #:query-rows!#
   #:query-row!
   #:query-row!#

   #:select-objs!
   #:select-objs!#
   #:select-obj!
   #:select-obj!#
   #:delete-obj!
   #:delete-obj!#
   #:insert-obj!
   #:insert-obj!#
   #:insert-objs!
   #:insert-objs!#
   #:insert-obj-returning!
   #:insert-obj-returning!#
   #:insert-objs-returning!
   #:insert-objs-returning!#
   #:update-obj!
   #:update-obj!#

   #:begin-transaction!
   #:commit-transaction!
   #:rollback-transaction!
   #:with-transaction
   ))
(cl:in-package :coalton-db/api-imp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  ;;;
  ;;; Low Level Query Ops (Return SQL Values)
  ;;;

  (declare query-sql-rows! ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> DbResult (List Row)))
  (define (query-sql-rows! cnxn qry)
    (run-query! cnxn
                (unwrap-query-container (ty:proxy-of cnxn) (to-query qry))))

  (declare query-sql-rows!# ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> List Row))
  (define (query-sql-rows!# cnxn qry)
    (r:ok-or-error (query-sql-rows! cnxn qry)))

  (declare query-sql-row! ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> DbResult Row))
  (define (query-sql-row! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (fn (input)
           (match input
             ((Nil) (err-out-of-vals))
             ((Cons row _) (Ok row))))))

  (declare query-sql-row!# ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> Row))
  (define (query-sql-row!# cnxn qry)
    (r:ok-or-error (query-sql-row! cnxn qry)))

  (declare execute-query! ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> DbResult Unit))
  (define (execute-query! cnxn qry)
    (execute-query!_ cnxn
                     (unwrap-query-container (ty:proxy-of cnxn) (to-query qry))))

  (declare execute-query!# ((DatabaseAdapter :d) (Queryable :q) => :d * :q -> Void))
  (define (execute-query!# cnxn qry)
    (r:ok-or-error (execute-query! cnxn qry))
    (values))

  (declare query-rows! ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d * :q -> DbResult (List :p)))
  (define (query-rows! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (fn (x)
           (traverse parse-row x))))

  (declare query-rows!# ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d * :q -> List :p))
  (define (query-rows!# cnxn qry)
    (r:ok-or-error (query-rows! cnxn qry)))

  (declare query-row! ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d * :q -> DbResult :p))
  (define (query-row! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (fn (input)
           (match input
             ((Nil) (err-out-of-vals))
             ((Cons row _) (parse-row row))))))

  (declare query-row!# ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d * :q -> :p))
  (define (query-row!# cnxn qry)
    (r:ok-or-error (query-row! cnxn qry))))

;;;
;;; FRM Query Ops
;;;

(coalton-toplevel
  (declare select-objs!_ ((DatabaseAdapter :d) (Persistable :p) => :d * Optional QueryOption -> DbResult (List :p)))
  (define (select-objs!_ cnxn opt?)
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
     (query-rows! cnxn  qry)
     prx-rst))

  (declare select-objs!#_ ((DatabaseAdapter :d) (Persistable :p) => :d * Optional QueryOption -> List :p))
  (define (select-objs!#_ cnxn opt)
    (r:ok-or-error (select-objs!_ cnxn opt)))

  (declare select-obj!_ ((DatabaseAdapter :d) (Persistable :p) => :d * Optional QueryOption -> DbResult :p))
  (define (select-obj!_ cnxn opt?)
    (let prx-rst = ty:Proxy)
    (let prx-obj = (ty:proxy-inner prx-rst))
    (let tbl-name = (.tbl-name (schema-for prx-obj)))
    (let qry =
      (match opt?
        ((None)
         (Select AllCols (From tbl-name)))
        ((Some opt)
         (Select AllCols (From tbl-name) opt))))
    (ty:as-proxy-of
     (query-row! cnxn  qry)
     prx-rst))

  (declare select-obj!#_ ((DatabaseAdapter :d) (Persistable :p) => :d * Optional QueryOption -> :p))
  (define (select-obj!#_ cnxn opt)
    (r:ok-or-error (select-obj!_ cnxn opt)))

  (declare delete-obj! ((DatabaseAdapter :d) (Persistable :p) => :d * :p -> DbResult Unit))
  (define (delete-obj! cnxn obj)
    (execute-query! cnxn (delete-obj-query obj)))

  (declare delete-obj!# ((DatabaseAdapter :d) (Persistable :p) => :d * :p -> Void))
  (define (delete-obj!# cnxn obj)
    (r:ok-or-error (delete-obj! cnxn obj))
    (values))

  (declare insert-obj! ((DatabaseAdapter :d) (Persistable :p) => :d * :p -> DbResult Unit))
  (define (insert-obj! cnxn obj)
    (execute-query! cnxn (insert-obj-query obj None)))

  (declare insert-obj!# ((DatabaseAdapter :d) (Persistable :p) => :d * :p -> Void))
  (define (insert-obj!# cnxn obj)
    (r:ok-or-error (insert-obj! cnxn obj))
    (values))

  (declare insert-objs! ((DatabaseAdapter :d) (Persistable :p) => :d * List :p -> DbResult Unit))
  (define (insert-objs! cnxn objs)
    (match (insert-objs-query objs None)
      ((None) (pure Unit))
      ((Some qry)
       (execute-query! cnxn qry))))

  (declare insert-objs!# ((DatabaseAdapter :d) (Persistable :p) => :d * List :p -> Void))
  (define (insert-objs!# cnxn objs)
    (r:ok-or-error (insert-objs! cnxn objs))
    (values))

  (declare insert-obj-returning! ((DatabaseAdapter :d) (Persistable :p) (ParseSqlRow :r) => :d * :p -> DbResult :r))
  (define (insert-obj-returning! cnxn obj)
    (let qry = (insert-obj-query obj (Some (Returning AllCols))))
    (query-row! cnxn qry))

  (declare insert-obj-returning!# ((DatabaseAdapter :d) (Persistable :p) (ParseSqlRow :r) => :d * :p -> :r))
  (define (insert-obj-returning!# cnxn obj)
    (r:ok-or-error (insert-obj-returning! cnxn obj)))

  (declare insert-objs-returning! ((DatabaseAdapter :d) (Persistable :p) (ParseSqlRow :r)
                                   => :d * List :p -> DbResult (List :r)))
  (define (insert-objs-returning! cnxn objs)
    (let qry? = (insert-objs-query objs (Some (Returning AllCols))))
    (match qry?
      ((None) (pure Nil))
      ((Some qry)
       (query-rows! cnxn qry))))

  (declare insert-objs-returning!# ((DatabaseAdapter :d) (Persistable :p) (ParseSqlRow :r) =>
                                    :d * List :p -> List :r))
  (define (insert-objs-returning!# cnxn objs)
    (r:ok-or-error (insert-objs-returning! cnxn objs)))

  (declare update-obj!_ ((DatabaseAdapter :d) (Persistable :p) => :d * :p * Optional (List String) -> DbResult Unit))
  (define (update-obj!_ cnxn obj where-cols?)
    (match (update-obj-query obj where-cols?)
      ((Some qry)
       (execute-query! cnxn qry))
      ((None)
       (pure Unit))))

  (declare update-obj!#_ ((DatabaseAdapter :d) (Persistable :p) => :d * :p * Optional (List String) -> Void))
  (define (update-obj!#_ cnxn obj where-cols?)
    (r:ok-or-error (update-obj!_ cnxn obj where-cols?))
    (values))
  )

(cl:defmacro select-objs! (cnxn cl:&optional where?)
  `(select-objs!_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-objs!# (cnxn cl:&optional where?)
  `(select-objs!#_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-obj! (cnxn cl:&optional where?)
  `(select-obj!_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-obj!# (cnxn cl:&optional where?)
  `(select-obj!#_ ,cnxn ,(optional-clause where?)))

(cl:defmacro update-obj! (cnxn obj cl:&optional where-cols)
  (cl:let ((where-cols-clause (cl:if where-cols
                                     `(Some (make-list ,@where-cols))
                                     `None)))
    `(update-obj!_ ,cnxn ,obj ,where-cols-clause)))

(cl:defmacro update-obj!# (cnxn obj cl:&optional where-cols)
  (cl:let ((where-cols-clause (cl:if where-cols
                                     `(Some (make-list ,@where-cols))
                                     `None)))
    `(update-obj!#_ ,cnxn ,obj ,where-cols-clause)))

;;;
;;; Transaction Support
;;;

(coalton-toplevel
  (declare begin-transaction! (DatabaseAdapter :d => :d -> DbResult Unit))
  (define (begin-transaction! cnxn)
    (execute-query! cnxn begin-tx-query))

  (declare commit-transaction! (DatabaseAdapter :d => :d -> DbResult Unit))
  (define (commit-transaction! cnxn)
    (execute-query! cnxn commit-tx-query))

  (declare rollback-transaction! (DatabaseAdapter :d => :d -> DbResult Unit))
  (define (rollback-transaction! cnxn)
    (execute-query! cnxn rollback-tx-query))
  )

(cl:defmacro with-transaction (cnxn cl:&body body)
  "Run a transaction. If the body errors, rollback the transaction and
return the exception. If the body succeeds, commit the transaction and
return the value of the last transaction in the `body`."
  (cl:let ((result-sym (cl:gensym "transaction-result")))
    `(catch
         (progn
           (begin-transaction! ,cnxn)
           (let ((,result-sym
                   (progn
                     ,@body)))
             (commit-transaction! ,cnxn)
             ,result-sym))
       (_ (rollback-transaction! ,cnxn)
          (Err (QueryError "Encountered a query error inside the transaction."))))))
