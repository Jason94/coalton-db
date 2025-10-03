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
   ))
(cl:in-package :coalton-db/api-imp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare query-sql-rows! ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> DbResult (List Row)))
  (define (query-sql-rows! cnxn qry)
    (run-query! cnxn
                (unwrap-query-container (ty:proxy-of cnxn) (to-query qry))))

  (declare query-sql-rows!# ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> List Row))
  (define (query-sql-rows!# cnxn qry)
    (r:ok-or-error (query-sql-rows! cnxn qry)))

  (declare query-sql-row! ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> DbResult Row))
  (define (query-sql-row! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (fn (input)
           (match input
             ((Nil) (err-out-of-vals))
             ((Cons row _) (Ok row))))))

  (declare query-sql-row!# ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> Row))
  (define (query-sql-row!# cnxn qry)
    (r:ok-or-error (query-sql-row! cnxn qry)))

  (declare execute-query! ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> DbResult Unit))
  (define (execute-query! cnxn qry)
    (execute-query!_ cnxn
                     (unwrap-query-container (ty:proxy-of cnxn) (to-query qry))))

  (declare execute-query!# ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> Unit))
  (define (execute-query!# cnxn qry)
    (r:ok-or-error (execute-query! cnxn qry)))

  (declare query-rows! ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d -> :q -> DbResult (List :p)))
  (define (query-rows! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (traverse parse-row)))

  (declare query-rows!# ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d -> :q -> List :p))
  (define (query-rows!# cnxn qry)
    (r:ok-or-error (query-rows! cnxn qry)))

  (declare query-row! ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d -> :q -> DbResult :p))
  (define (query-row! cnxn qry)
    (>>= (query-sql-rows! cnxn qry)
         (fn (input)
           (match input
             ((Nil) (err-out-of-vals))
             ((Cons row _) (parse-row row))))))

  (declare query-row!# ((DatabaseAdapter :d) (Queryable :q) (ParseSqlRow :p) =>
                        :d -> :q -> :p))
  (define (query-row!# cnxn qry)
    (r:ok-or-error (query-row! cnxn qry)))

  (declare select-objs!_ ((DatabaseAdapter :d) (Persistable :p) => :d -> Optional QueryOption -> DbResult (List :p)))
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

  (declare select-objs!#_ ((DatabaseAdapter :d) (Persistable :p) => :d -> Optional QueryOption -> List :p))
  (define (select-objs!#_ cnxn opt)
    (r:ok-or-error (select-objs!_ cnxn opt)))

  (declare select-obj!_ ((DatabaseAdapter :d) (Persistable :p) => :d -> Optional QueryOption -> DbResult :p))
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

  (declare select-obj!#_ ((DatabaseAdapter :d) (Persistable :p) => :d -> Optional QueryOption -> :p))
  (define (select-obj!#_ cnxn opt)
    (r:ok-or-error (select-obj!_ cnxn opt)))

  (declare delete-obj! ((DatabaseAdapter :d) (Persistable :p) => :d -> :p -> DbResult Unit))
  (define (delete-obj! cnxn obj)
    (execute-query! cnxn (delete-obj-query obj)))

  (declare delete-obj!# ((DatabaseAdapter :d) (Persistable :p) => :d -> :p -> Unit))
  (define (delete-obj!# cnxn obj)
    (r:ok-or-error (delete-obj! cnxn obj)))

  (declare insert-obj! ((DatabaseAdapter :d) (Persistable :p) => :d -> :p -> DbResult Unit))
  (define (insert-obj! cnxn obj)
    (execute-query! cnxn (insert-obj-query obj)))

  (declare insert-obj!# ((DatabaseAdapter :d) (Persistable :p) => :d -> :p -> Unit))
  (define (insert-obj!# cnxn obj)
    (r:ok-or-error (insert-obj! cnxn obj)))
  )

(cl:defmacro select-objs! (cnxn cl:&optional where?)
  `(select-objs!_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-objs!# (cnxn cl:&optional where?)
  `(select-objs!#_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-obj! (cnxn cl:&optional where?)
  `(select-obj!_ ,cnxn ,(optional-clause where?)))

(cl:defmacro select-obj!# (cnxn cl:&optional where?)
  `(select-obj!#_ ,cnxn ,(optional-clause where?)))
