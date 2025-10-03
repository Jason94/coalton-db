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
   #:select-objs
   #:execute-query
   ))
(cl:in-package :coalton-db/api-fp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
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
     prx-rst)))

(cl:defmacro select-objs (cl:&optional where?)
  `(select-objs_ ,(optional-clause where?)))
