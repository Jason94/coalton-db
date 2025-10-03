(cl:in-package :cl-user)
(defpackage :coalton-db/api-imp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/from-row
   #:coalton-db/queries
   #:coalton-db/api-helpers)
  (:local-nicknames
   (:r #:coalton-library/result)
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
  )
