(cl:in-package :cl-user)
(defpackage :coalton-db/api-imp
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/api-helpers)
  (:local-nicknames
   (:r #:coalton-library/result)
   (:ty #:coalton-library/types))
  (:export
   ;;; Library Public
   #:query-rows!
   #:query-rows!#
   ))
(cl:in-package :coalton-db/api-imp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare query-rows! ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> DbResult (List Row)))
  (define (query-rows! cnxn qry)
    (run-query! cnxn
                (unwrap-query-container (ty:proxy-of cnxn) (to-query qry))))

  (declare query-rows!# ((DatabaseAdapter :d) (Queryable :q) => :d -> :q -> List Row))
  (define (query-rows!# cnxn qry)
    (r:ok-or-error (query-rows! cnxn qry))))
