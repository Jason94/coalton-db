(defpackage coalton-db/tests/api-imp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
        #:coalton-db/to-row
        #:coalton-db/from-row
        #:coalton-db/schema
        #:coalton-db/queries
        #:coalton-db/db-m
        #:coalton-db/api-imp)
  (:local-nicknames
   (:rt #:coalton-library/monad/resultt)
   (:sq #:coalton-db/sqlite)
   (:db-c #:coalton-db/core)))
(in-package :coalton-db/tests/api-imp)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/api-imp-fiasco)
(coalton-fiasco-init #:coalton-db/tests/api-imp-fiasco)

;;;
;;; Test Sql Queries
;;;

(define-test test-run-one-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result =
    (query-rows! cnxn "SELECT 'Hello';"))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (>>= result
                         (fn (rows)
                           (parse-sql (i# 0 (i# 0 rows))))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result =
    (query-rows!# cnxn "SELECT 'Hello';"))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-sql (i# 0 (i# 0 result))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-hardcoded-placeholder ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (SqlQuery "SELECT ?;" (Values "Hello")))
  (let result = (query-rows!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-sql (i# 0 (i# 0 result))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-placeholders ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (Select (Values "Hello")))
  (let result = (query-rows!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-sql (i# 0 (i# 0 result))))
  (is (== (Ok "Hello") result-val)))

(define-test test-execute-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (DropTable "test" IfExists))
  (let result = (execute-query! cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok Unit) result)))

(define-test test-execute-query-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (DropTable "test" IfExists))
  (let result = (execute-query!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (is (== Unit result)))

;;;
;;; Test FRM
;;;

(coalton-toplevel
  (derive Eq)
  (define-struct SimpleUser
    (name String)
    (verified? Boolean))

  (define simple-user-table
    (make-schema
     "users"
     ((column "name" TextType PrimaryKey)
      (column "verified" BoolType))))

  (define-row-parser SimpleUser
    sql-value-parser
    sql-value-parser)

  (define-instance (ToRow SimpleUser)
    (define (to-row user)
      (build-row user .name .verified?))))

(define-test test-select-value ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (execute-query!# cnxn (CreateSchema simple-user-table))
  (execute-query!# cnxn (Insert (IntoTable "users")
                                (to-row user)))
  (let result = (query-vals! cnxn (Select AllCols
                                          (From "users"))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user))
          result)))
