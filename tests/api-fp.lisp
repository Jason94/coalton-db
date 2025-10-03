(defpackage coalton-db/tests/api-fp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
        #:coalton-db/schema
        #:coalton-db/to-row
        #:coalton-db/from-row
        #:coalton-db/queries
        #:coalton-db/db-m
        #:coalton-db/api-fp)
  (:local-nicknames
   (:rt #:coalton-library/monad/resultt)
   (:sq #:coalton-db/sqlite)
   (:db-c #:coalton-db/core)))
(in-package :coalton-db/tests/api-fp)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/api-fp-fiasco)
(coalton-fiasco-init #:coalton-db/tests/api-fp-fiasco)

;; NOTE: These tests are integration tests. In order to properly test the
;; api code, we're going to connect to an in-memory SQLite database.

;;;
;;; Test Sql Queries
;;;

(coalton-toplevel
  (declare simple-select (DB (DbResult String)))
  (define simple-select
    (rt:do-resultT
      (result <- (query-sql-rows "SELECT 'Hello';"))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-val val)))))

(define-test test-run-one-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn simple-select))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok "Hello") result)))

(coalton-toplevel
  (declare select-hardcoded-placeholder (ParseSqlValue :a => SqlValue -> DB (DbResult :a)))
  (define (select-hardcoded-placeholder val)
    (rt:do-resultT
      (let qry = (SqlQuery "SELECT ?;" (make-list val)))
      (result <- (query-sql-rows qry))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-val val)))))

(define-test test-run-one-query-hardcoded-placeholder ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn (select-hardcoded-placeholder (Value False))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok False) result)))

(coalton-toplevel
  (declare select-value (ParseSqlValue :a => SqlValue -> Db (DbResult :a)))
  (define (select-value val)
    (rt:do-resultT
      (let qry = (Select (Values val)))
      (result <- (query-sql-rows qry))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-val val)))))

(define-test test-run-one-query-placeholders ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn (select-value (Value False))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok False) result)))

(define-test test-execute-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn (execute-query (DropTable "test" IfExists))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok Unit) result)))

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

(define-test test-select-rows ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (query-rows (Select AllCols
                                  (From "users"))))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user))
          result)))
