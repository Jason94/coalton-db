(defpackage coalton-db/tests/api-fp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
        #:coalton-db/schema
        #:coalton-db/to-row
        #:coalton-db/from-row
        #:coalton-db/persistable
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

(define-test test-query-sql-row ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result =
    (run-db! cnxn
             (query-sql-row (Select (Values 1 2 3)))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (Values 1 2 3))
          result)))

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
      (build-row user .name .verified?)))

  (define-instance (Persistable SimpleUser)
    (define schema-for (const simple-user-table))
    (define (prop-for-col user col-name)
      (match col-name
        ("name" (Some (SqlText (.name user))))
        ("verified" (Some (SqlBool (.verified? user))))
        (_ None)))))

(define-test test-select-rows ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user1)))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user2)))
              (query-rows (Select AllCols
                                  (From "users"))))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user1 user2))
          result)))

(define-test test-select-row ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (query-row (Select AllCols
                                 (From "users"))))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user)
          result)))

(define-test test-select-objs ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user2)))
              (select-objs))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user user2))
          result)))

(define-test test-select-objs-where ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user2)))
              (select-objs (Where (Eq_ "name" (Value "Steve")))))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user))
          result)))

(define-test test-select-obj ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (select-obj))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user)
          result)))

(define-test test-select-obj-where ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (let result =
    (run-db! cnxn
             (do
              (execute-query (CreateSchema simple-user-table))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user)))
              (execute-query (Insert (IntoTable "users")
                                     (to-row user2)))
              (select-obj (Where (Eq_ "name" (Value "Steve")))))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user)
          result)))
