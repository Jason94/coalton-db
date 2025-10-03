(defpackage coalton-db/tests/api-imp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
        #:coalton-db/to-row
        #:coalton-db/from-row
        #:coalton-db/schema
        #:coalton-db/persistable
        #:coalton-db/queries
        #:coalton-db/db-m
        #:coalton-db/api-imp)
  (:local-nicknames
   (:r #:coalton-library/result)
   (:rt #:coalton-library/monad/resultt)
   (:sq #:coalton-db/sqlite)
   (:db-c #:coalton-db/core)))
(in-package :coalton-db/tests/api-imp)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/api-imp-fiasco)
(coalton-fiasco-init #:coalton-db/tests/api-imp-fiasco)

;; NOTE: These tests are integration tests. In order to properly test the
;; api code, we're going to connect to an in-memory SQLite database.

;;;
;;; Test Sql Queries
;;;

(define-test test-run-one-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result =
    (query-sql-rows! cnxn "SELECT 'Hello';"))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (>>= result
                         (fn (rows)
                           (parse-val (i# 0 (i# 0 rows))))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result =
    (query-sql-rows!# cnxn "SELECT 'Hello';"))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-val (i# 0 (i# 0 result))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-hardcoded-placeholder ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (SqlQuery "SELECT ?;" (Values "Hello")))
  (let result = (query-sql-rows!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-val (i# 0 (i# 0 result))))
  (is (== (Ok "Hello") result-val)))

(define-test test-run-one-query-placeholders ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (Select (Values "Hello")))
  (let result = (query-sql-rows!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (let result-val = (parse-val (i# 0 (i# 0 result))))
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

(define-test test-query-sql-row ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (Select (Values 1 2 3)))
  (let result = (query-sql-row! cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (Values 1 2 3))
          result)))

(define-test test-query-sql-row-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let qry = (Select (Values 1 2 3)))
  (let result = (query-sql-row!# cnxn qry))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Values 1 2 3)
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
        (_ None))))

  (define (setup-users cnxn users)
    (execute-query!# cnxn (CreateSchema simple-user-table))
    (for user in users
      (execute-query!# cnxn (Insert (IntoTable "users")
                                    (to-row user))))))

(define-test test-query-rows ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (query-rows! cnxn (Select AllCols
                                          (From "users"))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user1 user2))
          result)))

(define-test test-query-rows-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (query-rows!# cnxn (Select AllCols
                                           (From "users"))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (make-list user1 user2)
          result)))

(define-test test-query-row ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (let result = (query-row! cnxn (Select AllCols
                                           (From "users"))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user1)
          result)))

(define-test test-query-row-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (let result = (query-row!# cnxn (Select AllCols
                                          (From "users"))))
  (sq:disconnect-sqlite! cnxn)
  (is (== user1
          result)))

(define-test test-select-objs ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-objs! cnxn))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user1 user2))
          result)))

(define-test test-select-objs-where ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-objs! cnxn (Where (Eq_ "name" (Value "Steve")))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok (make-list user1))
          result)))

(define-test test-select-objs-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-objs!# cnxn))
  (sq:disconnect-sqlite! cnxn)
  (is (== (make-list user1 user2)
          result)))

(define-test test-select-objs-where-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-objs!# cnxn (Where (Eq_ "name" (Value "Steve")))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (make-list user1)
          result)))

(define-test test-select-obj ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (let result = (select-obj! cnxn))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user1)
          result)))

(define-test test-select-obj-where ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-obj! cnxn (Where (Eq_ "name" (Value "Steve")))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok user1)
          result)))

(define-test test-select-obj-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (let result = (select-obj!# cnxn))
  (sq:disconnect-sqlite! cnxn)
  (is (== user1
          result)))

(define-test test-select-obj-where-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (let user2 = (SimpleUser "Diane" True))
  (setup-users cnxn (make-list user1 user2))
  (let result = (select-obj!# cnxn (Where (Eq_ "name" (Value "Steve")))))
  (sq:disconnect-sqlite! cnxn)
  (is (== user1
          result)))

(define-test test-delete-obj ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (let delete-result = (delete-obj! cnxn user1))
  (let users = (the (DbResult (List SimpleUser))
                    (select-objs! cnxn)))
  (is (r:ok? delete-result))
  (is (== users
          (Ok Nil))))

(define-test test-delete-obj-unsafe ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let user1 = (SimpleUser "Steve" False))
  (setup-users cnxn (make-list user1))
  (delete-obj!# cnxn user1)
  (let users = (the (DbResult (List SimpleUser))
                    (select-objs! cnxn)))
  (is (== users
          (Ok Nil))))
