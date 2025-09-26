(defpackage coalton-db/tests/api-imp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
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
