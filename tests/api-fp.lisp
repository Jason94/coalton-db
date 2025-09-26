(defpackage coalton-db/tests/api-fp
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/util
        #:coalton-db/core
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
;; DB monad, we're going to connect to an in-memory SQLite database.

(coalton-toplevel
  (declare simple-select (DB (DbResult String)))
  (define simple-select
    (rt:do-resultT
      (result <- (query-rows "SELECT 'Hello';"))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-sql val)))))

(define-test test-run-one-query ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn simple-select))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok "Hello") result)))

(coalton-toplevel
  (declare select-hardcoded-placeholder (ParseSql :a => SqlValue -> DB (DbResult :a)))
  (define (select-hardcoded-placeholder val)
    (rt:do-resultT
      (let qry = (SqlQuery "SELECT ?;" (make-list val)))
      (result <- (query-rows qry))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-sql val)))))

(define-test test-run-one-query-hardcoded-placeholder ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn (select-hardcoded-placeholder (Value False))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok False) result)))

(coalton-toplevel
  (declare select-value (ParseSql :a => SqlValue -> Db (DbResult :a)))
  (define (select-value val)
    (rt:do-resultT
      (let qry = (Select (Values val)))
      (result <- (query-rows qry))
      (let val = (i# 0 (i# 0 result)))
      (pure (parse-sql val)))))

(define-test test-run-one-query-placeholders ()
  (let cnxn = (sq:connect-sqlite! ":memory:"))
  (let result = (run-db! cnxn (select-value (Value False))))
  (sq:disconnect-sqlite! cnxn)
  (is (== (Ok False) result)))
