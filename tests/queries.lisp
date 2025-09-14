(defpackage coalton-db/tests/queries
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/queries))
(in-package :coalton-db/tests/queries)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/queries-fiasco)
(coalton-fiasco-init #:coalton-db/tests/queries-fiasco)

(coalton-toplevel
  (declare norm (String -> String))
  (define (norm s)
    "Return S with every run of whitespace collapsed to a single space."
    (lisp String (s)
      (cl-ppcre:regex-replace-all "\\s+" s " "))))

(define-test test-select-constant ()
  (let (SqlQuery sql-str params) = (to-sql (Select (5))))
  (is (== (norm "SELECT ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5))
          params))
  (let (SqlQuery sql-str params) = (to-sql (Select ("Hello"))))
  (is (== (norm "SELECT ?;")
          (norm sql-str)))
  (is (== (make-list (SqlText "Hello"))
          params)))

(define-test test-select-multiple-constants ()
  (let (SqlQuery sql-str params) = (to-sql (Select (5 "Hello"))))
  (is (== (norm "SELECT ?, ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5) (SqlText "Hello"))
          params)))

(define-test test-select-constants-from-table ()
  (let (SqlQuery sql-str params) = (to-sql (Select (5) (From "test-table"))))
  (is (== (norm "SELECT ? FROM test-table;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5))
          params)))
