(defpackage coalton-db/tests/queries
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/queries)
  (:local-nicknames
   (:ty #:coalton-library/types)
   (:itr #:coalton-library/iterator)))
(in-package :coalton-db/tests/queries)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/queries-fiasco)
(coalton-fiasco-init #:coalton-db/tests/queries-fiasco)

(coalton-toplevel
  (declare norm (String -> String))
  (define (norm s)
    "Return S with every run of whitespace collapsed to a single space."
    (lisp String (s)
      (cl-ppcre:regex-replace-all "\\s+" s " ")))

  ;; NOTE: For the purpose of testing, we will use the same test adapter, unless
  ;; there is an explicit difference we need to test. The purpose of *this*
  ;; test suite is not to test that the different database adapters work properly.
  ;; The purpose is just to test that query generation responds to adapters correctly.
  (define-type TestAdapter1 TestAdapter1)

  (define-instance (DatabaseAdapter TestAdapter1)
    ;; TestAdapter1 uses a constant placeholder, '?', like SQLite
    (define (generate-placeholders _ placeholders)
      (map (const "?") placeholders)))

  (define to-sql-test1 (to-sql (the (ty:Proxy TestAdapter1) ty:Proxy)))

  (define-type TestAdapter2 TestAdapter2)

  (define-instance (DatabaseAdapter TestAdapter2)
    ;; TestAdapter2 uses an index-based placeholder - $1, $2, etc - like Postgres
    (define (generate-placeholders _ placeholders)
      (itr:collect! (map (fn (n)
                           (<> "$" (into n)))
                         (itr:range-increasing 1 0 (length placeholders))))))

  (define to-sql-test2 (to-sql (the (ty:Proxy TestAdapter2) ty:Proxy))))


(define-test test-select-constant ()
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select (Values 5))))
  (is (== (norm "SELECT ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5))
          params))
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select (Values "Hello"))))
  (is (== (norm "SELECT ?;")
          (norm sql-str)))
  (is (== (make-list (SqlText "Hello"))
          params)))

(define-test test-select-multiple-constants ()
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select (Values 5 "Hello"))))
  (is (== (norm "SELECT ?, ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5) (SqlText "Hello"))
          params)))

(define-test test-select-multiple-constants-pg-style-adapter ()
  (let (SqlQuery sql-str params) = (to-sql-test2 (Select (Values 5 "Hello"))))
  (is (== (norm "SELECT $0, $1;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5) (SqlText "Hello"))
          params)))

(define-test test-select-constants-from-table ()
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select (Values 5) (From "test-table"))))
  (is (== (norm "SELECT ? FROM test-table;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5))
          params)))

(define-test test-select-all-from-table ()
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select AllCols (From "test-table"))))
  (is (== (norm "SELECT * FROM test-table;")
          (norm sql-str)))
  (is (== (make-list)
          params)))

(define-test test-select-cols-from-table ()
  (let (SqlQuery sql-str params) = (to-sql-test1 (Select (Cols "id" "name") (From "test-table"))))
  (is (== (norm "SELECT id, name FROM test-table;")
          (norm sql-str)))
  (is (== (make-list)
          params)))
