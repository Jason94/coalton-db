(defpackage coalton-db/tests/queries
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/queries)
  (:local-nicknames
   (:opt #:coalton-library/optional)
   (:ty #:coalton-library/types)
   (:s #:coalton-library/string)
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
    (define (next-placeholder _ _)
      "?"))

  (define to-sql-test1 (to-sql (the (ty:Proxy TestAdapter1) ty:Proxy)))

  (define-type TestAdapter2 TestAdapter2)

  (define-instance (DatabaseAdapter TestAdapter2)
    ;; TestAdapter2 uses an index-based placeholder - $1, $2, etc - like Postgres
    (define (next-placeholder _ last-param-str?)
      (match last-param-str?
        ((None)
         "$0")
        ((Some last-param-str)
         (let last-n = (opt:from-some "Invalid last param provided"
                                      (do
                                       (last-num-str <- (s:strip-prefix "$" last-param-str))
                                       (s:parse-int last-num-str))))
         (<> "$" (into (+ 1 last-n)))))))

  (define to-sql-test2 (to-sql (the (ty:Proxy TestAdapter2) ty:Proxy))))

;;;
;;; SELECT Tests
;;;

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

;;;
;;; WHERE Tests
;;; (Technically this uses SELECT, but it's just to test WHERE)
;;;

(define-test test-select-where-true-or-false ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where True_))))
  (is (== (norm "SELECT id FROM test-table WHERE TRUE;")
          (norm sql-str)))
  (is (== (make-list)
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where False_))))
  (is (== (norm "SELECT id FROM test-table WHERE FALSE;")
          (norm sql-str)))
  (is (== (make-list)
          params)))

(define-test test-select-where-col-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Eq_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id = ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Eq_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? = id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Eq_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? = ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-pg-style-adapter ()
  (let (SqlQuery sql-str params) =
    (to-sql-test2 (Select (Cols "id")
                          (From "test-table")
                          (Where (Eq_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE $0 = $1;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-col-not-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Neq_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id <> ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Neq_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? <> id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-not-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Neq_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? <> ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-col-greater-than ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Gt_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id > ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Gt_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? > id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-greater-than ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Gt_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? > ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-col-less-than ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Lt_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id < ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Lt_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? < id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-less-than ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (Lt_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? < ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-col-greater-than-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (GtEq_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id >= ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (GtEq_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? >= id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-greater-than-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (GtEq_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? >= ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-col-less-than-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (LtEq_ "id" (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE id <= ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params))
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (LtEq_ (Value 123) "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE ? <= id;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123))
          params)))

(define-test test-select-where-multiple-values-less-than-equal ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (LtEq_ (Value 321) (Value 123))))))
  (is (== (norm "SELECT id FROM test-table WHERE ? <= ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 321) (SqlInt 123))
          params)))

(define-test test-select-where-is-null ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (IsNull_ "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE id IS NULL;")
          (norm sql-str)))
  (is (== (make-list)
          params)))

(define-test test-select-where-is-not-null ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1 (Select (Cols "id")
                          (From "test-table")
                          (Where (IsNotNull_ "id")))))
  (is (== (norm "SELECT id FROM test-table WHERE id IS NOT NULL;")
          (norm sql-str)))
  (is (== (make-list)
          params)))

(define-test test-select-where-and-two-cols ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1
     (Select (Cols "id")
             (From "test-table")
             (Where (And_ (Eq_ "id" (Value 123))
                          (Eq_ "name" (Value "Alice")))))))
  (is (== (norm "SELECT id FROM test-table WHERE (id = ?) AND (name = ?);")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123) (SqlText "Alice"))
          params)))

(define-test test-select-where-or-two-cols ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1
     (Select (Cols "id")
             (From "test-table")
             (Where (Or_ (Eq_ "id" (Value 123))
                          (Eq_ "name" (Value "Alice")))))))
  (is (== (norm "SELECT id FROM test-table WHERE (id = ?) OR (name = ?);")
          (norm sql-str)))
  (is (== (make-list (SqlInt 123) (SqlText "Alice"))
          params)))

(define-test test-select-where-not ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1
     (Select (Cols "id")
             (From "test-table")
             (Where (Not_ (Eq_ "id" (Value 5)))))))
  (is (== (norm "SELECT id FROM test-table WHERE NOT id = ?;")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5))
          params)))

;;;
;;; DELETE Tests
;;;

(define-test test-delete-all ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1
     (Delete (From "test-table"))))
  (is (== (norm "DELETE FROM test-table;")
          (norm sql-str)))
  (is (== (make-list)
          params)))

(define-test test-delete-where ()
  (let (SqlQuery sql-str params) =
    (to-sql-test1
     (Delete (From "test-table")
             (Where (And_ (Eq_ "id" (Value 5))
                          (Gt_ "date" (Value 100)))))))
  (is (== (norm "DELETE FROM test-table WHERE (id = ?) AND (date > ?);")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5) (SqlInt 100))
          params)))

(define-test test-delete-where-pg-style-adapter ()
  (let (SqlQuery sql-str params) =
    (to-sql-test2
     (Delete (From "test-table")
             (Where (And_ (Eq_ "id" (Value 5))
                          (Gt_ "date" (Value 100)))))))
  (is (== (norm "DELETE FROM test-table WHERE (id = $0) AND (date > $1);")
          (norm sql-str)))
  (is (== (make-list (SqlInt 5) (SqlInt 100))
          params)))
