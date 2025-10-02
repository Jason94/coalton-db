(defpackage coalton-db/tests/queries
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/queries
        #:coalton-db/util
        #:coalton-db/tests/test-utils)
  (:local-nicknames
   (:opt #:coalton-library/optional)
   (:ty #:coalton-library/types)
   (:s #:coalton-library/string)
   (:itr #:coalton-library/iterator)))
(in-package :coalton-db/tests/queries)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/queries-fiasco)
(coalton-fiasco-init #:coalton-db/tests/queries-fiasco)

;;;
;;; SELECT Tests
;;;

(define-test test-select-constant ()
  (let result = (to-sql-test1 (Select (Values 5))))
  (is-sql-eql "SELECT ?;" ((SqlInt 5))
              result)
  (let result2 = (to-sql-test1 (Select (Values "Hello"))))
  (is-sql-eql "SELECT ?;" ((SqlText "Hello"))
              result2))

(define-test test-select-multiple-constants ()
  (let result = (to-sql-test1 (Select (Values 5 "Hello"))))
  (is-sql-eql "SELECT ?, ?;" ((SqlInt 5) (SqlText "Hello"))
              result))

(define-test test-select-multiple-constants-pg-style-adapter ()
  (let result = (to-sql-test2 (Select (Values 5 "Hello"))))
  (is-sql-eql "SELECT $0, $1;" ((SqlInt 5) (SqlText "Hello"))
              result))

(define-test test-select-constants-from-table ()
  (let result = (to-sql-test1 (Select (Values 5) (From "test-table"))))
  (is-sql-eql "SELECT ? FROM test-table;" ((SqlInt 5))
              result))

(define-test test-select-all-from-table ()
  (let result = (to-sql-test1 (Select AllCols (From "test-table"))))
  (is-sql-eql "SELECT * FROM test-table;" ()
              result))

(define-test test-select-cols-from-table ()
  (let result = (to-sql-test1 (Select (Cols "id" "name") (From "test-table"))))
  (is-sql-eql "SELECT id, name FROM test-table;" ()
              result))

;;;
;;; WHERE Tests
;;; (Technically this uses SELECT, but it's just to test WHERE)
;;;

(define-test test-select-where-true-or-false ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where True_))))
  (is-sql-eql "SELECT id FROM test-table WHERE TRUE;" ()
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where False_))))
  (is-sql-eql "SELECT id FROM test-table WHERE FALSE;" ()
              result2))

(define-test test-select-where-col-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Eq_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id = ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (Eq_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? = id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Eq_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? = ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-multiple-values-pg-style-adapter ()
  (let result = (to-sql-test2 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Eq_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE $0 = $1;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-col-not-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Neq_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id <> ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (Neq_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? <> id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values-not-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Neq_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? <> ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-col-greater-than ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Gt_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id > ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (Gt_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? > id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values-greater-than ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Gt_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? > ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-col-less-than ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Lt_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id < ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (Lt_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? < id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values-less-than ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (Lt_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? < ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-col-greater-than-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (GtEq_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id >= ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (GtEq_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? >= id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values-greater-than-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (GtEq_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? >= ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-col-less-than-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (LtEq_ "id" (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE id <= ?;" ((SqlInt 123))
              result)
  (let result2 = (to-sql-test1 (Select (Cols "id")
                                       (From "test-table")
                                       (Where (LtEq_ (Value 123) "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? <= id;" ((SqlInt 123))
              result2))

(define-test test-select-where-multiple-values-less-than-equal ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (LtEq_ (Value 321) (Value 123))))))
  (is-sql-eql "SELECT id FROM test-table WHERE ? <= ?;" ((SqlInt 321) (SqlInt 123))
              result))

(define-test test-select-where-is-null ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (IsNull_ "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE id IS NULL;" ()
              result))

(define-test test-select-where-is-not-null ()
  (let result = (to-sql-test1 (Select (Cols "id")
                                      (From "test-table")
                                      (Where (IsNotNull_ "id")))))
  (is-sql-eql "SELECT id FROM test-table WHERE id IS NOT NULL;" ()
              result))

(define-test test-select-where-and-two-cols ()
  (let result = (to-sql-test1
                 (Select (Cols "id")
                         (From "test-table")
                         (Where (And_ (Eq_ "id" (Value 123))
                                      (Eq_ "name" (Value "Alice")))))))
  (is-sql-eql "SELECT id FROM test-table WHERE (id = ?) AND (name = ?);"
              ((SqlInt 123) (SqlText "Alice"))
              result))

(define-test test-select-where-or-two-cols ()
  (let result = (to-sql-test1
                 (Select (Cols "id")
                         (From "test-table")
                         (Where (Or_ (Eq_ "id" (Value 123))
                                     (Eq_ "name" (Value "Alice")))))))
  (is-sql-eql "SELECT id FROM test-table WHERE (id = ?) OR (name = ?);"
              ((SqlInt 123) (SqlText "Alice"))
              result))

(define-test test-select-where-not ()
  (let result = (to-sql-test1
                 (Select (Cols "id")
                         (From "test-table")
                         (Where (Not_ (Eq_ "id" (Value 5)))))))
  (is-sql-eql "SELECT id FROM test-table WHERE NOT id = ?;" ((SqlInt 5))
              result))

;;;
;;; DELETE Tests
;;;

(define-test test-delete-all ()
  (let result = (to-sql-test1
                 (Delete (From "test-table"))))
  (is-sql-eql "DELETE FROM test-table;" ()
              result))

(define-test test-delete-where ()
  (let result = (to-sql-test1
                 (Delete (From "test-table")
                         (Where (And_ (Eq_ "id" (Value 5))
                                      (Gt_ "date" (Value 100)))))))
  (is-sql-eql "DELETE FROM test-table WHERE (id = ?) AND (date > ?);"
              ((SqlInt 5) (SqlInt 100))
              result))

(define-test test-delete-where-pg-style-adapter ()
  (let result = (to-sql-test2
                 (Delete (From "test-table")
                         (Where (And_ (Eq_ "id" (Value 5))
                                      (Gt_ "date" (Value 100)))))))
  (is-sql-eql "DELETE FROM test-table WHERE (id = $0) AND (date > $1);"
              ((SqlInt 5) (SqlInt 100))
              result))

;;;
;;; INSERT Tests
;;;

(define-test test-insert-single-row-without-columns ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "test-table")
                         (Values 1 "Alice"))))
  (is-sql-eql "INSERT INTO test-table VALUES (?, ?);"
              ((SqlInt 1) (SqlText "Alice"))
              result))

(define-test test-insert-single-row-with-columns ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "test-table")
                         (Values 1 "Alice")
                         (Cols "id" "name"))))
  (is-sql-eql "INSERT INTO test-table (id, name) VALUES (?, ?);"
              ((SqlInt 1) (SqlText "Alice"))
              result))

(define-test test-insert-single-row-with-columns-pg-style-adapter ()
  (let result = (to-sql-test2
                 (Insert (IntoTable "test-table")
                         (Values 1 "Alice")
                         (Cols "id" "name"))))
  (is-sql-eql "INSERT INTO test-table (id, name) VALUES ($0, $1);"
              ((SqlInt 1) (SqlText "Alice"))
              result))

;;;
;;; UPDATE Tests
;;;

(define-test test-update-set-single-col ()
  (let result = (to-sql-test1
                 (Update "test-table"
                         (("name" "Bob")))))
  (is-sql-eql "UPDATE test-table SET name = ?;"
              ((SqlText "Bob"))
              result))

(define-test test-update-set-multiple-cols ()
  (let result = (to-sql-test1
                 (Update "test-table"
                         (("name" "Bob")
                          ("id" 1)))))
  (is-sql-eql "UPDATE test-table SET name = ?, id = ?;"
              ((SqlText "Bob") (SqlInt 1))
              result))

(define-test test-update-set-multiple-cols-pg-style-adapter ()
  (let result = (to-sql-test2
                 (Update "test-table"
                         (("name" "Bob")
                          ("id" 1)))))
  (is-sql-eql "UPDATE test-table SET name = $0, id = $1;"
              ((SqlText "Bob") (SqlInt 1))
              result))

(define-test test-update-set-multiple-cols-where ()
  (let result = (to-sql-test1
                 (Update "test-table"
                         (("name" "Bob")
                          ("id" 1))
                         (Where (And_ (GtEq_ "id" (Value 10))
                                      (IsNull_ "name"))))))
  (is-sql-eql "UPDATE test-table SET name = ?, id = ? WHERE (id >= ?) AND (name IS NULL);"
              ((SqlText "Bob") (SqlInt 1) (SqlInt 10))
              result))

;;;
;;; DROP TABLE Tests
;;;

(define-test test-drop-table ()
  (let result = (to-sql-test1 (DropTable "test-table")))
  (is-sql-eql "DROP TABLE test-table;" ()
              result))

(define-test test-drop-table-if-exists ()
  (let result = (to-sql-test1 (DropTable "test-table" IfExists)))
  (is-sql-eql "DROP TABLE IF EXISTS test-table;" ()
              result))

;;;
;;; CREATE TABLE Tests
;;;

(define-test test-create-table-one-col ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" ()
                  (("id" IntType PrimaryKey)))))
  (is-sql-eql (build-str "CREATE TABLE test-table ("
                         " id INTEGER PRIMARY KEY NOT NULL"
                         ");")
              ()
              result))

(define-test test-create-table-if-not-exists ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" (IfNotExists)
                  (("id" IntType PrimaryKey)))))
  (is-sql-eql (build-str "CREATE TABLE IF NOT EXISTS test-table ("
                         " id INTEGER PRIMARY KEY NOT NULL"
                         ");")
              ()
              result))

(define-test test-create-table-three-cols ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" ()
                  (("id" IntType PrimaryKey)
                   ("name" TextType)
                   ("checked" BoolType)))))
  (is-sql-eql (build-str "CREATE TABLE test-table ("
                         " id INTEGER PRIMARY KEY NOT NULL,"
                         " name TEXT NOT NULL,"
                         " checked BOOLEAN NOT NULL"
                         ");")
              ()
              result))

(define-test test-create-table-unique ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" ()
                  (("id" IntType PrimaryKey)
                   ("checked" BoolType Unique)))))
  (is-sql-eql (build-str "CREATE TABLE test-table ("
                         " id INTEGER PRIMARY KEY NOT NULL,"
                         " checked BOOLEAN UNIQUE NOT NULL"
                         ");")
              ()
              result))

(define-test test-create-table-two-col-props ()
  ;; NOTE: UNIQUE & PRIMARY KEY is redundant, but coalton-db isn't going to
  ;; enforce those kinds of checks. The library will allow those kinds of
  ;; things, and it works if it works.
  (let result =
    (to-sql-test1
     (CreateTable "test-table" ()
                  (("id" IntType PrimaryKey Unique)))))
  (is-sql-eql (build-str "CREATE TABLE test-table ("
                         " id INTEGER PRIMARY KEY UNIQUE NOT NULL"
                         ");")
              ()
              result))

(define-test test-create-table-nullable ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" (IfNotExists)
                  (("id" IntType PrimaryKey Nullable)))))
  (is-sql-eql (build-str "CREATE TABLE IF NOT EXISTS test-table ("
                         " id INTEGER PRIMARY KEY"
                         ");")
              ()
              result))

(define-test test-create-table-composite-primary-key ()
  (let result =
    (to-sql-test1
     (CreateTable "test-table" ()
                  (("id" IntType)
                   ("campaign" TextType))
                  ((CompositePrimaryKey "id" "campaign")))))
  (is-sql-eql (build-str "CREATE TABLE test-table ("
                         " id INTEGER NOT NULL,"
                         " campaign TEXT NOT NULL,"
                         " PRIMARY KEY (id, campaign)"
                         ");")
              ()
              result))
