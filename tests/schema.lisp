(defpackage coalton-db/tests/schema
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/util
        #:coalton-db/schema
        #:coalton-db/queries
        #:coalton-db/tests/test-utils
        )
  )
(in-package :coalton-db/tests/schema)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/schema-fiasco)
(coalton-fiasco-init #:coalton-db/tests/schema-fiasco)

(coalton-toplevel
  (define simple-table
    (make-schema
     "users"
     ((column "name" TextType)
      (column "age" IntType)))))

(define-test test-create-simple-schema ()
  (let result =
    (to-sql-test1 (CreateSchema simple-table)))
  (is-sql-eql (build-str "CREATE TABLE users ("
                         " id INTEGER PRIMARY KEY NOT NULL,"
                         " name TEXT NOT NULL,"
                         " age INTEGER NOT NULL"
                         ");")
              ()
              result))
