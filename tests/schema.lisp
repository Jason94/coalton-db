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

(coalton-toplevel
  (define custom-pkey-table
    (make-schema
     "users"
     ((column "custom-id" IntType PrimaryKey)
      (column "name" TextType)
      (column "age" IntType)))))

(define-test test-create-custom-pkey-schema ()
  (let result =
    (to-sql-test1 (CreateSchema custom-pkey-table)))
  (is-sql-eql (build-str "CREATE TABLE users ("
                         " custom-id INTEGER PRIMARY KEY NOT NULL,"
                         " name TEXT NOT NULL,"
                         " age INTEGER NOT NULL"
                         ");")
              ()
              result))

(coalton-toplevel
  (define properties-table
    (make-schema
     "users"
     ((column "name" TextType Unique)
      (column "age" IntType Nullable)))))

(define-test test-create-properties-schema ()
  (let result =
    (to-sql-test1 (CreateSchema properties-table)))
  (is-sql-eql (build-str "CREATE TABLE users ("
                         " id INTEGER PRIMARY KEY NOT NULL,"
                         " name TEXT UNIQUE NOT NULL,"
                         " age INTEGER"
                         ");")
              ()
              result))

(coalton-toplevel
  (define composite-pkey-table
    (make-schema
     "users"
     ((column "name" TextType)
      (column "age" IntType))
     ((CompositePrimaryKey "name" "age")))))

(define-test test-create-composite-pkey-schema ()
  (let result =
    (to-sql-test1 (CreateSchema composite-pkey-table)))
  (is-sql-eql (build-str "CREATE TABLE users ("
                         " name TEXT NOT NULL,"
                         " age INTEGER NOT NULL,"
                         " PRIMARY KEY (name, age)"
                         ");")
              ()
              result))
