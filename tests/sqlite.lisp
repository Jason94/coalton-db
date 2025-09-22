(defpackage coalton-db/tests/sqlite
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/queries
        #:coalton-db/util
        #:coalton-db/sqlite)
  (:local-nicknames
   (:rst #:coalton-library/result)
   (:ty #:coalton-library/types)))
(in-package :coalton-db/tests/sqlite)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/sqlite-fiasco)
(coalton-fiasco-init #:coalton-db/tests/sqlite-fiasco)

(define-test sqlite-integration-test ()
  (let cnxn = (connect-sqlite! ":memory:"))
  (run-query! cnxn
              (to-sql
               (ty:proxy-of cnxn)
               (CreateTable "Volunteers" ()
                            (("id" IntType)
                             ("campaign" TextType)
                             ("name" TextType)))))
  (run-query! cnxn
              (to-sql
               (ty:proxy-of cnxn)
               (Insert (IntoTable "Volunteers")
                       (Values 1 "product marketing" "Jane Doe")
                       (Cols "id" "campaign" "name"))))
  (let result = (run-query! cnxn
                            (to-sql
                             (ty:proxy-of cnxn)
                             (Select (Cols "id" "name")
                                     (From "Volunteers")))))
  (let id-raw = (i# 0 (i# 0 (rst:ok-or-error result))))
  (let name-raw = (i# 1 (i# 0 (rst:ok-or-error result))))
  (is (== (Ok 1)
          (parse-sql id-raw)))
  (is (== (Ok "Jane Doe")
          (parse-sql name-raw)))
  )
