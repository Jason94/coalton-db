(defpackage coalton-db/tests/sqlite
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/from-row
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
                             ("name" TextType)
                             ("flagged" BoolType)
                             ("nullable_bool" BoolType Nullable)
                             ("second_nullable_bool" BoolType Nullable)))))
  (run-query! cnxn
              (to-sql
               (ty:proxy-of cnxn)
               (Insert (IntoTable "Volunteers")
                       (Values 1 "product marketing"
                               "Jane Doe" True
                               (the (Optional Integer) None) (Some False))
                       (Cols "id" "campaign" "name" "flagged"
                             "nullable_bool" "second_nullable_bool"))))
  (let result = (run-query! cnxn
                            (to-sql
                             (ty:proxy-of cnxn)
                             (Select (Cols "id" "name" "flagged" "nullable_bool" "second_nullable_bool")
                                     (From "Volunteers")))))
  (disconnect-sqlite! cnxn)
  (let id-raw = (i# 0 (i# 0 (rst:ok-or-error result))))
  (let name-raw = (i# 1 (i# 0 (rst:ok-or-error result))))
  (let flagged-raw = (i# 2 (i# 0 (rst:ok-or-error result))))
  (let nullable-bool-raw = (i# 3 (i# 0 (rst:ok-or-error result))))
  (let second-nullable-bool-raw = (i# 4 (i# 0 (rst:ok-or-error result))))
  (is (== (Ok 1)
          (parse-val id-raw)))
  (is (== (Ok "Jane Doe")
          (parse-val name-raw)))
  (is (== (Ok True)
          (parse-val flagged-raw)))
  (is (== (Ok (the (Optional Boolean) None))
          (parse-val nullable-bool-raw)))
  (is (== (Ok (Some False))
          (parse-val second-nullable-bool-raw))))
