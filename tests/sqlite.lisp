(defpackage coalton-db/tests/sqlite
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/queries
        #:coalton-db/util)
  (:local-nicknames))
(in-package :coalton-db/tests/sqlite)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/sqlite-fiasco)
(coalton-fiasco-init #:coalton-db/tests/sqlite-fiasco)

;; (define-test sqlite-integration-test ()
;;   (let cnxn = (connect-sqlite! ":memory:"))
;;   (run-query! cnxn
;;               (CreateTable "test-table" ()
;;                            ((IntType ))
