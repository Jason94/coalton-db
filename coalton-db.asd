(defsystem "coalton-db"
  :long-name "coalton-sql-database"
  :version "0.1"
  :author "Jason Walker"
  :maintainer "Jason Walker"
  :mailto "Jason0@pm.me"
  :license "MIT"
  :depends-on ("alexandria"
               "sqlite"
               "coalton"
               "coalton-simple-io")
  :components ((:module "src"
                :serial t
                :components
                ((:file "utils")
                 (:file "core")
                 (:file "to-row")
                 (:file "queries")
                 (:file "schema")
                 (:file "persistable")
                 (:file "api-helpers")
                 (:file "dbm")
                 (:file "api-fp")
                 (:file "api-imp")
                 (:file "sqlite"))))
  :description "SQL Database library for Coalton."
  :in-order-to ((test-op (test-op "coalton-db/tests"))))

(defsystem "coalton-db/tests"
  :author "Jason Walker"
  :license "MIT"
  :depends-on ("coalton-db"
               "coalton/testing"
               "fiasco"
               "cl-ppcre")
  :components ((:module "tests"
                :serial t
                :components
                ((:file "test-utils")
                 (:file "to-row")
                 (:file "queries")
                 (:file "queries-to-row")
                 (:file "schema")
                 (:file "persistable")
                 (:file "sqlite")
                 (:file "api-fp")
                 (:file "api-imp")
                 (:file "package"))))
  :description "Test system for COALTON-DB."
  :perform (test-op (op c) (symbol-call '#:coalton-db/tests '#:run-tests)))

(defsystem "coalton-db/examples"
  :author "Jason Walker"
  :license "MIT"
  :depends-on ("coalton-db"
               "coalton-simple-io")
  :components ((:module "examples"
                :components
                ((:file "io-example-fp")
                 (:file "io-example-imp"))))
  :description "Test system for COALTON-DB.")
