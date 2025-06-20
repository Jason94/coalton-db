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
                :components
                ((:file "utils")
                 (:file "core")
                 (:file "db")
                 (:file "sqlite"))))
  :description "SQL Database library for Coalton."
  :in-order-to ((test-op (test-op "coalton-db/tests"))))

(defsystem "coalton-db/tests"
  :author "Jason Walker"
  :license "MIT"
  :depends-on ("coalton-db"
               "rove")
  :components ((:module "tests"
                :components
                ((:file "main"))))
  :description "Test system for COALTON-DB."
  :perform (test-op (op c) (symbol-call :rove :run c)))
