(defpackage #:coalton-db/tests
  (:use #:coalton #:coalton-prelude #:coalton-testing)
  (:export #:run-tests))
(in-package #:coalton-db/tests)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/fiasco-test-package)

(coalton-fiasco-init #:coalton-db/fiasco-test-package)

(cl:defun run-tests ()
  (fiasco:run-package-tests
   :packages '(#:coalton-db/tests/queries-fiasco
               #:coalton-db/tests/sqlite-fiasco
               #:coalton-db/tests/api-fp-fiasco
               #:coalton-db/tests/api-imp-fiasco
               )
   :interactive cl:t))
