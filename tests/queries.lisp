(defpackage coalton-db/tests/queries
  (:use #:coalton #:coalton-prelude #:coalton-testing
        ))
(in-package :coalton-db/tests/main)

(named-readtables:in-readtable coalton:coalton)

(coalton-fiasco-init #:coalton-db/fiasco-test-package)
