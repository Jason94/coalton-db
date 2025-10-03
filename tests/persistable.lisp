(defpackage coalton-db/tests/persistable
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/to-row
        #:coalton-db/persistable
        #:coalton-db/util
        #:coalton-db/queries
        #:coalton-db/tests/test-utils
        )
  )
(in-package :coalton-db/tests/persistable)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/persistable-fiasco)
(coalton-fiasco-init #:coalton-db/tests/persistable-fiasco)

