(defpackage coalton-db/tests/core
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core)
  (:local-nicknames))
(in-package :coalton-db/tests/core)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/core-fiasco)
(coalton-fiasco-init #:coalton-db/tests/core-fiasco)

