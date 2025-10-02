(defpackage coalton-db/tests/from-row
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-library/result
        #:coalton-db/core
        #:coalton-db/from-row
        #:coalton-db/util
        #:coalton-db/tests/test-utils
        )
  )
(in-package :coalton-db/tests/from-row)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/from-row-fiasco)
(coalton-fiasco-init #:coalton-db/tests/from-row-fiasco)

;;;
;;; Test Basic Parsers
;;;

(define-test test-parse-text ()
  (is (== (Ok "Test")
          (parse-row (Values "Test")))))

(define-test test-parse-int ()
  (is (== (Ok 10)
          (parse-row (Values 10)))))

(define-test test-parse-bool ()
  (is (== (Ok True)
          (parse-row (Values True)))))

(define-test test-parse-some ()
  (is (== (Ok (Some 10))
          (parse-row (Values 10)))))

(define-test test-parse-none ()
  (let result = (the (DbResult (Optional Integer))
                     (parse-row (Values (the (Optional Integer)
                                             None)))))
  (is (== (Ok None)
          result)))

(define-test test-parse-errors-type-mismatch ()
  (let result = (the (DbResult Integer)
                     (parse-row (Values "Text"))))
  (is (err? result)))

;;;
;;; Test Custom Record Parser
;;;

(coalton-toplevel
  (derive Eq)
  (define-struct SimpleUser
    (name String)
    (verified? Boolean))

  (define-row-parser SimpleUser
    sql-value-parser
    sql-value-parser)
  )

(define-test test-parse-row-simple-record ()
  (is (== (Ok (SimpleUser "Steve" False))
          (parse-row (Values "Steve" False)))))

(define-test test-parse-row-errors-with-too-few ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values "Steve"))))
  (is (err? result)))

(define-test test-parse-row-errors-with-too-many ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values "Steve" False "Extra"))))
  (is (err? result)))

(define-test test-parse-row-errors-type-mismatch ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values "Steve" "False"))))
  (is (err? result)))
