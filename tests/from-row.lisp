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
          (parse-row (Values_ "Test")))))

(define-test test-parse-int ()
  (is (== (Ok 10)
          (parse-row (Values_ 10)))))

(define-test test-parse-bool ()
  (is (== (Ok True)
          (parse-row (Values_ True)))))

(define-test test-parse-bool-true-str ()
  (is (== (Ok True)
          (parse-row (Values_ "TRUE")))))

(define-test test-parse-bool-false-str ()
  (is (== (Ok False)
          (parse-row (Values_ "FALSE")))))

(define-test test-parse-some ()
  (is (== (Ok (Some 10))
          (parse-row (Values_ 10)))))

(define-test test-parse-none ()
  (let result = (the (DbResult (Optional Integer))
                     (parse-row (Values_ (the (Optional Integer)
                                             None)))))
  (is (== (Ok None)
          result)))

(define-test test-parse-tuple ()
  (is (== (Ok (Tuple 10 "Text"))
          (parse-row (Values_ 10 "Text")))))

(define-test test-parse-errors-type-mismatch ()
  (let result = (the (DbResult Integer)
                     (parse-row (Values_ "Text"))))
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
          (parse-row (Values_ "Steve" False)))))

(define-test test-parse-row-errors-with-too-few ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values_ "Steve"))))
  (is (err? result)))

(define-test test-parse-row-errors-with-too-many ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values_ "Steve" False "Extra"))))
  (is (err? result)))

(define-test test-parse-row-errors-type-mismatch ()
  (let result = (the (DbResult SimpleUser)
                     (parse-row (Values_ "Steve" "False"))))
  (is (err? result)))
