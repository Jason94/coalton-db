(defpackage coalton-db/tests/to-row
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/to-row
        #:coalton-db/util
        #:coalton-db/queries
        #:coalton-db/tests/test-utils
        )
  )
(in-package :coalton-db/tests/to-row)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/to-row-fiasco)
(coalton-fiasco-init #:coalton-db/tests/to-row-fiasco)

;;
;; Test to-row
;;

(coalton-toplevel
  (define-struct User
    (name String))

  (define-instance (ToRow User)
    (define (to-row user)
      (build-row user .name))))

(define-test test-single-field ()
  (let vals = (to-row (User "Steve")))
  (is (== (Values "Steve")
          vals)))

(coalton-toplevel
 (define-struct User2
   (name String)
   (age Integer)
   (favorite-food (Optional String))
   (verified? Boolean))

 (define-instance (ToRow User2)
   (define (to-row user)
     (build-row user .name .age .favorite-food .verified?))))

(define-test test-multiple-fields ()
  (let vals = (to-row (User2 "Steve" 20 (Some "pizza") False)))
  (is (== (Values "Steve" 20 "pizza" False)
          vals)))

;;
;; Test Query DSL
;;

(define-test test-insert-single-field ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "users")
                         (User "Steve"))))
  (is-sql-eql "INSERT INTO users VALUES (?);"
              ((SqlText "Steve"))
              result))

(define-test test-insert-multiple-fields ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "users")
                         (User2 "Steve" 20 (Some "pizza") False))))
  (is-sql-eql "INSERT INTO users VALUES (?, ?, ?, ?);"
              ((SqlText "Steve")
               (SqlInt 20)
               (SqlText "pizza")
               (SqlBool False))
              result))
