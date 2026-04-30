(defpackage coalton-db/tests/queries-to-row
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/to-row
        #:coalton-db/queries
        #:coalton-db/util
        #:coalton-db/tests/test-utils))
(in-package :coalton-db/tests/queries-to-row)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/queries-to-row-fiasco)
(coalton-fiasco-init #:coalton-db/tests/queries-to-row-fiasco)

;;;
;;; INSERT ToRow Tests
;;;

(coalton-toplevel
  (define-struct User
    (name String))

  (define-instance (ToRow User)
    (define (to-row user)
      (build-row user .name))))

(define-test test-insert-to-row-single-field ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "users")
                         (User "Steve"))))
  (is-sql-eql "INSERT INTO users VALUES (?);"
              ((SqlText "Steve"))
              result))

(coalton-toplevel
 (define-struct User2
   (name String)
   (age Integer)
   (favorite-food (Optional String))
   (verified? Boolean))

 (define-instance (ToRow User2)
   (define (to-row user)
     (build-row user .name .age .favorite-food .verified?))))

(define-test test-insert-to-row-multiple-fields ()
  (let result = (to-sql-test1
                 (Insert (IntoTable "users")
                         (User2 "Steve" 20 (Some "pizza") False))))
  (is-sql-eql "INSERT INTO users VALUES (?, ?, ?, ?);"
              ((SqlText "Steve")
               (SqlInt 20)
               (SqlText "pizza")
               (SqlBool False))
              result))

;;;
;;; UPDATE ToRow Tests
;;;

;; (define-test test-update-to-row-multiple-fields ()
;;   (let result = (to-sql-test1
;;                  (Update "users"
;;                          (User "Steve")
