(cl:in-package :cl-user)
(defpackage :coalton-db/core
  (:use
   #:coalton
   #:coalton-prelude)
  (:local-nicknames
   (:ty #:coalton-library/types))
  (:export
   ;;; Library Public
   DatabaseAdapter
   next-placeholder

   SqlValue
   SqlInt
   SqlText
   SqlBool
   SqlNull
   Value
   Values

   ;;; Library Private
   ))

(in-package :coalton-db/core)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Database Adapter
;;;

(coalton-toplevel
  (define-class (DatabaseAdapter :a)
    (next-placeholder (ty:Proxy :a -> Optional String -> String))))

;;;
;;; Raw SQL Values
;;;

(coalton-toplevel
  (repr :lisp)
  (derive Eq)
  (define-type SqlValue
    "A runtime value inside of a SQL row."
    (SqlInt Integer)
    (SqlText String)
    (SqlBool Boolean)
    SqlNull)

  (inline)
  (declare Value (Into :a SqlValue => :a -> SqlValue))
  (define Value into)

  (define-instance (Into Integer SqlValue)
    (define into SqlInt))

  (define-instance (Into String SqlValue)
    (define into SqlText))

  (define-instance (Into Boolean SqlValue)
    (define into SqlBool))

  (define-instance (Into :a SqlValue => Into (Optional :a) SqlValue)
    (define (into a)
      (match a
        ((None) SqlNull)
        ((Some a) (into a))))))

(cl:defmacro Values (cl:&rest vals)
  "A list of raw SQL values."
  `(the (List SqlValue)
    (make-list ,@(cl:mapcar (cl:lambda (x)
                              `(into ,x))
                            vals))))
