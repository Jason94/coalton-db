(cl:in-package :cl-user)
(defpackage :coalton-db/to-row
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core)
  (:local-nicknames
   )
  (:export
   ;;; Library Public
   #:ToRow
   #:to-row

   #:build-row

   ;;; Library Private
   ))

(in-package :coalton-db/to-row)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define-class (ToRow :a)
    (to-row
     "Convert object to a Row of SqlValue's."
     (:a -> Row)))

  (define-instance (ToRow (List SqlValue))
    (define to-row id)))

(cl:defmacro build-row (obj cl:&rest funcs)
  `(Values ,@(cl:mapcar (cl:lambda (f)
                          `(,f ,obj))
                        funcs)))
