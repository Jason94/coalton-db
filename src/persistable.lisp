(cl:in-package :cl-user)
(defpackage :coalton-db/persistable
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   )
  (:local-nicknames
   )
  (:export
   ;;; Library Public
   #:Persistable

   ;;; Library Private
   ))

(in-package :coalton-db/persistable)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define-class (Persistable :a)
    ))
