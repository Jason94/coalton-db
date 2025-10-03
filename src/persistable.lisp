(cl:in-package :cl-user)
(defpackage :coalton-db/persistable
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/from-row
   #:coalton-db/schema
   )
  (:local-nicknames
   (:ty #:coalton-library/types)
   )
  (:export
   ;;; Library Public
   #:Persistable
   #:schema-for

   ;;; Library Private
   ))

(in-package :coalton-db/persistable)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define-class (ParseSqlRow :a => Persistable :a)
    (schema-for (ty:Proxy :a -> Schema)))
  )
