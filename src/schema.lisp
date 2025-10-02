(cl:in-package :cl-user)
(defpackage :coalton-db/schema
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/queries)
  (:local-nicknames
   (:q #:coalton-db/queries)
   )
  (:export
   ;;; Library Public
   #:Schema
   #:column
   #:make-schema
   #:CreateSchema

   ;;; Library Private
   ))

(in-package :coalton-db/schema)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define-struct Schema
    (tbl-name String)
    (col-specs (List q::ColumnDefinition)))

  (declare CreateSchema (Schema -> Query))
  (define (CreateSchema schema)
    (q::CreateTable%
     (.tbl-name schema)
     (make-list)
     (.col-specs schema)
     (make-list)))
  )

(cl:defmacro column (col-name col-type)
  `(q::ColumnDefinition ,col-name ,col-type (make-list) False))

(cl:defmacro make-schema (tbl-name col-clauses)
  `(Schema
    ,tbl-name
    (make-list
     (q::ColumnDefinition "id" IntType (make-list PrimaryKey) False)
     ,@col-clauses)))
