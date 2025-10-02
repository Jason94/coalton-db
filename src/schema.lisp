(cl:in-package :cl-user)
(defpackage :coalton-db/schema
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-library/experimental/loops
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/queries)
  (:local-nicknames
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
    (col-specs (List ColumnDefinition)))

  (declare contains-pkey? (Schema -> Boolean))
  (define (contains-pkey? s)
    (contains? PrimaryKey (>>= (.col-specs s) .properties)))

  (define default-pkey-col-def
     (ColumnDefinition "id" IntType (make-list PrimaryKey) False))

  (declare CreateSchema (Schema -> Query))
  (define (CreateSchema schema)
    (let col-specs =
      (if (contains-pkey? schema)
          (.col-specs schema)
          (Cons default-pkey-col-def (.col-specs schema))))
    (CreateTable%
     (.tbl-name schema)
     (make-list)
     col-specs
     (make-list)))
  )

(cl:defmacro column (col-name col-type cl:&rest properties)
  (col-clause-to-col-def-clause col-name col-type properties))

(cl:defmacro make-schema (tbl-name col-clauses)
  `(Schema
    ,tbl-name
    (make-list
     ,@col-clauses)))
