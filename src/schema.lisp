(cl:in-package :cl-user)
(defpackage :coalton-db/schema
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/queries
   )
  (:local-nicknames
   (:l #:coalton-library/list)
   )
  (:export
   ;;; Library Public
   #:column
   #:make-schema
   #:CreateSchema
   #:DropSchema

   ;;; Library Private
   #:pkey-col-names
   #:col-names
   #:non-pkey-col-names
   ))

(in-package :coalton-db/schema)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare contains-pkey? (List ColumnDefinition -> List TableProperty -> Boolean))
  (define (contains-pkey? col-specs tbl-specs)
    (or
     (contains? PrimaryKey (>>= col-specs .properties))
     (contains-where? is-composite-pkey? tbl-specs)))

  (define default-pkey-col-def
     (ColumnDefinition "id" IntType (make-list PrimaryKey) False))

  (declare generate-cols (List ColumnDefinition -> List TableProperty -> List ColumnDefinition))
  (define (generate-cols col-specs tbl-specs)
    "Based on the user-specified column and table specs, generate the full list of columns
for the SQL table."
    (if (contains-pkey? col-specs tbl-specs)
        col-specs
        (Cons default-pkey-col-def col-specs)))

  (declare CreateSchema% (Schema -> List CreateTableOption -> Query))
  (define (CreateSchema% schema create-opts)
    (CreateTable%
     (.tbl-name schema)
     create-opts
     (generate-cols (.col-specs schema) (.tbl-props schema))
     (.tbl-props schema)))

  (declare pkey-col-names (Schema -> List String))
  (define (pkey-col-names schema)
    (for col in (.col-specs schema)
      (when (contains? PrimaryKey (.properties col))
        (return (make-list (.col-name
                            (the ColumnDefinition col))))))
    (for tbl-prop in (.tbl-props schema)
      (match tbl-prop
        ((CompositePrimaryKey% col-names) (return col-names))))
    (error (build-str "Table " (.tbl-name schema) " defined without a primary key!")))

  (declare col-names (Schema -> List String))
  (define (col-names schema)
    (map .col-name (.col-specs schema)))

  (declare non-pkey-col-names (Schema -> List String))
  (define (non-pkey-col-names schema)
    (let pkeys = (pkey-col-names schema))
    (l:remove-if (fn (x) (contains? x pkeys))
                 (col-names schema)))
  )

(cl:defmacro column (col-name col-type cl:&rest properties)
  (col-clause-to-col-def-clause col-name col-type properties))

(cl:defmacro make-schema (tbl-name col-clauses cl:&optional tbl-prop-clauses)
  (cl:let ((user-col-specs (cl:gensym "user-col-specs"))
           (user-tbl-props (cl:gensym "user-tbl-props"))
           (col-specs (cl:gensym "col-specs")))
    `(let ((,user-col-specs (make-list ,@col-clauses))
           (,user-tbl-props (make-list ,@tbl-prop-clauses))
           (,col-specs (generate-cols ,user-col-specs ,user-tbl-props)))
       (Schema
        ,tbl-name
        ,col-specs
        ,user-tbl-props))))

(cl:defmacro CreateSchema (schema cl:&optional create-opts)
  `(CreateSchema% ,schema (make-list ,@create-opts)))

(cl:defmacro DropSchema (schema cl:&rest drop-opts)
  `(DropTable (.tbl-name ,schema) ,@drop-opts))
