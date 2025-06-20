(cl:in-package :cl-user)
(defpackage :coalton-db/core
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames
   (:l  #:coalton-library/list)
   (:m  #:coalton-library/ord-map)
   (:op #:coalton-library/optional)
   (:ty #:coalton-library/types)
   )
  ;; (:import-from
  ;;  )
  (:export
   ;;; Library Public
   #:SqlValue
   #:SqlInt
   #:SqlText
   #:SqlBool
   #:SqlNull

   #:TableName
   #:ColumnName

   #:SqlType
   #:IntType
   #:TextType
   #:BoolType

   #:DefaultOption
   #:ConstantValue
   #:CurrentTime
   #:CurrentDate
   #:CurrentTimestamp

   #:ColumnFlag
   #:PrimaryKey
   #:NotNullable
   #:Unique
   #:DefaultVal

   #:ColumnDef

   #:TableFlag
   #:CompositePrimaryKey
   #:CompositeUnique

   #:Relationship
   #:HasOne
   #:BelongsTo

   #:TableDef

   ;;; Library Private
   #:ConstantValue_

   #:is-default?
   #:has-default?

   #:ForeignKey_

   #:has-keys?
   #:rltn-table-name
   #:relationship-foreign-key

   #:all-flags
   #:column-names
   #:lookup-col!
   #:RowMap
   ))
(in-package :coalton-db/core)

(named-readtables:in-readtable coalton:coalton)

;;;;
;;;; COALTON-DB/CORE contains all of the code used for core SQL representations, including:
;;;; - Representing values returned from the database
;;;; - Defining schemas for tables in the database
;;;;

;;;
;;; SQL Values - Represent values returned from the database that have not been
;;; parsed into Coalton types yet, or Coalton values that have been serialized
;;; to be sent to the database.
;;;

(coalton-toplevel
  (repr :lisp)
  (define-type SqlValue
    "A runtime value inside of a SQL row."
    (SqlInt Integer)
    (SqlText String)
    (SqlBool Boolean)
    SqlNull)

  (derive-eq SqlValue ((SqlInt i) (SqlText t) (SqlBool b) SqlNull))

  ;; I think this is a bad idea because it's too tied to an individual DB.
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
        ((Some a) (into a)))))
  )

;;;
;;; Table Schema DSL - Define SQL table schemas in code.
;;;

(coalton-toplevel
  (define-type-alias TableName String)
  (define-type-alias ColumnName String)

  (define-type SqlType
    "The type of a SQL column or value."
    IntType
    TextType
    BoolType)

  (derive-eq SqlType (IntType TextType BoolType))

  (define-type DefaultOption
    "Possible values for a column default."
    (ConstantValue_ SqlValue)
    CurrentTime
    CurrentDate
    CurrentTimestamp)

  (derive-eq DefaultOption ((ConstantValue_ val) CurrentTime CurrentDate CurrentTimestamp))

  (declare ConstantValue (Into :a SqlValue => :a -> DefaultOption))
  (define (ConstantValue val)
    "A constant default value for a column."
    (ConstantValue_ (into val)))

  (define-type ColumnFlag
    "Flags on a SQL column."
    PrimaryKey
    NotNullable
    Unique
    (DefaultVal DefaultOption))

  (derive-eq ColumnFlag (PrimaryKey NotNullable Unique (DefaultVal v)))

  (define-struct ColumnDef
    (name ColumnName)
    (type SqlType)
    (flags (List ColumnFlag)))

  (declare is-default? (ColumnFlag -> Boolean))
  (define (is-default? flag)
    (match flag
      ((DefaultVal _) True)
      (_ False)))

  (declare has-default? (ColumnDef -> Boolean))
  (define (has-default? col)
    (op:some? (l:find is-default? (.flags col))))

  (define-type TableFlag
    "Flags on a SQL table."
    ;; Create a primary key on the first and remaining columns
    (CompositePrimaryKey ColumnName (List ColumnName))
    (CompositeUnique ColumnName (List ColumnName))
    ;; Stores corresponding keys as a tuple list of here -> there
    (ForeignKey_ TableName (List (Tuple ColumnName ColumnName)))))

(cl:defmacro ForeignKey (has-table here-key-there-key-pairs)
  "ForeignKey flag on a table. Can take either a String or a Persistable name
as the table, and takes a list of (here-key there-key) pairs for the columns.
Must specify at least one pair of keys!

Examples:
  (ForeignKey User ((\"user-id\" \"id\")))
  (ForeignKey \"User\" ((\"user-id\" \"id\")))
  (ForeignKey User ((\"user-id\" \"id\")
                    (\"proj-id\" \"proj-id\")))"
  (cl:if (cl:null here-key-there-key-pairs)
    (cl:error "Must supply at least one pair of keys!")
    `(ForeignKey_ (table-name (wrap-has-table-name ,has-table))
                  (make-list
                   ,@(cl:mapcar
                     #'(cl:lambda (pair)
                         `(Tuple ,(cl:first pair) ,(cl:second pair)))
                     here-key-there-key-pairs)))))

(coalton-toplevel
  (define-type Relationship
    (HasOne TableName)
    ;; List of (our key, their key)
    (BelongsTo_  TableName (List (Tuple ColumnName ColumnName))))

  (declare has-keys? (Relationship -> Boolean))
  (define (has-keys? rltn)
    "Return TRUE if RLTN stores the keys for the relationishp."
    (match rltn
      ((HasOne _) False)
      ((BelongsTo_ _ _) True)))

  (declare rltn-table-name (Relationship -> TableName))
  (define (rltn-table-name rltn)
    "Get the table name for the other side of a relationship."
    (match rltn
      ((HasOne name)
       name)
      ((BelongsTo_ name _)
       name)))

  (declare relationship-foreign-key (Relationship -> Optional TableFlag))
  (define (relationship-foreign-key rltn)
    "For RLTN, generate the foreign key(s) on the table that relationship is defined on."
    (match rltn
      ((HasOne _)
       None)
      ((BelongsTo_ other-tbl-name key-pairs)
       (Some (ForeignKey_ other-tbl-name key-pairs)))))

  (define-struct TableDef
    (name String)
    (columns (List ColumnDef))
    (flags (List TableFlag))
    (relationships (List Relationship)))

  (define-instance (Eq TableDef)
    (define (== a b)
      (== (.name a) (.name b))))
  )

(cl:defmacro BelongsTo (our-keys (other-table-name cl:&rest their-keys))
  "Example: (BelongsTo (\"user-id\" \"project-id\") (\"User\" \"id\" \"project-id\"))"
  (cl:let ((keypairs (cl:mapcar
                      (cl:lambda (a b)
                        `(Tuple ,a ,b))
                      our-keys
                      their-keys)))
    `(BelongsTo_
      ,other-table-name
      (make-list ,@keypairs))))

;;;
;;; TableDef utilities
;;;

(coalton-toplevel
  (declare all-flags (TableDef -> List TableFlag))
  (define (all-flags table)
    "Get all of the flags on a table, including those generated by relationship
definitions, etc."
    (<> (.flags table) (flatten-opts (map relationship-foreign-key (.relationships table)))))

  (declare column-names (TableDef -> List ColumnName))
  (define (column-names table)
    (map .name (.columns table)))

  (declare lookup-col! (TableDef -> ColumnName -> ColumnDef))
  (define (lookup-col! table name)
    (for col in (.columns table)
      (when (== (.name col) name)
        (return col)))
    (error (<> "Could not find column named " name)))

  (declare basic-column (String -> SqlType -> ColumnDef))
  (define (basic-column name type)
    "Create a SQL column definition with default flags:
* primary key?    = False
* not nullable?   = True
* unique?         = False"
    (ColumnDef name type (make-list)))

  (define-type-alias RowMap (m:Map ColumnName SqlValue))

  (define-instance (Into SqlValue String)
    (define (into val)
      (match val
        ((SqlInt i)
         (<> "SqlInt " (into i)))
        ((SqlText s)
         (<> "SqlText " s))
        ((SqlBool b)
         (if b
             "SqlBool True"
             "SqlBool False"))
        ((SqlNull)
         "SqlNull")))))
