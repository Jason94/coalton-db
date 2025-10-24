(cl:in-package :cl-user)
(defpackage :coalton-db/core
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames
   (:ty #:coalton-library/types))
  (:export
   ;;; Library Public
   #:SqlType
   #:IntType
   #:TextType
   #:BoolType

   #:SqlValue
   #:SqlInt
   #:SqlText
   #:SqlBool
   #:SqlNull
   #:Value
   #:Values
   #:Row

   #:DbError
   #:QueryConstructionError
   #:QueryError
   #:ResultParseError
   #:DbResult

   #:SqlQuery

   #:PrimaryKey
   #:Unique
   #:Default%
   #:Default_
   #:Nullable
   #:AutoIncrement

   #:Schema

   #:DatabaseAdapter

   ;;; Library Private
   #:next-placeholder
   #:run-query!
   #:auto-increment-syntax
   #:AutoIncrementSyntax
   #:execute-query!_

   #:wrap-raw-sql-value
   #:unwrap-sql-value

   #:ColumnDefinition
   #:TableProperty
   #:CompositePrimaryKey%
   #:SqlTable
   ))

(in-package :coalton-db/core)

(named-readtables:in-readtable coalton:coalton)

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

  ;; TODO: Replace using into for this with a custom ToSqlValue class, or something

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
        ((Some a) (into a)))))

  (define-type-alias Row (List SqlValue)))

(cl:defmacro Values (cl:&rest vals)
  "A list of raw SQL values."
  `(the (List SqlValue)
    (make-list ,@(cl:mapcar (cl:lambda (x)
                              `(into ,x))
                            vals))))

(cl:defun unwrap-sql-value (val)
  "Unwrap VAL and return the value inside it or a constant representation. Must be
a type that can be passed directly to a DB implementation as a bound value."
  (cl:cond
    ((cl:typep val 'SqlValue/SqlInt)
     (SqlValue/SqlInt-_0 val))
    ((cl:typep val 'SqlValue/SqlText)
     (SqlValue/SqlText-_0 val))
    ((cl:typep val 'SqlValue/SqlBool)
     (SqlValue/SqlBool-_0 val))
    ((cl:typep val 'SqlValue/SqlNull)
     cl:nil)
    (cl:t (cl:error (cl:format cl:nil "Unknown SQL Value: ~a" val)))))

(cl:defun wrap-raw-sql-value (raw-val)
  "Wrap VAL in the appropriate SqlValue."
  (cl:cond
    ((cl:not raw-val)
     SqlNull)
    ((cl:typep raw-val 'cl:integer)
     (SqlInt raw-val))
    ((cl:typep raw-val 'cl:string)
     (SqlText raw-val))
    (cl:t (cl:error (cl:format cl:nil "Unknown SQL type: ~a" raw-val)))))

;;;
;;; Universal error type
;;;

(coalton-toplevel
  (derive Eq)
  (define-type DbError
    (QueryConstructionError String)
    (QueryError String)
    (ResultParseError String))

  (define-instance (Signalable DbError)
    (define (error err)
      (match err
        ((QueryConstructionError str)
         (error str))
        ((QueryError str)
         (error str))
        ((ResultParseError str)
         (error str)))))

  (define-type-alias DbResult (Result DbError)))

;;;
;;; SQL Query
;;;

(coalton-toplevel
  (define-type SqlQuery
    "A query that has been 'compiled' to a SQL query string and bound parameters."
    (SqlQuery String (List SqlValue))))

;;;
;;; Table/Schema Definitions
;;;

(coalton-toplevel

  (derive Eq)
  (define-type ColumnProperty
    PrimaryKey
    Unique
    (Default% SqlValue))

  (define-type GhostColumnProperty
    "Keywords used in the syntax, but not inserted as column propertiese into the
column definition."
    Nullable
    "SQL defaults to Nullable, but coalton-db defaults to Not-Nullable. To support
that, coalton-db inserts 'NOT NULL' by default, and does *not* do that if the
`Nullable` 'ghost' property is used in the definition."
    AutoIncrement
    "Different adapters write AutoIncrement before/after the 'PRIMARY KEY' modifier,
so we can't serialize it directly into the sql query string.")

  (repr :enum)
  (derive Eq)
  (define-type SqlType
    IntType
    TextType
    BoolType)

  (define-struct ColumnDefinition
    (col-name String)
    (col-type SqlType)
    (properties (List ColumnProperty))
    ;; TODO: Convert these to a (List GhostColumnProperty)
    (nullable? Boolean)
    (auto-increment? Boolean))

  (define-type-alias SqlTable String)

  (derive Eq)
  (define-type TableProperty
    (CompositePrimaryKey% (List SqlTable)))

  (define-struct Schema
    (tbl-name String)
    (col-specs (List ColumnDefinition))
    (tbl-props (List TableProperty))))

(cl:defmacro Default_ (val)
  `(Default% (into ,val)))

;;;
;;; Database Adapter
;;;

(coalton-toplevel
  (define-struct AutoIncrementSyntax
    "Store SQL strings to be inserted before and/or after 'PRIMARY KEY' in an
AutoIncrement column."
    (before-pkey String)
    (after-pkey String))

  (define-class (DatabaseAdapter :a)
    (next-placeholder (ty:Proxy :a -> Optional String -> String))
    (auto-increment-syntax (ty:Proxy :a -> AutoIncrementSyntax))
    (run-query! (:a -> SqlQuery -> DbResult (List Row))))

  ;; NOTE: Depending on the underlying database library, it might be worth exposing
  ;; this to DatabaseAdapter.
  (declare execute-query!_ (DatabaseAdapter :a => :a -> SqlQuery -> DbResult Unit))
  (define (execute-query!_ cnxn qry)
    (map (const Unit) (run-query! cnxn qry))))
