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
   SqlValue
   SqlInt
   SqlText
   SqlBool
   SqlNull
   Value
   Values
   Row

   DbError
   QueryError
   ResultParseError
   DbResult

   ParseSql
   parse-sql

   SqlQuery

   DatabaseAdapter
   next-placeholder
   run-query!

   ;;; Library Private
   #:wrap-raw-sql-value
   #:unwrap-sql-value
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
    (QueryError String)
    (ResultParseError String))

  (define-instance (Signalable DbError)
    (define (error err)
      (match err
        ((QueryError str)
         (error str))
        ((ResultParseError str)
         (error str)))))

  (define-type-alias DbResult (Result DbError)))

;;;
;;; Parse SQL Values
;;;

(coalton-toplevel
  (define-class (ParseSql :a)
    (parse-sql (SqlValue -> DbResult :a)))

  (define-instance (ParseSql Integer)
    (define (parse-sql val)
      (match val
        ((SqlInt i) (Ok i))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to an integer.")))))))

  (define-instance (ParseSql String)
    (define (parse-sql val)
      (match val
        ((SqlText i) (Ok i))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to a string.")))))))

  (define-instance (ParseSql Boolean)
    (define (parse-sql val)
      (match val
        ((SqlBool b) (Ok b))
        ((SqlText s)
         (cond
           ((== s "FALSE") (Ok False))
           ((== s "TRUE") (Ok True))
           (True (Err (ResultParseError
                       (<> (<> "Could not convert " (force-string val))
                           " to a boolean."))))))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to a boolean.")))))))

  (define-instance (ParseSql :a => ParseSql (Optional :a))
    (define (parse-sql val)
      (match val
        ((SqlNull) (Ok None))
        (_ (map Some (parse-sql val)))))))

;;;
;;; SQL Query
;;;

(coalton-toplevel
  (define-type SqlQuery
    "A query that has been 'compiled' to a SQL query string and bound parameters."
    (SqlQuery String (List SqlValue))))

;;;
;;; Database Adapter
;;;

(coalton-toplevel
  (define-class (DatabaseAdapter :a)
    (next-placeholder (ty:Proxy :a -> Optional String -> String))
    (run-query! (:a -> SqlQuery -> DbResult (List Row)))))
