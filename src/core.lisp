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

   ParseError
   ParseResult
   ParseSql
   parse-sql

   SqlQuery
   QueryError

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
    ;; ((coalton (== (lisp SqlType () type) IntType))
    ;;  (SqlInt raw-val))
    ;; ((coalton (== (lisp SqlType () type) TextType))
    ;;  (SqlText raw-val))
    ;; ((coalton (== (lisp SqlType () type) BoolType))
    ;;  (SqlBool raw-val))
    (cl:t (cl:error (cl:format cl:nil "Unknown SQL type: ~a" raw-val)))))

;;;
;;; Parse SQL Values
;;;

(coalton-toplevel
  (define-type-alias ParseError String)
  (define-type-alias ParseResult (Result ParseError))

  (define-class (ParseSql :a)
    (parse-sql (SqlValue -> ParseResult :a)))

  (define-instance (ParseSql Integer)
    (define (parse-sql val)
      (match val
        ((SqlInt i) (Ok i))
        (_ (Err (<> (<> "Could not convert " (force-string val))
                    " to an integer."))))))

  (define-instance (ParseSql String)
    (define (parse-sql val)
      (match val
        ((SqlText i) (Ok i))
        (_ (Err (<> (<> "Could not convert " (force-string val))
                    " to a string.")))))))

;;;
;;; SQL Query
;;;

(coalton-toplevel
  (define-type SqlQuery
    "A query that has been 'compiled' to a SQL query string and bound parameters."
    (SqlQuery String (List SqlValue)))

  (derive Eq)
  (define-type QueryError
    (QueryError String))

  (define-instance (Signalable QueryError)
    (define (error (QueryError str))
      (error str)))

  (define-type-alias QueryResult (Result QueryError)))

;;;
;;; Database Adapter
;;;

(coalton-toplevel
  (define-class (DatabaseAdapter :a)
    (next-placeholder (ty:Proxy :a -> Optional String -> String))
    (run-query! (:a -> SqlQuery -> QueryResult (List Row)))))
