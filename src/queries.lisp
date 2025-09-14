(cl:in-package :cl-user)
(defpackage :coalton-db/queries
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames)
  (:export
   ;;; Library Public
   SqlValue
   SqlInt
   SqlText
   SqlBool
   SqlNull

   SqlQuery

   Query
   Select
   From

   to-sql
   ;;; Library Private
   ))

(in-package :coalton-db/queries)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (repr :lisp)
  (derive Eq)
  (define-type SqlValue
    "A runtime value inside of a SQL row."
    (SqlInt Integer)
    (SqlText String)
    (SqlBool Boolean)
    SqlNull)

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

  (define-type SqlQuery
    "A query that has been 'compiled' to a SQL query string and bound parameters."
    (SqlQuery String (List SqlValue))))

(coalton-toplevel
  (define-type Query
    "Representation of a SQL query."
    (Select% (List SqlValue) (Optional String))))

(cl:defmacro Select (vals cl:&optional from)
  "Select the given selectable objects in a SQL query."
  (cl:let ((from-clause (cl:if from
                          `(Some ,from)
                          `None)))
    `(Select% (make-list ,@(cl:mapcar (cl:lambda (x)
                                        `(into ,x))
                                      vals))
              ,from-clause)))

(coalton-toplevel
  (declare From (String -> String))
  (define From id))

(coalton-toplevel
  (declare to-sql (Query -> SqlQuery))
  (define (to-sql qry)
    "Convert a Query object to a SQL string that can be run in a database."
    (match qry
      ((Select% vals from-qry)
       (let placeholders = (join-str ", " (map (const "?") vals)))
       (let from-sql =
         (match from-qry
           ((Some from-table)
            (build-str " FROM " from-table))
           ((None)
            "")))
       (SqlQuery
        (build-str "SELECT " placeholders from-sql ";")
        vals)))))
