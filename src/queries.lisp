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
   Values
   AllCols
   From

   to-sql
   ;;; Library Private
   ))

(in-package :coalton-db/queries)

(cl:declaim (cl:optimize (cl:speed 0) (cl:space 0) (cl:debug 3)))

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
  (define-type SelectTarget
    "Things that can be selected against."
    (Values% (List SqlValue))
    AllCols
    )

  (define-type-alias FromStatement String)

  (define-type Query
    "Representation of a SQL query."
    (Select% SelectTarget (Optional FromStatement))))

(cl:defmacro Values (cl:&rest vals)
  "Select literal SQL values."
  `(Values% (make-list ,@(cl:mapcar (cl:lambda (x)
                                      `(into ,x))
                                    vals))))

(cl:defmacro Select (vals cl:&optional from)
  "Select the given selectable objects in a SQL query."
  (cl:let ((from-clause (cl:if from
                          `(Some ,from)
                          `None)))
    `(Select% ,vals ,from-clause)))

(coalton (Values 4))
(coalton (values% (make-list (into "hi"))))
(coalton (the SqlValue (SqlInt 4)))

(coalton-toplevel
  (declare From (String -> FromStatement))
  (define From id))

(coalton-toplevel
  (declare to-sql (Query -> SqlQuery))
  (define (to-sql qry)
    "Convert a Query object to a SQL string that can be run in a database."
    (match qry
      ((Select% select-target from-qry)
       (let (Tuple select-sql select-params) =
         (match select-target
           ((Values% vals)
            (let placeholders = (join-str ", " (map (const "?") vals)))
            (let select-sql = (build-str "SELECT " placeholders))
            (Tuple select-sql vals))
           ((AllCols)
            (Tuple "SELECT *" (make-list)))))
       (let from-sql =
         (match from-qry
           ((Some from-table)
            (build-str " FROM " from-table))
           ((None)
            "")))
       (SqlQuery
        (build-str select-sql from-sql ";")
        select-params)))))
