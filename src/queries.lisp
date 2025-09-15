(cl:in-package :cl-user)
(defpackage :coalton-db/queries
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames
   (:ty #:coalton-library/types)
   (:itr #:coalton-library/iterator))
  (:export
   ;;; Library Public
   SqlValue
   SqlInt
   SqlText
   SqlBool
   SqlNull

   SqlQuery

   DatabaseAdapter
   generate-placeholders

   Query
   Select
   Values
   AllCols
   Cols
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
  (define-class (DatabaseAdapter :a)
    (generate-placeholders (ty:Proxy :a -> List SqlValue -> List String))))

(coalton-toplevel
  (define-type SelectTarget
    "Things that can be selected against."
    (Values% (List SqlValue))
    AllCols
    (Cols% (List String))
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

(cl:defmacro Cols (cl:&rest cols)
  "Select columns."
  `(Cols% (make-list ,@cols)))

(cl:defmacro Select (vals cl:&optional from)
  "Select the given selectable objects in a SQL query."
  (cl:let ((from-clause (cl:if from
                          `(Some ,from)
                          `None)))
    `(Select% ,vals ,from-clause)))

(coalton-toplevel
  (declare From (String -> FromStatement))
  (define From id))

(coalton-toplevel
  (declare to-sql (DatabaseAdapter :a => ty:Proxy :a -> Query -> SqlQuery))
  (define (to-sql db-adptr-proxy qry)
    "Convert a Query object to a SQL string that can be run in a database."
    (match qry
      ((Select% select-target from-qry)
       ;; The SQL chunks are the things to be joined by the generated placeholders
       (let (Tuple select-sql-chunks select-params) =
         (match select-target
           ((Values% vals)
            (let placeholder-commas = (itr:collect! (itr:repeat-for ", " (max 0 (- (length vals) 1)))))
            (let select-sql = (Cons "SELECT " placeholder-commas))
            (Tuple select-sql vals))
           ((AllCols)
            (Tuple (make-list "SELECT *") (make-list)))
           ((Cols% cols)
            (Tuple (make-list (build-str "SELECT " (join-str ", " cols))) (make-list)))))
       (let from-sql =
         (match from-qry
           ((Some from-table)
            (build-str " FROM " from-table))
           ((None)
            "")))
       (let placeholders = (generate-placeholders db-adptr-proxy select-params))
       (let all-sql-chunks = (<> select-sql-chunks (make-list from-sql)))
       (let sql-chunks-and-placeholders = (the (List String) (itr:collect! (itr:interleave! (itr:into-iter all-sql-chunks)
                                                                                            (itr:into-iter placeholders)))))
       (let all-sql = (fold <> "" sql-chunks-and-placeholders))
       (SqlQuery
        (build-str all-sql ";")
        select-params)))))
