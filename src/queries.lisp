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
    (Select% (List SqlValue))))

(cl:defmacro Select (cl:&rest vals)
  "Select the given selectable objects in a SQL query."
  `(Select% (make-list ,@(cl:mapcar (cl:lambda (x)
                                      `(into ,x))
                                    vals))))
  ;; (declare Select (Into :a SqlValue => :a -> Query))
  ;; (define (Select val)
  ;;   "Select the given selectable object in a SQL query."
  ;;   (Select% (into val)))

(coalton-toplevel
  (declare to-sql (Query -> SqlQuery))
  (define (to-sql qry)
    (match qry
      ((Select% vals)
       (let placeholders = (join-str ", " (map (const "?") vals)))
       (SqlQuery
        (build-str "SELECT " placeholders ";")
        vals)))))
