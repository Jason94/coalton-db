(cl:in-package :cl-user)
(defpackage :coalton-db/persistable
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/from-row
   #:coalton-db/queries
   #:coalton-db/schema
   )
  (:local-nicknames
   (:l #:coalton-library/list)
   (:op #:coalton-library/optional)
   (:ty #:coalton-library/types)
   )
  (:export
   ;;; Library Public
   #:Persistable
   #:schema-for
   #:prop-for-col

   ;;; Library Private
   #:pkey-col-val-pairs
   #:schema-for-obj
   #:tbl-name-for-obj
   #:pkey-cnd-for
   ))

(in-package :coalton-db/persistable)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define-class (ParseSqlRow :a => Persistable :a)
    (schema-for (ty:Proxy :a -> Schema))
    (prop-for-col
     "Get the property value on this Persistable corresponding to
the column with the given name, if any."
     (:a -> String -> Optional SqlValue)))

  (inline)
  (declare schema-for-obj (Persistable :p => :p -> Schema))
  (define (schema-for-obj p)
    (schema-for (ty:proxy-of p)))

  (inline)
  (declare tbl-name-for-obj (Persistable :p => :p -> String))
  (define (tbl-name-for-obj p)
    (.tbl-name (schema-for-obj p)))

  (declare pkey-col-val-pairs (Persistable :p => :p -> Tuple (List String) (List SqlValue)))
  (define (pkey-col-val-pairs obj)
    "Get all of the column names and corresponding values for the primary key on `obj`."
    (let pkey-col-names = (pkey-col-names (schema-for (ty:proxy-of obj))))
    (let pkey-vals =
      (op:from-some (build-str (force-string obj) " is missing one or more primary key values.")
                    (traverse (prop-for-col obj)
                              pkey-col-names)))
    (Tuple pkey-col-names pkey-vals))

  (declare pkey-cnd-for (Persistable :p => :p -> RowCondition))
  (define (pkey-cnd-for obj)
    (let (Tuple pkey-names pkey-vals) = (pkey-col-val-pairs obj))
    (let zipped-pkeys = (l:zip pkey-names pkey-vals))
    (let initial-condition =
      (op:from-some "Object is missing primary key values."
                    (map (fn ((Tuple col val))
                           (Eq_ col val))
                         (head zipped-pkeys))))
    (fold
     (fn (existing-cond (Tuple col val))
       (And_ existing-cond (Eq_ col val)))
     initial-condition
     (match (tail zipped-pkeys)
       ((Some l) l)
       ((None) Nil))))
  )
