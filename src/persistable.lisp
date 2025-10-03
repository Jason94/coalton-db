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

   #:delete-obj-query
   #:insert-obj-query
   #:insert-objs-query

   ;;; Library Private
   #:pkey-col-val-pairs
   #:schema-for-obj
   #:tbl-name-for-obj
   #:pkey-cnd-for
   #:col-names-for
   #:sql-vals-for
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

  (declare col-names-for (Persistable :p => :p -> List String))
  (define (col-names-for obj)
    (col-names (schema-for-obj obj)))

  (declare sql-vals-for (Persistable :p => :p -> List Sqlvalue))
  (define (sql-vals-for obj)
    "Get all of the SQL values for `obj`s data, in column order."
    (op:from-some "Object missing data for column."
                  (traverse (prop-for-col obj) (col-names-for obj))))
  )

;;;
;;; FRM Queries
;;;

(coalton-toplevel
  (declare delete-obj-query (Persistable :p => :p -> Query))
  (define (delete-obj-query obj)
    (Delete (From (tbl-name-for-obj obj))
            (Where (pkey-cnd-for obj))))

  (declare insert-obj-query (Persistable :p => :p -> Query))
  (define (insert-obj-query obj)
    (Insert (IntoTable (tbl-name-for-obj obj))
            (sql-vals-for obj)
            ;; NOTE: Maybe using the col-names isn't necessary?
            (map LiteralColumn% (col-names-for obj))))

  (declare insert-objs-query (Persistable :p => List :p -> Optional Query))
  (define (insert-objs-query objs)
    "Generate a query to insert `objs`. If empty, returns `None`."
    (match objs
      ((Nil) None)
      ((Cons fst _)
       (let cols = (map LiteralColumn% (col-names-for fst)))
       (let vals = (the (List SqlValue)
                        (>>= objs sql-vals-for)))
       (Some
        (Insert (IntoTable (tbl-name-for-obj fst))
                vals
                cols)))))

  )
