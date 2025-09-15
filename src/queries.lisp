(cl:in-package :cl-user)
(defpackage :coalton-db/queries
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames
   (:c #:coalton-library/cell)
   (:lp #:coalton-library/experimental/loops)
   (:s #:coalton-library/string)
   (:ty #:coalton-library/types)
   (:itr #:coalton-library/iterator))
  (:export
   ;;; Library Public
   SqlValue
   SqlInt
   SqlText
   SqlBool
   SqlNull
   Value

   SqlQuery

   DatabaseAdapter
   next-placeholder

   RowCondition
   Value
   True_
   False_
   Eq_
   Neq_

   Where

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

  (define-type SqlQuery
    "A query that has been 'compiled' to a SQL query string and bound parameters."
    (SqlQuery String (List SqlValue))))

(coalton-toplevel
  (define-class (DatabaseAdapter :a)
    (next-placeholder (ty:Proxy :a -> Optional String -> String))))

(coalton-toplevel
  (define-type RowConditionTarget
    "A column/value in a WHERE/etc clause."
    (Col_ String)
    (Value_ SqlValue))

  (define-instance (Into String RowConditionTarget)
    (inline)
    (define into Col_))

  (define-instance (Into SqlValue RowConditionTarget)
    (inline)
    (define into Value_))

  ;; NOTE: Everything will be exported with a suffix `_` for consistency of the
  ;; RowCondition API. Those with % will be wrapped in conversion macros.
  (define-type RowCondition
    "A condition to filter a query."
    True_
    False_
    (Eq% RowConditionTarget RowConditionTarget)
    (Neq% RowConditionTarget RowConditionTarget))

  (define-type QueryOption
    "Options to modify a query."
    (Where RowCondition)))

(cl:defmacro Eq_ (a b)
  `(Eq% (into ,a) (into ,b)))

(cl:defmacro Neq_ (a b)
  `(Neq% (into ,a) (into ,b)))

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
    (Select% SelectTarget (Optional FromStatement) (Optional QueryOption))))

(cl:defmacro Values (cl:&rest vals)
  "Select literal SQL values."
  `(Values% (make-list ,@(cl:mapcar (cl:lambda (x)
                                      `(into ,x))
                                    vals))))

(cl:defmacro Cols (cl:&rest cols)
  "Select columns."
  `(Cols% (make-list ,@cols)))

(cl:defmacro Select (vals cl:&optional from cl:&rest query-opts)
  "Select the given selectable objects in a SQL query."
  (cl:let ((from-clause (cl:if from
                          `(Some ,from)
                          `None))
           (opts-clause (cl:if query-opts
                          `(Some ,(cl:first query-opts))
                          `None)))
    `(Select% ,vals ,from-clause ,opts-clause)))

(coalton-toplevel
  (declare From (String -> FromStatement))
  (define From id))

(coalton-toplevel
  (declare get-placeholders! (DatabaseAdapter :a => ty:Proxy :a -> c:Cell (Optional String) -> UFix -> List String))
  (define (get-placeholders! db-adptr-proxy last-param-str n)
    "Get the next `n` placeholder strings. Will set `last-param-str` to the end of the returned list."
    (lp:collecttimes (_ n)
      (let next-param-str = (next-placeholder db-adptr-proxy (c:read last-param-str)))
      (c:write! last-param-str (Some next-param-str))
      next-param-str))

  (declare get-next-placeholder! (DatabaseAdapter :a => ty:Proxy :a -> c:Cell (Optional String) -> String))
  (define (get-next-placeholder! db-adptr-proxy last-param-str)
    "Get the next placeholder string, set it as the new `last-param-str`, and return."
    (let result = (next-placeholder db-adptr-proxy (c:read last-param-str)))
    (c:write! last-param-str (Some result))
    result)

  (declare row-cnd-tgt-to-sql! (DatabaseAdapter :a => ty:Proxy :a -> c:Cell (Optional String) -> RowConditionTarget
                                                -> (Tuple String (List SqlValue))))
  (define (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str tgt)
    (match tgt
      ((Col_ col-name)
       (Tuple col-name (make-list)))
      ((Value_ val)
       (Tuple (get-next-placeholder! db-adptr-proxy last-param-str) (make-list val)))))

  (declare row-condition-to-sql! (DatabaseAdapter :a => ty:Proxy :a -> c:Cell (Optional String) -> RowCondition
                                                  -> (Tuple String (List SqlValue))))
  (define (row-condition-to-sql! db-adptr-proxy last-param-str row-cnd)
    (match row-cnd
      ((True_)
       (Tuple "TRUE" (make-list)))
      ((False_)
       (Tuple "FALSE" (make-list)))
      ((Eq% a b)
       (let (Tuple sql-a params-a) =
         (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str a))
       (let (Tuple sql-b params-b) =
         (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str b))
       (Tuple (build-str sql-a " = " sql-b) (<> params-a params-b)))
      ((Neq% a b)
       (let (Tuple sql-a params-a) =
         (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str a))
       (let (Tuple sql-b params-b) =
         (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str b))
       (Tuple (build-str sql-a " <> " sql-b) (<> params-a params-b)))))

  (declare to-sql (DatabaseAdapter :a => ty:Proxy :a -> Query -> SqlQuery))
  (define (to-sql db-adptr-proxy qry)
    "Convert a Query object to a SQL string that can be run in a database."
    (let last-param-str = (the (c:Cell (Optional String)) (c:new None)))
    (match qry
      ((Select% select-target from-qry query-opts)
       (let (Tuple select-sql select-params) =
         (match select-target
           ((Values% vals)
            (let placeholders = (join-str ", " (map (fn (_) (get-next-placeholder! db-adptr-proxy last-param-str))
                                                    vals)))
            (let select-sql = (build-str "SELECT " placeholders))
            (Tuple select-sql vals))
           ((AllCols)
            (Tuple "SELECT *" (make-list)))
           ((Cols% cols)
            (Tuple (build-str "SELECT " (join-str ", " cols)) (make-list)))))
       (let from-sql =
         (match from-qry
           ((Some from-table)
            (build-str " FROM " from-table))
           ((None)
            "")))
       (let (Tuple opts-sql opts-params) =
         (match query-opts
           ((Some (Where cnd))
            (let (Tuple cnd-sql cnd-params) =
              (row-condition-to-sql! db-adptr-proxy last-param-str cnd))
            (Tuple
             (build-str " WHERE " cnd-sql)
             cnd-params))
           ((None)
            (Tuple "" (make-list)))))
       (SqlQuery
        (build-str select-sql from-sql opts-sql ";")
        (<> select-params opts-params))))))
