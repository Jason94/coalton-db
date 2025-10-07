(cl:in-package :cl-user)
(defpackage :coalton-db/queries
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core
   #:coalton-db/to-row
   #:coalton-db/util)
  (:local-nicknames
   (:c #:coalton-library/cell)
   (:lp #:coalton-library/experimental/loops)
   (:s #:coalton-library/string)
   (:ty #:coalton-library/types)
   (:lst #:coalton-library/list)
   (:op #:coalton-library/optional)
   (:itr #:coalton-library/iterator))
  (:export
   ;;; Library Public
   #:Cols

   #:RowCondition
   #:True_
   #:False_
   #:Eq_
   #:Neq_
   #:Gt_
   #:GtEq_
   #:Lt_
   #:LtEq_
   #:IsNull_
   #:IsNotNull_
   #:Not_
   #:And_
   #:Or_

   #:Where

   #:Query
   #:Select
   #:AllCols
   #:From
   #:Delete
   #:Insert
   #:IntoTable
   #:Update
   #:DropTable
   #:IfExists
   #:CreateTable
   #:IfNotExists
   #:CompositePrimaryKey

   #:to-sql

   ;;; Library Private

   #:col-clause-to-col-def-clause
   #:is-composite-pkey?
   #:CreateTable%
   #:CreateTableOption
   #:QueryOption
   #:LiteralColumn%
   #:unwrap-col-name
   #:SetTarget
   #:Update%
   ))

(in-package :coalton-db/queries)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Columns
;;;

(coalton-toplevel
  ;; TODO: Either revisit this abstraction or use it more consistently
  (define-type SqlColumn
    (LiteralColumn% String))

  (inline)
  (declare unwrap-col-name (SqlColumn -> String))
  (define (unwrap-col-name (LiteralColumn% s))
    s)

  (define-instance (Into String SqlColumn)
    (inline)
    (define into LiteralColumn%)))

(cl:defmacro Cols (cl:&rest cols)
  "SQL columns."
  `(make-list ,@(cl:mapcar (cl:lambda (col-clause)
                             `(the SqlColumn (into ,col-clause)))
                           cols)))

;;;
;;; Row Conditions
;;;

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
    (Neq% RowConditionTarget RowConditionTarget)
    (Gt% RowConditionTarget RowConditionTarget)
    (GtEq% RowConditionTarget RowConditionTarget)
    (Lt% RowConditionTarget RowConditionTarget)
    (LtEq% RowConditionTarget RowConditionTarget)
    (IsNull% RowConditionTarget)
    (IsNotNull% RowConditionTarget)
    (Not_ RowCondition)
    (And_ RowCondition RowCondition)
    (Or_ RowCondition RowCondition))

  (inline)
  (declare Eq_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (Eq_ a b)
    (Eq% (inline (into a)) (inline (into b))))

  (inline)
  (declare Neq_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (Neq_ a b)
    (Neq% (inline (into a)) (inline (into b))))

  (inline)
  (declare Gt_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (Gt_ a b)
    (Gt% (inline (into a)) (inline (into b))))

  (inline)
  (declare GtEq_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (GtEq_ a b)
    (GtEq% (inline (into a)) (inline (into b))))

  (inline)
  (declare Lt_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (Lt_ a b)
    (Lt% (inline (into a)) (inline (into b))))

  (inline)
  (declare LtEq_ ((Into :a RowConditionTarget) (Into :b RowConditionTarget) => :a -> :b -> RowCondition))
  (define (LtEq_ a b)
    (LtEq% (inline (into a)) (inline (into b))))

  (inline)
  (declare IsNull_ (Into :a RowConditionTarget => :a -> RowCondition))
  (define (IsNull_ a)
    (IsNull% (inline (into a))))

  (inline)
  (declare IsNotNull_ (Into :a RowConditionTarget => :a -> RowCondition))
  (define (IsNotNull_ a)
    (IsNotNull% (inline (into a))))

  (define-type QueryOption
    "Options to modify a query."
    (Where RowCondition)))

;;;
;;; Outer Query API
;;;

(coalton-toplevel
  ;;;
  ;;; SELECT Syntax
  ;;;

  (define-type-alias FromStatement String)

  (define-type SelectTarget
    "Things that can be selected against."
    (Values% (List SqlValue))
    AllCols
    (Cols% (List SqlColumn)))

  (define-instance (Into (List SqlValue) SelectTarget)
    (inline)
    (define into Values%))

  (define-instance (Into (List SqlColumn) SelectTarget)
    (inline)
    (define into Cols%))

  (declare From (String -> FromStatement))
  (define From id)

  ;;;
  ;;; INSERT Syntax
  ;;;

  (define-type IntoStatement
    (IntoTable SqlTable))

  (declare into-stmt->tbl-name (IntoStatement -> String))
  (define (into-stmt->tbl-name (IntoTable tbl-name))
    tbl-name)

  ;;;
  ;;; UPDATE Syntax
  ;;;

  (define-type SetTarget
    (SetTarget SqlColumn SqlValue))

  ;;;
  ;;; DROP Syntax
  ;;;

  (define-type DropOption
    IfExists)

  ;;;
  ;;; CREATE TABLE Syntax
  ;;;


  (derive Eq)
  (define-type CreateTableOption
    IfNotExists)

  (declare is-composite-pkey? (TableProperty -> Boolean))
  (define (is-composite-pkey? tbl-prop)
    (match tbl-prop
      ((CompositePrimaryKey% _) True)))

  ;;;
  ;;; Query Type
  ;;;

  (define-type Query
    "Representation of a SQL query."
    (Select% SelectTarget (Optional FromStatement) (Optional QueryOption))
    (Delete% FromStatement (Optional QueryOption))
    (Insert% IntoStatement (List SqlValue) (Optional (List SqlColumn)))
    (Update% SqlTable (List SetTarget) (Optional QueryOption))
    (DropTable% SqlTable (Optional DropOption))
    (CreateTable% String (List CreateTableOption) (List ColumnDefinition) (List TableProperty))))

;;;
;;; Syntax Sugar Wrappers
;;;

(cl:defmacro Select (vals cl:&optional from cl:&rest query-opts)
  "Select the given selectable objects in a SQL query."
  (cl:let ((from-clause (cl:if from
                          `(Some (into ,from))
                          `None))
           (opts-clause (cl:if query-opts
                          `(Some ,(cl:first query-opts))
                          `None)))
    `(Select% (the SelectTarget (into ,vals)) ,from-clause ,opts-clause)))

(cl:defmacro Delete (from cl:&optional query-opts)
  "Delete the given table in a SQL query."
  (cl:let ((opts-clause (cl:if query-opts
                           `(Some ,query-opts)
                           `None)))
    `(Delete% ,from ,opts-clause)))

(cl:defmacro Insert (into-stmt values cl:&optional cols)
  "Insert values into the given table in a SQL query."
  (cl:let ((cols-clause (cl:if cols
                               `(Some ,cols)
                               `None)))
    `(Insert% ,into-stmt (to-row ,values) ,cols-clause)))

(cl:defmacro Update (tbl set-tuples cl:&rest query-opts)
  "Update values in the given table in a SQL query."
  (cl:let ((opts-clause (cl:if query-opts
                           `(Some ,(cl:first query-opts))
                           `None)))
  `(Update%
    ,tbl
    (make-list
     ,@(cl:mapcar (cl:lambda (set-tuple)
                    `(SetTarget
                      (into ,(cl:first set-tuple))
                      (into ,(cl:second set-tuple))))
                  set-tuples))
    ,opts-clause)))

(cl:defmacro DropTable (tbl cl:&optional drop-opt)
  "Drop a table in a SQL query."
  (cl:let ((drop-clause (cl:if drop-opt
                               `(Some ,drop-opt)
                               `None)))
  `(DropTable% ,tbl ,drop-clause)))

(cl:defun col-clause-to-col-def-clause (name type properties)
  (cl:let* ((concrete-properties (cl:remove-if (cl:lambda (sym)
                                                 (cl:equalp sym 'Nullable))
                                               properties))
            (col-is-nullable? (cl:find 'Nullable properties))
            (nullable-clause (cl:if col-is-nullable?
                                    'True
                                    'False)))
    `(ColumnDefinition ,name ,type (make-list ,@concrete-properties) ,nullable-clause)))

(cl:defmacro CompositePrimaryKey (first-col cl:&rest rem-cols)
  "Create a table with a multi-column primary key in a SQL query."
  (cl:let ((cols (cl:cons first-col rem-cols)))
    `(CompositePrimaryKey% (make-list ,@cols))))

(cl:defmacro CreateTable (tbl-name opts-clauses col-clauses cl:&optional tbl-prop-clauses)
  "Create a table in a SQL query."
  (cl:let ((col-def-clauses (cl:mapcar (cl:lambda (col-clause)
                                         (col-clause-to-col-def-clause
                                          (cl:first col-clause)
                                          (cl:second col-clause)
                                          (cl:cddr col-clause)))
                                       col-clauses))
           (tbl-prop-clause `(make-list ,@tbl-prop-clauses)))
    `(CreateTable% ,tbl-name (make-list ,@opts-clauses) (make-list ,@col-def-clauses) ,tbl-prop-clause)))

;;;
;;; Compile Query -> SQL
;;;

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

  (declare col-to-sql (SqlColumn -> String))
  (define (col-to-sql col)
    (match col
      ((LiteralColumn% col-name)
       col-name)))

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
    (let const-op = (fn (val) (Tuple val (make-list))))
    (let bin-op =
      (fn (op a b)
        (let (Tuple sql-a params-a) =
          (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str a))
        (let (Tuple sql-b params-b) =
          (row-cnd-tgt-to-sql! db-adptr-proxy last-param-str b))
        (Tuple (build-str sql-a " " op " " sql-b) (<> params-a params-b))))
    (let col-suffix =
      (fn (a suffix err-msg)
        (match a
          ((Col_ col)
           (Tuple (build-str col " " suffix) (make-list)))
          ((Value_ _)
           (error err-msg)))))
    (let recur-bin-op =
      (fn (op a b)
        (let (Tuple sql-a params-a) =
          (row-condition-to-sql! db-adptr-proxy last-param-str a))
        (let (Tuple sql-b params-b) =
          (row-condition-to-sql! db-adptr-proxy last-param-str b))
        (Tuple (build-str "(" sql-a ") " op " (" sql-b ")") (<> params-a params-b))))
    (match row-cnd
      ((True_)        (const-op "TRUE"))
      ((False_)       (const-op "FALSE"))
      ((Eq% a b)      (bin-op "=" a b))
      ((Neq% a b)     (bin-op "<>" a b))
      ((Gt% a b)      (bin-op ">" a b))
      ((GtEq% a b)    (bin-op ">=" a b))
      ((Lt% a b)      (bin-op "<" a b))
      ((LtEq% a b)    (bin-op "<=" a b))
      ((IsNull% a)    (col-suffix a "IS NULL" "Cannot check null against a value."))
      ((IsNotNull% a) (col-suffix a "IS NOT NULL" "Cannot check null against a value."))
      ((And_ a b)     (recur-bin-op "AND" a b))
      ((Or_ a b)      (recur-bin-op "OR" a b))
      ((Not_ cnd)
       (let (Tuple cnd-sql cnd-params) =
         (row-condition-to-sql! db-adptr-proxy last-param-str cnd))
       (Tuple (build-str "NOT " cnd-sql) cnd-params))))

  (declare col-def-to-sql (ColumnDefinition -> String))
  (define (col-def-to-sql col-def)
    (let type-sql = (match (.col-type col-def)
                      ((IntType) "INTEGER")
                      ((TextType) "TEXT")
                      ((BoolType) "BOOLEAN")))
    (let prop-to-sql = (fn (prop)
                         (match prop
                           ((PrimaryKey) "PRIMARY KEY")
                           ((Unique) "UNIQUE"))))
    (let props-sql =
      (join-str " " (map prop-to-sql (.properties col-def))))
    (let props-pad = (if (== props-sql "")
                         ""
                         " "))
    (let nullable-sql = (if (.nullable? col-def)
                            ""
                            " NOT NULL"))
    (build-str " " (.col-name col-def) " " type-sql props-pad props-sql nullable-sql))

  (declare create-table-opt-to-sql (CreateTableOption -> String))
  (define (create-table-opt-to-sql opt)
    (match opt
      ((IfNotExists) "IF NOT EXISTS")))

  (declare table-prop-to-sql (TableProperty -> String))
  (define (table-prop-to-sql prop)
    (match prop
      ((CompositePrimaryKey% tables)
       (build-str "PRIMARY KEY (" (join-str ", " tables) ")"))))

  (declare insert-into-values-sql (DatabaseAdapter :a => List SqlValue -> Optional (List SqlColumn)
                                                   -> ty:Proxy :a -> c:Cell (Optional String) -> String))
  (define (insert-into-values-sql vals cols? db-prx last-param-str)
    (let convert-chunk =
      (fn (vals-chunk)
         (build-str
          "("
          (join-str ", " (map (fn (_) (get-next-placeholder! db-prx last-param-str)) vals-chunk))
          ")")))
    (let placeholders =
      (match cols?
        ((None)
         (Some (convert-chunk vals)))
        ((Some cols)
         (let chunked-vals = (chunk-list (length cols) vals))
         (match chunked-vals
           ((Nil) None)
           ((Cons fst-chunk chunks)
            (Some
             (fold (fn (str vals-chunk)
                     (build-str
                      str
                      ", "
                      (convert-chunk vals-chunk)))
                   (convert-chunk fst-chunk)
                   chunks)))))))
    (op:from-some (build-str "Didn't supply enough values to insert into " (force-string cols?))
                  (map (<> "VALUES ") placeholders)))

  (declare to-sql (DatabaseAdapter :a => ty:Proxy :a -> Query -> SqlQuery))
  (define (to-sql db-adptr-proxy qry)
    "Convert a Query object to a SQL string that can be run in a database."
    (let last-param-str = (the (c:Cell (Optional String)) (c:new None)))
    (let query-opts-to-sql =
      (fn (query-opts)
        (match query-opts
          ((Some (Where cnd))
           (let (Tuple cnd-sql cnd-params) =
             (row-condition-to-sql! db-adptr-proxy last-param-str cnd))
           (Tuple
            (build-str " WHERE " cnd-sql)
            cnd-params))
          ((None)
           (Tuple "" (make-list))))))
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
            (Tuple (build-str "SELECT "
                              (join-str ", " (map col-to-sql cols)))
                   (make-list)))))
       (let from-sql =
         (match from-qry
           ((Some from-table)
            (build-str " FROM " from-table))
           ((None)
            "")))
       (let (Tuple opts-sql opts-params) = (query-opts-to-sql query-opts))
       (SqlQuery
        (build-str select-sql from-sql opts-sql ";")
        (<> select-params opts-params)))
      ((Delete% from-qry query-opts)
       (let (Tuple opts-sql opts-params) = (query-opts-to-sql query-opts))
       (SqlQuery
        (build-str "DELETE FROM " from-qry opts-sql ";")
        opts-params))
      ((Insert% into-stmt insert-vals cols?)
       (let vals-sql = (insert-into-values-sql insert-vals cols? db-adptr-proxy last-param-str))
       (let cols-sql =
         (match cols?
           ((None) "")
           ((Some cols)
            (build-str " ("
                       (join-str ", " (map col-to-sql cols))
                       ") "))))
       (let insert-sql = (build-str "INSERT INTO "
                                    (into-stmt->tbl-name into-stmt)
                                    cols-sql
                                    " " vals-sql ";"))
       (SqlQuery insert-sql insert-vals))
      ((Update% tbl set-targets query-opts)
       (let set-sqls =
         (map (fn ((SetTarget col _))
                (build-str (col-to-sql col) " = "
                           (get-next-placeholder! db-adptr-proxy last-param-str)))
              set-targets))
       (let set-sql = (join-str ", " set-sqls))
       (let set-vals = (map (fn ((SetTarget _ val)) val) set-targets))
       (let (Tuple opts-sql opts-params) = (query-opts-to-sql query-opts))
       (SqlQuery
        (build-str "UPDATE " tbl " SET " set-sql opts-sql ";")
        (<> set-vals opts-params)))
      ((DropTable% tbl drop-opt)
       (let opt-sql = (match drop-opt
                        ((None) "")
                        ((Some (IfExists)) " IF EXISTS ")))
       (SqlQuery
        (build-str "DROP TABLE " opt-sql tbl ";")
        (make-list)))
      ((CreateTable% tbl-name create-table-opts col-defs tbl-props)
       (let col-defs-sql = (join-str ", " (map col-def-to-sql col-defs)))
       (let tbl-props-sql =
         (if (== Nil tbl-props)
             ""
             (build-str ", " (join-str ", " (map table-prop-to-sql tbl-props)))))
       (let create-table-opts-sql =
         (join-str ""
                   (map (fn (opt)
                          (<> (create-table-opt-to-sql opt) " "))
                        create-table-opts)))
       (SqlQuery
        (build-str "CREATE TABLE " create-table-opts-sql tbl-name " ( "
                   col-defs-sql
                   tbl-props-sql
                   ");")
        (make-list))))))
