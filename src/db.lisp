(cl:in-package :cl-user)
(defpackage :coalton-db/db
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util)
  (:local-nicknames
   (:sl #:sqlite)
   (:ev #:coalton-library/monad/environment)
   (:c  #:coalton-library/cell)
   (:rs #:coalton-library/result)
   (:m  #:coalton-library/ord-map)
   (:l  #:coalton-library/list)
   (:f  #:coalton-library/monad/free)
   (:ft #:coalton-library/monad/freet)
   (:rt #:coalton-library/monad/resultt)
   (:s  #:coalton-library/string)
   (:tp #:coalton-library/tuple)
   (:it #:coalton-library/iterator)
   (:ty #:coalton-library/types)
   (:op #:coalton-library/optional)
   (:io #:simple-io/io)
   (:tm #:simple-io/term)
   )
  (:import-from
   #:coalton-library/classes
   #:lift)
  (:export
   #:ColumnName
   #:SqlType
   #:IntType
   #:TextType
   #:BoolType

   #:SqlValue
   #:SqlInt
   #:SqlText
   #:SqlBool
   #:SqlNull

   #:DefaultOption
   #:ConstantValue
   #:CurrentTime
   #:CurrentDate
   #:CurrentTimestamp

   #:ColumnFlag
   #:PrimaryKey
   #:NotNullable
   #:Unique
   #:DefaultVal

   #:TableFlag
   #:CompositePrimaryKey
   #:CompositeUnique
   #:ForeignKey

   #:ColumnDef
   #:.name
   #:.type
   #:.flags

   #:TableDef
   #:.columns
   #:column
   #:basic-column
   #:table

   #:PersistParsingError
   #:RowParser
   #:run-row-parser
   #:parse-row_
   #:parse-row
   #:parse-int
   #:parse-text
   #:parse-bool
   #:parse-null

   #:build-row

   #:QueryError
   #:QueryResult
   #:ResultParseError

   #:Persistable
   #:schema
   #:to-row
   #:from-row

   #:MonadDatabase
   #:query-none
   #:query-rows

   #:make-column-map

   #:DbOp

   #:RowCondition
   #:Eq_
   #:Neq
   #:Gt_
   #:GtEq_
   #:Lt_
   #:LtEq_
   #:And_
   #:Or_
   #:Not_
   #:IsNull
   #:IsNotNull

   #:execute-sql
   #:execute-query
   #:execute-sqls
   #:execute-queries
   #:ensure-schema
   #:delete-all
   #:delete-where
   #:insert-row
   #:select-where
   #:select-all
   #:update-where
   #:update-all
   #:with-transaction
   #:do-cancel
   #:enable-query-debugging
   #:disable-query-debugging

   #:run-dbop
   #:DbProgram

   #:SqlLiteConnection
   #:connect-sqlite!
   #:disconnect-sqlite!
   #:run-sqlite!
   #:run-with-sqlite-connection!
   ))

(in-package :coalton-db/db)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Table Schema DSL
;;;

(coalton-toplevel
  (repr :lisp)
  (define-type SqlValue
    "A runtime value inside of a SQL row."
    (SqlInt Integer)
    (SqlText String)
    (SqlBool Boolean)
    SqlNull)

  (derive-eq SqlValue ((SqlInt i) (SqlText t) (SqlBool b) SqlNull))

  (define-type-alias TableName String)
  (define-type-alias ColumnName String)

  (define-type SqlType
    "The type of a SQL column or value."
    IntType
    TextType
    BoolType)

  (derive-eq SqlType (IntType TextType BoolType))

  (define-type DefaultOption
    "Possible values for a column default."
    (ConstantValue_ SqlValue)
    CurrentTime
    CurrentDate
    CurrentTimestamp)

  (derive-eq DefaultOption ((ConstantValue_ val) CurrentTime CurrentDate CurrentTimestamp))

  (declare ConstantValue (Into :a SqlValue => :a -> DefaultOption))
  (define (ConstantValue val)
    "A constant default value for a column."
    (ConstantValue_ (into val)))

  (define-type ColumnFlag
    "Flags on a SQL column."
    PrimaryKey
    NotNullable
    Unique
    (DefaultVal DefaultOption))

  (derive-eq ColumnFlag (PrimaryKey NotNullable Unique (DefaultVal v)))

  (define-struct ColumnDef
    (name ColumnName)
    (type SqlType)
    (flags (List ColumnFlag)))

  (declare is-default? (ColumnFlag -> Boolean))
  (define (is-default? flag)
    (match flag
      ((DefaultVal _) True)
      (_ False)))

  (declare has-default? (ColumnDef -> Boolean))
  (define (has-default? col)
    (op:some? (l:find is-default? (.flags col))))

  (define-type TableFlag
    "Flags on a SQL table."
    ;; Create a primary key on the first and remaining columns
    (CompositePrimaryKey ColumnName (List ColumnName))
    (CompositeUnique ColumnName (List ColumnName))
    ;; Stores corresponding keys as a tuple list of here -> there
    (ForeignKey% TableName (List (Tuple ColumnName ColumnName)))
    )

  (define-struct TableDef
    (name String)
    (columns (List ColumnDef))
    (flags (List TableFlag)))

  (declare column-names (TableDef -> List ColumnName))
  (define (column-names table)
    (map .name (.columns table)))

  (declare lookup-col! (TableDef -> ColumnName -> ColumnDef))
  (define (lookup-col! table name)
    (for col in (.columns table)
      (when (== (.name col) name)
        (return col)))
    (error (<> "Could not find column named " name)))

  (declare basic-column (String -> SqlType -> ColumnDef))
  (define (basic-column name type)
    "Create a SQL column definition with default flags:
* primary key?    = False
* not nullable?   = True
* unique?         = False"
    (ColumnDef name type (make-list)))

  (define-type-alias RowMap (m:Map ColumnName SqlValue))

  (define-instance (Into SqlValue String)
    (define (into val)
      (match val
        ((SqlInt i)
         (<> "SqlInt " (into i)))
        ((SqlText s)
         (<> "SqlText " s))
        ((SqlBool b)
         (if b
             "SqlBool True"
             "SqlBool False"))
        ((SqlNull)
         "SqlNull")))))

(cl:defmacro column (column-name type column-flags)
  "Construct a column with COLUMN-NAME, TYPE, and COLUMN-FLAGS.
COLUMN-NAME: String
TYPE: SqlType
COLUMN-FLAGS: List ColumnFlag"
  `(ColumnDef ,column-name ,type (make-list ,@column-flags)))

(cl:defmacro table (table-name columns table-flags)
  "Simple macro to construct a table with TABLE-NAME, COLUMNS, and TABLE-FLAGS.
Table-Name: String
Columns: List ColumnDef
Table-Flags: List TableFlag"
  `(TableDef ,table-name (make-list ,@columns) (make-list ,@table-flags)))

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

;; TODO: This should probably return a result, maybe? Maybe it's better this way,
;;       since this is just internal?
(cl:defun wrap-raw-sql-value (type raw-val)
  "Given SqlType TYPE, wrap VAL in the appropriate SqlValue."
  (cl:cond
    ((cl:not raw-val)
     SqlNull)
    ((coalton (== (lisp SqlType () type) IntType))
     (SqlInt raw-val))
    ((coalton (== (lisp SqlType () type) TextType))
     (SqlText raw-val))
    ((coalton (== (lisp SqlType () type) BoolType))
     (SqlBool raw-val))
    (cl:t (cl:error (cl:format cl:nil "Unknown SQL type: ~a" type)))))

;;;
;;; Row Parser (SQL -> Persistable)
;;;

(coalton-toplevel
  (define-type-alias PersistParsingError String)

  (define-type (RowParser :a)
    (RowParser (RowMap -> Result PersistParsingError (Tuple :a RowMap))))

  (declare run-row-parser (RowParser :a -> RowMap -> Result PersistParsingError (Tuple :a RowMap)))
  (define (run-row-parser (RowParser f))
    f)

  (define-instance (Functor RowParser)
    (define (map f (RowParser p))
      (RowParser (fn (input)
                   (match (p input)
                     ((Ok (Tuple a rest))
                      (Ok (Tuple (f a) rest)))
                     ((Err e)
                      (Err e)))))))

  (define-instance (Applicative RowParser)
    (define (pure x)
      (RowParser (fn (input)
                   (Ok (Tuple x input)))))
    (define (liftA2 a->b->c (RowParser pa) (RowParser pb))
      (RowParser (fn (input)
                   (do
                    ((Tuple a rest1) <- (pa input))
                    ((Tuple b rest2) <- (pb rest1))
                    (pure (Tuple (a->b->c a b) rest2)))))))

  (declare parse-row_ (RowParser :a -> RowMap -> Result PersistParsingError :a))
  (define (parse-row_ (RowParser p) input)
    "Run a row parser on a RowMap of ColumnName->SqlValue, producing a parsed result.
Meant to be used inside PARSE-ROW macro, but could be called on its own."
    (do
     ((Tuple result rest) <- (p input))
     (if (map-empty? rest)
         (Ok result)
         (Err "Ran out of values to parse"))))

  (declare parse-int (ColumnName -> RowParser Integer))
  (define (parse-int col)
    (RowParser (fn (input)
                 (match (m:lookup input col)
                   ((None)
                    (Err (<> "Could not find column: " col)))
                   ((Some (SqlInt i))
                    (Ok (Tuple i (from-some "Could not remove key already found!"
                                            (m:remove input col)))))
                   ((Some val)
                    (Err (<> "Expected SqlInt, received: " (into val))))))))

  (declare parse-text (ColumnName -> RowParser String))
  (define (parse-text col)
    (RowParser (fn (input)
                 (match (m:lookup input col)
                   ((None)
                    (Err (<> "Could not find column: " col)))
                   ((Some (SqlText s))
                    (Ok (Tuple s (from-some "Could not remove key already found!"
                                            (m:remove input col)))))
                   ((Some val)
                    (Err (<> "Expected SqlText, received: " (into val))))))))

  (declare parse-bool (ColumnName -> RowParser Boolean))
  (define (parse-bool col)
    (RowParser (fn (input)
                 (match (m:lookup input col)
                   ((None)
                    (Err (<> "Could not find column: " col)))
                   ((Some (SqlBool b))
                    (Ok (Tuple b (from-some "Could not remove key already found!"
                                            (m:remove input col)))))
                   ((Some val)
                    (Err (<> "Expected SqlBool, received: " (into val))))))))

  (declare parse-null (ColumnName -> (ColumnName -> RowParser :a) -> RowParser (Optional :a)))
  (define (parse-null col make-parser)
    (RowParser (fn (input)
                 (match (m:lookup input col)
                   ((None)
                    (Err (<> "Could not find column: " col)))
                   ((Some (SqlNull))
                    (Ok (Tuple None (from-some "Could not remove key already found!"
                                               (m:remove input col)))))
                   (_
                    (do
                     ((Tuple a rest) <- (run-row-parser (make-parser col) input))
                     (pure (Tuple (Some a) rest)))))))))

(cl:defmacro parse-row (constructor col-vals cl:&body body)
  "Simple macro that calls liftAn on a constructor function and parsers.

Example:
  (parse-row User col-vals
    (parse-text \"Name\")
    (parse-null \"Age\" parse-int))
=>
  (parse-row_ (liftAn User
                (parse-text \"Name\")
                (parse-null \"Age\" parse-int))
              col-vals)
"
  `(parse-row_ (liftAn ,constructor ,@body) ,col-vals))

;;;
;;; Row Builder (Persistable -> Sql)
;;;

(coalton-toplevel
  ;; Like above, I think this is a bad idea because it's too tied to an individual DB.
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
        ((Some a) (into a))))))

(cl:defmacro build-row (obj cl:&body body)
  "Helper macro for building `to-row` implementations for Persistable instances.

Expects:
  (build-row <obj>
    (<column name 1> <conversion function 1>)
    (<column name 2> <conversion function 2>)
    ...)

and converts it to:
  (m:collect (make-list
               (Tuple <column name 1> (into (<conversion function 1> <obj>)))
               ...))

As such, the output of the conversion functions must be a type that has an
(Into :a SqlValue) instance. Currently, that is:
  - Integer
  - String
  - Boolean
  - Optional :a

Example:
      (build-row user
                 (\"Name\" .name)
                 (\"Age\" .age))
=>
      (m:collect (make-list
                  (Tuple \"Name\" (into (.name user)))
                  (Tuple \"Age\" (into (.age user)))))
"
  (cl:labels ((col-form (form)
                (cl:let ((col-name (cl:first form))
                         (fn (cl:second form)))
                  `(Tuple ,col-name (into (,fn ,obj))))))
    `(m:collect (make-list
                 ,@(cl:mapcar #'col-form body)))))

;;;
;;; Persistable: For serialization to/from the database
;;;

(coalton-toplevel
  (define-type QueryError
    (ResultParseError PersistParsingError)
    (QueryError String)
    NotInTxError)

  (define-type-alias (QueryResult :a) (Result QueryError :a))

  (declare flatten-parsing-errors (Result QueryError (Result PersistParsingError :a)
                                   -> Result QueryError :a))
  (define (flatten-parsing-errors res)
    (match res
      ((Err e) (Err e))
      ((Ok res2)
       (match res2
         ((Err parse-err)
          (Err (ResultParseError parse-err)))
         ((Ok a) (Ok a))))))

  (define-instance (Into QueryError String)
    (define (into err)
      (match err
        ((QueryError msg) msg)
        ((ResultParseError msg) (<> "Parsing error: " msg))
        ((NotInTxError)
         "Attempted to commit or rollback a transaction without being in a transaction."))))

  (define-class (ty:RuntimeRepr :a => Persistable :a)
    (schema (ty:Proxy :a -> TableDef))
    (to-row (:a -> m:Map ColumnName SqlValue))
    (from-row (m:Map ColumnName SqlValue -> Result PersistParsingError :a)))

  (define-class (HasTableName :a)
    "Allows the user to specify 'User' or '\"User\"' interchangeably in the DSL.
IMPORTANT: Make sure to wrap external API calls in a macro that wraps the wrap-has-table-name
macro!"
    (table-name (:a -> TableName)))

  (define-instance (HasTableName String)
    (define table-name id))

  (define-instance (Persistable :a => HasTableName (ty:Proxy :a))
    (define (table-name tbl-prox)
      (.name (schema tbl-prox)))))

(cl:defmacro wrap-has-table-name (x)
  "Converts:

(wrap-has-table-name \"User\") => \"User\"
(wrap-has-table-name User) => (the (ty:Proxy User) ty:Proxy)"
  (cl:cond
    ((cl:stringp x)
     x)
    ((cl:symbolp x)
     `(the (ty:Proxy ,x) ty:Proxy))
    (cl:t
     (cl:error (cl:format cl:nil "Invalid HasTableName specifier: ~a" x)))))

(cl:defmacro ForeignKey (has-table here-key-there-key-pairs)
  "ForeignKey flag on a table. Can take either a String or a Persistable name
as the table, and takes a list of (here-key there-key) pairs for the columns.
Must specify at least one pair of keys!

Examples:
  (ForeignKey User ((\"user-id\" \"id\")))
  (ForeignKey \"User\" ((\"user-id\" \"id\")))
  (ForeignKey User ((\"user-id\" \"id\")
                    (\"proj-id\" \"proj-id\")))"
  (cl:if (cl:null here-key-there-key-pairs)
    (cl:error "Must supply at least one pair of keys!")
    `(ForeignKey% (table-name (wrap-has-table-name ,has-table))
                  (make-list
                   ,@(cl:mapcar
                     #'(cl:lambda (pair)
                         `(Tuple ,(cl:first pair) ,(cl:second pair)))
                     here-key-there-key-pairs)))))

;;;
;;; MonadDatabase interface
;;;

(coalton-toplevel
  (define-struct Query
    (sql String)
    (bound-vals (List SqlValue)))

  (declare unbound-query (String -> Query))
  (define (unbound-query sql)
    (Query sql (make-list)))

  (define-class (Monad :m => MonadDatabase :m)
    "A Monad M capable of executing sql statements and returning result type T."
    (query-none (Query -> :m (Result QueryError Unit)))
    (query-rows (Query -> List ColumnDef -> :m (Result QueryError (List (List SqlValue))))))

  (declare make-column-map (List ColumnDef -> List SqlValue -> m:Map ColumnName SqlValue))
  (define (make-column-map cols vals)
    "Given an order list of COLS and VALS, construct the col-name->val map."
    (let map = (c:new m:empty))
    (let get-col-name = (the (ColumnDef -> String) .name))
    (for (Tuple col val) in (it:zip! (it:into-iter cols) (it:into-iter vals))
      (c:write! map
                (m:insert-or-replace (c:read map)
                                     (get-col-name col)
                                     val)))
    (c:read map)))

;;;
;;; DbOp Free Monad Transformer
;;;

(coalton-toplevel
  (define-type (DbOpF :next)
    (RunNonQuerySQL Query (QueryResult Unit -> :next))
    (RunNonQuerySQLs (List Query) (QueryResult Unit -> :next))
    (SelectRows Query (List ColumnDef) (QueryResult (List (m:Map ColumnName SqlValue)) -> :next))
    (BeginTx :next)
    (CommitTx :next)
    (RollbackTx :next)
    (EnableQueryDebug :next)
    (DisableQueryDebug :next)
    )

  (define-instance (Functor DbOpF)
    (define (map f dbop-f)
      (match dbop-f
        ((RunNonQuerySQL q cont) (RunNonQuerySQL q (map f cont)))
        ((RunNonQuerySQLs q cont) (RunNonQuerySQLs q (map f cont)))
        ((SelectRows q r cont) (SelectRows q r (map f cont)))
        ((BeginTx cont) (BeginTx (f cont)))
        ((CommitTx cont) (CommitTx (f cont)))
        ((RollbackTx cont) (RollbackTx (f cont)))
        ((EnableQueryDebug cont) (EnableQueryDebug (f cont)))
        ((DisableQueryDebug cont) (DisableQueryDebug (f cont)))
    )))

  (define-type-alias DbOp (ft:FreeT DbOpF)))

;;;
;;; SQL Condition DSL
;;;

(coalton-toplevel
  (define-type RowCondition
    "A condition that can be true or false for a DB row. The string
argument is the column name."
    (Eq_           ColumnName SqlValue)
    (Neq           ColumnName SqlValue)
    (Gt_           ColumnName SqlValue)
    (GtEq_         ColumnName SqlValue)
    (Lt_           ColumnName SqlValue)
    (LtEq_         ColumnName SqlValue)
    (And_          RowCondition RowCondition)
    (Or_           RowCondition RowCondition)
    (Not_          RowCondition)
    (IsNull        ColumnName)
    (IsNotNull     ColumnName))

  (declare row-condition-bound-vals (RowCondition -> List SqlValue))
  (define (row-condition-bound-vals cnd)
    (match cnd
      ((Eq_ _ val) (make-list val))
      ((Neq _ val) (make-list val))
      ((Gt_ _ val) (make-list val))
      ((GtEq_ _ val) (make-list val))
      ((Lt_ _ val) (make-list val))
      ((LtEq_ _ val) (make-list val))
      ((And_ a b) (<> (row-condition-bound-vals a)
                      (row-condition-bound-vals b)))
      ((Or_ a b) (<> (row-condition-bound-vals a)
                     (row-condition-bound-vals b)))
      ((Not_ a) (row-condition-bound-vals a))
      ((IsNull _) (make-list))
      ((IsNotNull _) (make-list)))))

;;;
;;; Rendering to SQL
;;;

(coalton-toplevel
  (declare escape-sql (String -> String))
  (define (escape-sql str)
    (let escape-char =
      (fn (c)
        (cond
          ((== c #\') "''")
          (True (into c)))))
    (it:fold! <> "" (map escape-char (s:chars str))))

  (declare render-bound-vals (Query -> String))
  (define (render-bound-vals q)
    "Create the standard representation for an list of bound values: (?, ?, ...)
Important Note: Not used in all queries!"
    (build-str
     "(" (join-str ", " (map (const "?") (.bound-vals q))) ")"))

  (declare render-sql-value (SqlValue -> String))
  (define (render-sql-value val)
    (match val
      ((SqlInt x) (into x))
      ((SqlText x) (<> "'" (<> (escape-sql x) "'")))
      ((SqlBool x) (if x "TRUE" "FALSE"))
      ((SqlNull) "NULL")))

  (declare stringify-condition ((SqlValue -> String) -> RowCondition -> String))
  (define (stringify-condition to-str cnd)
    (rec (f (RowCondition -> String))
         ((rem cnd))
      (match rem
        ((Eq_ col val)
         (<> col (<> " = " (to-str val))))
        ((Neq col val)
         (<> col (<> " <> " (to-str val))))
        ((Gt_ col val)
         (<> col (<> " > " (to-str val))))
        ((GtEq_ col val)
         (<> col (<> " >= " (to-str val))))
        ((Lt_ col val)
         (<> col (<> " < " (to-str val))))
        ((LtEq_ col val)
         (<> col (<> " <= " (to-str val))))
        ((And_ c1 c2)
         (<> "(" (<> (f c1) (<> " AND " (<> (f c2) ")")))))
        ((Or_ c1 c2)
         (<> "(" (<> (f c1) (<> " OR " (<> (f c2) ")")))))
        ((Not_ c)
         (<> "NOT (" (<> (f c) ")")))
        ((IsNull col)
         (<> col " IS NULL"))
        ((IsNotNull col)
         (<> col " IS NOT NULL"))
        )))

  (declare render-condition (RowCondition -> String))
  (define render-condition (stringify-condition render-sql-value))

  (declare render-condition-bound (RowCondition -> Tuple String (List SqlValue)))
  (define (render-condition-bound cnd)
    "Render a condition as SQL with placeholder ?'s and a list of bound values."
    (Tuple
     (stringify-condition (const "?") cnd)
     (row-condition-bound-vals cnd)))

  (declare render-sql-type (SqlType -> String))
  (define (render-sql-type sql-type)
    (match sql-type
      ((IntType) "INTEGER")
      ((TextType) "TEXT")
      ((BoolType) "BOOL")))

  (declare render-default-option (DefaultOption -> Tuple String (List SqlValue)))
  (define (render-default-option opt)
    (match opt
      ((ConstantValue_ val)
       (Tuple (render-sql-value val) (make-list)))
      ((CurrentTime)
       (Tuple "CURRENT_TIME" (make-list)))
      ((CurrentDate)
       (Tuple "CURRENT_DATE" (make-list)))
      ((CurrentTimestamp)
       (Tuple "CURRENT_TIMESTAMP" (make-list)))))

  (declare render-col-flag (ColumnFlag -> Tuple String (List SqlValue)))
  (define (render-col-flag flag)
    (match flag
      ((PrimaryKey) (Tuple "PRIMARY KEY" (make-list)))
      ((NotNullable) (Tuple "NOT NULL" (make-list)))
      ((Unique) (Tuple "UNIQUE" (make-list)))
      ((DefaultVal def-opt)
       (let (Tuple sql bound-vals) = (render-default-option def-opt))
       (Tuple (<> "DEFAULT " sql) bound-vals))))

  (declare render-col-def (ColumnDef -> Tuple String (List SqlValue)))
  (define (render-col-def col)
    (let (Tuple flag-sql flag-bound-vals) =
      (fold (fn ((Tuple acc-sql acc-bound-vals) flag)
              (let (Tuple new-sql new-bound-vals) = (render-col-flag flag))
              (Tuple (build-str acc-sql " " new-sql) (<> acc-bound-vals new-bound-vals)))
            (Tuple "" (make-list))
            (.flags col)))
    (Tuple
     (build-str
      (.name col) " " (render-sql-type (.type col)) " "
      flag-sql)
     flag-bound-vals))

  (declare render-table-flag (TableFlag -> String))
  (define (render-table-flag flag)
    (match flag
      ((CompositePrimaryKey c1 cs)
       (build-str
        "PRIMARY KEY (" (join-str ", " (Cons c1 cs)) ")"))
      ((CompositeUnique c1 cs)
       (build-str
        "UNIQUE (" (join-str ", " (Cons c1 cs)) ")"))
      ((ForeignKey% foreign-table-name key-pairs)
       (let here-keys-sql =
         (build-str "(" (join-str ", " (map tp:fst key-pairs)) ")"))
       (let there-keys-sql =
         (build-str "(" (join-str ", " (map tp:snd key-pairs)) ")"))
       (build-str
        "FOREIGN KEY " here-keys-sql newline
          "REFERENCES " foreign-table-name there-keys-sql))))

  (declare render-table-queries (Boolean -> TableDef -> List Query))
  (define (render-table-queries overwrite table)
    (let (Tuple columns-sql column-bound-vals) =
      (fold (fn ((Tuple acc-sql acc-bound-vals) col)
              (let (Tuple new-sql new-bound-vals) = (render-col-def col))
              (Tuple (<> acc-sql (make-list new-sql)) (<> acc-bound-vals new-bound-vals)))
            (Tuple (make-list) (make-list))
            (.columns table)))
    (let flags-sql = (map render-table-flag (.flags table)))
    (let create-query =
      (Query
       (build-str
        "CREATE TABLE IF NOT EXISTS " (.name table) " (" newline
        (join-str (<> "," newline) (<> columns-sql flags-sql)) newline
        ");")
       column-bound-vals))
    (if overwrite
        (make-list
         (unbound-query
          (build-str
           "DROP TABLE IF EXISTS " (.name table) ";"))
         create-query)
        (make-list create-query)))

  (declare render-schema-queries (List TableDef -> Boolean -> List Query))
  (define (render-schema-queries tables overwrite)
    (>>= tables (render-table-queries overwrite))))

;;;
;;; Render DB Sql
;;;

(coalton-toplevel
  (declare render-optional-cnd (Optional RowCondition -> String))
  (define (render-optional-cnd cnd?)
    "Returns either ' WHERE <cnd str>' or ''"
    (match cnd?
      ((Some cnd) (<> " WHERE " (render-condition cnd)))
      ((None) "")))

  (declare begin-tx-query Query)
  (define begin-tx-query (unbound-query "begin transaction"))

  (declare commit-tx-query Query)
  (define commit-tx-query (unbound-query "commit transaction"))

  (declare rollback-tx-query Query)
  (define rollback-tx-query (unbound-query "rollback transaction"))

  (declare remove-col-on-insert? (TableDef -> ColumnName -> SqlValue -> Boolean))
  (define (remove-col-on-insert? table col-name val)
    (and (== SqlNull val)
         (has-default? (lookup-col! table col-name))))

  (declare insert-row-query (TableDef -> m:Map ColumnName SqlValue -> Query))
  (define (insert-row-query table col-val)
    (let pairs = (the (List (Tuple ColumnName SqlValue))
                      (it:collect!
                       (it:filter!
                        (fn ((Tuple col-name sql-val))
                          (not (remove-col-on-insert? table col-name sql-val)))
                        (m:entries col-val)))))
    (let col-names = (map tp:fst pairs))
    (let vals = (map (compose render-sql-value tp:snd) pairs))
    (unbound-query
     (build-str
      "INSERT INTO " (.name table) " "
      "(" (join-str "," col-names) ") VALUES"
      "(" (join-str "," vals) ");")))

  (declare select-query (TableDef -> Optional RowCondition -> Query))
  (define (select-query table cnd?)
    (match cnd?
      ((None)
       (unbound-query
        (build-str
         "SELECT * FROM " (.name table) ";")))
      ((Some cnd)
       (let (Tuple cnd-sql cnd-bound-vals) = (render-condition-bound cnd))
       (Query
        (build-str
         "SELECT * FROM " (.name table) " WHERE " cnd-sql ";")
        cnd-bound-vals))))

  (declare delete-row-query (TableDef -> Optional RowCondition -> Query))
  (define (delete-row-query table cnd?)
    (match cnd?
      ((None)
       (unbound-query
        (build-str
         "DELETE FROM " (.name table) ";")))
      ((Some cnd)
       (let (Tuple cnd-sql cnd-bound-vals) = (render-condition-bound cnd))
       (Query
        (build-str "DELETE FROM " (.name table) " WHERE " cnd-sql ";")
        cnd-bound-vals))))

  (declare update-query (TableDef -> m:Map ColumnName SqlValue -> Optional RowCondition -> Query))
  (define (update-query table col-vals cnd?)
    (let col-val-pairs = (the (List (Tuple ColumnName SqlValue)) (it:collect! (m:entries col-vals))))
    (let set-exprs = (map (fn ((Tuple col _))
                            (build-str col " = ?" ))
                          col-val-pairs))
    (let set-bound-vals = (map tp:snd col-val-pairs))
    (let (Tuple cnd-sql cnd-bound-vals) = (from-opt (Tuple "" (make-list))
                                                    (map render-condition-bound cnd?)))
    (let cnd-statement = (if (== "" cnd-sql)
                             ""
                             (build-str newline "WHERE " cnd-sql)))
    (Query
     (build-str
      "UPDATE " (.name table) newline
      "SET " (join-str (<> "," newline) set-exprs)
      cnd-statement ";")
     (<> set-bound-vals cnd-bound-vals))))

;;;
;;; DB Raw SQL API
;;;

(coalton-toplevel
  (declare execute-sql (Monad :m => String -> DbOp :m (QueryResult Unit)))
  (define (execute-sql sql)
    "Execute an unbound query with no return value."
    (f:liftF (RunNonQuerySQL (unbound-query sql) id)))

  (declare execute-query (Monad :m => Query -> DbOp :m (QueryResult Unit)))
  (define (execute-query query)
    "Execute a bound query with no return value."
    (f:liftF (RunNonQuerySQL query id)))

  (declare execute-sqls (Monad :m => List String -> DbOp :m (QueryResult Unit)))
  (define (execute-sqls sqls)
    "Execute multiple unbound queries with no return value."
    (f:liftF (RunNonQuerySQLs (map unbound-query sqls) id)))

  (declare execute-queries (Monad :m => List Query -> DbOp :m (QueryResult Unit)))
  (define (execute-queries queries)
    "Execute multiple bound queries with no return value."
    (f:liftF (RunNonQuerySQLs queries id))))

;;;
;;; DB Persist API
;;;

(coalton-toplevel
  (declare ensure-schema ((Monad :m) => List TableDef -> Boolean -> DbOp :m (QueryResult Unit)))
  (define (ensure-schema tables overwrite)
    (execute-queries (render-schema-queries tables overwrite)))

  (declare delete-all_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> DbOp :m (QueryResult Unit)))
  (define (delete-all_ tbl-prox)
    (execute-query (delete-row-query (schema tbl-prox) None)))

  (declare delete-where_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> RowCondition -> DbOp :m (QueryResult Unit)))
  (define (delete-where_ tbl-prox cnd)
    (execute-query (delete-row-query (schema tbl-prox) (Some cnd))))

  (declare insert-row ((Monad :m) (Persistable :a) => :a -> DbOp :m (QueryResult Unit)))
  (define (insert-row obj)
    (execute-query (insert-row-query (schema (ty:proxy-of obj)) (to-row obj))))

  (declare select-where_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> Optional RowCondition -> DbOp :m (QueryResult (List :a))))
  (define (select-where_ tbl-prox cnd?)
    (let query = (select-query (schema tbl-prox) cnd?))
    (let parse-rows = (compose (traverse flatten-parsing-errors) (traverse (map from-row))))
    (f:liftF (SelectRows query (.columns (schema tbl-prox)) parse-rows)))

  (declare select-all_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> DbOp :m (QueryResult (List :a))))
  (define (select-all_ tbl-prox)
    (select-where_ tbl-prox None))

  (declare update-where_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> m:Map ColumnName SqlValue -> RowCondition -> DbOp :m (QueryResult Unit)))
  (define (update-where_ tbl-prox new-vals cnd)
    (execute-query (update-query (schema tbl-prox) new-vals (Some cnd))))

  (declare update-all_ ((Monad :m) (Persistable :a) => ty:Proxy :a -> m:Map ColumnName SqlValue -> DbOp :m (QueryResult Unit)))
  (define (update-all_ tbl-prox new-vals)
    (execute-query (update-query (schema tbl-prox) new-vals None)))

  (declare enable-query-debugging (Monad :m => DbOp :m (QueryResult Unit)))
  (define enable-query-debugging
    "Begin query debugging. Each run query, outputs the SQL and the result to the console."
    (f:liftF (EnableQueryDebug (Ok Unit))))

  (declare disable-query-debugging (Monad :m => DbOp :m (QueryResult Unit)))
  (define disable-query-debugging
    "Disable query debugging."
    (f:liftF (DisableQueryDebug (Ok Unit))))

  (declare with-transaction (Monad :m => DbOp :m (QueryResult :a) -> DbOp :m (QueryResult :a)))
  (define (with-transaction op)
    "Execute the given database operation inside of a transaction. If the operation returns an Err value,
rollback the transaction and bubble the error. Otherwise, commit the transaction and return the Ok value of OP.
If an intermediate query fails but the entire transaction returns an Ok value, it will commit!"
    (do
     (f:LiftF (BeginTx Unit))
     (result <- op)
     (match result
       ((Err _)
        (f:LiftF (RollbackTx Unit)))
       ((Ok _)
        (f:LiftF (CommitTx Unit))))
     (pure result))))

(cl:defmacro do-cancel (cl:&body body)
  "Perform a series of steps that return (QueryResult :a), and immediately fail if one returns an Err."
  `(rt:do-resultT ,@body))

;;; Macros to help with using proxies for the functions that require it:

(cl:defmacro select-all (table)
  `(select-all_ (the (ty:Proxy ,table) ty:Proxy)))

(cl:defmacro delete-all (table)
  `(delete-all_ (the (ty:Proxy ,table) ty:Proxy)))

(cl:defmacro delete-where (table cnd)
  `(delete-where_ (the (ty:Proxy ,table) ty:Proxy) ,cnd))

(cl:defmacro update-where (table new-vals cnd)
  `(update-where_ (the (ty:Proxy ,table) ty:Proxy) ,new-vals ,cnd))

(cl:defmacro update-all (table new-vals)
  `(update-all_ (the (ty:Proxy ,table) ty:Proxy) ,new-vals))

;;;
;;; Running DbOp's
;;;

(coalton-toplevel
  (inline)
  (declare run-db-qry-none_ (MonadDatabase :m => Boolean -> Query -> :m (QueryResult Unit)))
  (define (run-db-qry-none_ debugging? qry)
    (if debugging?
        (>>=
         (query-none qry)
         (fn (res)
           (when debugging?
             (traceobject "Query:" qry))
           (traceobject "Result:" res)
           (pure res)))
        (query-none qry)))

  (declare run-dbop_ (MonadDatabase :m => DbOp :m (QueryResult :a) -> Boolean -> :m (QueryResult :a)))
  (define (run-dbop_ op debugging?)
    (do
     (step <- (ft:run-freeT op))
     (match step
       ((ft:Val a) (pure a))
       ((ft:FreeF op)
        (match op
          ((RunNonQuerySQL qry next)
           (do
            (result <- (run-db-qry-none_ debugging? qry))
            (run-dbop_ (next result) debugging?)))
          ((RunNonQuerySQLs queries next)
           (do
            (results <- (traverse (run-db-qry-none_ debugging?) queries))
            (let result = (map (const Unit) (sequence results)))
            (run-dbop_ (next result) debugging?)))
          ((SelectRows qry cols next)
           (when debugging?
             (traceobject "Query:" qry))
           (do
            (result <- (query-rows qry cols))
            (let _ = (if debugging? (traceobject "Result:" result) Unit))
            (let col-map-result = (map (map (make-column-map cols)) result))
            (run-dbop_ (next col-map-result) debugging?)))
          ;; NOTE: Not checking for nested tx's because SQLite will fail anyway, and that might be
          ;; intended behavior for other DB's.
          ((BeginTx next)
           (do
            (query-none begin-tx-query)
            (run-dbop_ next debugging?)))
          ((CommitTx next)
           (do
            (query-none commit-tx-query)
            (run-dbop_ next debugging?)))
          ((RollbackTx next)
           (do
            (query-none rollback-tx-query)
            (run-dbop_ next debugging?)))
          ((EnableQueryDebug next)
           (run-dbop_ next True))
          ((DisableQueryDebug next)
           (run-dbop_ next False)))))))

  (declare run-dbop (MonadDatabase :m => DbOp :m (QueryResult :a) -> :m (QueryResult :a)))
  (define (run-dbop op)
    (run-dbop_ op False)))


;;;
;;; SQLite Wrapper
;;;

(coalton-toplevel
  (repr :native sl:sqlite-handle)
  (define-type SqlLiteConnection)

  (declare connect-sqlite! (String -> SqlLiteConnection))
  (define (connect-sqlite! connection-spec)
    (lisp :a (connection-spec)
      (sl:connect connection-spec)))

  (declare disconnect-sqlite! (SqlLiteConnection -> Unit))
  (define (disconnect-sqlite! connection)
    (lisp :a (connection)
      (sl:disconnect connection))
    Unit)

  (define-instance (MonadDatabase (ev:EnvT SqlLiteConnection io:IO))
    (define (query-none (Query sql bound-vals))
      (do
       (connection <- ev:ask)
       (lift (io:wrap-io
               (lisp :x (connection sql bound-vals)
                 (cl:let ((unwrapped-bound-vals (cl:mapcar #'unwrap-sql-value bound-vals)))
                   (cl:handler-case
                       (cl:progn
                         (cl:apply #'sl:execute-non-query (cl:cons connection (cl:cons sql unwrapped-bound-vals)))
                         (Ok Unit))
                     (cl:error (e)
                       (Err (QueryError (cl:format cl:nil "~a" e)))))))))))
    (define (query-rows (Query sql bound-vals) cols)
      (do
       (connection <- ev:ask)
       (let types = (map .type cols))
       (lift (io:wrap-io
               (lisp :x (connection sql bound-vals types)
                 (cl:handler-case
                     (cl:progn
                       (cl:let* ((unwrapped-bound-vals (cl:mapcar #'unwrap-sql-value bound-vals))
                                 (rows (cl:apply
                                        #'sl:execute-to-list
                                        (cl:cons connection (cl:cons sql unwrapped-bound-vals)))))
                         (Ok (cl:mapcar
                              (cl:lambda (row)
                                (cl:mapcar #'wrap-raw-sql-value types row))
                              rows))))
                   (cl:error (e)
                     (Err (QueryError (cl:format cl:nil "~a" e)))))))))))

  (declare run-sqlite!_ (ev:EnvT SqlLiteConnection io:IO :a -> SqlLiteConnection -> :a))
  (define (run-sqlite!_ op connection)
    (io:run! (ev:run-envT op connection)))

  (declare run-sqlite! (DbOp (ev:EnvT SqlLiteConnection io:IO) (QueryResult :a) -> SqlLiteConnection -> QueryResult :a))
  (define run-sqlite! (compose run-sqlite!_ run-dbop))

  (declare run-with-sqlite-connection! (SqlLiteConnection -> DbOp (ev:EnvT SqlLiteConnection io:IO) (QueryResult :a) -> QueryResult :a))
  (define (run-with-sqlite-connection! connection op)
    (run-sqlite!_ (run-dbop op) connection)))

;;;
;;; Helper Type Aliases
;;;

(coalton-toplevel
  (define-type-alias (DbProgram :cnxn :a ) (DbOp (ev:EnvT :cnxn io:IO) (QueryResult :a))))
