(cl:in-package :cl-user)
(defpackage :coalton-db/db-ex
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/db
   #:coalton-db/sqlite)
  (:local-nicknames
   (:ax #:alexandria)
   (:sl #:sqlite)
   (:ev #:coalton-library/monad/environment)
   (:c  #:coalton-library/cell)
   (:rs #:coalton-library/result)
   (:m  #:coalton-library/ord-map)
   (:l  #:coalton-library/list)
   (:f  #:coalton-library/monad/free)
   (:ft #:coalton-library/monad/freet)
   (:s  #:coalton-library/string)
   (:tp #:coalton-library/tuple)
   (:it #:coalton-library/iterator)
   (:ty #:coalton-library/types)
   (:op #:coalton-library/optional)
   (:rt #:coalton-library/monad/resultt)
   (:io #:simple-io/io)
   (:tm #:simple-io/term)
   ))

(in-package :coalton-db/db-ex)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Test code
;;;

(coalton-toplevel
  (define user-table
    (table
     "User"
     ((column "Name" TextType (NotNullable Unique))
      (column "Age" IntType ((DefaultVal (ConstantValue 100))
                             NotNullable)))
     ((CompositePrimaryKey "Name" (make-list "Age")))
     :relationships ((HasOne "Post"))))

  (define-struct User
    (name String)
    (age (Optional Integer))
    (post (Rel Post)))

  (define-instance (Into User String)
    (define (into user)
      (build-str
       "User: " (.name user))))

  (define-instance (Persistable User)
    (define schema (const user-table))
    (define (to-row user)
      (build-row user
                 ("Name" .name)
                 ("Age" .age)))
    (define (from-row col-vals)
      (parse-row User col-vals
        (parse-text "Name")
        (parse-null "Age" parse-int)
        (parse-empty-rel))))

  (define-struct Post
    (user-name String)
    (title String)
    (user (Rel User)))

  (define post-table
    (table
     "Post"
     ((column "UserName" TextType ())
      (column "Title" TextType ()))
     ((CompositeUnique "UserName" (make-list "Title")))
     :relationships ((BelongsTo ("UserName") ("User" "Name")))))

  (define-instance (Persistable Post)
    (define schema (const post-table))
    (define (to-row post)
      (build-row post
        ("UserName" .user-name)
        ("Title" .title)))
    (define (from-row col-vals)
      (parse-row Post col-vals
        (parse-text "UserName")
        (parse-text "Title")
        (parse-empty-rel)))))

(coalton-toplevel
  (define (runop op)
    (let cnxn = (connect-sqlite! "test.db"))
    (let result = (run-sqlite! op cnxn))
    (disconnect-sqlite! cnxn)
    result)

  (declare functional-operation (DbProgram SqlLiteConnection (List User)))
  (define functional-operation
    (do-cancel
     enable-query-debugging
     (ensure-schema (make-list user-table post-table) True)
     (delete-all User)
     (insert-row (User "Steve" None (empty-rel)))
     (insert-row (User "Bob" (Some 12) (empty-rel)))
     (delete-where User (Eq_ "Name" (SqlText "Bob")))
     (update-where User
                   (m:collect (make-list (Tuple "Name"
                                                (SqlText "Steven"))))
                   (Eq_ "Name" (SqlText "Steve")))
     (update-all User
                 (m:collect (make-list (Tuple "Age"
                                              (SqlInt 4)))))
     (with-transaction
         (do-cancel
           (insert-row (User "Susan" (Some 25) (empty-rel)))
           (insert-row (User "Jim" None (empty-rel)))))
     (results <- (select-all User))
     (pure (pure results))))

  (declare imperitive-ex (Unit -> QueryResult (List User)))
  (define (imperitive-ex)
    (let cnxn = (connect-sqlite! "test.db"))
    (let run-query = (run-with-sqlite-connection! cnxn))
    (run-query (ensure-schema (make-list user-table) True))
    (run-query (delete-all User))
    (run-query (insert-row (User "Steve" None (empty-rel))))
    (run-query (insert-row (User "Bob" (Some 12) (empty-rel))))
    (run-query (delete-where User (Eq_ "Name" (SqlText "Bob"))))
    (run-query (update-where User
                             (m:collect (make-list (Tuple "Name"
                                                          (SqlText "Steven"))))
                             (Eq_ "Name" (SqlText "Steve"))))
    (run-query (update-all User
                           (m:collect (make-list (Tuple "Age"
                                                        (SqlInt 4))))))
    (let results = (run-sqlite! (select-all User)
                                cnxn))
    (match results
      ((Ok users)
       (for user in users
         (trace (to-string user))))
      ((Err e)
       (trace (<> "Failed: " (to-string e)))))
    (disconnect-sqlite! cnxn)
    results))

;; (coalton-toplevel
;;   (define res (runop functional-operation)))
;; (coalton res)

;; (coalton (runop (execute-sql "toheun2ch3pn23hp3h,.nte")))
;; (coalton (runop (select-all User)))

(coalton (runop
          (with-transaction
              (do-cancel
               (ensure-schema (make-list post-table user-table) True)
               (insert-row (User "Bob" None (empty-rel)))
               (insert-row (Post "Bob" "This is my first post!" (empty-rel)))
               (users <- (select-all User))
               (let user = (op:from-some "Couldn't find user" (l:head users)))
               (! Post .post user)
               (posts <- (select-all Post))
               (let post = (op:from-some "No post" (l:head posts)))
               enable-query-debugging
               (! User .user post)
               (! User .user post)
               (pure (Ok post))
               ))))

;; (coalton (imperitive-ex))

