(defpackage coalton-db/tests/persistable
  (:use #:coalton #:coalton-prelude #:coalton-testing
        #:coalton-db/core
        #:coalton-db/schema
        #:coalton-db/to-row
        #:coalton-db/from-row
        #:coalton-db/persistable
        #:coalton-db/util
        #:coalton-db/queries
        #:coalton-db/tests/test-utils
        )
  )
(in-package :coalton-db/tests/persistable)

(named-readtables:in-readtable coalton:coalton)

(fiasco:define-test-package #:coalton-db/tests/persistable-fiasco)
(coalton-fiasco-init #:coalton-db/tests/persistable-fiasco)

;; NOTE: We're currently testing select, delete, & insert directly
;; in the api-fp and api-imp integration tests. They actually didn't
;; call anything originally from Persistable, so that seemed reasonable.
;;
;; BUT a little query layer emerged in the Persistable module, so some
;; of the advanced functionality for those is getting tested here now.
;; It might be worth re-negotiating the scope of the persistable & api
;; tests at some point.

;; (coalton-toplevel
;;   (derive Eq)
;;   (define-struct SimpleUser
;;     (name String)
;;     (verified? Boolean))

;;   (define simple-user-table
;;     (make-schema
;;      "users"
;;      ((column "name" TextType PrimaryKey)
;;       (column "verified" BoolType (Default_ True)))))

;;   (define-row-parser SimpleUser
;;     sql-value-parser
;;     sql-value-parser)

;;   (define-instance (ToRow SimpleUser)
;;     (define (to-row user)
;;       (build-row user .name .verified?)))

;;   (define-instance (Persistable SimpleUser)
;;     (define schema-for (const simple-user-table))
;;     (define (prop-for-col user col-name)
;;       (match col-name
;;         ("name" (Some (into (.name user))))
;;         ("verified" (Some (into (.verified? user))))
;;         (_ None))))

;;   (derive Eq)
;;   (define-struct NewUser
;;     (name String)
;;     (verified? (Defaultable Boolean)))

;;   (define-instance (ToRow NewUser)
;;     (define (to-row user)
;;       (build-row user .name .verified?)))

;;   (define-instance (Persistable NewUser)
;;     (define schema-for (const simple-user-table))
;;     (define (prop-for-col user col-name)
;;       (match col-name
;;         ("name" (Some (into (.name user))))
;;         ("verified" (Some (into (.verified? user))))
;;         (_ None)))))
