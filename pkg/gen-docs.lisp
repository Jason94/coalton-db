(defpackage #:coalton-db/docs
  (:use #:cl)
  (:export #:write-docs))

(in-package #:coalton-db/docs)

(defun write-docs (&key
                     (pathname (merge-pathnames #p"docs/index.html"
                                               (asdf:system-source-directory "coalton-db")))
                     (packages (mapcar
                                (lambda (p)
                                  (coalton/doc/model::make-coalton-package
                                   (find-package p)
                                   :reexported-symbols t))
                                (list
                                 'coalton-db/core
                                 'coalton-db/queries
                                 'coalton-db/schema
                                 'coalton-db/to-row
                                 'coalton-db/from-row
                                 'coalton-db/persistable
                                 'coalton-db/api-imp
                                 'coalton-db/db-m
                                 'coalton-db/api-fp
                                 'coalton-db/sqlite
                                 )))
                     (remote-path "https://github.com/Jason94/coalton-db/tree/master"))
  (coalton/doc:write-documentation
   pathname
   packages
   :local-path (namestring (asdf:system-source-directory "coalton-db"))
   :remote-path remote-path
   :backend :html))
