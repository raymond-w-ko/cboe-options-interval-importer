(ns app.options
  (:import
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet])
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.core.cache :as c]
   [clojure.core.cache.wrapped :as cw]
   [clojure.core.async :refer [go go-loop >! >!! <! <!! chan put! thread timeout]]
   [clojure.core.async.impl.protocols :refer [WritePort]]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]

   [com.climate.claypoole :as cp]
   [tick.alpha.api :as t]

   [app.macros :refer [->hash field cond-let]]
   [app.utils :refer [get-zips zip->buffered-reader intern-date take-batch]]
   [app.db :refer [create-db-connection create-insert-option-pstmt
                   create-select-option-pstmt select-option insert-option
                   create-insert-option-interval-pstmt
                   insert-option-interval-batch
                   sql-query
                   start-bulk-loading! stop-bulk-loading!]]
   ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce options-ch (chan (* 1024 1024)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn lookup-or-create-option-id*
  [select-option-pstmt insert-option-pstmt k]
  (cond-let
   :let [option-id (select-option select-option-pstmt k)]
   option-id (do (comment println "EXISTS" option-id)
                 option-id)

   :let [option-id (insert-option insert-option-pstmt k)]
   :return option-id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-option-id*
  [options-cache value-fn k]
  (cw/lookup-or-miss options-cache k value-fn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-line-for-import-options [args *seen
                                       i ^java.lang.String line]
  (let [arr (.split line ",")
        underlying_symbol (field arr "underlying_symbol")
        root (field arr "root")
        expiration (field arr "expiration")
        strike (field arr "strike")
        option_type (field arr "option_type")

        k [underlying_symbol root expiration strike option_type]]
    (go
      (when-not (contains? @*seen k)
        (>! options-ch k)
        (vswap! *seen conj k)))))

(defn process-zip-for-import-options [args zip-file]
  (let [{:keys [^BufferedReader reader fname]} (zip->buffered-reader zip-file)
        *seen (volatile! #{})]

    (loop [i 0
           line (.readLine reader)]

      (when line (process-line-for-import-options args *seen i line))

      ;; last entry reached
      (when-not line (println fname i))
      (when line
        (when (= 0 (mod i 1000000))
          (println fname i))
        (recur (inc i) (.readLine reader))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-args []
  (let [db (create-db-connection)
        options-cache (cw/lru-cache-factory {} :threshold (* 1024 1024))

        select-option-pstmt (create-select-option-pstmt db)
        insert-option-pstmt (create-insert-option-pstmt db)

        lookup-or-create-option-id (partial lookup-or-create-option-id*
                                            select-option-pstmt
                                            insert-option-pstmt)
        get-option-id (fn [k]
                        (get-option-id* options-cache lookup-or-create-option-id k))]
    (->hash db options-cache select-option-pstmt insert-option-pstmt
            lookup-or-create-option-id get-option-id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn run []
  (let [{:as args :keys [get-option-id]} (create-args)]
    (->> (get-zips)
         (cp/pmap 2 (partial process-zip-for-import-options args)))
    (go-loop []
      (let [k (<! options-ch)]
        (get-option-id k)
        (recur)))))

