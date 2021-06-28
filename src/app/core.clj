(ns app.core
  (:import
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]
   [java.sql DriverManager Statement PreparedStatement ResultSet])
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io :refer []]
   [com.climate.claypoole :as cp]
   [clojure.string :as str]
   [clojure.core.cache :as c]
   [clojure.core.cache.wrapped :as cw]
   [clojure.core.async :refer [go go-loop >! >!! <! <!! chan put!]]
   
   [app.macros :refer [->hash field cond-let]]
   [app.config :refer [zip-dir]]
   [app.db :refer [create-db-connection create-insert-option-pstmt create-select-option-pstmt
		   select-option insert-option
		   sql-query]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)
(defonce *roots (atom #{}))
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

(defn get-option-id*
  [options-cache value-fn k]
  (cw/lookup-or-miss options-cache k value-fn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-line [args *seen
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
       (>!! options-ch k)
       (vswap! *seen conj k)))))

(defn process-zip [args zip-file]
  (let [is (io/input-stream zip-file)
        zip-stream (ZipInputStream. is)
        ;; assume one and only one entry
        entry (.getNextEntry zip-stream)
        fname (.getName entry)
        reader (-> (InputStreamReader. zip-stream)
                   (BufferedReader. (* 1024 1024 8)))
	
	*seen (volatile! #{})]
    (.readLine reader) ;; skip header

    (loop [i 0
           line (.readLine reader)]

      (when line (process-line args *seen i line))
      
      ;; last entry reached
      (when-not line (println fname i))
      (when line
        (when (= 0 (mod i 1000000))
          (println fname i))
        (recur (inc i) (.readLine reader))))))

(defn get-zips []
  (->> (io/file zip-dir)
       (file-seq)
       (filter #(str/ends-with? (str %) ".zip"))
       (sort-by str)))

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

(defn main
  [_]
  (println "BEGIN")

  (let [{:as args :keys [get-option-id]} (create-args)]
    (->> (get-zips)
	 ; (take 2)
	 (cp/pmap 2 (partial process-zip args)))
    
    (go-loop []
      (let [k (<!! options-ch)]
	(get-option-id k)
	(recur)))
    
    (println "END")))
