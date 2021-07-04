(ns app.utils
  (:import
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet])
  (:require
   [clojure.core.async
    :as async
    :refer [go go-loop >! <! chan put! alts! close! timeout]]
   [clojure.string :as str]
   [clojure.core.cache :as c]
   [clojure.core.cache.wrapped :as cw]
   [clojure.java.io :as io]

   [tick.alpha.api :as t]
   
   [app.config :refer [zip-dir]]
   [app.macros :refer [cond-let]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-zips []
  (->> (io/file zip-dir)
       (file-seq)
       (filter #(str/ends-with? (str %) ".zip"))
       (sort-by str)))

(defn zip->buffered-reader [zip-file]
  (let [is (io/input-stream zip-file)
        zip-stream (ZipInputStream. is)
	;; assume one and only one entry
        entry (.getNextEntry zip-stream)
        fname (.getName entry)
        reader (-> (InputStreamReader. zip-stream)
                   (BufferedReader. (* 1024 1024 32)))]
    (.readLine reader) ;; skip first row since it is a header
    {:reader reader
     :fname fname}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def intern-date
  (let [C (cw/basic-cache-factory {})
	->date (fn [date-string] (t/date date-string))]
    (fn [date-string]
      (cw/lookup-or-miss C date-string ->date))))

(def intern-date-time
  (let [C (cw/basic-cache-factory {})
	->dt (fn [dt-string] (t/date-time dt-string))]
    (fn [dt-string]
      (cw/lookup-or-miss C dt-string ->dt))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn take-batch [n t ch]
  (let [out (chan)
	coll (transient [])]
    (go-loop [i 0]
      (cond-let
       (= i n) (do (put! out (persistent! coll))
		   (close! out))
       
       
       :let [[v _] (alts! [ch (timeout t)])]
       (nil? v) (do (put! out (persistent! coll))
		    (close! out))

       :return (do (conj! coll v)
		   (recur (inc i)))))
    out))
