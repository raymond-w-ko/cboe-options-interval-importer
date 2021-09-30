(ns app.option-intervals
  (:import
   [java.lang String]
   [java.util UUID]
   [java.util.zip ZipInputStream]
   [java.nio.charset Charset]
   [java.io LineNumberReader InputStreamReader BufferedReader FileWriter]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet]
   
   [org.lmdbjava Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp Dbi DbiFlags PutFlags]
   
   [app.types DbKey DbValue])
  (:require
   [fipp.edn :refer [pprint] :rename {pprint fipp}]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.core.cache :as c]
   [clojure.core.cache.wrapped :as cw]

   [clojure.core.async
    :as async
    :refer [go go-loop >! >!! <! <!! chan put! thread timeout close! to-chan
            pipeline pipeline-blocking]]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]

   [com.climate.claypoole :as cp]
   [tick.alpha.api :as t]

   [app.macros :as mac :refer [->hash field cond-let]]
   [app.utils :refer [get-zips zip->buffered-reader intern-date take-batch array-type]]
   [app.s3 :refer [get-object-keys
                   zip-object-key->line-seq]]
   [app.lmdb :as lmdb]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-measurement-loop [{:keys [*num-items]}]
  (let [start-instant (System/currentTimeMillis)]
    (go-loop []
      (let [ms (- (System/currentTimeMillis) start-instant)
            sec (/ ms 1000.0)
            rate (/ @*num-items sec)]
        (debug "processing rate" (int rate) "items/sec"))
      (<! (timeout (* 10 1000)))
      (recur))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-line
  [args ^String line]
  (let [tokens (.split line ",")
        db-key (DbKey/fromCsvLineTokens tokens)
        db-value (DbValue/fromCsvLineTokens tokens)]
    [(.toBuffer db-key) (.toBuffer db-value)]))

(defn process-zip
  "Assumes that this runs in separate thread."
  [{:as args :keys [interval-bundle-ch]} zip-object-key]
  (info "processing zip-object-key" zip-object-key)
  (let [{:keys [lines ^ZipInputStream zip-stream]}
        (zip-object-key->line-seq zip-object-key)

        xf (comp (drop 1)
                 (map (partial process-line args))
                 (partition-all 10000))
        items (eduction xf lines)]
    (loop [x (first items)
           items (rest items)]
      (when x
        (>!! interval-bundle-ch x)
        (recur (first items) (rest items))))
    (.close zip-stream)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-interval-data [args intervals]
  (lmdb/put-buffers intervals))

(defn import-option-intervals [{:as args :keys [*num-items]}]
  (let [object-keys (->> (get-object-keys "spx/")
                         (filter #(str/includes? % "2020-"))
                         (sort)
                         (take 1))
        interval-bundle-ch (chan 8)
        args (mac/args interval-bundle-ch)]
    (thread
      (->> object-keys
           (map (partial process-zip args))
           (dorun))
      (close! interval-bundle-ch))

    (loop [intervals (<!! interval-bundle-ch)]
      (when intervals
        (load-interval-data args intervals)
        (swap! *num-items + (count intervals))
        (recur (<!! interval-bundle-ch))))))

(defn create-args []
  (let [*num-items (atom 0)
        env (lmdb/create-write-env)
        db (lmdb/open-db env)]
    (->hash *num-items env db)))

(defn run []
  (let [{:as args :keys [env]} (create-args)]
    (start-measurement-loop args)
    (with-open [^Dbi env env]
      (import-option-intervals args))
    :done))
