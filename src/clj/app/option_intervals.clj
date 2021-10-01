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
   [org.joda.time Instant Period Days Duration]
   [org.joda.time.format DateTimeFormat DateTimeFormatter]

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
  [^String line]
  (.split line ","))

(defn convert-tokens-to-buffers
  [tokens]
  (let [db-key (DbKey/fromCsvLineTokens tokens)
        db-value (DbValue/fromCsvLineTokens tokens)]
    [(.toBuffer db-key) (.toBuffer db-value)]))

(def quote-datetime-formatter (DateTimeFormat/forPattern "yyyy-MM-dd HH:mm:ss"))
(def ->quote-datetime-instant
  (let [C (cw/lru-cache-factory {} :threshold (* 1024 1024))
        f (fn [s] (Instant/parse s quote-datetime-formatter))]
    (fn [s]
      (cw/lookup-or-miss C s f))))

(def exp-date-formatter (DateTimeFormat/forPattern "yyyy-MM-dd"))
(def ->expiration-date-instant
  (let [C (cw/lru-cache-factory {} :threshold (* 1024 1024))
        f (fn [s]
            (-> (Instant/parse s exp-date-formatter)
                ;; 4PM EST is when options expire
                (.plus (new Duration (* (+ 12 4) 60 60 1000)))))]
    (fn [s]
      (cw/lookup-or-miss C s f))))

(defn is-dte-in-range? [max-dte ^"[Ljava.lang.String;" tokens]
  (let [^Instant now (-> (field tokens "quote_datetime")
                         (->quote-datetime-instant))
        ^Instant dte (-> (field tokens "expiration")
                         (->expiration-date-instant))
        days (-> (Days/daysBetween now dte)
                 .getDays)]
    ; (debug (.toMutableDateTime now))
    ; (debug (.toMutableDateTime dte))
    (< days max-dte)))
(comment
  (is-dte-in-range? (+ 31 31 1) (into-array ["" "2021-01-01 09:31:00" "" "2021-01-31"]))
  (is-dte-in-range? (+ 31 31 1) (into-array ["" "2021-01-01 09:31:00" "" "2021-06-30"]))
  (is-dte-in-range? (+ 31 31 1) (into-array ["" "2020-01-02 09:31:00" "" "2020-01-02"]))
  (is-dte-in-range? (+ 31 31 1) (into-array ["" "2020-01-02 09:31:00" "" "2020-01-17"])))

(defn process-zip
  "Assumes that this runs in separate thread."
  [{:keys [interval-bundle-ch]} zip-object-key]
  (info "processing zip-object-key" zip-object-key)
  (let [{:keys [lines ^ZipInputStream zip-stream]}
        (zip-object-key->line-seq zip-object-key)

        xf (comp (drop 1)
                 (map process-line)
                 (filter (partial is-dte-in-range? (+ 1 31)))
                 (map convert-tokens-to-buffers)
                 (partition-all 10000))
        items (eduction xf lines)]
    (loop [x (first items)
           items (rest items)]
      (when x
        (>!! interval-bundle-ch x)
        (recur (first items) (rest items))))
    (.close zip-stream)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-interval-data [{:keys [env db]} intervals]
  (lmdb/put-buffers env db intervals))

(defn >=-than-year? [year path]
  (let [a (str/last-index-of path "/")
        path (subs path (inc a))
        b (str/index-of path "-")
        path (subs path 0 b)
        c (str/last-index-of path "_")
        token (subs path (inc c))]
    (>= (Integer/parseInt token) year)))
(comment
  (->> (get-object-keys "spx/")
       (filter (partial >=-than-year? 2016))))

(defn import-option-intervals [{:as args :keys [*num-items]}]
  (let [object-keys (->> (get-object-keys "spx/")
                         (filter (partial >=-than-year? 2016))
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
        env (lmdb/create-write-env "./dbs/spx")
        db (lmdb/open-db env)]
    (->hash *num-items env db)))

(defn run []
  (let [{:as args :keys [env]} (create-args)]
    (start-measurement-loop args)
    (with-open [^Env env env]
      (import-option-intervals args))
    :done))
