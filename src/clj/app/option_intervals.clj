(ns app.option-intervals
  (:import
   [java.lang String]

   [java.io File LineNumberReader InputStreamReader BufferedReader]
   [org.lmdbjava Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp Dbi DbiFlags PutFlags]
   [java.time Instant LocalDateTime ZonedDateTime ZoneId]
   [java.time.format DateTimeFormatter]
   [java.time.temporal ChronoUnit]

   [app.types DbKey DbValue])
  (:require
   [fipp.edn :refer [pprint] :rename {pprint fipp}]
   [com.climate.claypoole :as cp]
   [clojure.java.io :as io]
   [clojure.core.cache.wrapped :as cw]

   [clojure.core.async
    :as async
    :refer [go go-loop >! >!! <! <!! chan put! thread timeout close! to-chan
            pipeline pipeline-blocking]]

   [app.macros :as mac :refer [->hash field cond-let cond-xlet]]
   [app.utils :refer [get-zips zip-file->buffered-reader intern-date take-batch array-type]]
   [app.lmdb :as lmdb]

   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-line
  [^String line]
  (.split line ","))

(defn convert-tokens-to-buffers
  [tokens]
  (let [db-key (DbKey/fromCsvLineTokens tokens)
        db-value (DbValue/fromCsvLineTokens tokens)]
    [(.toBuffer db-key) (.toBuffer db-value)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def new-york-zone-id (ZoneId/of "America/New_York"))

(defn str->instant [formatter s]
  (-> (LocalDateTime/parse s formatter)
      (ZonedDateTime/of new-york-zone-id)
      (.toInstant)))

; (def exp-date-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd"))
(def quote-datetime-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))
(def ->quote-datetime-instant
  (let [C (cw/lru-cache-factory {} :threshold (* 1 1024))
        f (fn [s] (str->instant quote-datetime-formatter s))]
    (fn [s]
      (cw/lookup-or-miss C s f))))

(def ->expiration-date-instant
  (let [C (cw/lru-cache-factory {} :threshold (* 1 1024))
        f (fn [s]
            ;; 4PM EST is when options expire
            (str->instant quote-datetime-formatter (str s " 16:00:00")))]
    (fn [s]
      (cw/lookup-or-miss C s f))))

(def is-dte-in-range?
  (let [C (cw/lru-cache-factory {} :threshold (* 1 1024))
        f (fn [[max-dte now dte]]
            (let [^Instant now (->quote-datetime-instant now)
                  ^Instant dte (->expiration-date-instant dte)
                  days (.between ChronoUnit/DAYS now dte)]
              (< days max-dte)))]
    (fn [args]
      (cw/lookup-or-miss C args f))))

(defn is-line-in-dte-range? [max-dte ^"[Ljava.lang.String;" tokens]
  (let [now (field tokens "quote_datetime")
        dte (field tokens "expiration")]
    (is-dte-in-range? [max-dte now dte])))

(comment
  (->quote-datetime-instant "2022-03-13 01:00:00")
  (->quote-datetime-instant "2022-03-13 02:00:00")
  (->quote-datetime-instant "2022-03-13 03:00:00")
  
  (->quote-datetime-instant "2022-11-06 00:00:00")
  (->quote-datetime-instant "2022-11-06 01:00:00")
  (->quote-datetime-instant "2022-11-06 02:00:00")
  
  
  (->expiration-date-instant "2021-01-31")
  (is-line-in-dte-range? (+ 63) (into-array ["" "2021-01-01 09:31:00" "" "2021-01-31"]))
  (is-line-in-dte-range? (+ 63) (into-array ["" "2021-01-01 09:31:00" "" "2021-06-30"]))
  (is-line-in-dte-range? (+ 63) (into-array ["" "2020-01-02 09:31:00" "" "2020-01-02"]))
  (is-line-in-dte-range? (+ 63) (into-array ["" "2020-01-02 09:31:00" "" "2020-01-17"])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-zip-file
  "Assumes that this runs in separate thread."
  [{:keys [interval-bundle-ch]} zip-file]
  (info "processing zip-file" zip-file)
  (let [{:keys [^BufferedReader reader
                fname]}
        (zip-file->buffered-reader zip-file)
        lines (line-seq reader)
        roots (transient #{})

        xf (comp (drop 1)
                 (map process-line)
                 (map #(conj! roots (field % "root")))
                 ; (filter (partial is-line-in-dte-range? (+ 1 7)))
                 ; (map convert-tokens-to-buffers)
                 (partition-all 100000))]
    (transduce xf
               (fn
                 ([] nil)
                 ([_] nil)
                 ([_ bundle] (>!! interval-bundle-ch bundle)))
               lines)
    (spit (format "out/%s.edn" fname) (pr-str (persistent! roots)))
    (.close reader)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-interval-data [{:keys [env db]} intervals]
  ; (lmdb/put-buffers env db intervals)
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn import-option-intervals!
  [{:as args :keys [src-dir zip-files-xf *num-items]}]
  (let [zip-files (->> (get-zips src-dir)
                       (into [] zip-files-xf))
        interval-bundle-ch (chan 4)
        start-instant (System/currentTimeMillis)
        args (mac/args interval-bundle-ch)]
    (dorun (map (fn [^File file] (.exists file)) zip-files))
    
    (thread
      (try
        (->> zip-files
             (cp/pmap 4 (partial process-zip-file args))
             (dorun))
        (finally
          (close! interval-bundle-ch))))

    (loop [intervals (<!! interval-bundle-ch)]
      (let [ms (- (System/currentTimeMillis) start-instant)
            sec (/ ms 1000.0)
            n @*num-items
            rate (/ n sec)]
        (debugf "num items: %d rate: %d items/sec" n (int rate)))

      (when intervals
        ; (load-interval-data args intervals)
        (swap! *num-items + (count intervals))
        (recur (<!! interval-bundle-ch))))

    nil))

(defn create-base-args []
  (let [*num-items (atom 0)]
    (->hash *num-items)))

(defn import! [{:as args :keys [lmdb-env-dir]}]
  (assert (.exists (io/file lmdb-env-dir)))
  (with-open [env (lmdb/create-write-env lmdb-env-dir)]
    (cond-xlet
     :let [args (mac/args env)]
     :return (import-option-intervals! args)))
  (debug "import! finished"))
