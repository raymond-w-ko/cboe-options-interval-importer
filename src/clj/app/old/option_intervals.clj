(ns app.old.option-intervals
  (:import
   [java.lang String]
   [java.util UUID]
   [java.util.zip ZipInputStream]
   [java.nio.charset Charset]
   [java.io LineNumberReader InputStreamReader BufferedReader FileWriter]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet])
  (:require
   [clojure.pprint :refer [pprint]]
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
   [app.utils :refer [get-zips zip->buffered-reader intern-date take-batch
                      array-type]]
   [app.db :refer [create-db-connection create-insert-option-pstmt
                   create-select-option-pstmt select-option insert-option
                   create-insert-option-interval-pstmt insert-option-interval-batch
                   create-load-option-intervals-data-pstmt load-option-intervals-file
                   start-bulk-loading! stop-bulk-loading!]]
   [app.s3 :refer [get-object-keys
                   zip-object-key->line-seq]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def select-options-sql
  (str "select option_id, underlying_symbol, root, expiration, strike, option_type "
       "from `options`"))

(defn create-option-id-map [{:keys [^Connection conn]}]
  (let [^Statement stmt (.createStatement conn)
        _ (.setFetchSize stmt 10000)
        rs (.executeQuery stmt select-options-sql)]
    (loop [coll (transient {})]
      (if (.next rs)
        (let [option_id (.getInt rs 1)
              underlying_symbol (.getString rs 2)
              root (.getString rs 3)
              expiration (.getString rs 4)
              strike (.getFloat rs 5)
              option_type (.getString rs 6)

              k [underlying_symbol root expiration strike option_type]
              v option_id]
          ; (when (contains? coll k)
          ;   (error (pr-str k))
          ;   (assert false "key already exists"))
          (recur (assoc! coll k v)))
        (persistent! coll)))))

(defn create-args []
  (let [*num-items (atom 0)
        db (create-db-connection)
        _ (start-bulk-loading! db)
        ->option-id (create-option-id-map db)
        insert-option-interval-pstmt (create-insert-option-interval-pstmt db)
        load-option-intervals-data-pstmt (create-load-option-intervals-data-pstmt db)]
    (debug "->option-id count" (count ->option-id))
    (->hash db *num-items ->option-id
            insert-option-interval-pstmt load-option-intervals-data-pstmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def *num-option-intervals (atom 0))

(defn process-line
  [{:keys [->option-id]} ^java.lang.String line]
  (let [arr (.split line ",")

        underlying_symbol (field arr "underlying_symbol")

        root (field arr "root")
        expiration (field arr "expiration")
        option_type (field arr "option_type")
        strike (field arr "strike")

        k [underlying_symbol root expiration strike option_type]
        option-id (get ->option-id k)

        ; quote_datetime (field arr "quote_datetime")
        ; open (field arr "open")
        ; high (field arr "high")
        ; low (field arr "low")
        ; close (field arr "close")
        ; trade_volume (field arr "trade_volume")
        ; bid_size (field arr "bid_size")
        ; bid (field arr "bid")
        ; ask_size (field arr "ask_size")
        ; ask (field arr "ask")

        ; underlying_bid (field arr "underlying_bid")
        ; underlying_ask (field arr "underlying_ask")
        ; active_underlying_price (field arr "active_underlying_price")

        ; implied_underlying_price (field arr "implied_underlying_price")
        ; implied_volatility (field arr "implied_volatility")
        ; delta (field arr "delta")
        ; theta (field arr "theta")
        ; vega (field arr "vega")
        ; rho (field arr "rho")

        ; open_interest (field arr "open_interest")
        ]
    (when-not option-id
      (error "no option_id for" (pr-str k))
      (assert false))
    [option-id arr]))

(defn process-zip
  "Assumes that this runs in separate thread."
  [{:as args :keys [interval-bundle-ch]} zip-object-key]
  (info "processing zip-object-key" zip-object-key)
  (let [{:keys [lines ^ZipInputStream zip-stream]}
        (zip-object-key->line-seq zip-object-key)

        items (->> lines
                   (drop 1)
                   (map (partial process-line args))
                   (partition-all 1000000))]
    (loop [x (first items)
           items (rest items)]
      (when x
        (>!! interval-bundle-ch x)
        (recur (first items) (rest items))))
    (.close zip-stream)))

(defmacro write-column [field-name]
  `(do (let [~'x (field ~'arr ~field-name)]
         (.write ~'o (str ~'x)))
       (.write ~'o "\t")))

(defn load-interval-data [{:as args :keys []} intervals]
  (let [uuid (str (UUID/randomUUID))
        path (str "/tmp/" uuid ".tsv")]
    (with-open [o (FileWriter. path (Charset/forName "ISO-8859-1") false)]
      (->> intervals
           (map (fn [[option-id #^"[Ljava.lang.String;" arr]]
                  (.write o (String/valueOf option-id))
                  (.write o "\t")
                  (write-column "quote_datetime")

                  (write-column "open")
                  (write-column "high")
                  (write-column "low")
                  (write-column "close")

                  (write-column "trade_volume")
                  (write-column "bid_size")
                  (write-column "bid")
                  (write-column "ask_size")
                  (write-column "ask")

                  (write-column "implied_underlying_price")
                  (write-column "implied_volatility")

                  (write-column "delta")
                  (write-column "gamma")
                  (write-column "theta")
                  (write-column "vega")
                  (write-column "rho")

                  (write-column "open_interest")

                  (.write o "\n")))
           (dorun)))
    (load-option-intervals-file args path)
    (io/delete-file path)))

(defn import-option-intervals [{:as args :keys [*num-items]}]
  (let [object-keys (->> (get-object-keys "spx/")
                         (sort))
        interval-bundle-ch (chan 2)
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

(defn run []
  (let [args (create-args)]
    (start-measurement-loop args)
    (import-option-intervals args)
    :done))
