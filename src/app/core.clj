(ns app.core
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
   
   [app.prices :as prices]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)
(defonce *roots (atom #{}))
(defonce options-ch (chan (* 1024 1024)))
(defonce *start-instant (atom nil))
(defonce *num-processed-items (atom 0))

(defonce option-intervals-ch (chan 200000))

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

(defn create-import-options-args []
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

(defn import-options []
  (let [{:as args :keys [get-option-id]} (create-import-options-args)]
    (->> (get-zips)
         (cp/pmap 2 (partial process-zip-for-import-options args)))
    (go-loop []
      (let [k (<! options-ch)]
        (get-option-id k)
        (recur)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def select-options-sql
  (str "select option_id, underlying_symbol, root, expiration, option_type, strike "
       "from `options`"))

(defn create-option-id-map [{:keys [^Connection conn]}]
  (let [^Statement stmt (.createStatement conn)
        _ (.setFetchSize stmt 10000)
        rs (.executeQuery stmt select-options-sql)]
    (loop [i 0
           coll (transient {})]
      (if (.next rs)
        (let [option_id (.getString rs 1)
              underlying_symbol (.getString rs 2)
              root (.getString rs 3)
              expiration (.getString rs 4)
              option_type (.getString rs 5)
              strike (.getFloat rs 6)

              k [underlying_symbol root expiration option_type strike]
              v option_id]
          ; (assert (not (contains? coll k)) (pr-str k))
          (recur (inc i) (assoc! coll k v)))
        (do (println "options row count" i)
            (persistent! coll))))))

(defn create-import-option-intervals-args []
  (let [db (create-db-connection)
        ->option-id (create-option-id-map db)
        insert-option-interval-pstmt (create-insert-option-interval-pstmt db)]
    (println "->option-id count" (count ->option-id))
    (->hash db insert-option-interval-pstmt ->option-id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def *num-option-intervals (atom 0))

(defn process-line-for-import-option-intervals
  [{:keys [->option-id]} i ^java.lang.String line]
  (let [arr (.split line ",")

        underlying_symbol (field arr "underlying_symbol")

        root (field arr "root")
        expiration (field arr "expiration")
        option_type (field arr "option_type")
        strike (field arr "strike")

        k [underlying_symbol root expiration option_type strike]
        option-id (get ->option-id k)

        quote_datetime (field arr "quote_datetime")
        open (field arr "open")
        high (field arr "high")
        low (field arr "low")
        close (field arr "close")
        trade_volume (field arr "trade_volume")
        bid_size (field arr "bid_size")
        bid (field arr "bid")
        ask_size (field arr "ask_size")
        ask (field arr "ask")

        underlying_bid (field arr "underlying_bid")
        underlying_ask (field arr "underlying_ask")
        active_underlying_price (field arr "active_underlying_price")

        implied_underlying_price (field arr "implied_underlying_price")
        implied_volatility (field arr "implied_volatility")
        delta (field arr "delta")
        theta (field arr "theta")
        vega (field arr "vega")
        rho (field arr "rho")
        
        open_interest (field arr "open_interest")]
    (when-not option-id
      (error "no option_id for" k)
      (assert false))
    ; (>!! option-intervals-ch arr)
    arr))

(defn process-zip-for-import-option-intervals [args zip-file]
  (let [{:keys [^BufferedReader reader fname]} (zip->buffered-reader zip-file)
        f (partial process-line-for-import-option-intervals args)]

    (loop [i 0
           line (.readLine reader)]

      (when line (f i line))

      ; (if-not line
      ;   (info fname i)
      ;   (when (= 0 (mod i 1000000)) (info fname i)))

      (if line
        (recur (inc i) (.readLine reader))
        (swap! *num-option-intervals + i)))

    (.close reader)
    :done))

(def *done-import-option-intervals (atom false))

(defn import-option-intervals []
  (let [args (create-import-option-intervals-args)]
    (println "args constructed")

    (thread
     (->> (get-zips)
          (take 1)
          (cp/pmap 8 (partial process-zip-for-import-option-intervals args))
          (doall))

     (reset! *done-import-option-intervals true)

     :done-input)

    (start-bulk-loading!)

    (loop []
      (let [batch (<!! (take-batch 100000 2000 option-intervals-ch))]
        (cond-let
         (nil? batch) (if-not @*done-import-option-intervals
                        (recur)
                        :done)
         :else (do (when (< 0 (count batch))
                     (insert-option-interval-batch args batch))
                   (recur)))))
    
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn internal-consistency-check! []
  (assert (= (t/date "1970-01-01") (t/date "1970-01-01")))
  (assert (not= (t/date "1970-01-01") (t/date "1970-01-02"))))

(defn start-measurement-loop []
  (go-loop []
    (let [ms (- (System/currentTimeMillis) @*start-instant)
          sec (/ ms 1000.0)
          rate (/ @*num-processed-items sec)]
      (debug "processing rate" rate "items/sec"))
    (<! (timeout (* 5 1000)))
    (recur)))

(defn -main []
  (println (/ (-> (Runtime/getRuntime) .maxMemory) 1024 1024) "G")
  (println "BEGIN")
  (internal-consistency-check!)

  (reset! *start-instant (System/currentTimeMillis))
  
  ; (start-measurement-loop)
  ; (import-options)
  ; (<!! (import-option-intervals))

  (prices/run)

  ; (debug "num option-intervals" @*num-option-intervals)
  (println "END"))
