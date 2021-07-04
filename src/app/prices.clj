(ns app.prices
  (:import
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet])
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io :refer []]
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
   [app.config :refer [zip-dir]]
   [app.utils :refer [get-zips zip->buffered-reader intern-date-time take-batch]]
   [app.db :refer [create-db-connection]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def *num-option-intervals (atom 0))
(def *done (atom false))

(defn process-line! [args i line m]
  (let [arr (.split line ",")

        underlying_symbol (field arr "underlying_symbol")
        quote_datetime (-> (field arr "quote_datetime")
                           (str/replace " " "T"))
        bid (field arr "underlying_bid")
        ask (field arr "underlying_ask")
        active_price (field arr "active_underlying_price")
        k [underlying_symbol quote_datetime]
        v [bid ask active_price]]
    (if (contains? m k)
      (when (not= v (get m k))
        (error "mismatch bid/ask/active_price" k v)
        (assert false)))
    (assoc! m k v)))

(defn process-zip! [args zip-file]
  (let [{:keys [^BufferedReader reader fname]} (zip->buffered-reader zip-file)
        f (partial process-line! args)]

    (loop [i 0
           line (.readLine reader)
           m (transient {})]
      (if-not line
        (info fname i)
        (when (= 0 (mod i 1000000)) (info fname i)))

      (if line
        (let [m (f i line m)]
          (recur (inc i) (.readLine reader) m))
        (let [ret (persistent! m)]
          (info "zip prices map size" (count ret))
          ret)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn merge-maps [m a]
  (assert (map? m))
  (assert (map? a))
  (dorun
   (for [[k v] a]
     (when (contains? m k)
       (assert (= v (get m k))))))
  (merge m a))

(defn sort-series [m]
  (let [coll (for [[[sym dt] [bid ask price]] m]
               [sym (intern-date-time dt) bid ask price])
        sf (fn [[_ t]] [_ t])]
    (sort-by sf coll)))

(defn print-series [coll]
  (dorun
   (for [x coll]
     (println x))))

(defn insert-into-db [{:keys [db insert-prices-pstmt]} coll]
  (let [{:keys [conn]} db
        pstmt ^PreparedStatement insert-prices-pstmt
        add-batch!
        (fn [batch]
          (dorun
           (for [[sym dt bid ask active_price] batch]
             (doto pstmt
               (.setString 1 sym)
               (.setString 2 (.toString dt))
               (.setString 3 bid)
               (.setString 4 ask)
               (.setString 5 active_price)
               (.addBatch))))
          (.executeBatch pstmt))]
    (->> (partition 1000 coll)
         (map add-batch!)
         (dorun))
    (.close conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def insert-sql
  (str "insert into `prices` ("
       "symbol, quote_datetime, bid, ask, active_price"
       ") "
       "values(?, ?, ?, ?, ?)"))
(defn create-args []
  (let [{:as db :keys [conn]} (create-db-connection)
        insert-prices-pstmt (.prepareStatement conn insert-sql)]
    (->hash db insert-prices-pstmt)))

(defn run []
  (let [args (create-args)]
    (println "args constructed")

    (->> (get-zips)
         ; (take 2)
         (cp/pmap 8 (partial process-zip! args))
         (reduce merge-maps {})
         (sort-series)
         (insert-into-db args))))
