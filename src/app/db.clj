(ns app.db
  (:import
   [java.sql DriverManager Statement PreparedStatement])
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [app.macros :refer [->hash field cond-let]]
   [app.config :refer [db-conn-string]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-db-connection []
  (let [conn (DriverManager/getConnection db-conn-string)]
    {:conn conn}))

(defn sql-query [{:keys [conn]} query]
  (let [stmt (.createStatement conn)]
    (.executeQuery conn query)))

(defn start-bulk-loading! [{:keys [conn]}]
  (let [stmt (.createStatement conn)]
    (doto stmt
      (.execute "SET SESSION sql_log_bin=0;")
      (.execute "SET SESSION rocksdb_bulk_load_allow_sk=1;")
      (.execute "SET SESSION rocksdb_bulk_load=1;"))))

(defn stop-bulk-loading! [{:keys [conn]}]
  (let [stmt (.createStatement conn)]
    (doto stmt
      (.execute "SET SESSION sql_log_bin=0;")
      (.execute "SET SESSION rocksdb_bulk_load_allow_sk=0;")
      (.execute "SET SESSION rocksdb_bulk_load=0;"))))

(defn create-select-option-pstmt [{:keys [conn]}]
  (let [sql (str "select option_id from `options` where "
                 "underlying_symbol=? "
                 "and root=? "
                 "and expiration=? "
                 "and option_type=? "
                 "and strike=? ")]
    (.prepareStatement conn sql)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-insert-option-pstmt [{:keys [conn]}]
  (let [sql (str "insert into `options` "
                 "(underlying_symbol, root, expiration, strike, option_type) "
                 "values(?,?,?,?,?)")]
    (.prepareStatement conn sql)))

(defn insert-option-batch [{:keys [insert-option-pstmt]} coll]
  (let [pstmt ^PreparedStatement insert-option-pstmt]
    (dorun
     (for [[underlying_symbol root expiration strike option_type] coll]
       
       (do (.setString pstmt 1 underlying_symbol)
           (.setString pstmt 2 root)
           (.setString pstmt 3 expiration)
           (.setString pstmt 4 strike)
           (.setString pstmt 5 option_type)
           (.addBatch pstmt))))
    (.executeBatch pstmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-insert-price-pstmt [{:keys [conn]}]
  (let [sql (str "insert into `prices` "
                 "(symbol, quote_datetime, bid, ask, active_price) "
                 "values(?,?,?,?,?)")]
    (.prepareStatement conn sql)))

(defn insert-price-batch [{:keys [insert-price-pstmt]} coll]
  (let [pstmt ^PreparedStatement insert-price-pstmt]
    (dorun
     (for [[sym quote_datetime bid ask active_price] coll]
       (do (.setString pstmt 1 sym)
           (.setString pstmt 2 quote_datetime)
           (.setString pstmt 3 bid)
           (.setString pstmt 4 ask)
           (.setString pstmt 5 active_price)
           (.addBatch pstmt))))
    (.executeBatch pstmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-insert-option-interval-pstmt [{:keys [conn]}]
  (let [sql (str "insert into `option_intervals` ("
                 "option_id,"
                 "quote_datetime,"
                 "open,high,low,close,"
                 "trade_volume,"
                 "bid_size,bid,"
                 "ask_size,ask,"
                 "implied_underlying_price,"
                 "implied_volatility, delta, gamma, theta, vega, rho,"
                 "open_interest) "
                 "values("
                 "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
    (.prepareStatement conn sql)))

(defn insert-option-interval-batch
  [{:keys [insert-option-interval-pstmt]} coll]
  (let [pstmt ^PreparedStatement insert-option-interval-pstmt]
    (loop [x (first coll)
           coll (rest coll)]
      (when x
        (let [[option-id arr] x]
          (doto pstmt
            (.setInt 1 option-id)
            (.setString 2 (field arr "quote_datetime"))

            (.setString 3 (field arr "open"))
            (.setString 4 (field arr "high"))
            (.setString 5 (field arr "low"))
            (.setString 6 (field arr "close"))

            (.setString 7 (field arr "trade_volume"))
            (.setString 8 (field arr "bid_size"))
            (.setString 9 (field arr "bid"))
            (.setString 10 (field arr "ask_size"))
            (.setString 11 (field arr "ask"))

            (.setString 12 (field arr "implied_underlying_price"))
            (.setString 13 (field arr "implied_volatility"))

            (.setString 14 (field arr "delta"))
            (.setString 15 (field arr "gamma"))
            (.setString 16 (field arr "theta"))
            (.setString 17 (field arr "vega"))
            (.setString 18 (field arr "rho"))

            (.setString 19 (field arr "open_interest"))
            (.addBatch)))
        (recur (first coll) (rest coll))))
    (.executeLargeBatch pstmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-load-option-intervals-data-pstmt [{:keys [conn]}]
  (let [sql (str "load data local infile ? "
                 "into table `option_intervals` ("
                 "option_id,"
                 "quote_datetime,"
                 "open,high,low,close,"
                 "trade_volume,"
                 "bid_size,bid,"
                 "ask_size,ask,"
                 "implied_underlying_price,"
                 "implied_volatility, delta, gamma, theta, vega, rho,"
                 "open_interest) ")]
    (.prepareStatement conn sql)))

(defn load-option-intervals-file
  [{:keys [load-option-intervals-data-pstmt]} path]
  (let [pstmt ^PreparedStatement load-option-intervals-data-pstmt]
    (.setString pstmt 1 path)
    (.execute pstmt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn select-option
  [select-option-pstmt
   [underlying_symbol root expiration strike option_type]]
  (let [pstmt ^PreparedStatement select-option-pstmt]
    (.setString pstmt 1 underlying_symbol)
    (.setString pstmt 2 root)
    (.setString pstmt 3 expiration)
    (.setString pstmt 4 option_type)
    (.setString pstmt 5 strike)
    
    (let [rs (.executeQuery pstmt)]
      (if (.next rs)
        (.getInt rs "option_id")
        nil))))

(defn insert-option
  [insert-option-stmt
   [underlying_symbol root expiration strike option_type]]
  
  ; (println [underlying_symbol root expiration strike option_type])
  (let [pstmt ^PreparedStatement insert-option-stmt]
    (.setString pstmt 1 underlying_symbol)
    (.setString pstmt 2 root)
    (.setString pstmt 3 expiration)
    (.setString pstmt 4 option_type)
    (.setString pstmt 5 strike)
    (.executeUpdate pstmt)
    (let [rs (.getGeneratedKeys pstmt)]
      (.next rs)
      (.getLong rs 1))))
