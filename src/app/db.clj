(ns app.db
  (:import
   [java.sql DriverManager Statement PreparedStatement])
  (:require
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
  (doto conn
    (.execute "SET SESSION sql_log_bin=0;")
    (.execute "SET SESSION rocksdb_bulk_load_allow_sk=1;")
    (.execute "SET SESSION rocksdb_bulk_load=1;")))

(defn stop-bulk-loading! [{:keys [conn]}]
  (doto conn
    (.execute "SET SESSION sql_log_bin=0;")
    (.execute "SET SESSION rocksdb_bulk_load_allow_sk=0;")
    (.execute "SET SESSION rocksdb_bulk_load=0;")))

(defn create-select-option-pstmt [{:keys [conn]}]
  (let [sql (str "select option_id from `options` where "
                 "underlying_symbol=? "
                 "and root=? "
                 "and expiration=? "
                 "and option_type=? "
                 "and strike=? ")]
    (.prepareStatement conn sql)))

(defn create-insert-option-pstmt [{:keys [conn]}]
  (let [sql (str "insert into `options` "
                 "(underlying_symbol, root, expiration, option_type, strike) "
                 "values(?,?,?,?,?)")]
    (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)))

(defn create-insert-option-interval-pstmt-with-option-id-lookup [{:keys [conn]}]
  (let [option-id-sql (str "("
                           "select option_id from `options` where "
                           "underlying_symbol=? "
                           "and root=? "
                           "and expiration=? "
                           "and option_type=? "
                           "and strike=? "
                           "limit 1"
                           ")")
        sql (str "insert into `option_intervals` ("
                 "option_id,"
                 "quote_datetime,"
                 "open,high,low,close,"
                 "trade_volume,"
                 "bid_size,bid,"
                 "ask_size,ask,"
                 "underlying_bid,underlying_ask,"
                 "implied_underlying_price, active_underlying_price,"
                 "implied_volatility, delta, gamma, theta, vega, rho,"
                 "open_interest) "
                 "values("
                 option-id-sql ","
                 "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
    (.prepareStatement conn sql)))

(defn create-insert-option-interval-pstmt [{:keys [conn]}]
  (let [sql (str "insert into `option_intervals` ("
                 "option_id,"
                 "quote_datetime,"
                 "open,high,low,close,"
                 "trade_volume,"
                 "bid_size,bid,"
                 "ask_size,ask,"
                 "underlying_bid,underlying_ask,"
                 "implied_underlying_price, active_underlying_price,"
                 "implied_volatility, delta, gamma, theta, vega, rho,"
                 "open_interest) "
                 "values("
                 "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
    (.prepareStatement conn sql)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn insert-option-batch [{:keys [insert-option-stmt]} coll]
  (let [pstmt ^PreparedStatement insert-option-stmt]
    (dorun
     (for [[underlying_symbol root expiration strike option_type] coll]
       (do (.setString pstmt 1 underlying_symbol)
           (.setString pstmt 2 root)
           (.setString pstmt 3 expiration)
           (.setString pstmt 4 strike)
           (.setString pstmt 5 option_type)
           (.addBatch pstmt))))
    (.executeBatch pstmt)))

(defn insert-option-interval-batch-with-option-id-lookup
  [{:keys [insert-option-interval-pstmt]} coll]
  (let [pstmt ^PreparedStatement insert-option-interval-pstmt]
    (dorun
     (for [arr coll]
       (doto pstmt
         (.setString 1 (field arr "underlying_symbol"))
         (.setString 2 (field arr "root"))
         (.setString 3 (field arr "expiration"))
         (.setString 4 (field arr "option_type"))
         (.setString 5 (field arr "strike"))

         (.setString 6 (field arr "quote_datetime"))
         (.setString 7 (field arr "open"))
         (.setString 8 (field arr "high"))
         (.setString 9 (field arr "low"))
         (.setString 10 (field arr "close"))
         (.setString 11 (field arr "trade_volume"))
         (.setString 12 (field arr "bid_size"))
         (.setString 13 (field arr "bid"))
         (.setString 14 (field arr "ask_size"))
         (.setString 15 (field arr "ask"))
         (.setString 16 (field arr "underlying_bid"))
         (.setString 17 (field arr "underlying_ask"))

         (.setString 18 (field arr "implied_underlying_price"))
         (.setString 19 (field arr "active_underlying_price"))
         (.setString 20 (field arr "implied_volatility"))

         (.setString 21 (field arr "delta"))
         (.setString 22 (field arr "gamma"))
         (.setString 23 (field arr "theta"))
         (.setString 24 (field arr "vega"))
         (.setString 25 (field arr "rho"))

         (.setString 26 (field arr "open_interest"))
         (.addBatch))))
    (.executeBatch pstmt)))

(defn insert-option-interval-batch
  [{:keys [insert-option-interval-pstmt]} coll]
  (let [pstmt ^PreparedStatement insert-option-interval-pstmt]
    (dorun
     (for [arr coll]
       (doto pstmt
         (.setInt 1 (field arr "option_id"))
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
         (.setString 12 (field arr "underlying_bid"))
         (.setString 13 (field arr "underlying_ask"))

         (.setString 14 (field arr "implied_underlying_price"))
         (.setString 15 (field arr "active_underlying_price"))
         (.setString 16 (field arr "implied_volatility"))

         (.setString 17 (field arr "delta"))
         (.setString 18 (field arr "gamma"))
         (.setString 19 (field arr "theta"))
         (.setString 20 (field arr "vega"))
         (.setString 21 (field arr "rho"))

         (.setString 24 (field arr "open_interest"))
         (.addBatch))))
    (.executeBatch pstmt)))

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
