(ns app.db
  (:import
   [java.sql DriverManager Statement PreparedStatement])
  (:require
   [app.config :refer [db-conn-string]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-db-connection []
  (let [conn (DriverManager/getConnection db-conn-string)]
    {:conn conn}))

(defn sql-query [{:keys [conn]} query]
  (let [stmt (.createStatement conn)]
    (.executeQuery conn query)))

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

