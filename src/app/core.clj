(ns app.core
  (:import
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader])
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io :refer []]
   [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def field-names
	["underlying_symbol"
	 "quote_datetime"
	 "root"
	 "expiration"
	 "strike"
	 "option_type"
	 "open"
	 "high"
	 "low"
	 "close"
	 "trade_volume"
	 "bid_size"
	 "bid"
	 "ask_size"
	 "ask"
	 "underlying_bid"
	 "underlying_ask"
	 "implied_underlying_price"
	 "active_underlying_price"
	 "implied_volatility"
	 "delta"
	 "gamma"
	 "theta"
	 "vega"
	 "rho"
	 "open_interest"])
(def field-name-indexes (into {} (map-indexed (fn [i x] [x i]) field-names)))

(defmacro field [arr k]
  (let [i (get field-name-indexes k)
        transformer (case k
                      "delta" `(Double/parseDouble)
                      nil)]
    `(-> (aget ~arr ~i)
         ~@transformer)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce *roots (atom #{}))

(defn process-zip [m zip-file]
  (let [is (io/input-stream zip-file)
        zip-stream (ZipInputStream. is)
        ;; assume one and only one entry
        entry (.getNextEntry zip-stream)
        fname (.getName entry)
        reader (-> (InputStreamReader. zip-stream)
                   (BufferedReader. (* 1024 1024 8)))]
    (println fname)

    ;; skip header
    (.readLine reader)

    (loop [i 0
           line (.readLine reader)]
      (when-not line
        (println i))
      (when line
        (let [arr (.split line ",")]
          (swap! *roots conj (field arr "root")))
        (when (= 0 (mod i 1000000))
          (println i))
        (recur (inc i) (.readLine reader))))

    (update m :processed-csvs conj fname)))

(def zip-dir "/mnt/f/CBOE/SPX")
(defn get-zips []
  (->> (io/file zip-dir)
       (file-seq)
       (filter #(str/ends-with? (str %) ".zip"))
       (sort-by str)))

(defn main
  [_]
  (->> (get-zips)
       ; (take 1)
       (reduce process-zip {:processed-csvs []}))
  (spit "roots.edn" (pr-str @*roots)))
