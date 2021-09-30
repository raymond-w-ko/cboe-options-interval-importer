(ns app.repl
  (:import
   [java.time Instant]
   [java.util.concurrent TimeUnit]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.channels Channels]
   [org.lmdbjava Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp DbiFlags PutFlags]
   [org.agrona MutableDirectBuffer]
   [org.agrona.concurrent UnsafeBuffer]
   
   [app.types DbKey DbValue])
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [app.lmdb :refer [spit-buffer
                     path->bytes]]))

(defn sort-roots []
  (->> (slurp "roots.edn")
       (read-string)
       (sort)
       (vec)
       (pr-str)
       (spit "roots.edn")))
(comment (sort-roots))

(defn max-root-size []
  (->> (slurp "roots.edn")
       (read-string)
       (map count)
       (apply max)))
(comment (max-root-size))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn price-conversion []
  (DbValue/priceStringToInt "100.251")
  (DbValue/priceStringToInt "100.25")
  (DbValue/priceStringToInt "-100.25"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn write-db-key-buf []
  (let [line-tokens (into-array ["^SPX"
                                 "2021-01-02 09:31:00"
                                 "SPXW"
                                 "2021-01-31"
                                 "5000.000"
                                 "P"])
        db-key (DbKey/fromCsvLineTokens line-tokens)]
    (spit-buffer "key.hex" (.toBuffer db-key))))
(comment (write-db-key-buf))

(defn read-db-key-buf []
  (let [barr (path->bytes "key.hex")
        buf (new UnsafeBuffer barr)
        db-key (DbKey/fromBuffer buf)]
    (debug "timestamp" (.-quoteDateTime db-key)
           "root" (.-root db-key)
           "strike" (.-strike db-key)
           "optionType" (.-optionType db-key)
           "expirationDate" (.-expirationDate db-key))))
(comment (read-db-key-buf))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn write-db-value-buf []
  (let [line-tokens (into-array
                     ["" "" "" "" "" ""
                      ;; open, high, low, close
                      "1.23" "2.34" "3.45" "4.56"
                      ;; vol, BS, bid, AS, ask
                      "42" "1" "0.99" "2" "1.01"
                      ;; ubid, uask
                      "99.99" "100.01"

                      ;; iuprice, uprice
                      "101.02" "100.00"

                      ;; iv, greeks
                      "0.16" "0.50" "0.03" "0.01" "0.02" "0.03"

                      "69"])
        db-value (DbValue/fromCsvLineTokens line-tokens)]
    (spit-buffer "value.hex" (.toBuffer db-value))))
(comment (write-db-value-buf))

(defn read-db-value-buf []
  (let [barr (path->bytes "value.hex")
        buf (new UnsafeBuffer barr)
        db-v (DbValue/fromBuffer buf)]
    (debug (.-open db-v) (.-high db-v) (.-low db-v) (.-close db-v))
    (debug (.-trade_volume db-v) (.-bid_size db-v) (.-bid db-v) (.-ask_size db-v) (.-ask db-v))
    (debug (.-underlying_bid db-v) (.-underlying_ask db-v))
    (debug (.-implied_underlying_price db-v) (.-active_underlying_price db-v))
    (debug (.-implied_volatility db-v)
           (.-delta db-v) (.-gamma db-v) (.-theta db-v) (.-vega db-v) (.-rho db-v))
    (debug (.-open_interest db-v))
    nil))
(comment (read-db-value-buf))


