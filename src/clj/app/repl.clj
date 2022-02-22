(ns app.repl
  (:import
   [java.util.concurrent TimeUnit]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.channels Channels]
   
   [org.lmdbjava Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp DbiFlags PutFlags KeyRange]
   [org.agrona MutableDirectBuffer]
   [org.agrona.concurrent UnsafeBuffer]
   
   [app.types DbKey DbValue Utils])
  (:require
   [clojure.java.io :as io]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [app.utils :refer [iter-seq]]
   [app.lmdb :as lmdb :refer [spit-buffer path->bytes]]))

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
  (Utils/ParsePriceString "100.251")
  (Utils/ParsePriceString "100.25")
  (Utils/ParsePriceString "-100.25"))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn verify-lmdb []
  (with-open [env (-> (Env/create ByteBufferProxy/PROXY_OPTIMAL)
                      (.setMapSize (* 1024 1024 1024 512))
                      (.setMaxDbs Verifier/DBI_COUNT)
                      (.open (io/file "./lmdb-verifier")
                             (into-array org.lmdbjava.EnvFlags [])))]
    (let [v (new Verifier env)]
      (debugf "verifier verified %d items" (.runFor v 3 TimeUnit/SECONDS)))))
(comment (verify-lmdb))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-key [db-key]
  (debug "timestamp" (.toMutableDateTime (.-quoteDateTime db-key))
         "root" (.-root db-key)
         "optionType" (.-optionType db-key)
         "expirationDate" (.-expirationDate db-key)
         "strike" (.-strike db-key)))

(defonce *first-db-key (atom nil))
(defn decode-from-db-test []
  (let [env (lmdb/create-read-env)
        db (lmdb/open-db env)]
    (with-open [txn (.txnRead env)
                c (.openCursor db txn)]
      (.seek c SeekOp/MDB_FIRST)
      (.seek c SeekOp/MDB_NEXT)
      (let [key-buf (.key c)
            db-key (DbKey/fromBuffer key-buf)]
        (reset! *first-db-key db-key)
        (print-key db-key)))
    (.close env)))
(comment (decode-from-db-test))

(defn seek-from-db-test []
  (let [env (lmdb/create-read-env)
        db (lmdb/open-db env)]
    (with-open [txn (.txnRead env)]
      (let [begin-range-key
            (let [k (new DbKey)]
              (set! (.-quoteDateTime k)
                    (.-quoteDateTime @*first-db-key))
              (set! (.-root k) "SPXW  ")
              (set! (.-optionType k) \P)
              (set! (.-expirationDate k) "0000-00-00")
              (.toBuffer k))
            end-range-key
            (let [k (new DbKey)]
              (set! (.-quoteDateTime k)
                    (Instant/parse "2020-01-02T09:32:00.000-05:00"))
              (set! (.-root k) "      ")
              (set! (.-optionType k) \P)
              (set! (.-expirationDate k) "0000-00-00")
              (.toBuffer k))
            iterable (.iterate db txn (KeyRange/closedOpen begin-range-key end-range-key))
            xs (iter-seq iterable)]
        (->> xs
             (map (fn [x]
                    (-> (.key x)
                        (DbKey/fromBuffer)
                        (print-key))))
             (dorun))
        ; (loop [flag (.hasNext iter)]
        ;   (when flag
        ;     (let [x (.next iter)]
        ;       (-> (.key x)
        ;           (DbKey/fromBuffer)
        ;           (print-key)))
        ;     (recur (.hasNext iter))))
        ; (spit "key1.hex" (DbKey/UnsafeBufferToHex (.key (first xs))))
        ; (spit "key2.hex" (DbKey/UnsafeBufferToHex (.key (second xs))))
        ; (debug (.capacity (.key (first xs))))
        ; (spit "dump.txt" (with-out-str (dorun (map #(-> % .key DbKey/fromBuffer print-key) xs))))
        ; (spit "dump.txt" (->> (map #(-> % .key DbKey/UnsafeBufferToHex) xs)
        ;                       (interpose "\n")
        ;                       (apply str)))
        nil))
    (.close env)))
(comment (decode-from-db-test))
(comment (seek-from-db-test))
