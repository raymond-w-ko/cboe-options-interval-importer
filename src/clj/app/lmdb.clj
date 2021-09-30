(ns app.lmdb
  (:import
   [java.time Instant]
   [java.util.concurrent TimeUnit]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.channels Channels]
   [org.lmdbjava Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp DbiFlags PutFlags]
   [org.agrona MutableDirectBuffer]
   [org.agrona.concurrent UnsafeBuffer]

   app.DbKey
   app.DbValue)
  (:require
   [app.macros :refer [cond-xlet]]
   [clojure.java.io :as io]
   [clj-java-decompiler.core :refer [decompile]]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def dangerous-env-flags
  (into-array org.lmdbjava.EnvFlags [EnvFlags/MDB_FIXEDMAP
                                     EnvFlags/MDB_MAPASYNC
                                     EnvFlags/MDB_NOMETASYNC
                                     EnvFlags/MDB_NOSYNC
                                     ; EnvFlags/MDB_NORDAHEAD
                                     ]))
(def db-max-size (* 1024 1024 1024 512))
(defonce ^org.lmdbjava.Env env
  (-> (Env/create DirectBufferProxy/PROXY_DB)
      (.setMapSize db-max-size)
      (.setMaxDbs 1)
      (.open (io/file "./lmdb") dangerous-env-flags)))
(defonce ^org.lmdbjava.Dbi db
  (.openDbi
   env "SPX"
   ^"[Lorg.lmdbjava.DbiFlags;" (into-array org.lmdbjava.DbiFlags [DbiFlags/MDB_CREATE])))

(defn verify []
  (let [env
        (-> (Env/create ByteBufferProxy/PROXY_OPTIMAL)
            (.setMapSize (* 1024 1024 1024 512))
            (.setMaxDbs Verifier/DBI_COUNT)
            (.open (io/file "./lmdb-verifier") (into-array org.lmdbjava.EnvFlags [])))
        v (new Verifier env)]
    (debugf "verifier verified %d items" (.runFor v 3 TimeUnit/SECONDS))
    (.close env)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-stats []
  (with-open [txn (.txnRead env)]
    (let [stat (.stat db txn)]
      (debug "branchPages" (.-branchPages stat))
      (debug "depth" (.-depth stat))
      (debug "entries" (.-entries stat))
      (debug "leafPages" (.-leafPages stat))
      (debug "overflowPages" (.-overflowPages stat))
      (debug "pageSize" (.-pageSize stat))
      nil)))

; (defn test-rw []
;   (let [key-bb (ByteBuffer/allocateDirect (.getMaxKeySize env))
;         val-bb (ByteBuffer/allocateDirect 1024)
;         ^MutableDirectBuffer k (new UnsafeBuffer key-bb)
;         ^MutableDirectBuffer v (new UnsafeBuffer val-bb)

;         put-flags (into-array PutFlags [])]
;     (with-open [txn (.txnWrite env)]
;       (with-open [c (.openCursor db txn)]
;         (.putStringWithoutLengthUtf8 k 0 "foo")
;         (.putStringWithoutLengthUtf8 v 0 "bar")
;         (.put c k v put-flags))
;       (.commit txn))

;     (print-stats)

;     (with-open [txn (.txnWrite env)]
;       (with-open [c (.openCursor db txn)]
;         (.delete db txn k))
;       (.commit txn))

;     (print-stats)))

(defn spit-buffer [fname ^UnsafeBuffer buf]
  (with-open [f (io/output-stream fname)]
    (let [ch (Channels/newChannel f)]
      (.write ch (.byteBuffer buf)))))

(defn path->bytes
  (^bytes
   [path]
   (with-open [in (io/input-stream path)
               out (java.io.ByteArrayOutputStream.)]
     (io/copy in out)
     (.toByteArray out))))

(defn put-buffers [xs]
  (let [put-flags (into-array PutFlags [])]
    (with-open [txn (.txnWrite env)]
      (with-open [c (.openCursor db txn)]
        (dorun
         (for [[key-buf val-buf] xs]
           (.put c key-buf val-buf put-flags))))
      (.commit txn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn decode-from-db-test []
  (with-open [txn (.txnRead env)]
    (with-open [c (.openCursor db txn)]
      (.seek c SeekOp/MDB_FIRST)
      (.seek c SeekOp/MDB_FIRST)
      (let [key-buf (.key c)
            db-key (DbKey/fromBuffer key-buf)]
        (debug "\n"
               "timestamp" (.-quoteTimestamp db-key)
               "root" (.-root db-key)
               "\n"
               "optionType" (.-optionType db-key)
               "expirationDate" (.-expirationDate db-key)
               "strike" (.-strike db-key))))))

(defn --repl []
  (verify)
  ; (test-rw)
  (print-stats)

  (DbValue/priceStringToInt "100.251")
  (DbValue/priceStringToInt "100.25")
  (DbValue/priceStringToInt "-100.25")

  (let [t (-> (Instant/now) .getEpochSecond)
        db-key (new DbKey t "SPXPM" \P "2020-12-31" "5000")]
    (spit-buffer "key.hex" (.toBuffer db-key)))
  (let [barr (path->bytes "key.hex")
        buf (new UnsafeBuffer barr)
        db-key (DbKey/fromBuffer buf)]
    (debug "\n"
           "timestamp" (.-quoteTimestamp db-key) "root" (.-root db-key)
           "strike" (.-strike db-key))
    (debug "\n"
           "optionType" (.-optionType db-key) "expirationDate" (.-expirationDate db-key))
    nil)

  (let [x (into-array
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
        db-v (DbValue/fromCsvLineTokens x)]
    (spit-buffer "value.hex" (.toBuffer db-v)))

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
    nil)
  
  (decode-from-db-test)

  nil)
(comment (--repl))
