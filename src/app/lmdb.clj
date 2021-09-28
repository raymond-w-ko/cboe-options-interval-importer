(ns app.lmdb
  (:import
   [java.util.concurrent TimeUnit]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.channels Channels]
   [org.lmdbjava Env DirectBufferProxy Verifier ByteBufferProxy Txn
    SeekOp DbiFlags PutFlags]
   [org.agrona MutableDirectBuffer]
   [org.agrona.concurrent UnsafeBuffer])
  (:require
   [app.macros :refer [cond-xlet]]
   [clojure.java.io :as io]
   [clj-java-decompiler.core :refer [decompile]]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def db-max-size (* 1024 1024 1024 512))
(defonce ^org.lmdbjava.Env env
  (-> (Env/create DirectBufferProxy/PROXY_DB)
      (.setMapSize db-max-size)
      (.setMaxDbs 1)
      (.open (io/file "./lmdb") (into-array org.lmdbjava.EnvFlags []))))
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

(defn test-rw []
  (let [key-bb (ByteBuffer/allocateDirect (.getMaxKeySize env))
        val-bb (ByteBuffer/allocateDirect 1024)
        ^MutableDirectBuffer k (new UnsafeBuffer key-bb)
        ^MutableDirectBuffer v (new UnsafeBuffer val-bb)
        
        put-flags (into-array PutFlags [])]
    (with-open [txn (.txnWrite env)]
      (with-open [c (.openCursor db txn)]
        (.putStringWithoutLengthUtf8 k 0 "foo")
        (.putStringWithoutLengthUtf8 v 0 "bar")
        (.put c k v put-flags))
      (.commit txn))
    
    (print-stats)
    
    (with-open [txn (.txnWrite env)]
      (with-open [c (.openCursor db txn)]
        (.delete db txn k))
      (.commit txn))
    
    (print-stats)))

(defn ^Integer date-string->int
  [^String s]
  (let [year (-> (.substring s 0 4) Integer/parseInt)
        month (-> (.substring s 5 7) Integer/parseInt)
        day (-> (.substring s 8 10) Integer/parseInt)]
    (-> (unchecked-multiply 10000 year)
        (unchecked-add (unchecked-multiply 100 month))
        (unchecked-add day))))

(comment (date-string->int "2020-01-02"))

(defn ^UnsafeBuffer option->buf
  "key size should be sum of:
  5 root (SPX has a lot of different and exotic types)
  1 option type ('P' or 'C')
  4 expiration date (remove hyphens, treat as int)
  4 option strike price (no decimals)
  8 quote timestamp (int64 to prepare for year 2038 problem )
  ---------------------
  22 bytes

  Returns a org.agrona.concurrent.UnsafeBuffer"
  [^String root ^Character option-type ^Integer expiration-date ^Integer strike
   ^Long quote-timestamp]
  (let [bb (ByteBuffer/allocateDirect (+ 5 1 4 4 8))
        buf (new UnsafeBuffer bb)]
    (cond-xlet
     :let [idx 0]

     :do (.putStringWithoutLengthUtf8 buf idx root)
     :let [idx (+ idx 5)]

     :do (.putChar buf idx option-type)
     :let [idx (+ idx 1)]

     :do (.putInt buf idx (date-string->int expiration-date) ByteOrder/BIG_ENDIAN)
     :let [idx (+ idx 4)]

     :do (.putInt buf idx strike ByteOrder/BIG_ENDIAN)
     :let [idx (+ idx 4)]

     :do (.putLong buf idx quote-timestamp ByteOrder/BIG_ENDIAN)
     :let [idx (+ idx 8)]

     :return buf)))

(defn --repl []
  (verify)
  (test-rw)
  (print-stats)
  (with-open [f (io/output-stream "key.hex")]
    (let [ch (Channels/newChannel f)
          k (option->buf "SPXPM" \P "2020-12-31" 5000 0)]
      (.write ch (.byteBuffer k))))
  nil)
(comment (--repl))
