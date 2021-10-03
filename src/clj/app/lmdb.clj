(ns app.lmdb
  (:import
   [java.time Instant]
   [java.util.concurrent TimeUnit]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.channels Channels]
   [org.lmdbjava
    Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn Stat
    SeekOp DbiFlags PutFlags]
   [org.agrona MutableDirectBuffer]
   [org.agrona.concurrent UnsafeBuffer]

   [app.types DbKey DbValue])
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

(def db-max-size (* 1024 1024 1024 575))
(def normal-read-env-flags (into-array org.lmdbjava.EnvFlags
                                       [EnvFlags/MDB_RDONLY_ENV]))
(def dangerous-fast-write-env-flags
  (into-array org.lmdbjava.EnvFlags
              [EnvFlags/MDB_FIXEDMAP
               EnvFlags/MDB_MAPASYNC
               EnvFlags/MDB_NOMETASYNC
               EnvFlags/MDB_NOSYNC
               EnvFlags/MDB_NORDAHEAD]))
(defn create-read-env
  (^org.lmdbjava.Env
   [^String db-dir]
   (-> (Env/create DirectBufferProxy/PROXY_DB)
       (.setMapSize db-max-size)
       (.setMaxDbs 1)
       (.open (io/file db-dir) normal-read-env-flags))))
(defn  create-write-env
  (^org.lmdbjava.Env
   [^String db-dir]
   (-> (Env/create DirectBufferProxy/PROXY_DB)
       (.setMapSize db-max-size)
       (.setMaxDbs 1)
       (.open (io/file db-dir) dangerous-fast-write-env-flags))))

(def ^"[Lorg.lmdbjava.DbiFlags;" normal-db-flags
  (into-array org.lmdbjava.DbiFlags [DbiFlags/MDB_CREATE]))
(defn open-db
  (^org.lmdbjava.Dbi
   [^org.lmdbjava.Env env]
   (.openDbi env "main" normal-db-flags))
  (^org.lmdbjava.Dbi
   [^org.lmdbjava.Env env
    ^String db-name]
   (.openDbi env db-name normal-db-flags)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^Stat get-stats
  [^org.lmdbjava.Env env
   ^org.lmdbjava.Dbi db]
  (with-open [txn (.txnRead env)]
    (let [stat (.stat db txn)]
      stat)))

(defn print-stats
  [env db]
  (let [stat (get-stats env db)]
    (debug "branchPages" (.-branchPages stat))
    (debug "depth" (.-depth stat))
    (debug "entries" (.-entries stat))
    (debug "leafPages" (.-leafPages stat))
    (debug "overflowPages" (.-overflowPages stat))
    (debug "pageSize" (.-pageSize stat))))

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

(def put-buffer-flags (into-array PutFlags [PutFlags/MDB_NOOVERWRITE]))
(defn put-buffers
  "pairs is a sequence of [key-buffer, value-buffer]."
  [^org.lmdbjava.Env env
   ^org.lmdbjava.Dbi db
   pairs]
  (with-open [txn (.txnWrite env)]
    (with-open [c (.openCursor db txn)]
      (letfn [(f [[key-buf val-buf]]
                (when-not (.get db txn key-buf)
                  (.put c key-buf val-buf put-buffer-flags)))]
        (dorun (map f pairs))))
    (.commit txn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
