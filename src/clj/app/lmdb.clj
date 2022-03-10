(ns app.lmdb
  (:import
   [java.nio.channels Channels]
   [org.lmdbjava
    Env EnvFlags DirectBufferProxy Verifier ByteBufferProxy Txn Stat SeekOp DbiFlags PutFlags]
   [org.agrona.concurrent UnsafeBuffer])
  (:require
   [clojure.java.io :as io]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def db-max-size (* 1024 1024 1024 (+ 1024 512)))
(def normal-read-env-flags (into-array org.lmdbjava.EnvFlags
                                       [EnvFlags/MDB_RDONLY_ENV
                                        EnvFlags/MDB_NORDAHEAD]))
(def write-env-flags (into-array org.lmdbjava.EnvFlags
                                 [EnvFlags/MDB_NORDAHEAD]))
(def dangerous-fast-write-env-flags (into-array org.lmdbjava.EnvFlags
                                                [EnvFlags/MDB_WRITEMAP
                                                 EnvFlags/MDB_MAPASYNC
                                                 EnvFlags/MDB_NOMETASYNC
                                                 EnvFlags/MDB_NOSYNC
                                                 EnvFlags/MDB_NORDAHEAD]))
(defn create-read-env
  (^org.lmdbjava.Env
   [^String db-dir]
   (-> (Env/create DirectBufferProxy/PROXY_DB)
       (.setMapSize db-max-size)
       (.setMaxDbs 64)
       (.open (io/file db-dir) normal-read-env-flags))))
(defn create-write-env
  (^org.lmdbjava.Env
   [^String db-dir]
   (-> (Env/create DirectBufferProxy/PROXY_DB)
       (.setMapSize db-max-size)
       (.setMaxDbs 64)
       (.open (io/file db-dir) write-env-flags))))
(defn  create-dangerous-write-env
  (^org.lmdbjava.Env
   [^String db-dir]
   (-> (Env/create DirectBufferProxy/PROXY_DB)
       (.setMapSize db-max-size)
       (.setMaxDbs 64)
       (.open (io/file db-dir) dangerous-fast-write-env-flags))))

(def ^"[Lorg.lmdbjava.DbiFlags;" normal-db-flags
  (into-array org.lmdbjava.DbiFlags [DbiFlags/MDB_CREATE]))
(defn open-db
  (^org.lmdbjava.Dbi [^org.lmdbjava.Env env ^String db-name]
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

(def default-put-buffer-flags (into-array PutFlags [PutFlags/MDB_NOOVERWRITE]))
(def overwrite-put-buffer-flags (into-array PutFlags []))
(defn put-buffers
  "pairs is a sequence of [key-buffer, value-buffer]."
  ([env db pairs] (put-buffers env db false pairs))
  ([^org.lmdbjava.Env env
    ^org.lmdbjava.Dbi db
    overwrite
    pairs]
   (with-open [txn (.txnWrite env)]
     (with-open [c (.openCursor db txn)]
       (letfn [(f [[key-buf val-buf]]
                 (.put c key-buf val-buf (if overwrite
                                           overwrite-put-buffer-flags
                                           default-put-buffer-flags)))]
         (dorun (map f pairs))))
     (.commit txn))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
