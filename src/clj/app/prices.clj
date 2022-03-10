(ns app.prices
  (:import
   [java.io File]
   [java.sql DriverManager Connection Statement PreparedStatement ResultSet]
   [java.nio ByteBuffer]

   [org.agrona.concurrent UnsafeBuffer]
   [org.lmdbjava Dbi CursorIterable$KeyVal]

   [org.apache.avro Schema$Parser]
   [org.apache.avro.generic GenericData$Record]
   [org.apache.hadoop.conf Configuration]
   [org.apache.parquet.avro AvroParquetWriter AvroParquetWriter]
   [org.apache.parquet.hadoop ParquetFileWriter$Mode]
   [org.apache.parquet.hadoop.metadata CompressionCodecName]

   [org.apache.hadoop.fs Path]

   [app.types Utils])
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.edn :as edn]

   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [taoensso.nippy :as nippy]

   [com.climate.claypoole :as cp]

   [app.macros :as mac :refer [->hash field cond-xlet]]
   [app.utils :refer []]
   [app.lmdb :as lmdb]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^ByteBuffer clone-to-memaligned-buffer [^bytes src]
  (let [buf (ByteBuffer/allocateDirect (alength src))]
    (.put buf src)
    buf))

(defn unthaw-unsafe-buffer [^UnsafeBuffer src]
  (let [buf (make-array Byte/TYPE (.capacity src))]
    (.getBytes src 0 buf)
    (nippy/thaw buf)))

(defn process-edn-file! [{:keys [env db]} ^File edn-file]
  (debug (-> edn-file .getPath))
  (let [xs (-> edn-file
               .getPath
               slurp
               edn/read-string)
        ->kv-buffers
        (fn [{:keys [quote-datetime bid ask price]}]
          (assert quote-datetime)
          (assert bid)
          (assert ask)
          (assert price)
          (let [t (Utils/QuoteDateTimeToInstant quote-datetime)
                m {:bid (Float/parseFloat bid)
                   :ask (Float/parseFloat ask)
                   :price (Float/parseFloat price)}
                k (->> (Utils/InstantToByteBuffer t)
                       (new UnsafeBuffer))
                v (->> (nippy/freeze m)
                       (clone-to-memaligned-buffer)
                       (new UnsafeBuffer))]
            [k v]))]
    (->> (mapv ->kv-buffers xs)
         (lmdb/put-buffers env db true)
         (dorun))))

(defn import!
  [{:as args :keys [edn-src-dir lmdb-env-dir db-name]}]
  (assert (.exists (io/file lmdb-env-dir)))
  (with-open [env (lmdb/create-write-env lmdb-env-dir)]
    (with-open [db (lmdb/open-db env db-name)]
      (cond-xlet
       :let [args (mac/args env db)]
       :do (->> (io/file edn-src-dir)
                (file-seq)
                (filter (fn [^File f] (.isFile f)))
                (sort)
                (cp/pmap 4 (partial process-edn-file! args))
                (dorun))))
    (.sync env true))
  (debug "import! finished"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-kv [^CursorIterable$KeyVal x]
  (let [key-buf (.key x)
        val-buf (.val x)]
    (debug (Utils/ByteBufferToInstant key-buf 0))
    (debug (-> val-buf unthaw-unsafe-buffer))))

(defn walk+print [{:keys [txn ^Dbi db]}]
  (cond-xlet
   :let [c (.iterate db txn)
         iter (.iterator c)
         x (.next iter)]
   :do (print-kv x)
   :let [x (->> (repeatedly 1700000 #(.next iter))
                (last))]
   :do (print-kv x)))

(defn read+print-some-prices [{:as args :keys [lmdb-env-dir db-name]}]
  (with-open [env (lmdb/create-read-env lmdb-env-dir)]
    (with-open [db (lmdb/open-db env db-name)]
      (with-open [txn (.txnRead env)]
        (cond-xlet
         :let [args (mac/args env db txn)]
         :do (walk+print args))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def prices-schema (-> (new Schema$Parser)
                       (.parse (slurp "prices.avsc"))))

(defn write-parquet!
  [{:as args :keys [lmdb-env-dir
                    db-name
                    ^String output-path]}]
  (with-open [env (lmdb/create-read-env lmdb-env-dir)]
    (with-open [db (lmdb/open-db env db-name)]
      (let [args (mac/args env db)
            configuration (new Configuration)]
        (with-open [writer (-> (AvroParquetWriter/builder (new Path output-path))
                               (.withSchema prices-schema)
                               (.withConf configuration)
                               (.withCompressionCodec CompressionCodecName/SNAPPY)
                               (.withWriteMode ParquetFileWriter$Mode/OVERWRITE)
                               (.build))]
          (let [record (new GenericData$Record prices-schema)]
            (.put record "t" (new Long 0))
            (.put record "price" (new Float 420.69))
            (.write writer record)))

        nil))))
