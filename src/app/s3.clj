(ns app.s3
  (:import
   [java.io ByteArrayInputStream]
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]

   [com.amazonaws.auth BasicAWSCredentials AWSStaticCredentialsProvider]
   [com.amazonaws.services.s3 AmazonS3ClientBuilder]
   [com.amazonaws.services.s3.iterable S3Objects]
   [com.amazonaws.services.s3.model PutObjectRequest ObjectMetadata AmazonS3Exception]
   
   [alex.mojaki.s3upload StreamTransferManager])
  (:require
   [app.creds :refer [creds-provider]]
   [app.config :refer [aws-access-key-id aws-secret-access-key
                       bucket zip-bucket-dir]]
   [app.utils :refer [->core-filename str->bytes]]
   [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce s3 (-> (AmazonS3ClientBuilder/standard)
                (.withCredentials creds-provider)
                (.withRegion "us-east-1")
                (.build)))

(defn get-object [object-key]
  (.getObject s3 bucket object-key))

(defn exists-object? [object-key]
  (if-let [ret (try (.getObjectMetadata s3 bucket object-key)
                    (catch AmazonS3Exception e
                      false))]
    ret
    false))

(defn get-zip-objects []
  (let [objs (S3Objects/withPrefix s3 bucket zip-bucket-dir)]
    (for [obj (-> objs .iterator iterator-seq)]
      obj)))

(defn get-zip-object-keys []
  (->> (get-zip-objects)
       (mapv #(.getKey %))))

(defn ^ZipInputStream zip-object-key->input-stream [zip-object-key]
  (-> (get-object zip-object-key)
      (.getObjectContent)
      (ZipInputStream.)))

(defn zip-object-key->line-seq [zip-object-key]
  (let [zip-stream (zip-object-key->input-stream zip-object-key)
        entry (.getNextEntry zip-stream)
        fname (.getName entry)
        lines (-> (InputStreamReader. zip-stream)
                  (BufferedReader. (* 1024 1024 32))
                  (line-seq))]
    {:lines lines
     :zip-stream zip-stream
     :fname fname}))

(defn open-object-output-stream! [object-key]
  (let [man (-> (StreamTransferManager. bucket object-key s3)
                (.numStreams 1)
                (.numUploadThreads 1)
                (.queueCapacity 2)
                (.partSize 32))
        streams (.getMultiPartOutputStreams man)]
    {:manager man
     :output-stream (.get streams 0)}))

(defn close-object-output-stream! [{:keys [output-stream manager]}]
  (.close output-stream)
  (.complete manager))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn put-string-object [object-key s]
  (let [is (ByteArrayInputStream. (str->bytes s))
        metadata (ObjectMetadata.)
        por (PutObjectRequest. bucket object-key is metadata)]
    (.putObject s3 por)))

(comment (put-string-object "test/put-test.txt" "Hello, Sekai!"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn --repl []
  (let [{:as stream :keys [output-stream]} (open-object-output-stream! "test/foo.txt")]
    (.write output-stream (str->bytes "Hello World!"))
    (close-object-output-stream! stream))

  (->> (get-zip-object-keys)
       (map ->core-filename)
       (println))
  (comment let [lines (->> (get-zip-object-keys)
                   (first)
                   (zip-object-key->line-seq))]
    (time (println (reduce (fn [acc x] (inc acc)) 0 (:lines lines)))))
  
  nil)

(comment (--repl))
