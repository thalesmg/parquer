;;--------------------------------------------------------------------
;; Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;--------------------------------------------------------------------
(ns writer-oracle
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io])
  (:import
   (java.util Base64)
   (org.apache.parquet.avro AvroParquetReader)
   (org.apache.parquet.conf PlainParquetConfiguration)
   (org.apache.parquet.io SeekableInputStream InputFile)))

(defn avro->json
  [avro]
  (-> avro
      .toString
      json/read-str))

(defn new-seekable-input-stream
  [in-ba]
  (let [pos (atom 0)]
    (letfn [(read1-byte []
              (let [x (aget in-ba @pos)]
                (swap! pos inc)
                (byte x)))
            (read1 []
              (let [x (read1-byte)
                    x-u (bit-and x 0xff)]
                (int x-u)))
            (read-array [out-array]
              (let [to-read (alength out-array)]
                (doseq [i (range to-read)]
                  (let [x (read1-byte)]
                    (aset-byte out-array i x)))))]
      (proxy [SeekableInputStream] []
        (read
          ([]
           (read1))
          ([byte-buffer]
           :todo))
        (getPos []
          @pos)
        (seek [new-pos]
          (reset! pos new-pos))
        (readFully
          ([out-array]
           (if (bytes? out-array)
             (read-array out-array)
             ;; java.nio.ByteBuffer
             (let [to-read (.remaining out-array)
                   tmp (byte-array to-read (byte 0))]
               (read-array tmp)
               (.put out-array
                     tmp
                     (+ (.position out-array) (.arrayOffset out-array))
                     (.remaining out-array)))))
          ([out-array start len]
           (doseq [i (range len)]
             (let [x (read1-byte)]
               (aset-byte out-array (+ start i) x)))))))))

(defn new-mem-input-file
  [in-ba]
  (proxy [InputFile] []
    (getLength []
      (alength in-ba))
    (newStream []
      (new-seekable-input-stream in-ba))))

(defn read-parquet-avro
  [input-file]
  (with-open [reader (AvroParquetReader/genericRecordReader
                      input-file
                      (PlainParquetConfiguration.
                       {"parquet.avro.readInt96AsFixed" "true"}))]
    (loop [record (.read reader)
           acc []]
      (if record
        (recur (.read reader)
               (->> record
                    avro->json
                    (conj acc)))
        acc))))

(defn read-parquet
  [data-raw]
  (with-open [in (io/input-stream data-raw)
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (-> out
        .toByteArray
        new-mem-input-file
        read-parquet-avro)))

(defn decode64
  [x]
  (->> x
       (.decode (Base64/getDecoder))))

(defn println-err
  [& args]
  (binding [*out* *err*]
    (apply println args)))

(defn -main
  [& _args]
  (doseq [input-raw64 (line-seq (java.io.BufferedReader. *in*))]
    ;; (println-err "Received input:" input-raw64)
    (try
      (let [input-raw (decode64 input-raw64)
            results (read-parquet input-raw)]
        (-> results json/write-str println))
      (catch Exception e
        (println-err)
        (.printStackTrace e)
        (println-err "error: " (.getMessage e))
        (-> {:error (.getMessage e)} json/write-str println)))))
