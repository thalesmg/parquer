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
(ns ble-oracle
  (:require
   [clojure.data.json :as json])
  (:import
   (java.util Base64)
   (org.apache.parquet.bytes DirectByteBufferAllocator)
   (org.apache.parquet.column.values.rle RunLengthBitPackingHybridEncoder)))

(defn encode64
  [x]
  (->> x
       (.encodeToString (Base64/getEncoder))))

(defn pack
  [{:keys [:values :bit-width :initial-capacity :page-size :allocator]
    :or {initial-capacity 10
         page-size 10
         allocator (DirectByteBufferAllocator.)}}]
  (let [enc (RunLengthBitPackingHybridEncoder. bit-width initial-capacity page-size allocator)]
    (doseq [v values]
      (.writeInt enc v))
    (-> enc
        .toBytes
        .toByteArray)))

(defn -main
  [& _args]
  (doseq [input-raw (line-seq (java.io.BufferedReader. *in*))]
    (let [{:keys [:values :bit-width]} (json/read-str
                                        input-raw
                                        :key-fn keyword)
          packed (pack {:values values :bit-width bit-width})
          packed64 (encode64 packed)]
      (println packed64))))
