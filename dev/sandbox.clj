(require '[taoensso.telemere :as log])

(log/set-min-level! :debug)

(import '(org.apache.parquet.column.values.rle RunLengthBitPackingHybridDecoder))
(import '(org.apache.parquet.column Dictionary Encoding))
(import '(org.apache.parquet.column.page DictionaryPage))
(import '(org.apache.parquet.column.values.dictionary DictionaryValuesReader PlainValuesDictionary PlainValuesDictionary$PlainBinaryDictionary))
(import '(org.apache.parquet.bytes BytesInput ByteBufferInputStream))
(import '(java.nio ByteBuffer))
(import '(org.apache.parquet.hadoop ParquetFileReader ParquetFileWriter ParquetReader))
(import '(org.apache.parquet.hadoop.api ReadSupport))
(import '(org.apache.parquet.avro AvroReadSupport AvroParquetWriter))
(import '(org.apache.parquet.io
          LocalOutputFile OutputFile PositionOutputStream
          LocalInputFile InputFile SeekableInputStream))
(import '(org.apache.parquet ParquetReadOptions))
(import '(org.apache.parquet.format PageHeader Util))
(import 'org.apache.hadoop.fs.Path)
(import '(org.apache.parquet.column.values.rle
          RunLengthBitPackingHybridEncoder
          RunLengthBitPackingHybridDecoder))
(import '(org.apache.parquet.bytes DirectByteBufferAllocator))
(import '(org.apache.parquet.schema MessageType MessageTypeParser))
(import '(org.apache.parquet.format.converter ParquetMetadataConverter))
(import '(org.apache.parquet.cli BaseCommand))
(import '(org.apache.parquet.cli.commands CatCommand))
(import '(org.apache.parquet.cli.util Schemas))
(import '(org.slf4j LoggerFactory))
(import '(org.apache.avro.generic GenericData GenericData$Array))
(import '(org.apache.avro.generic GenericRecordBuilder))
;; (import '(org.apache.avro Schema))


(require '[clojure.java.io :as io])
(require '[clojure.pprint :as pp])
(require '[clojure.data.json :as json])

(comment
  ;; https://mvnrepository.com/artifact/org.apache.avro/avro

  (add-libs '{org.apache.iceberg/iceberg-core {:mvn/version "1.10.0"}
              org.apache.avro/avro {:mvn/version "1.12.1"}})

  (import '(org.apache.iceberg Schema))
  (import '(org.apache.iceberg.types Types$NestedField))

  (def sc
   (let [is-optional true
         ;; avro-util (org.apache.iceberg.avro.AvroSchemaUtil.)
         ]
     (-> (Schema.
          (into-array
           Types$NestedField
           [(Types$NestedField/of 1 is-optional "date" (org.apache.iceberg.types.Types$DateType/get))
            (Types$NestedField/of 2 is-optional "time" (org.apache.iceberg.types.Types$TimeType/get))
            ]))
         ;; (.convert avro-util)
         ;; (org.apache.iceberg.avro.AvroSchemaUtil/convert "mytable")
         )))

  (-> sc
      (org.apache.iceberg.avro.AvroSchemaUtil/convert "mytable")
      .toString
      println
      )

  )

(defn inspect
  [x & {:keys [:label] :or {label :>>>>>>>>>>>>>}}]
  (pp/pprint {label x})
  x)

(def mystream (io/input-stream (byte-array [0, 0, 0, 4, 1, 1, 4, 0])))

(def decoder (RunLengthBitPackingHybridDecoder. 2 mystream))

(def raw-dict-bytes
  #_(byte-array
   [21, 4, 21, 12, 21, 30, 76, 21, 2, 21, 0, 18, 0, 0,
    2, 0, 0, 0, 77, 115   ]
   )
  ;; works; decompressed
  (byte-array [2, 0, 0, 0, 77, 115])
  #_(byte-array
     [21, 4, 21, 12, 21, 30, 76, 21, 2, 21, 0, 18, 0, 0, 40, 181, 47, 253, 32, 6,
      49, 0, 0, 2, 0, 0, 0, 77, 115, 21, 0, 21, 18, 21, 36, 44, 21, 4, 21, 16, 21,
      6, 21, 6, 28, 54, 0, 40, 2, 77, 115, 24, 2, 77, 115, 0, 0, 0, 40, 181, 47,
      253, 32, 9, 73, 0, 0, 2, 0, 0, 0, 4, 1, 1, 4, 0]))

(def dict-page
  (let [dictionary-size 1
        encoding (Encoding/PLAIN)
        bytes-input (BytesInput/from raw-dict-bytes)]
    (DictionaryPage. bytes-input dictionary-size encoding)))

(def plain-bin-dict
  (PlainValuesDictionary$PlainBinaryDictionary. dict-page))

(def raw-data-bytes-decompressed
  (byte-array
   [2, 0, 0, 0, 4, 1, 1, 4, 0]))

(def dict-val-reader
  (let [value-count 2]
   (doto (DictionaryValuesReader. plain-bin-dict)
     (.initFromPage
      value-count
      (ByteBufferInputStream/wrap
       [(ByteBuffer/wrap raw-data-bytes-decompressed)])))))

#_(def pf
  (ParquetFileReader.
   (LocalInputFile. (.toPath (io/file "boh.parquet")))
   (-> (ParquetReadOptions/builder)
       (.useStatsFilter false)
       (.useDictionaryFilter false)
       (.useRecordFilter false)
       (.useBloomFilter false)
       .build))
  )

(Util/readPageHeader
 (io/input-stream
  (byte-array
   [21, 0, 21, 12, 21, 12, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 0, 21, 0, 18, 0, 0])))

#_(def col-desc1
  (-> pf
      .getFileMetaData
      .getSchema
      .getColumns
      first))

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
        .toByteArray
        vec)))

(defn show-bits
  [x]
  (pp/cl-format nil "~8,'0B" (bit-and x 0xff)))

(defn to-unsigned
  [x]
  (mod x 256))

(defmacro juxt+
  [& fns]
  (let [x (gensym)]
    `(fn [~x]
       ~(mapv #(list % x) fns))))

(defn avro->json
  [avro]
  (-> avro
      .toString
      json/read-str))

(defn get-avro-schema
  [filepath]
  (let [logger (LoggerFactory/getLogger BaseCommand)
        base-cmd (CatCommand. logger 10)
        conf (-> base-cmd .getConf)
        uri (.qualifiedURI base-cmd filepath)
        schema (Schemas/fromParquet conf uri)]
    schema))

(defn get-file-reader
  [filepath]
  (let [in-file (-> filepath io/file .toPath (LocalInputFile.))
        file-reader (ParquetFileReader/open in-file)]
    file-reader))

(defn get-fmd
  [filepath]
  (-> filepath
      get-file-reader
      .getFileMetaData))

(defn get-parquet-schema
  [filepath]
  (-> (get-fmd filepath)
      .getSchema))

(defn read-parquet
  [filepath & {:keys [:conf]}]
  (let [logger (LoggerFactory/getLogger BaseCommand)
        hconf (when conf
                (let [hconf (org.apache.hadoop.conf.Configuration.)]
                  (doseq [[k v] conf]
                    (.set hconf k v))
                  hconf))
        base-cmd (-> (CatCommand. logger 10)
                     (cond->
                         conf (.setConf hconf)))
        conf (-> base-cmd .getConf)
        uri (.qualifiedURI base-cmd filepath)
        schema (Schemas/fromParquet conf uri)
        openDataFile (-> BaseCommand
                         (.getDeclaredMethod
                          "openDataFile"
                          (into-array Class [String org.apache.avro.Schema]))
                         (doto (.setAccessible true)))
        data (.invoke openDataFile base-cmd (into-array Object [filepath schema]))]
    (-> data
        .iterator
        iterator-seq)))

(defn read-parquet-as-json
  [filepath & opts]
  (->> (apply read-parquet filepath opts)
       (mapv avro->json)))

(defn parse-avro-schema
  [schema]
  (let [parser (org.apache.avro.Schema$Parser.)]
    (if (map? schema)
      (.parse parser (json/write-str schema))
      (.parse parser schema))))

(defn parse-parquet-schema
  [schema]
  (-> schema
      MessageTypeParser/parseMessageType))

(defn parquet-schema<->avro
  [parquet-schema]
  (let [converter (org.apache.parquet.avro.AvroSchemaConverter.)]
    (.convert converter parquet-schema)))

(defn parse-parquet-schema-to-avro
  [schema]
  (-> schema
      parse-parquet-schema
      parquet-schema<->avro))

(defn open-avro-schema
  [filepath]
  (parse-avro-schema (slurp filepath)))

(defn new-pos-output-stream
  [out]
  (let [pos (atom 0)]
   (proxy [PositionOutputStream] []
     (getPos []
       @pos)
     (flush []
       (.flush out))
     (close []
       (.flush out)
       (.close out))
     #_(write
       ([x]
        (prn x)
        (if (bytes? x)
          (do
            (.write ^bytes x out)
            (swap! pos + (.length x)))
          (do
            (.write x out)
            (swap! pos inc))))
       ([b off len]
        (.write out b off len)
        (swap! pos + len)))
     (write
       ([x]
        (if (bytes? x)
          (do
            (.write out ^btyes x)
            (swap! pos + (alength x)))
          (do
            (.write out x)
            (swap! pos inc))))
       ([b off len]
        (.write out b off len)
        (swap! pos + len))))))

(defn new-mem-output-file
  ;; [^java.io.ByteArrayOutputStream out]
  [out]
  (proxy [OutputFile] []
    (create [_block-size-hint]
      (new-pos-output-stream out))
    (createOrOverwrite [_block-size-hint]
      (new-pos-output-stream out))
    (supportsBlockSize []
      false)
    (defaultBlockSize []
      0)))

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
                (prn :x>>>>>>> x :x-u>>>>>>> x-u)
                (int x-u)))
            (read-array [out-array]
              (let [to-read (alength out-array)]
                (doseq [i (range to-read)]
                  (let [x (read1-byte)]
                    (prn :x>>>>>>> x :i>>>>>>> i)
                    (aset-byte out-array i x)))))
            ]
    (proxy [SeekableInputStream] []
      (read
        ([]
         (read1))
        ([byte-buffer]
         (prn :booooooom>>>>>>>>>>>>>)
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
             (prn :to-read>>>>>>> to-read
                  :.pos>>>>>>> (.position out-array)
                  :.off>>>>>>>>> (.arrayOffset out-array)
                  :tmplen>>>>>>> (alength tmp)
                  :pos>>>>>>>>>> @pos
                  )
             (read-array tmp)
             ;; (prn (vec tmp))
             (.put out-array
                   tmp
                   (+ (.position out-array) (.arrayOffset out-array))
                   (.remaining out-array))
             )
           ))
        ([out-array start len]
         (prn :start>>>>>>> start :len>>>>>>> len :pos>>>>>>>>> @pos)
         (doseq [i (range len)]
           (let [x (read1-byte)]
             (prn :x>>>>>>> x :i>>>>>>> i)
             (aset-byte out-array (+ start i) x)))))
      ))))

(defn new-mem-input-file
  [in-ba]
  (proxy [InputFile] []
    (getLength []
      (alength in-ba))
    (newStream []
      (new-seekable-input-stream in-ba))))

(declare to-avro)

(defn resolve-avro-union
  [x union]
  (let [schemas (-> union .getTypes .iterator iterator-seq)]
    (loop [[t & ts] schemas]
      (cond
        (nil? x) x
        (nil? t) (throw (ex-info "no matching type"
                                 {:schemas (map str schemas) :value x}))

        (and (= org.apache.avro.Schema$Type/RECORD (.getType t))
             (map? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/MAP (.getType t))
             (map? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/ARRAY (.getType t))
             (vector? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/INT (.getType t))
             (int? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/LONG (.getType t))
             (int? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/STRING (.getType t))
             (string? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/BYTES (.getType t))
             (string? x)) (to-avro (.getBytes x) t)

        (and (= org.apache.avro.Schema$Type/BOOLEAN (.getType t))
             (boolean? x)) (to-avro x t)

        (and (= org.apache.avro.Schema$Type/FIXED (.getType t))
             (string? x)) (to-avro
                           (->> x
                                .getBytes
                                (org.apache.avro.generic.GenericData$Fixed. t))
                           t)

        :else (recur ts)))))

(defn to-avro
  [x schema]
  (let [t (.getType schema)]
    (condp some [t]
      #{org.apache.avro.Schema$Type/UNION} (resolve-avro-union x schema)

      #{org.apache.avro.Schema$Type/RECORD}
      (let [builder (GenericRecordBuilder. schema)]
        (doseq [[k v] x]
          (let [in-sc (-> schema (.getField k) .schema)]
            (.set builder k (to-avro v in-sc))))
        (.build builder))

      #{org.apache.avro.Schema$Type/MAP}
      (let [value-sc (.getValueType schema)]
        (prn :>>>>>>>>>>> value-sc)
        (GenericData$Array.
         schema
         (mapv (fn [[k v]]
                 (-> (GenericRecordBuilder. schema)
                     (.set "key" k)
                     (.set "value" (to-avro v value-sc))
                     .build))
               x)))

      #{org.apache.avro.Schema$Type/ARRAY}
      (let [in-sc (.getElementType schema)]
        (GenericData$Array. schema (mapv #(to-avro % in-sc) x)))

      x)))

(defn write-with-avro-schema
  [avro-schema values]
  (let [out-buffer (java.io.ByteArrayOutputStream.)
        outfile (new-mem-output-file out-buffer)
        writer (-> (org.apache.parquet.avro.AvroParquetWriter/builder outfile)
                   (.withSchema avro-schema)
                   .build)]
    (doseq [v values]
      #_(.write writer (map->avro-record v avro-schema))
      (.write writer (to-avro v avro-schema))
      )
    (.close writer)
    out-buffer))

(defn write-with-avro-schema-to-file
  [avro-schema values outpath & {:keys [:conf]}]
  (try
    (io/delete-file outpath)
    (catch Exception _))
  (let [outfile (LocalOutputFile. (-> outpath io/file .toPath))
        writer (-> (org.apache.parquet.avro.AvroParquetWriter/builder outfile)
                   (.withSchema avro-schema)
                   (cond->
                       conf (.withConf (org.apache.parquet.conf.PlainParquetConfiguration. conf)))
                   .build)]
    (doseq [v values]
      #_(.write writer (map->avro-record v avro-schema))
      (.write writer (to-avro v avro-schema))
      )
    (.close writer)))

(defn write-with-parquet-schema-to-file
  [parquet-schema values outpath & {:keys [:conf]}]
  (try
    (io/delete-file outpath)
    (catch Exception _))
  (let [outfile (LocalOutputFile. (-> outpath io/file .toPath))
        avro-schema (parquet-schema<->avro parquet-schema)
        writer (-> (org.apache.parquet.avro.AvroParquetWriter/builder outfile)
                   (.withSchema avro-schema)
                   (cond->
                       conf (.withConf (org.apache.parquet.conf.PlainParquetConfiguration. conf)))
                   .build)]
    (doseq [v values]
      (.write writer (to-avro v avro-schema)))
    (.close writer)))

(defn write-with-avro-schema-to-temp-file
  [avro-schema values]
  (let [outpath (java.nio.file.Files/createTempFile
                 "parquer-"
                 nil
                 (into-array java.nio.file.attribute.FileAttribute []))]
    (-> outpath .toFile .deleteOnExit)
    (-> outpath .toFile .delete)
    (try
      (write-with-avro-schema-to-file avro-schema values (-> outpath .toString))
      (-> outpath .toFile slurp)
      (finally
        (-> outpath .toFile .delete)))))

(defn read-parquet-avro
  [input-file & {:keys [:conf]}]
  (let [reader (->
                (if conf
                  (org.apache.parquet.avro.AvroParquetReader/genericRecordReader input-file conf)
                  (org.apache.parquet.avro.AvroParquetReader/genericRecordReader input-file)))]
    (loop [record (.read reader)
           acc []]
      (if record
        (recur (.read reader)
               (->> record
                    avro->json
                    (conj acc)))
        (do
          (.close reader)
          acc)))))

(defn open-parquet-reader
  [filepath]
  (-> filepath
      io/file
      .toPath
      LocalInputFile.
      ParquetFileReader/open))

(defn inspect-fmd
  [filepath]
  (-> filepath
      open-parquet-reader
      .getFileMetaData))

(def schema2
  (parse-avro-schema
   {"type" "record"
    "name" "root"

    "fields"
    [{"name" "nest"
      "default" nil

      "type"
      ["null"
       {"type" "record"
        "name" "nest"

        "fields"
        [{"name" "thing"
          "default" nil

          "type"
          ["null"
           {"name" "list"
            "type" "array"

            "items"
            {"type" "array"
             "name" "element"
             "items" ["null" "string"]}}]}]}]}]}))

(comment
  (pack {:values (repeat 8 0) :bit-width 2})

  (->> (pack {:values (range 8) :bit-width 3})
       (drop 1)
       (map show-bits))
  ;; => ("10001000" "11000110" "11111010")

  (->> (pack {:values (range 8) :bit-width 2})
       (drop 1)
       (map to-unsigned))

  (->> (pack {:values (range 20) :bit-width 2})
       (drop 1)
       (map to-unsigned))

  (-> pf
      (.readRowGroup 0)
      (.getPageReader col-desc1)
      .readPage)

  (-> (ParquetReader/builder
       (AvroReadSupport.)
       (org.apache.hadoop.fs.Path. "boh.parquet"))
      .build)

  (let [bit-width 2
        initial-capacity 10
        page-size 10
        allocator (DirectByteBufferAllocator.)]
    (-> (RunLengthBitPackingHybridEncoder.
         bit-width
         initial-capacity
         page-size
         allocator)
        (doto
         (.writeInt 0)
          (.writeInt 0)
          (.writeInt 0)
          (.writeInt 0)
          (.writeInt 0)
          (.writeInt 0)
          (.writeInt 0))
        .toBytes
        .toByteArray
        vec))

  (let [klass RunLengthBitPackingHybridEncoder
        enc (RunLengthBitPackingHybridEncoder.
             2                            ;;bit-width
             10                           ;;initial-capacity
             10                           ;;page-size
             (DirectByteBufferAllocator.) ;;allocator
             )
        f (-> klass
              (.getDeclaredField "packer")
              (doto (.setAccessible true)))]
    (-> f
        (.get enc)
        .getClass))
  ;; org.apache.parquet.column.values.bitpacking.ByteBitPackingLE$Packer2

  (let [sc (MessageTypeParser/parseMessageType
            (str "message Document {\n"
                 "  required int64 DocId;\n"
                 "  optional group Links {\n"
                 "    repeated int64 Backward;\n"
                 "    repeated int64 Forward; }\n"
                 "  repeated group Name {\n"
                 "    repeated group Language {\n"
                 "      required binary Code;\n"
                 "      required binary Country; }\n"
                 "    optional binary Url; }}"))]
    sc)

  (let [klass ParquetMetadataConverter
        inst (ParquetMetadataConverter.)
        f (-> klass
              (.getDeclaredMethod "toParquetSchema" (into-array Class [MessageType]))
              (doto (.setAccessible true)))
        sc (MessageTypeParser/parseMessageType
            (str "message Document {\n"
                 "  required int64 DocId;\n"
                 "  optional group Links {\n"
                 "    repeated int64 Backward;\n"
                 "    repeated int64 Forward; }\n"
                 "  repeated group Name {\n"
                 "    repeated group Language {\n"
                 "      required binary Code;\n"
                 "      required binary Country; }\n"
                 "    optional binary Url; }}"))]
    (-> (.invoke f inst (into-array [sc]))
        ;; pp/pprint
        (nth 7)
        ((juxt+ .type
                .repetition_type
                .converted_type
                .logicalType))))

  (write-with-avro-schema
   (open-avro-schema "nested.avsc")
   [{"bar" 1
     "qux" []
     "quux" []
     "location" []}])

  (let [schema1
        (parse-avro-schema
         {"type" "record"
          "name" "root"

          "fields"
          [{"name" "nest"
            "default" nil

            "type"
            ["null"
             {"type" "record"
              "name" "nest"

              "fields"
              [{"name" "thing"
                "default" nil

                "type"
                ["null"
                 {"type" "record"
                  "name" "thing"

                  "fields"
                  [{"name" "array"
                    "type"
                    {"type" "array"

                     "items"
                     {"type" "array"
                      "items" "string"}}}]}]}]}]}]})]
    (to-avro {"nest" {"thing" [["hi" "world"]]}} schema1)
    (write-with-avro-schema
     schema2
     [{"nest" {"thing" [["hi" "world"]]}}]))

  (-> (str
       "
        message root {
          optional group nest {
            optional group thing (LIST) {
              repeated group list {
                optional binary element (STRING);
              }
            }
          }
        }
       ")
      ;; parse-parquet-schema-to-avro
      parse-parquet-schema)

  (-> (str
       "
        message root {
          optional group nest {
            optional group thing (LIST) {
              repeated group list {
                optional binary element (STRING);
              }
            }
          }
        }
       ")
      parse-parquet-schema)

  (-> (org.apache.log4j.Logger/getRootLogger)
      (.setLevel (org.apache.log4j.Level/toLevel "DEBUG")))

  (-> (java.util.logging.Logger/getLogger (.getName BaseCommand))
      (doto
       (.addHandler (doto (java.util.logging.ConsoleHandler.)
                      (.setLevel java.util.logging.Level/ALL)))
        (.setLevel java.util.logging.Level/ALL)))

  (-> schema2
      (write-with-avro-schema
       [{"nest" {"thing" [["hi" "world"]]}}])
      ;; (->> (spit "bbb.parquet"))
      )

  (-> schema2
      (write-with-avro-schema-to-temp-file
       [{"nest" {"thing" [["hi" "world"]]}}])
      (#(spit "bbb.parquet" % :encoding "UTF-8"))
      ;; (->> (spit "bbb.parquet"))
      )

  (write-with-avro-schema-to-file
   schema2
   [{"nest" {"thing" [["hi" "world"]]}}]
   "bbb.parquet")

  (write-with-avro-schema-to-file
   schema2
   [{"nest" {"thing" [["hi" "world" nil]]}}]
   "bbb.parquet"
   :conf {"parquet.avro.write-old-list-structure" "false"})

  (->
   (parse-parquet-schema-to-avro
    (str
     "
        message root {
          optional group nest {
            optional group thing (LIST) {
              repeated group list {
                repeated binary element (STRING);
              }
            }
          }
        }
       "))
   .toString
   json/read-str)

  (write-with-parquet-schema-to-file
   (parse-parquet-schema
    (str
     "
        message root {
          optional group nest {
            optional group thing (LIST) {
              repeated group list {
                optional binary element (STRING);
              }
            }
          }
        }
       "))
   [{"nest" {"thing" [nil]}}]
   "bbb.parquet"
   :conf {"parquet.avro.write-old-list-structure" "false"})

  (write-with-parquet-schema-to-file
   (parse-parquet-schema
    (str
     "
        message root {
          optional group array_of_arrays (LIST) {
            repeated group list {
              required group element (LIST) {
                repeated group list {
                  optional binary element (STRING);
                }
              }
            }
          }
        }
       "))
   [{"array_of_arrays" [{"element" [nil {"element" "hi"} nil {"element" "world"}]}]}]
   "bbb.parquet"
   :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-parquet-schema-to-file
   (parse-parquet-schema
    (str
     "
        message root {
          optional group array_of_arrays (LIST) {
            repeated group list {
              required group element (LIST) {
                repeated group list {
                  optional binary element (STRING);
                }
              }
            }
          }
        }
       "))
   [{"array_of_arrays" [{"element" [nil {"element" "hi"}]}]}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (org.apache.avro.Schema/parse
    (json/write-str
     {"type" "record"
      "name" "root"

      "fields"
      [{"name" "array_of_arrays"

        "type"
        {"type" "array"
         "items"
         {"type" "array"
          "items" "string"
          }}
        }]
      }))
   [{"array_of_arrays" [["hi" "world"]]}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (read-parquet-as-json "aaa.parquet")

  (read-parquet-as-json "bbb.parquet")
  (read-parquet-as-json "bbb.parquet" :conf {})

  (mapv read-parquet-as-json ["aaa.parquet" "bbb.parquet"])

  (inspect-fmd "bbb.parquet")

  (write-with-avro-schema-to-file
   (org.apache.avro.Schema/parse
    (json/write-str
     {"type" "record"
      "name" "root"

      "fields"
      [{"name" "x"

        "type" {"type" "array"
                "items" "string"}}]}))
   [{"x" []} {"x" ["hey" "world"]}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
        message root {
          repeated group f0 {
            optional binary f1;
          }
        }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" []} {"f0" [{"f1" "hi"} {"f1" nil} {}]}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
        message root {
          repeated group f0 {
            optional binary f1;
          }
        }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" []} {"f0" [{"f1" "hi"} {"f1" nil} {}]} {"f0" nil}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             optional group nest {
               optional group thing (LIST) {
                 repeated group list (LIST) {
                   repeated binary element (STRING);
                 }
               }
             }
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"nest" {"thing" []}}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   ;; flatten_3_test_
   (-> (parse-parquet-schema
        (str
         "
           message root {
             optional group nest {
               optional group thing (LIST) {
                 repeated group list (LIST) {
                   required binary element (STRING);
                 }
               }
             }
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"nest" {"thing" nil}}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             optional boolean f0;
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" true} {"f0" true} {"f0" false} {"f0" false} {"f0" false} {"f0" false} {"f0" false} {"f0" false} {"f0" true}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             required group f0 (LIST) {
               repeated group array {
                 optional boolean f1;
               }
             }
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" [{"f1" false} {"f1" true}]} {"f0" []} {"f0" nil} {"f0" [{"f1" true} {}]}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             optional fixed_len_byte_array(3) f0;
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" "hey"}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             repeated int32 f0;
             optional binary f1 (STRING);
           }
       "))
       (inspect {:label :parquet>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>})
       parquet-schema<->avro
       (inspect {:label :avro>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>}))
   [{"f0" [1] "f1" "oi"}]
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (write-with-avro-schema-to-file
   (-> (parse-parquet-schema
        (str
         "
           message root {
             required group f1 (MAP) {
               repeated group key_value (MAP_KEY_VALUE) {
                 required binary key (STRING);
                 required int64 value;
               }
             }
           }
       "))
       parquet-schema<->avro)
   []
   "bbb.parquet"
   ;; :conf {"parquet.avro.write-old-list-structure" "false"}
   )

  (with-open [in (io/input-stream "bbb.parquet")
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (prn :>>>>>>>>>>>>>>> out)
    (let [in-ba (.toByteArray out)]
      (prn :>>>>>>>>>>>>>>> in-ba)
      (prn :>>>>>>>>>>>>>>> (vec in-ba))
      (prn :size>>>>>>>>>>>>>>> (alength in-ba))
      (read-parquet-avro (new-mem-input-file in-ba))
      ))

  (-> (slurp "./test/sample_avro_schemas/timestamps1.avsc")
      parse-avro-schema
      parquet-schema<->avro
      )

  :ok)
