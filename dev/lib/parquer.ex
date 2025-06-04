##--------------------------------------------------------------------
## Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##--------------------------------------------------------------------
defmodule Parquer do
  require Parquer.Records
  require Record

  alias Parquer.Records, as: Rec
  alias Parquer.RecordsHelper, as: RecH

  require RecH

  @magic_not_encrypted "PAR1"
  @magic_encrypted "PARE"
  @magic_byte_size 4

  @parquer_parquet_type_boolean 0
  @parquer_parquet_type_byte_array 6

  @parquer_parquet_encoding_plain 0

  @parquer_parquet_pagetype_data_page 0
  @parquer_parquet_pagetype_dictionary_page 2

  @parquer_parquet_compressioncodec_uncompressed 0
  @parquer_parquet_compressioncodec_snappy 1
  @parquer_parquet_compressioncodec_gzip 2
  @parquer_parquet_compressioncodec_lzo 3
  @parquer_parquet_compressioncodec_brotli 4
  @parquer_parquet_compressioncodec_lz4 5
  @parquer_parquet_compressioncodec_zstd 6
  @parquer_parquet_compressioncodec_lz4_raw 7

  Record.defrecord(
    :t_compact,
    Record.extract(
      :t_compact,
      from_lib: "thrift/src/thrift_compact_protocol.erl"
    )
  )

  Record.defrecord(
    :t_membuffer,
    Record.extract(
      :t_membuffer,
      from_lib: "thrift/src/thrift_membuffer_transport.erl"
    )
  )

  Record.defrecord(
    :t_transport,
    Record.extract(
      :t_transport,
      from_lib: "thrift/src/thrift_transport.erl"
    )
  )

  Record.defrecord(
    :protocol,
    Record.extract(
      :protocol,
      from_lib: "thrift/src/thrift_protocol.erl",
      includes: [
        "deps/thrift/include"
      ]
    )
  )

  defmacro unquote(:>>>)(left, right) do
    quote do
      case unquote(left) do
        {st, :ok} ->
          st |> unquote(right)

        {st, err} ->
          {:error, err, st}
      end
    end
  end

  def test0() do
    file = "/tmp/bah1"
    {fd, writer} = file_writer(file)

    try do
      write_magic(writer) >>>
        dummy_write_footer()
    after
      :file.close(fd)
    end
  end

  def dummy_write_footer(writer) do
    fmd =
      Rec.fileMetaData(
        version: 1,
        schema: dummy_schema(),
        num_rows: 0,
        row_groups: dummy_row_groups(),
        key_value_metadata: :undefined,
        created_by: :undefined,
        column_orders: :undefined,
        encryption_algorithm: :undefined,
        footer_signing_key_metadata: :undefined
      )

    serialized = serialize({:parquer_parquet_types.struct_info(:fileMetaData), fmd})
    size = :erlang.iolist_size(serialized)
    size_bin = <<size::signed-integer-little-size(32)>>

    writer
    |> :thrift_protocol.write(serialized) >>>
      :thrift_protocol.write(size_bin) >>>
      write_magic()
  end

  def encode_value(val, :binary) when is_binary(val) do
    size = byte_size(val)
    size_enc = int_le_4bytes(size)
    {<<size_enc::binary, val::binary>>, size + 4}
  end

  def int_le_4bytes(val) do
    <<val::size(4 * 8)-little>>
  end

  # must be a tree with a single root
  def dummy_schema() do
    [
      Rec.schemaElement(
        type: :undefined,
        type_length: :undefined,
        repetition_type: :undefined,
        name: "root",
        num_children: 1,
        converted_type: :undefined,
        scale: :undefined,
        precision: :undefined,
        field_id: :undefined,
        logicalType: :undefined
      ),
      Rec.schemaElement(
        type: @parquer_parquet_type_byte_array,
        type_length: :undefined,
        repetition_type: :undefined,
        name: "col_str",
        num_children: :undefined,
        converted_type: :undefined,
        scale: :undefined,
        precision: :undefined,
        field_id: :undefined,
        logicalType: :undefined
      )
    ]
  end

  def dummy_data_page_header(opts) do
    Rec.pageHeader(
      type: :undefined,
      uncompressed_page_size: :undefined,
      compressed_page_size: :undefined,
      crc: :undefined,
      data_page_header:
        Rec.dataPageHeader(
          num_values: :undefined,
          encoding: @parquer_parquet_encoding_plain,
          definition_level_encoding: :undefined,
          repetition_level_encoding: :undefined,
          statistics: :undefined
        ),
      index_page_header: :undefined,
      dictionary_page_header: :undefined,
      data_page_header_v2: :undefined
    )
  end

  def dummy_row_groups() do
    [
      Rec.rowGroup(
        columns: dummy_column_chunks(),
        total_byte_size: :undefined,
        num_rows: 1,
        sorting_columns: :undefined,
        file_offset: :undefined,
        total_compressed_size: :undefined,
        ordinal: :undefined
      )
    ]
  end

  def dummy_column_chunks() do
    [
      Rec.columnChunk(
        file_path: :undefined,
        file_offset: 0,
        meta_data: :undefined,
        offset_index_offset: :undefined,
        offset_index_length: :undefined,
        column_index_offset: :undefined,
        column_index_length: :undefined,
        crypto_metadata: :undefined,
        encrypted_column_metadata: :undefined
      )
    ]
  end

  def test() do
    parquer_parquet_encoding_plain = 0
    parquer_parquet_encoding_rle = 3
    parquer_parquet_pagetype_data_page = 0
    parquer_parquet_type_byte_array = 6
    parquer_parquet_compressioncodec_uncompressed = 0
    parquer_parquet_fieldrepetitiontype_required = 0
    parquer_parquet_fieldrepetitiontype_optional = 1
    value = "hello!"
    {enc_value, enc_size} = Parquer.encode_value(value, :binary)
    definition_level_bin =
      :parquer_rle_bp_hybrid_encoder.new(2)
      |> :parquer_rle_bp_hybrid_encoder.write_int(1)
      |> :parquer_rle_bp_hybrid_encoder.to_bytes()
      |> then(& [int_le_4bytes(:erlang.iolist_size(&1)) | &1])
    enc_size = enc_size + :erlang.iolist_size(definition_level_bin)

    page_header =
      Rec.pageHeader(
        type: parquer_parquet_pagetype_data_page,
        uncompressed_page_size: enc_size,
        compressed_page_size: enc_size,
        crc: :undefined,
        data_page_header:
          Rec.dataPageHeader(
            num_values: 1,
            encoding: parquer_parquet_encoding_plain,
            # definition_level_encoding: parquer_parquet_encoding_plain,
            definition_level_encoding: parquer_parquet_encoding_rle,
            repetition_level_encoding: parquer_parquet_encoding_plain,
            statistics: :undefined
          ),
        # data_page_header: :undefined,
        index_page_header: :undefined,
        dictionary_page_header: :undefined,
        data_page_header_v2: :undefined,
        # data_page_header_v2: Rec.dataPageHeaderV2(
        #   num_values: 1,
        #   num_nulls: 0,
        #   num_rows: 1,
        #   encoding: parquer_parquet_encoding_plain,
        #   definition_levels_byte_length: 0,
        #   repetition_levels_byte_length: 0,
        #   is_compressed: false,
        #   statistics: :undefined
        # )
      )

    page_header_bin = serialize(page_header |> get_struct_info())

    page_header_size = :erlang.iolist_size(page_header_bin)

    file_metadata =
      Rec.fileMetaData(
        version: 2,
        schema: [
          Rec.schemaElement(
            type: :undefined,
            type_length: :undefined,
            repetition_type: :undefined,
            name: "root",
            num_children: 1,
            converted_type: :undefined,
            scale: :undefined,
            precision: :undefined,
            field_id: :undefined,
            logicalType: :undefined
          ),
          Rec.schemaElement(
            type: parquer_parquet_type_byte_array,
            type_length: :undefined,
            # repetition_type: parquer_parquet_fieldrepetitiontype_required,
            repetition_type: parquer_parquet_fieldrepetitiontype_optional,
            name: "col_str",
            num_children: :undefined,
            converted_type: :undefined,
            scale: :undefined,
            precision: :undefined,
            field_id: 1,
            logicalType: Rec.logicalType(sTRING: Rec.stringType())
          )
        ],
        num_rows: 1,
        row_groups: [
          Rec.rowGroup(
            columns: [
              Rec.columnChunk(
                file_path: :undefined,
                file_offset: 0,
                meta_data: Rec.columnMetaData(
                  type: parquer_parquet_type_byte_array,
                  encodings: [parquer_parquet_encoding_plain],
                  path_in_schema: ["col_str"],
                  codec: parquer_parquet_compressioncodec_uncompressed,
                  num_values: 1,
                  total_uncompressed_size: enc_size + page_header_size,
                  total_compressed_size: enc_size + page_header_size,
                  key_value_metadata: :undefined,
                  data_page_offset: byte_size(@magic_not_encrypted),
                  index_page_offset: :undefined,
                  dictionary_page_offset: :undefined,
                  statistics: :undefined,
                  encoding_stats: :undefined,
                  bloom_filter_offset: :undefined,
                  bloom_filter_length: :undefined,
                  size_statistics: :undefined,
                  geospatial_statistics: :undefined
                ),
                offset_index_offset: :undefined,
                offset_index_length: :undefined,
                column_index_offset: :undefined,
                column_index_length: :undefined,
                crypto_metadata: :undefined,
                encrypted_column_metadata: :undefined
              )
            ],
            total_byte_size: page_header_size,
            num_rows: 1,
            sorting_columns: :undefined,
            file_offset: 4,
            total_compressed_size: page_header_size,
            ordinal: :undefined
          )
        ],
        key_value_metadata: :undefined,
        created_by: "parquer 0.0.0",
        column_orders: :undefined,
        encryption_algorithm: :undefined,
        footer_signing_key_metadata: :undefined
      )

    writer = mem_writer()

    file_metadata_bin =
      serialize({:parquer_parquet_types.struct_info(:fileMetaData), file_metadata})

    file_metadata_size = :erlang.iolist_size(file_metadata_bin)
    file_metadata_size_enc = <<file_metadata_size::size(4 * 8)-little-unsigned>>

    writer
    |> write_magic() >>>
      :thrift_protocol.write(page_header |> get_struct_info()) >>>
      :thrift_protocol.write(definition_level_bin) >>>
      :thrift_protocol.write(enc_value) >>>
      :thrift_protocol.write(file_metadata |> get_struct_info()) >>>
      :thrift_protocol.write(file_metadata_size_enc) >>>
      write_magic()
  end
  # recompile() ; Parquer.bah() |> elem(0) |> Parquer.get_buffer() |> then(& File.write!("./test.parquet", &1))

  def get_struct_info(rec) do
    type = elem(rec, 0)
    {:parquer_parquet_types.struct_info(type), rec}
  end

  def write_magic(writer) do
    :thrift_protocol.write(writer, @magic_not_encrypted)
  end

  def file_writer(file) do
    File.rm(file)
    {:ok, fd} = :file.open(file, [:read, :write, :binary])
    {:ok, transport} = :thrift_file_transport.new(fd)
    {:ok, writer} = :thrift_compact_protocol.new(transport)
    {fd, writer}
  end

  def mem_writer() do
    {:ok, transport} = :thrift_membuffer_transport.new()
    {:ok, writer} = :thrift_compact_protocol.new(transport)
    writer
  end

  def serialize(thing) do
    writer = mem_writer()
    {writer, :ok} = :thrift_protocol.write(writer, thing)

    get_buffer(writer)
  end

  def get_buffer(proto) do
    proto
    |> protocol(:data)
    |> t_compact(:transport)
    |> t_transport(:state)
    |> t_membuffer(:buffer)
  end

  def put_buffer(proto, buffer) do
    compact = protocol(proto, :data)
    transport = t_compact(compact, :transport)
    membuffer = t_transport(transport, :state)
    membuffer = t_membuffer(membuffer, buffer: buffer)
    transport = t_transport(transport, state: membuffer)
    compact = t_compact(compact, transport: transport)
    protocol(proto, data: compact)
  end

  def skip_buffer_bytes(proto, num_bytes) do
    buffer = get_buffer(proto)
    to_drop = min(byte_size(buffer) * 8, 8 * num_bytes)
    <<_::size(to_drop), buffer::binary>> = buffer
    put_buffer(proto, buffer)
  end

  def deserialize(proto, type) when is_tuple(proto) do
    what =
      if type in :parquer_parquet_types.struct_names() do
        :parquer_parquet_types.struct_info(type)
      else
        type
      end

    case :thrift_protocol.read(proto, what) do
      {proto, {:ok, res}} when is_tuple(res) ->
        res =
          res
          |> Tuple.to_list()
          |> then(&[type | &1])
          |> List.to_tuple()
          |> RecH.to_map()

        {:ok, res, proto}

      {proto, {:ok, res}} ->
        {:ok, res, proto}

      {_proto, res} ->
        res
    end
  end

  def deserialize(bin, type) when is_binary(bin) do
    {:ok, transport} = :thrift_membuffer_transport.new(bin)
    {:ok, proto} = :thrift_compact_protocol.new(transport)
    deserialize(proto, type)
  end

  def read(path) do
    {:ok, fd} = :file.open(path, [:read, :binary])
    # assert
    {:ok, @magic_not_encrypted} =
      :file.pread(fd, {:eof, -4}, 4)
      |> IO.inspect()

    {:ok, [<<meta_size::size(4 * 8)-little-unsigned>>, @magic_not_encrypted]} =
      :file.pread(fd, [{{:eof, -8}, 4}, {{:eof, -4}, 4}])
      |> IO.inspect()

    IO.inspect(meta_size)

    {:ok, fmd_bytes} =
      :file.pread(fd, {:eof, -(meta_size + 8)}, meta_size)
      |> IO.inspect()

    deserialize(fmd_bytes, :fileMetaData)
    |> IO.inspect()

    :ok
  end

  def inspect_fmd(filepath, opts \\ []) do
    shift_size = Keyword.get(opts, :shift_size, 0)
    fd = File.open!(filepath, [:read, :binary])
    try do
      # assert
      {:ok, [<<meta_size::size(4 * 8)-little-unsigned>>, @magic_not_encrypted]} =
        :file.pread(fd, [{{:eof, -8}, 4}, {{:eof, -4}, 4}])
      IO.inspect(meta_size)

      {:ok, fmd_bytes} =
        :file.pread(fd, {:eof, -(meta_size + 8) + shift_size}, meta_size)

      IO.inspect(fmd_bytes, limit: 50)
      IO.inspect(:binary.encode_hex(fmd_bytes), limit: 50)

      {:ok, transport} = :thrift_membuffer_transport.new(fmd_bytes)
      {:ok, proto} = :thrift_compact_protocol.new(transport)

      {_proto, {:ok, res}} =
        :thrift_protocol.read(proto, :parquer_parquet_types.struct_info(:fileMetaData))

      res
      |> Tuple.to_list()
      |> then(&[:fileMetaData | &1])
      |> List.to_tuple()
      |> RecH.to_map()
    after
      File.close(fd)
    end
  end

  def inspect_col(filepath, opts \\ []) do
    nth_rg = Keyword.get(opts, :nth_rg, 0)
    nth_col = Keyword.get(opts, :nth_col, 0)
    fmd = inspect_fmd(filepath)
    col = get_in(fmd, [:row_groups, Access.at!(nth_rg), :columns, Access.at!(nth_col)])
    %{
      file_offset: file_offset,
      meta_data: %{
        codec: codec_num,
        total_compressed_size: col_size,
      }
    } = col
    fd = File.open!(filepath, [:read, :binary])
    try do
      IO.inspect(%{file_offset: file_offset, col_size: col_size})
      {:ok, raw_rg} = :file.pread(fd, {:bof, max(file_offset, 4)}, col_size)
      {:ok, header, proto1} = deserialize(raw_rg, :pageHeader)
      |> IO.inspect(label: :first_header)
      case header do
        %{
          type: @parquer_parquet_pagetype_dictionary_page,
          compressed_page_size: dict_size
        } ->
          {dict_header_bytes_comp, rest} =
            proto1
            |> get_buffer()
            |> :erlang.split_binary(dict_size)
          dict_header_bytes = decompress(dict_header_bytes_comp, codec_num)
          |> IO.inspect(label: :dict_header_bytes)
          {:ok, data_header, proto2} =
            proto1
            |> skip_buffer_bytes(dict_size)
            |> deserialize(:pageHeader)
            |> IO.inspect(label: :data_header)
          num_values = case data_header do
            %{data_page_header: %{num_values: n}} -> n
            %{data_page_header_v2: %{num_values: n}} -> n
          end
          data_raw_comp = get_buffer(proto2)
          data_raw = decompress(data_raw_comp, codec_num)
          rep_width = Keyword.get(opts, :rep_width, 1)
          def_width = Keyword.get(opts, :def_width, 2)
          ## fixme: hard-coding that length is prepended due to assuming v1 page header
          IO.inspect(%{data_raw: data_raw})
          <<size::32-little, data::size(size)-binary, rest::binary>> = data_raw
          repetitions = decode_rle(data, rep_width, num_values)
          IO.inspect(%{raw: data_raw, size: size, data: data, rest: rest, xs: repetitions})
          <<size::32-little, data::size(size)-binary, rest::binary>> = rest
          definitions = decode_rle(data, def_width, num_values)
          IO.inspect(%{size: size, data: data, rest: rest, xs: definitions})
          <<data_bit_width::8-little, rest::binary>> = rest
          dict_indices = decode_rle(rest, data_bit_width, num_values)
          IO.inspect(%{size: data_bit_width, rest: rest, xs: dict_indices})
          %{
            dict: %{
              header: header,
              contents: decode_plain(dict_header_bytes),
            },
            data: %{
              header: data_header,
              contents: {repetitions, definitions, dict_indices}
            }
          }

        %{
          type: @parquer_parquet_pagetype_data_page,
          compressed_page_size: size
        } ->
          num_values = case header do
            %{data_page_header: %{num_values: n}} -> n
            %{data_page_header_v2: %{num_values: n}} -> n
          end
          data_raw_comp = get_buffer(proto1)
          data_raw = decompress(data_raw_comp, codec_num)
          rep_width = Keyword.get(opts, :rep_width, 1)
          def_width = Keyword.get(opts, :def_width, 2)
          ## fixme: hard-coding that length is prepended due to assuming v1 page header
          IO.inspect(%{data_raw: data_raw})
          <<size::32-little, data::size(size)-binary, rest::binary>> = data_raw
          repetitions = decode_rle(data, rep_width, num_values)
          IO.inspect(%{raw: data_raw, size: size, data: data, rest: rest, xs: repetitions}, label: :reps)
          <<size::32-little, data::size(size)-binary, rest::binary>> = rest
          definitions = decode_rle(data, def_width, num_values)
          IO.inspect(%{size: size, data: data, rest: rest, xs: definitions}, label: :defs)
          data = decode_plain(rest)
          IO.inspect(%{xs: data}, label: :data)
          {repetitions, definitions, data}
      end
    after
      File.close(fd)
    end
  end

  def decode_plain(raw) do
    decode_plain(raw, _acc = [])
  end

  def decode_plain(<<>>, acc) do
    Enum.reverse(acc)
  end
  def decode_plain(<<size::32-little, data::size(size)-binary, rest::binary>>, acc) do
    decode_plain(rest, [data | acc])
  end

  def decode_rle(data, bit_width, n) do
    deco = :parquer_rle_bp_hybrid_decoder.new(bit_width, data)
    {vals, _} = Enum.map_reduce(1..n, deco, fn _, s -> :parquer_rle_bp_hybrid_decoder.read(s) end)
    vals
  end

  def decompress(raw, codec_num) do
    case codec_num do
      @parquer_parquet_compressioncodec_uncompressed ->
        raw

      @parquer_parquet_compressioncodec_snappy ->
        {:ok, raw} = :snappyer.decompress(raw)
        raw

      @parquer_parquet_compressioncodec_zstd ->
        raw = :ezstd.decompress(raw)
        {true, raw} = {is_binary(raw), raw}
        raw
    end
  end

  # TestParquetParser.testPaperExample
  def schema0() do
    :parquer_schema.root(
      "Document",
      [
        :parquer_schema.int64(
          "DocId",
          :required
        ),
        :parquer_schema.group(
          "Links",
          :optional,
          [
            :parquer_schema.int64("Backward", :repeated),
            :parquer_schema.int64("Forward", :repeated),
          ]
        ),
        :parquer_schema.group(
          "Name",
          :repeated,
          [
            :parquer_schema.group(
              "Language",
              :repeated,
              [
                :parquer_schema.binary("Code", :required),
                :parquer_schema.binary("Country", :required),
              ]
            ),
            :parquer_schema.binary("Url", :optional)
          ]
        )
      ]
    )
  end

  # fastparquet/test-data/nested.parq
  def schema1() do
    :parquer_schema.root(
      "root",
      [
        :parquer_schema.group(
          "nest",
          :optional,
          [
            :parquer_schema.group(
              "thing",
              :optional,
              %{converted_type: :list},
              [
                :parquer_schema.group(
                  "list",
                  :repeated,
                  [
                    :parquer_schema.string("element", :optional)
                  ]
                )
              ]
            )
          ]
        ),
      ]
    )
  end

  def schema2() do
    :parquer_schema.root(
      "root",
      [
        :parquer_schema.group(
          "nest",
          :optional,
          [
            :parquer_schema.group(
              "thing",
              :optional,
              [
                :parquer_schema.group(
                  "maybe",
                  :optional,
                  [
                    :parquer_schema.string("element", :required)
                  ]
                )
              ]
            )
          ]
        ),
      ]
    )
  end

  def schema3() do
    :parquer_schema.root(
      "root",
      [
        :parquer_schema.group(
          "nest",
          :optional,
          [
            :parquer_schema.group(
              "thing",
              :optional,
              [
                :parquer_schema.group(
                  "list",
                  :repeated,
                  [
                    :parquer_schema.string("element", :repeated)
                  ]
                )
              ]
            )
          ]
        ),
      ]
    )
  end

  def test1() do
    Parquer.schema1()
    |> :parquer_writer.new(%{})
    |> :parquer_writer.append_records(
         List.duplicate(%{"nest" => %{"thing" => [["hi", "world"]]}}, 10)
       )
    |> elem(1)
    |> :parquer_writer.close()
    |> then(&File.write!("./aaa.parquet", &1))
  end

  def test2() do
    Parquer.schema3()
    |> :parquer_writer.new(%{})
    |> tap(& &1 |> :parquer_writer.inspect() |> IO.inspect())
    |> :parquer_writer.append_records(
         List.duplicate(%{"nest" => %{"thing" => [["hi", "world"]]}}, 10)
       )
    |> elem(1)
    |> :parquer_writer.close()
    |> then(&File.write!("./aaa.parquet", &1))
  end

  ## mimicking parquet.avro.write-old-list-structure=false
  def schema4() do
    :parquer_schema.root(
      "root",
      [
        :parquer_schema.group(
          "array_of_arrays",
          :optional,
          %{converted_type: :list},
          [
            :parquer_schema.group(
              "list",
              :repeated,
              [
                :parquer_schema.group(
                  "element",
                  :required,
                  [
                    :parquer_schema.group(
                      "element",
                      :required,
                      %{converted_type: :list},
                      [
                        :parquer_schema.group(
                          "list",
                          :repeated,
                          [
                            :parquer_schema.group(
                              "element",
                              :required,
                              [
                                :parquer_schema.string("element", :optional)
                              ]
                            )
                          ]
                        )
                      ]
                    )
                  ]
                )
              ]
            )
          ]
        ),
      ]
    )
  end

  ## mimicking parquet.avro.write-old-list-structure=true
  def schema5() do
    :parquer_schema.root(
      "root",
      [
        :parquer_schema.group(
          "array_of_arrays",
          :optional,
          %{converted_type: :list},
          [
            :parquer_schema.group(
              "array",
              :repeated,
              [
                :parquer_schema.group(
                  "element",
                  :required,
                  %{converted_type: :list},
                  [
                    :parquer_schema.group(
                      "array",
                      :repeated,
                      [
                        :parquer_schema.string("element", :optional)
                      ]
                    )
                  ]
                )
              ]
            )
          ]
        ),
      ]
    )
  end

  def test3() do
    Parquer.schema4()
    |> :parquer_writer.new(%{})
    |> tap(& &1 |> :parquer_writer.inspect() |> IO.inspect())
    |> :parquer_writer.append_records(
      [
        %{"array_of_arrays" =>
          [%{"element" => [:undefined, %{"element" => "hi"}]}]}
      ]
    )
    |> elem(1)
    |> :parquer_writer.close()
    |> then(&File.write!("./aaa.parquet", &1))
  end

  def test4() do
    Parquer.schema5()
    |> :parquer_writer.new(%{})
    |> tap(& &1 |> :parquer_writer.inspect() |> IO.inspect())
    |> :parquer_writer.append_records(
      [
        %{"array_of_arrays" =>
          [%{"element" => [:undefined, %{"element" => "hi"}]}]}
      ]
    )
    |> elem(1)
    |> :parquer_writer.close()
    |> then(&File.write!("./aaa.parquet", &1))
  end

  def smoke_test_optional() do
    :parquer_schema.root("root", [:parquer_schema.string("f0", :optional)])
    |> :parquer_writer.new(%{})
    |> tap(& &1 |> :parquer_writer.inspect() |> IO.inspect())
    |> :parquer_writer.append_records(
      [
        %{"f0" => "hello"}
      ]
    )
    |> elem(2)
    |> :parquer_writer.close()
    |> elem(0)
    |> then(&File.write!("./aaa.parquet", &1))
  end
end
