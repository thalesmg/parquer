%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(parquer_thrift_utils).

%% API
-export([
  serialize/1,

  file_metadata/1,
  column_chunk/1,
  column_metadata/1,
  row_group/1,
  data_page_header_v1/1,
  data_page_header_v2/1,
  dict_page_header/1,
  page_header/1,
  schema_element/1
]).

-include("parquer.hrl").
-include("parquer_parquet_types.hrl").
-include("parquer_thrift.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% From thrift, not exposed...
-record(protocol, {module, data}).
-record(t_compact, {transport,
  % state for pending boolean fields
  read_stack,
  read_value,
  write_stack,
  write_id
}).
-record(t_transport, {
  module,
  state
}).
-record(t_membuffer, {
  buffer
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

serialize(Struct) ->
  Type = element(1, Struct),
  Info = parquer_parquet_types:struct_info(Type),
  MemWriter0 = mem_writer(),
  {MemWriter, ok} = thrift_protocol:write(MemWriter0, {Info, Struct}),
  get_buffer(MemWriter).

file_metadata(Params) ->
  #'fileMetaData'{
    version = maps:get(?version, Params),
    schema = maps:get(?schema, Params),
    num_rows = maps:get(?num_rows, Params),
    row_groups = maps:get(?row_groups, Params)
  }.

column_chunk(Params) ->
  #'columnChunk'{
    file_offset = maps:get(?offset, Params, 0),
    meta_data = maps:get(?metadata, Params)
  }.

column_metadata(Params) ->
  #'columnMetaData'{
    type = primitive_type_of(maps:get(?type, Params)),
    num_values = maps:get(?num_values, Params),
    encodings = lists:map(fun encoding_of/1, maps:get(?encodings, Params)),
    path_in_schema = maps:get(?path_in_schema, Params),
    codec = codec_of(maps:get(?codec, Params)),
    total_uncompressed_size = maps:get(?total_uncompressed_size, Params),
    total_compressed_size = maps:get(?total_compressed_size, Params),
    data_page_offset = maps:get(?data_page_offset, Params),
    dictionary_page_offset = maps:get(?dict_page_offset, Params, ?undefined)
  }.

row_group(Params) ->
  #'rowGroup'{
    columns = maps:get(?column_chunks, Params),
    total_byte_size = maps:get(?total_uncompressed_size, Params),
    num_rows = maps:get(?num_rows, Params),
    %% file_offset = maps:get(?file_offset, Params),
    total_compressed_size = maps:get(?total_compressed_size, Params)
  }.

data_page_header_v1(Params) ->
  #'dataPageHeader'{
     num_values = maps:get(?num_values, Params),
     encoding = encoding_of(maps:get(?encoding, Params)),
     definition_level_encoding = encoding_of(maps:get(?definition_level_encoding, Params)),
     repetition_level_encoding = encoding_of(maps:get(?repetition_level_encoding, Params))
  }.

data_page_header_v2(Params) ->
  #'dataPageHeaderV2'{
     num_values = maps:get(?num_values, Params),
     num_nulls = maps:get(?num_nulls, Params),
     num_rows = maps:get(?num_rows, Params),
     encoding = encoding_of(maps:get(?encoding, Params)),
     definition_levels_byte_length = maps:get(?definition_levels_byte_length, Params),
     repetition_levels_byte_length = maps:get(?repetition_levels_byte_length, Params)
  }.

dict_page_header(Params) ->
  #'dictionaryPageHeader'{
    num_values = maps:get(?num_values, Params),
    encoding = encoding_of(maps:get(?encoding, Params)),
    is_sorted = maps:get(?is_sorted, Params)
  }.

page_header(Params) ->
  #'pageHeader'{
    type = page_type_of(maps:get(?page_type, Params)),
    uncompressed_page_size = maps:get(?uncompressed_page_size, Params),
    compressed_page_size = maps:get(?compressed_page_size, Params),
    data_page_header = maps:get(?data_page_header_v1, Params, ?undefined),
    data_page_header_v2 = maps:get(?data_page_header_v2, Params, ?undefined),
    dictionary_page_header = maps:get(?dict_page_header, Params, ?undefined)
  }.

schema_element(Params) ->
  #'schemaElement'{
    field_id = maps:get(?id, Params, ?undefined),
    name = maps:get(?name, Params),
    type_length = maps:get(?type_length, Params, ?undefined),
    %% logicalType = logical_type_of(maps:get(?logical_type, Params, ?undefined)),
    converted_type = converted_type_of(maps:get(?converted_type, Params, ?undefined)),
    repetition_type = repetition_type_of(maps:get(?repetition, Params)),
    num_children = maps:get(?num_children, Params, ?undefined),
    type = primitive_type_of(maps:get(?primitive_type, Params, ?undefined))
  }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mem_writer() ->
  {ok, Transport} = thrift_membuffer_transport:new(),
  {ok, Writer} = thrift_compact_protocol:new(Transport),
  Writer.

get_buffer(MemWriter) ->
  #protocol{
    data = #t_compact{
      transport = #t_transport{
        state = #t_membuffer{
          buffer = Buffer
        }
      }
    }
  } = MemWriter,
  Buffer.

encoding_of(?ENCODING_PLAIN) -> ?Parquer_parquet_Encoding_PLAIN;
encoding_of(?ENCODING_PLAIN_DICT) -> ?Parquer_parquet_Encoding_PLAIN_DICTIONARY;
encoding_of(?ENCODING_RLE) -> ?Parquer_parquet_Encoding_RLE;
encoding_of(?ENCODING_RLE_DICT) -> ?Parquer_parquet_Encoding_RLE_DICTIONARY.

primitive_type_of(undefined) -> undefined;
primitive_type_of(?BOOLEAN) -> ?Parquer_parquet_Type_BOOLEAN;
primitive_type_of(?INT32) -> ?Parquer_parquet_Type_INT32;
primitive_type_of(?INT64) -> ?Parquer_parquet_Type_INT64;
primitive_type_of(?INT96) -> ?Parquer_parquet_Type_INT96;
primitive_type_of(?FLOAT) -> ?Parquer_parquet_Type_FLOAT;
primitive_type_of(?DOUBLE) -> ?Parquer_parquet_Type_DOUBLE;
primitive_type_of(?BYTE_ARRAY) -> ?Parquer_parquet_Type_BYTE_ARRAY;
primitive_type_of(?FIXED_LEN_BYTE_ARRAY) -> ?Parquer_parquet_Type_FIXED_LEN_BYTE_ARRAY.

converted_type_of(undefined) -> undefined;
converted_type_of(?CONVERTED_TYPE_UTF8) -> ?Parquer_parquet_ConvertedType_UTF8;
converted_type_of(?CONVERTED_TYPE_LIST) -> ?Parquer_parquet_ConvertedType_LIST;
converted_type_of(?CONVERTED_TYPE_MAP) -> ?Parquer_parquet_ConvertedType_MAP.

repetition_type_of(?REPETITION_OPTIONAL) -> ?Parquer_parquet_FieldRepetitionType_OPTIONAL;
repetition_type_of(?REPETITION_REPEATED) -> ?Parquer_parquet_FieldRepetitionType_REPEATED;
repetition_type_of(?REPETITION_REQUIRED) -> ?Parquer_parquet_FieldRepetitionType_REQUIRED.

page_type_of(?PAGE_TYPE_DATA_PAGE_V1) -> ?Parquer_parquet_PageType_DATA_PAGE;
page_type_of(?PAGE_TYPE_DATA_PAGE_V2) -> ?Parquer_parquet_PageType_DATA_PAGE_V2;
page_type_of(?PAGE_TYPE_DICT_PAGE) -> ?Parquer_parquet_PageType_DICTIONARY_PAGE.

codec_of(?COMPRESSION_NONE) -> ?Parquer_parquet_CompressionCodec_UNCOMPRESSED;
codec_of(?COMPRESSION_ZSTD) -> ?Parquer_parquet_CompressionCodec_ZSTD;
codec_of(?COMPRESSION_SNAPPY) -> ?Parquer_parquet_CompressionCodec_SNAPPY.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
