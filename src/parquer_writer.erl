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
-module(parquer_writer).

%% API
-export([
  new/2,
  append_records/2,
  close/1
]).

%% Debugging only
-export([inspect/1]).

-export_type([t/0, write_metadata/0]).

-include("parquer.hrl").
-include("parquer_thrift.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% 1 mb
-define(DEFAULT_MAX_ROW_GROUP_BYTES, 1024 * 1024).

-define(DEFAULT_COMPRESSION, ?COMPRESSION_ZSTD).

-define(compressed, compressed).
-define(compressed_size, compressed_size).
-define(concrete_dictionary, concrete_dictionary).
-define(def_level_bin, def_level_bin).
-define(enable_dictionary, enable_dictionary).
-define(num_dict_keys, num_dict_keys).
-define(path, path).
-define(rep_level_bin, rep_level_bin).
-define(uncompressed, uncompressed).
-define(uncompressed_size, uncompressed_size).
-define(use_data_page_header_v2, use_data_page_header_v2).

%% State
-record(writer, {
  schema,
  closed_row_groups = [],
  columns,
  has_data = false,
  num_rows = 0,
  offset = 0,
  opts
}).

%% Column state
-record(c, {
  path,
  repetition,
  primitive_type,
  max,
  min,
  max_definition_level,
  max_repetition_level,
  num_values = 0,
  num_nulls = 0,
  num_rows = 0,
  %% N.B. these are in reverse order
  definition_levels = [],
  %% N.B. these are in reverse order
  repetition_levels = [],
  data
}).

%% Boolean values
-record(bools, {
  encoder
}).

%% Other values
-record(data, {
  byte_size = 0,
  enable_dictionary,
  dictionary = #{},
  values = []
}).

-record(write_meta, {
  num_rows,
  total_compressed_size,
  total_uncompressed_size,
  dict_page_header_size,
  dict_uncompressed_size,
  dict_compressed_size,
  data_page_header_size,
  data_uncompressed_size,
  data_compressed_size
}).

-opaque t() :: #writer{}.

-doc """

## Options

  * `enable_dictionary` - whether to use dictionary encoding.  Can be set for all columns
    as a single boolean, or individually by providing a map of column paths with segments
    separated by dots (e.g.: `<<"my.nested.column">>`) to boolean values.  Only applies to
    columns that are not boolean.  Default is `true` (enabled for all columns).

  * `default_compression` - the compression algorithm to be used when it's not specified
    per column.
    Valid values: `none` | `snappy` | `zstd`.
    Defaults to `zstd`.

  * `use_data_page_header_v2` - whether to write data page headers using the V2 format.
    Defaults to `false`.

""".
-type writer_opts() :: #{
  ?default_compression => compression(),
  ?use_data_page_header_v2 => boolean(),
  any() => term()
}.
-type data() :: parquer_zipper:data().
-type compression() :: ?COMPRESSION_NONE | ?COMPRESSION_SNAPPY | ?COMPRESSION_ZSTD.

-type write_metadata() :: #{
  num_rows => non_neg_integer(),
  atom() => term()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec new(map(), writer_opts()) -> t().
new(Schema, #{} = Opts0) ->
  DefaultOpts = #{
    ?default_compression => ?DEFAULT_COMPRESSION,
    ?enable_dictionary => true,
    ?use_data_page_header_v2 => false
  },
  Opts = maps:merge(DefaultOpts, Opts0),
  FlatSchema = parquer_schema:flatten(Schema),
  Columns = initialize_columns(FlatSchema, Opts),
  #writer{schema = FlatSchema, columns = Columns, opts = Opts}.

-spec append_records(t(), [data()]) -> {iodata(), [write_metadata()], t()}.
append_records(#writer{} = Writer0, Records) when is_list(Records) ->
  Writer1 =
    lists:foldl(
      fun(Record, WriterAcc) ->
          Cols = lists:map(
            fun(C) -> append_to_column(C, Record) end,
            WriterAcc#writer.columns),
          WriterAcc#writer{has_data = true, columns = Cols}
      end,
      Writer0,
      Records),
  maybe_emit_row_group(Writer1).

-spec close(t()) -> {iodata(), [write_metadata()]}.
close(#writer{} = Writer) ->
  do_close(Writer).

%%------------------------------------------------------------------------------
%% Debugging only
%%------------------------------------------------------------------------------

inspect(#writer{} = W) ->
  #{
    schema => W#writer.schema,
    closed_row_groups => W#writer.closed_row_groups,
    columns => lists:map(fun inspect/1, W#writer.columns),
    has_data => W#writer.has_data,
    num_rows => W#writer.num_rows,
    offset => W#writer.offset,
    opts => W#writer.opts
  };
inspect(#c{} = C) ->
  #{
    path => C#c.path,
    repetition => C#c.repetition,
    primitive_type => C#c.primitive_type,
    max => C#c.max,
    min => C#c.min,
    max_definition_level => C#c.max_definition_level,
    max_repetition_level => C#c.max_repetition_level,
    num_values => C#c.num_values,
    num_nulls => C#c.num_nulls,
    num_rows => C#c.num_rows,
    definition_levels => lists:reverse(C#c.definition_levels),
    repetition_levels => lists:reverse(C#c.repetition_levels),
    data => C#c.data
  }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

initialize_columns(FlatSchema, Opts) ->
  lists:filtermap(
    fun(ColSc) ->
        case parquer_schema:is_leaf(ColSc) of
          false ->
            false;
          true ->
            {true, initial_column_state(ColSc, Opts)}
        end
    end,
    FlatSchema).

initial_column_state(LeafColumnSchema, Opts) ->
  #{ ?path := [_Root | Path]
   , ?name := Name
   , ?repetition := Repetition
   , ?primitive_type := PrimitiveType
   , ?max_definition_level := MaxDefLevel
   , ?max_repetition_level := MaxRepLevel
   } = LeafColumnSchema,
  KeyRepetitions = Path ++ [{Name, Repetition}],
  #c{
    path = KeyRepetitions,
    repetition = Repetition,
    primitive_type = PrimitiveType,
    max = undefined,
    min = undefined,
    max_repetition_level = MaxRepLevel,
    max_definition_level = MaxDefLevel,
    repetition_levels = [],
    definition_levels = [],
    data = init_data(PrimitiveType, KeyRepetitions, Opts)
  }.

init_data(?BOOLEAN, _KeyRepetitions, _Opts) ->
  BitWidth = 1,
  Encoder = parquer_rle_bp_hybrid_encoder:new(BitWidth),
  #bools{encoder = Encoder};
init_data(_PrimitiveType, KeyRepetitions, Opts) ->
  {KeyPath, _} = lists:unzip(KeyRepetitions),
  IsDictEnabled = is_dictionary_enabled(KeyPath, Opts),
  #data{
    enable_dictionary = IsDictEnabled,
    dictionary = #{},
    values = []
  }.

append_to_column(#c{} = Col0, Record) ->
  #c{path = KeyRepetitions} = Col0,
  Zipper = parquer_zipper:new(KeyRepetitions, Record),
  Col1 = Col0#c{num_rows = Col0#c.num_rows + 1},
  do_append_to_column(Col1, parquer_zipper:next(Zipper)).

do_append_to_column(Col0, Zipper0) ->
  case parquer_zipper:read(Zipper0) of
    false ->
      Col0;
    {RepLevel, DefLevel, ?undefined} ->
      Col1 = Col0#c{
        num_values = Col0#c.num_values + 1,
        num_nulls = Col0#c.num_nulls + 1
      },
      Col2 = append_definition_level(Col1, DefLevel),
      Col3 = append_repetition_level(Col2, RepLevel),
      do_append_to_column(Col3, parquer_zipper:next(Zipper0));
    {RepLevel, DefLevel, Val} ->
      Col1 = Col0#c{
        num_values = Col0#c.num_values + 1
      },
      Col2 = append_definition_level(Col1, DefLevel),
      Col3 = append_repetition_level(Col2, RepLevel),
      Col4 = append_data(Col3, Val),
      do_append_to_column(Col4, parquer_zipper:next(Zipper0))
  end.

append_data(#c{primitive_type = ?BOOLEAN} = Col0, Bool) ->
  #bools{encoder = Encoder0} = Data0 = Col0#c.data,
  Encoder1 =
    case Bool of
      true -> parquer_rle_bp_hybrid_encoder:write_int(Encoder0, 1);
      false -> parquer_rle_bp_hybrid_encoder:write_int(Encoder0, 0)
    end,
  Col0#c{data = Data0#bools{encoder = Encoder1}};
append_data(#c{} = Col0, Datum) ->
  #data{values = Values0} = Data0 = Col0#c.data,
  %% TODO: handle dictionary key size overflow.
  Data1 = ensure_key(Data0, Datum),
  Values1 = [Datum | Values0],
  Data2 = Data1#data{values = Values1},
  Col0#c{data = Data2}.

%% WRONG: should not write def level if schema is flat _and_ required.
append_definition_level(#c{max_definition_level = 0} = Col0, _DefLevel) ->
  Col0;
append_definition_level(#c{definition_levels = DefinitionLevels0} = Col0, DefLevel) ->
  Col0#c{definition_levels = [DefLevel | DefinitionLevels0]}.

append_repetition_level(#c{max_repetition_level = 0} = Col0, _RepLevel) ->
  Col0;
append_repetition_level(#c{repetition_levels = RepetitionLevels0} = Col0, RepLevel) ->
  Col0#c{repetition_levels = [RepLevel | RepetitionLevels0]}.

ensure_key(#data{dictionary = Dictionary} = D, Datum) when is_map_key(Datum, Dictionary) ->
  D;
ensure_key(#data{dictionary = Dictionary0, byte_size = ByteSize0} = D0, Datum) ->
  Dictionary = Dictionary0#{Datum => true},
  ByteSize = ByteSize0 + estimate_datum_byte_size(Datum),
  D0#data{
    dictionary = Dictionary,
    byte_size = ByteSize
  }.

%% TODO: other types
estimate_datum_byte_size(Bin) when is_binary(Bin) ->
  byte_size(Bin);
estimate_datum_byte_size(Int) when is_integer(Int) ->
  %% Maximum int size is 64 bits, excluding the deprecated int96 type.
  <<U:64/unsigned>> = <<Int:64/signed>>,
  ceil(math:log2(U) / 8).

%% todo: how to improve?
estimate_byte_size(#c{data = #bools{encoder = Encoder}}) ->
  parquer_rle_bp_hybrid_encoder:estimate_byte_size(Encoder);
estimate_byte_size(#c{data = #data{byte_size = ByteSize}}) ->
  ByteSize.

-spec do_close(t()) -> {iodata(), [write_metadata()]}.
do_close(#writer{} = Writer0) ->
  {IOData0, WriteMetaOut, Writer1} =
    case Writer0#writer.has_data of
      true ->
        close_row_group(Writer0);
      false ->
        {[], [], Writer0}
    end,
  RowGroups = lists:reverse(Writer1#writer.closed_row_groups),
  FMD = parquer_thrift_utils:file_metadata(#{
     %% todo: check what to put here
     ?version => 1,
     ?schema => schema_to_thrift(Writer1),
     ?num_rows => Writer1#writer.num_rows,
     ?row_groups => RowGroups
  }),
  FMDBin = parquer_thrift_utils:serialize(FMD),
  FMDBinSize = iolist_size(FMDBin),
  {[IOData0, FMDBin, encode_data_size(FMDBinSize), ?MAGIC_NOT_ENCRYPTED], WriteMetaOut}.

-spec maybe_emit_row_group(t()) -> {iodata(), write_metadata(), t()}.
maybe_emit_row_group(#writer{} = Writer0) ->
  EstimatedByteSize =
    lists:foldl(
      fun(C, Acc) -> estimate_byte_size(C) + Acc end,
      0,
      Writer0#writer.columns),
  %% TODO: make configurable
  case EstimatedByteSize >= ?DEFAULT_MAX_ROW_GROUP_BYTES of
    true ->
      close_row_group(Writer0);
    false ->
      {[], [], Writer0}
  end.

close_row_group(#writer{} = Writer0) ->
  {MMagic, Writer1} = maybe_emit_magic(Writer0),
  {IODataMetaAndChunks, Writer2} =
    lists:mapfoldl(
      fun(Col, WriterAcc0) ->
          {IOData, WriteMeta} = serialize_column(Col, WriterAcc0),
          {Chunk, WriterAcc1} = mk_column_chunk(WriteMeta, Col, WriterAcc0),
          {{IOData, WriteMeta, Chunk}, WriterAcc1}
      end,
      Writer1,
      Writer1#writer.columns),
  {IOData, WriteMetas, ColumnChunks} = lists:unzip3(IODataMetaAndChunks),
  {NumRows, TotalUncompSize, TotalCompSize} =
    lists:foldl(
      fun(WriteMeta, {RowsAcc, UncompAcc, CompAcc}) ->
          {
           RowsAcc + WriteMeta#write_meta.num_rows,
           UncompAcc + WriteMeta#write_meta.total_uncompressed_size,
           CompAcc + WriteMeta#write_meta.total_compressed_size
          }
      end,
      {0, 0, 0},
      WriteMetas),
  RowGroup = parquer_thrift_utils:row_group(#{
    ?column_chunks => ColumnChunks,
    ?total_uncompressed_size => TotalUncompSize,
    ?total_compressed_size => TotalCompSize,
    ?num_rows => NumRows
  }),
  Writer3 = Writer2#writer{
    columns = initialize_columns(Writer2#writer.schema, Writer2#writer.opts),
    num_rows = Writer2#writer.num_rows + NumRows,
    closed_row_groups = [RowGroup | Writer2#writer.closed_row_groups]
  },
  WriteMetasOut = lists:map(fun write_metadata_out/1, WriteMetas),
  {[MMagic, IOData], WriteMetasOut, Writer3}.

maybe_emit_magic(#writer{offset = 0} = Writer0) ->
  %% File just started.
  emit_magic(Writer0);
maybe_emit_magic(#writer{} = Writer) ->
  Writer.

emit_magic(#writer{} = Writer0) ->
  Writer = Writer0#writer{offset = byte_size(?MAGIC_NOT_ENCRYPTED)},
  {?MAGIC_NOT_ENCRYPTED, Writer}.

serialize_column(#c{data = #bools{}} = C, Writer) ->
  #c{
    data = #bools{encoder = Encoder},
    num_values = NumValues,
    num_nulls = NumNulls
  } = C,
  %% Serialize repetition levels, definition levels, data.
  RepLevelBin = serialize_repetition_levels(C),
  DefLevelBin = serialize_definition_levels(C),
  DataBin0 = parquer_rle_bp_hybrid_encoder:to_bytes(Encoder),
  DataBin = [encode_bit_width(1) | DataBin0],
  ColDataBin = [RepLevelBin, DefLevelBin, DataBin],
  ColDataBinComp = compress_data(ColDataBin, C, Writer),
  %% Serialize data page header v2 and page header
  DataPageHeaderV2 = parquer_thrift_utils:data_page_header_v2(#{
    ?num_values => NumValues,
    ?num_nulls => NumNulls,
    ?num_rows => C#c.num_rows,
    ?encoding => ?ENCODING_PLAIN,
    ?definition_levels_byte_length => iolist_size(DefLevelBin),
    ?repetition_levels_byte_length => iolist_size(RepLevelBin)
    %% TODO
    %% ?is_compressed => TODO
    %% ?statistics => TODO
  }),
  ColDataBinSize = iolist_size(ColDataBin),
  ColDataBinCompSize = iolist_size(ColDataBinComp),
  PageHeader = parquer_thrift_utils:page_header(#{
    ?page_type => ?PAGE_TYPE_DATA_PAGE_V2,
    ?uncompressed_page_size => ColDataBinSize,
    ?compressed_page_size => ColDataBinCompSize,
    ?data_page_header_v2 => DataPageHeaderV2
  }),
  PageHeaderBin = parquer_thrift_utils:serialize(PageHeader),
  IOData = [PageHeaderBin, ColDataBinComp],
  PageHeaderBinSize = iolist_size(PageHeaderBin),
  WriteMeta = #write_meta{
    num_rows = C#c.num_rows,
    total_compressed_size = PageHeaderBinSize + ColDataBinCompSize,
    total_uncompressed_size = PageHeaderBinSize + ColDataBinSize,
    data_page_header_size = PageHeaderBinSize,
    data_uncompressed_size = ColDataBinSize,
    data_compressed_size = ColDataBinCompSize
  },
  {IOData, WriteMeta};
serialize_column(#c{data = #data{enable_dictionary = true}} = C, #writer{} = W) ->
  %% TODO: handle fallback to plain encoding when dictionary overflows
  %% Serialize dictionary (plain)
  #{ ?concrete_dictionary := ConcreteDict
   , ?compressed := DictBinComp
   , ?uncompressed_size := DictBinSize
   , ?compressed_size := DictBinCompSize
   } = DictSerializedInfo = serialize_dictionary(C, W),
  %% Serialize dictionary page header
  PageHeaderDictBin = serialize_dictionary_page_header(DictSerializedInfo),
  %% Serialize repetition level, definition level, data. (RLE/BP hybrid)
  #{ ?compressed := ColDataBinComp
   , ?def_level_bin := _DefLevelBin
   , ?rep_level_bin := _RepLevelBin
   , ?uncompressed_size := ColDataBinSize
   , ?compressed_size := ColDataBinCompSize
   } = DataSerializedInfo = serialize_data_with_dictionary(C, W, ConcreteDict),
  PageHeaderDataBin = serialize_data_page_v1(DataSerializedInfo, C),
  IOData = [PageHeaderDictBin, DictBinComp, PageHeaderDataBin, ColDataBinComp],
  PageHeaderDictBinSize = iolist_size(PageHeaderDictBin),
  PageHeaderDataBinSize = iolist_size(PageHeaderDataBin),
  TotalCompSize = PageHeaderDictBinSize + DictBinCompSize + PageHeaderDataBinSize + ColDataBinCompSize,
  TotalSize = PageHeaderDictBinSize + DictBinSize + PageHeaderDataBinSize + ColDataBinSize,
  WriteMeta = #write_meta{
    num_rows = C#c.num_rows,
    total_compressed_size = TotalCompSize,
    total_uncompressed_size = TotalSize,
    dict_page_header_size = PageHeaderDictBinSize,
    dict_uncompressed_size = DictBinSize,
    dict_compressed_size = DictBinCompSize,
    data_page_header_size = PageHeaderDataBinSize,
    data_uncompressed_size = ColDataBinSize,
    data_compressed_size = ColDataBinCompSize
  },
  {IOData, WriteMeta}.

serialize_repetition_levels(#c{max_repetition_level = 0}) ->
  [];
serialize_repetition_levels(#c{} = C) ->
  #c{
    repetition_levels = RevRepLevels,
    max_repetition_level = MaxRepLevel
  } = C,
  serialize_levels(RevRepLevels, MaxRepLevel).

serialize_definition_levels(#c{max_definition_level = 0}) ->
  [];
serialize_definition_levels(#c{} = C) ->
  #c{
    definition_levels = RevDefLevels,
    max_definition_level = MaxDefLevel
  } = C,
  serialize_levels(RevDefLevels, MaxDefLevel).

serialize_levels(RevLevels, MaxLevel) ->
  Levels = lists:reverse(RevLevels),
  BitWidth = bit_width_of(MaxLevel),
  Encoder0 = parquer_rle_bp_hybrid_encoder:new(BitWidth),
  Encoder1 = lists:foldl(
               fun(L, Acc) -> parquer_rle_bp_hybrid_encoder:write_int(Acc, L) end,
               Encoder0,
               Levels),
  RLEBytes = parquer_rle_bp_hybrid_encoder:to_bytes(Encoder1),
  [encode_data_size(iolist_size(RLEBytes)) | RLEBytes].

serialize_dictionary(#c{} = C, #writer{} = W) ->
  #c{
    data = #data{dictionary = Dictionary},
    primitive_type = PrimitiveType
  } = C,
  {DictBin, ConcreteDict, _} = maps:fold(
     fun(V, _, {BinAcc0, DictAcc0, N0}) ->
         BinAcc = append_datum_bin(BinAcc0, V, PrimitiveType),
         DictAcc = DictAcc0#{V => N0},
         N = N0 + 1,
         {BinAcc, DictAcc, N}
     end,
     {<<>>, #{}, 0},
     Dictionary
    ),
  DictBinComp = compress_data(DictBin, C, W),
  NumDictKeys = map_size(ConcreteDict),
  DictBinSize = iolist_size(DictBin),
  DictBinCompSize = iolist_size(DictBinComp),
  #{ ?num_dict_keys => NumDictKeys
   , ?concrete_dictionary => ConcreteDict
   , ?uncompressed => DictBin
   , ?compressed => DictBinComp
   , ?uncompressed_size => DictBinSize
   , ?compressed_size => DictBinCompSize
   }.

serialize_dictionary_page_header(SerializedInfo) ->
  #{ ?num_dict_keys := NumDictKeys
   , ?uncompressed_size := DictBinSize
   , ?compressed_size := DictBinCompSize
   } = SerializedInfo,
  DictPageHeader = parquer_thrift_utils:dict_page_header(#{
    ?num_values => NumDictKeys,
    ?encoding => ?ENCODING_PLAIN_DICT,
    %% ?is_sorted => false
    ?is_sorted => ?undefined
  }),
  PageHeaderDict = parquer_thrift_utils:page_header(#{
    ?page_type => ?PAGE_TYPE_DICT_PAGE,
    ?uncompressed_page_size => DictBinSize,
    ?compressed_page_size => DictBinCompSize,
    ?dict_page_header => DictPageHeader
  }),
  parquer_thrift_utils:serialize(PageHeaderDict).

serialize_data_with_dictionary(#c{} = C, #writer{} = W, ConcreteDict) ->
  #c{data = #data{values = RevValues}} = C,
  RepLevelBin = serialize_repetition_levels(C),
  DefLevelBin = serialize_definition_levels(C),
  DataBitWidth = bit_width_of(max(0, map_size(ConcreteDict) - 1)),
  DataEncoder0 = parquer_rle_bp_hybrid_encoder:new(DataBitWidth),
  DataEncoder1 =
    lists:foldl(
      fun(V, Acc) ->
          I = maps:get(V, ConcreteDict),
          parquer_rle_bp_hybrid_encoder:write_int(Acc, I)
      end,
      DataEncoder0,
      lists:reverse(RevValues)),
  DataBin0 = parquer_rle_bp_hybrid_encoder:to_bytes(DataEncoder1),
  DataBin = [encode_bit_width(DataBitWidth) | DataBin0],
  ColDataBin = [RepLevelBin, DefLevelBin, DataBin],
  ColDataBinComp = compress_data(ColDataBin, C, W),
  ColDataBinSize = iolist_size(ColDataBin),
  ColDataBinCompSize = iolist_size(ColDataBinComp),
  #{ ?uncompressed => ColDataBin
   , ?compressed => ColDataBinComp
   , ?def_level_bin => DefLevelBin
   , ?rep_level_bin => RepLevelBin
   , ?uncompressed_size => ColDataBinSize
   , ?compressed_size => ColDataBinCompSize
   }.

serialize_data_page_v1(SerializedInfo, #c{data = #data{enable_dictionary = true}} = C) ->
  #c{num_values = NumValues} = C,
  #{ ?uncompressed_size := ColDataBinSize
   , ?compressed_size := ColDataBinCompSize
   } = SerializedInfo,
  DataPageHeader = parquer_thrift_utils:data_page_header_v1(#{
    ?num_values => NumValues,
    ?encoding => ?ENCODING_PLAIN_DICT,
    ?definition_level_encoding => ?ENCODING_RLE,
    ?repetition_level_encoding => ?ENCODING_RLE
    %% TODO
    %% ?is_compressed => TODO
    %% ?statistics => TODO
  }),
  PageHeaderData = parquer_thrift_utils:page_header(#{
    ?page_type => ?PAGE_TYPE_DATA_PAGE_V1,
    ?uncompressed_page_size => ColDataBinSize,
    ?compressed_page_size => ColDataBinCompSize,
    ?data_page_header_v1 => DataPageHeader
  }),
  parquer_thrift_utils:serialize(PageHeaderData).

serialize_data_page_v2(SerializedInfo, #c{data = #data{enable_dictionary = true}} = C) ->
  #c{num_values = NumValues, num_nulls = NumNulls} = C,
  #{ ?uncompressed_size := ColDataBinSize
   , ?compressed_size := ColDataBinCompSize
   , ?def_level_bin := DefLevelBin
   , ?rep_level_bin := RepLevelBin
   } = SerializedInfo,
  DataPageHeader = parquer_thrift_utils:data_page_header_v2(#{
    ?num_values => NumValues,
    ?num_nulls => NumNulls,
    ?num_rows => C#c.num_rows,
    ?encoding => ?ENCODING_PLAIN_DICT,
    ?definition_levels_byte_length => iolist_size(DefLevelBin),
    ?repetition_levels_byte_length => iolist_size(RepLevelBin)
    %% TODO
    %% ?is_compressed => TODO
    %% ?statistics => TODO
  }),
  PageHeaderData = parquer_thrift_utils:page_header(#{
    ?page_type => ?PAGE_TYPE_DATA_PAGE_V2,
    ?uncompressed_page_size => ColDataBinSize,
    ?compressed_page_size => ColDataBinCompSize,
    ?data_page_header_v2 => DataPageHeader
  }),
  parquer_thrift_utils:serialize(PageHeaderData).

%% TODO: add other types
append_datum_bin(BinAcc, Datum, ?BYTE_ARRAY) when is_binary(Datum) ->
  Size = encode_data_size(byte_size(Datum)),
  <<BinAcc/binary, Size/binary, Datum/binary>>;
append_datum_bin(BinAcc, Datum, ?INT32) when is_integer(Datum) ->
  Size = 32,
  <<BinAcc/binary, Size:32/little, Datum:32/little>>.

bit_width_of(0) ->
  0;
bit_width_of(Level) ->
  floor(math:log2(Level) + 1).

encode_bit_width(BitWidth) ->
  <<BitWidth:8>>.

encode_data_size(Size) ->
  <<Size:32/little>>.

compress_data(DataBin, Col, Writer) ->
  {Compression, Opts} = get_compression(Col, Writer),
  do_compress_data(DataBin, Compression, Opts).

get_compression(_Col, #writer{opts = Opts}) ->
  %% todo: per-column compression and compression opts
  CompressionOpts = #{},
  Compression = maps:get(?default_compression, Opts, ?DEFAULT_COMPRESSION),
  {Compression, CompressionOpts}.

do_compress_data(DataBin, ?COMPRESSION_NONE, _Opts) ->
  DataBin;
do_compress_data(DataBin, ?COMPRESSION_ZSTD, _Opts) ->
  case ezstd:compress(iolist_to_binary(DataBin)) of
    {error, Reason} ->
      error({compression_failed, ?COMPRESSION_ZSTD, Reason, DataBin});
    Res when is_binary(Res) ->
      Res
  end;
do_compress_data(DataBin, ?COMPRESSION_SNAPPY, _Opts) ->
  case snappyer:compress(DataBin) of
    {ok, Res} ->
      Res;
    {error, Reason} ->
      error({compression_failed, ?COMPRESSION_SNAPPY, Reason, DataBin})
  end.

mk_column_chunk(WriteMeta, Col, Writer0) ->
  {PathInSchema, _} = lists:unzip(Col#c.path),
  {Compression, _} = get_compression(Col, Writer0),
  ColumnChunk = parquer_thrift_utils:column_chunk(#{
    ?offset => Writer0#writer.offset,
    ?metadata => parquer_thrift_utils:column_metadata(#{
      ?type => Col#c.primitive_type,
      ?num_values => Col#c.num_values,
      %% fixme: might change if dictionary fell back to plain
      ?encodings => encodings_for(Col),
      ?path_in_schema => PathInSchema,
      ?codec => Compression,
      ?total_uncompressed_size => WriteMeta#write_meta.total_uncompressed_size,
      ?total_compressed_size => WriteMeta#write_meta.total_compressed_size,
      ?data_page_offset => data_page_offset(WriteMeta, Writer0),
      ?dict_page_offset => dict_page_offset(WriteMeta, Writer0)
    })
  }),
  Writer = Writer0#writer{
    offset = Writer0#writer.offset + WriteMeta#write_meta.total_compressed_size
  },
  {ColumnChunk, Writer}.

encodings_for(#c{primitive_type = ?BOOLEAN}) ->
  [?ENCODING_RLE];
encodings_for(#c{}) ->
  %% fixme: might change if dictionary fell back to plain
  [?ENCODING_PLAIN, ?ENCODING_RLE, ?ENCODING_RLE_DICT].

dict_page_offset(#write_meta{dict_page_header_size = undefined}, _Writer) ->
  undefined;
dict_page_offset(#write_meta{}, #writer{offset = Offset}) ->
  %% Dict comes first, right at the current offset.
  Offset.

data_page_offset(#write_meta{dict_page_header_size = undefined}, #writer{offset = Offset}) ->
  %% Without dict, data comes right at the current offset.
  Offset;
data_page_offset(#write_meta{} = WriteMeta, #writer{offset = BaseOffset}) ->
  %% Dict comes first, then data.
  #write_meta{
    dict_page_header_size = DictHeaderSize,
    dict_compressed_size = DictSize
  } = WriteMeta,
  BaseOffset + DictHeaderSize + DictSize.

schema_to_thrift(#writer{schema = Schema}) ->
  lists:map(fun parquer_thrift_utils:schema_element/1, Schema).

write_metadata_out(#write_meta{} = WM) ->
  #{
     num_rows => WM#write_meta.num_rows
   }.

is_dictionary_enabled(KeyPath, Opts) ->
  ColKeyPath = iolist_to_binary(lists:join($., KeyPath)),
  case Opts of
    #{?enable_dictionary := #{ColKeyPath := Bool}} ->
      Bool;
    #{?enable_dictionary := Bool} ->
      Bool
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
