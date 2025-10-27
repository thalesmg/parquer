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
-ifndef(PARQUER_HRL).
-define(PARQUER_HRL, true).

-define(MAGIC_NOT_ENCRYPTED, <<"PAR1">>).

%% Repetition enum
-define(REPETITION_REQUIRED, required).
-define(REPETITION_OPTIONAL, optional).
-define(REPETITION_REPEATED, repeated).

%% Primitive types
-define(BOOLEAN, bool).
-define(INT32, int32).
-define(INT64, int64).
%% deprecated
-define(INT96, int96).
-define(FLOAT, float).
-define(DOUBLE, double).
-define(BYTE_ARRAY, byte_array).
-define(FIXED_LEN_BYTE_ARRAY, fixed_len_byte_array).

%% Logical type names
-define(lt_string, string).
-define(lt_list, list).
-define(lt_map, map).
-define(lt_enum, enum).
-define(lt_decimal, decimal).
-define(lt_date, date).
-define(lt_uuid, uuid).
-define(lt_unknown, unknown).
-define(lt_json, json).
-define(lt_bson, bson).
-define(lt_float16, float16).
-define(lt_time, time).
-define(lt_timestamp, timestamp).
-define(lt_int, int).
-define(lt_variant, variant).
-define(lt_geometry, geometry).
-define(lt_geography, geography).

%% Converted types
-define(CONVERTED_TYPE_UTF8, utf8).
-define(CONVERTED_TYPE_LIST, list).
-define(CONVERTED_TYPE_MAP, map).
-define(CONVERTED_TYPE_MAP_KEY_VALUE, map_key_value).
-define(CONVERTED_TYPE_ENUM, enum).

-define(undefined, undefined).
-define(null, null).

-define(algorithm, algorithm).
-define(bit_width, bit_width).
-define(converted_type, converted_type).
-define(crs, crs).
-define(default_compression, default_compression).
-define(fields, fields).
-define(id, id).
-define(is_adjusted_to_utc, is_adjusted_to_utc).
-define(is_signed, is_signed).
-define(logical_type, logical_type).
-define(max_definition_level, max_definition_level).
-define(max_repetition_level, max_repetition_level).
-define(name, name).
-define(num_children, num_children).
-define(num_nulls, num_nulls).
-define(num_rows, num_rows).
-define(num_values, num_values).
-define(offset, offset).
-define(precision, precision).
-define(primitive_type, primitive_type).
-define(repetition, repetition).
-define(scale, scale).
-define(specification_version, specification_version).
-define(type_length, type_length).
-define(unit, unit).

-define(time_unit_millis, millis).
-define(time_unit_micros, micros).
-define(time_unit_nanos, nanos).

-define(COMPRESSION_NONE, none).
-define(COMPRESSION_SNAPPY, snappy).
-define(COMPRESSION_ZSTD, zstd).

-endif.
