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
-module(parquer_thrift_utils_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parquer.hrl").
-include("parquer_parquet_types.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

all_converted_types() ->
    [
        ?CONVERTED_TYPE_LIST,
        ?CONVERTED_TYPE_UTF8,
        ?CONVERTED_TYPE_ENUM,
        ?CONVERTED_TYPE_MAP,
        ?CONVERTED_TYPE_MAP_KEY_VALUE
    ].

logical_type(T) ->
    logical_type(T, #{}).
logical_type(T, Extra) ->
    Extra#{?name => T}.

all_logical_types() ->
    TimeUnit1 = ?time_unit_millis,
    TimeUnit2 = ?time_unit_micros,
    TimeUnit3 = ?time_unit_nanos,
    [
        logical_type(?lt_string),
        logical_type(?lt_list),
        logical_type(?lt_map),
        logical_type(?lt_enum),
        logical_type(?lt_decimal, #{?precision => 8, ?scale => 2}),
        logical_type(?lt_date),
        logical_type(?lt_uuid),
        logical_type(?lt_unknown),
        logical_type(?lt_json),
        logical_type(?lt_bson),
        logical_type(?lt_float16),
        logical_type(?lt_time, #{?unit => TimeUnit1, ?is_adjusted_to_utc => true}),
        logical_type(?lt_timestamp, #{?unit => TimeUnit2, ?is_adjusted_to_utc => false}),
        logical_type(?lt_timestamp, #{?unit => TimeUnit3, ?is_adjusted_to_utc => true}),
        logical_type(?lt_int, #{?bit_width => 3, ?is_signed => true}),
        logical_type(?lt_variant, #{?specification_version => 10}),
        logical_type(?lt_geometry, #{?crs => <<"crs">>}),
        logical_type(?lt_geography, #{?crs => <<"crs">>, ?algorithm => 123})
    ].

schema_element_params(LogicalType, ConvertedType) ->
    #{
        ?name => <<"col">>,
        ?repetition => ?REPETITION_REQUIRED,
        ?logical_type => LogicalType,
        ?converted_type => ConvertedType
    }.

try_serialize(Type, Struct) ->
    try
        _ = parquer_thrift_utils:serialize(Struct)
    catch
        K:E:S ->
            ?debugFmt("\n\n~p failed to encode:\n  ~p:~p\n  ~p", [Type, K, E, S]),
            erlang:raise(K, E, S)
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

%% Only to check we have clauses for all constants; i.e., it doesn't crash.
logical_type_coverage_test_() ->
    [
        ?_test(begin
            Struct = parquer_thrift_utils:schema_element(
                schema_element_params(LogicalType, ?undefined)
            ),
            try_serialize(LogicalType, Struct)
        end)
     || LogicalType <- all_logical_types()
    ].

%% Only to check we have clauses for all constants; i.e., it doesn't crash.
converted_type_coverage_test_() ->
    [
        ?_test(begin
            Struct = parquer_thrift_utils:schema_element(
                schema_element_params(?undefined, ConvertedType)
            ),
            try_serialize(ConvertedType, Struct)
        end)
     || ConvertedType <- all_converted_types()
    ].
