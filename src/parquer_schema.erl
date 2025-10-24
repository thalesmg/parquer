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
-module(parquer_schema).

%% API
-export([
    is_leaf/1,
    flatten/1,

    %% Composite types
    root/2,
    group/3,
    group/4,
    type/2,
    type/3,

    %% Logical types helpers
    string/2,
    string/3,
    binary/2,
    binary/3,
    list/3,
    list/4,
    map/3,
    map/4,

    %% Logical types
    lt_string/0,
    lt_list/0,
    lt_map/0,
    lt_enum/0,
    lt_decimal/1,
    lt_timestamp/1,

    %% Primitive types
    bool/2,
    bool/3,
    int32/2,
    int32/3,
    int64/2,
    int64/3,
    int96/2,
    int96/3,
    float/2,
    float/3,
    double/2,
    double/3,
    byte_array/2,
    byte_array/3,
    fixed_len_byte_array/2,
    fixed_len_byte_array/3
]).

-include("parquer.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(path, path).

-define(IS_REPETITION(R),
    (R == ?REPETITION_REQUIRED orelse
        R == ?REPETITION_OPTIONAL orelse
        R == ?REPETITION_REPEATED)
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

is_leaf(#{?fields := _}) ->
    false;
is_leaf(#{?num_children := _}) ->
    false;
is_leaf(_) ->
    true.

flatten(RootSchema) ->
    Context = #{
        ?path => [],
        ?max_definition_level => 0,
        ?max_repetition_level => 0
    },
    do_flatten(RootSchema, Context).

%%------------------------------------------------------------------------------
%% Composite types
%%------------------------------------------------------------------------------

root(Name, Fields) ->
    group(Name, ?REPETITION_REPEATED, _Opts = #{}, Fields).

group(Name, Repetition, Fields) ->
    group(Name, Repetition, _Opts = #{}, Fields).
group(Name, Repetition, Opts0, Fields) when ?IS_REPETITION(Repetition), is_list(Fields) ->
    Opts = Opts0#{?fields => Fields},
    type(Name, Repetition, Opts).

type(Name, Repetition) ->
    type(Name, Repetition, _Opts = #{}).

type(Name, Repetition, #{} = Opts) when
    is_binary(Name),
    ?IS_REPETITION(Repetition)
->
    Base = #{?name => Name, ?repetition => Repetition},
    Extra = maps:with(
        [
            ?id,
            ?logical_type,
            ?primitive_type,
            ?converted_type,
            ?type_length,
            ?fields
        ],
        Opts
    ),
    maps:merge(Base, Extra).

%%------------------------------------------------------------------------------
%% Logical types
%%------------------------------------------------------------------------------

string(Name, Repetition) ->
    string(Name, Repetition, _Opts = #{}).
string(Name, Repetition, #{} = Opts0) ->
    Opts = Opts0#{
        ?logical_type => lt_string(),
        ?converted_type => ?CONVERTED_TYPE_UTF8
    },
    byte_array(Name, Repetition, Opts).

binary(Name, Repetition) ->
    binary(Name, Repetition, _Opts = #{}).
binary(Name, Repetition, #{} = Opts) ->
    byte_array(Name, Repetition, Opts).

list(Name, ListRepetition, Fields) ->
    list(Name, ListRepetition, _Opts = #{}, Fields).
list(Name, ListRepetition, Opts0, Fields) when
    ListRepetition == ?REPETITION_OPTIONAL;
    ListRepetition == ?REPETITION_REQUIRED
->
    Opts = Opts0#{
        ?logical_type => lt_list(),
        ?converted_type => ?CONVERTED_TYPE_LIST
    },
    group(Name, ListRepetition, Opts, Fields).

map(Name, MapRepetition, #{} = ValueType) ->
    map(Name, MapRepetition, _Opts = #{}, ValueType).
map(Name, MapRepetition, Opts0, #{} = ValueType) ->
    Opts = Opts0#{
        ?logical_type => lt_map(),
        ?converted_type => ?CONVERTED_TYPE_MAP
    },
    KVOpts = #{?converted_type => ?CONVERTED_TYPE_MAP_KEY_VALUE},
    group(Name, MapRepetition, Opts, [
        group(<<"key_value">>, ?REPETITION_REPEATED, KVOpts, [
            string(<<"key">>, ?REPETITION_REQUIRED),
            ValueType
        ])
    ]).

%%------------------------------------------------------------------------------
%% Primitive types
%%------------------------------------------------------------------------------

bool(Name, Repetition) ->
    bool(Name, Repetition, _Opts = #{}).
bool(Name, Repetition, #{} = Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?BOOLEAN}).

int32(Name, Repetition) ->
    int32(Name, Repetition, _Opts = #{}).
int32(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?INT32}).

int64(Name, Repetition) ->
    int64(Name, Repetition, _Opts = #{}).
int64(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?INT64}).

int96(Name, Repetition) ->
    int96(Name, Repetition, _Opts = #{}).
int96(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?INT96}).

float(Name, Repetition) ->
    float(Name, Repetition, _Opts = #{}).
float(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?FLOAT}).

double(Name, Repetition) ->
    double(Name, Repetition, _Opts = #{}).
double(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?DOUBLE}).

byte_array(Name, Repetition) ->
    byte_array(Name, Repetition, _Opts = #{}).
byte_array(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?BYTE_ARRAY}).

fixed_len_byte_array(Name, Repetition) ->
    fixed_len_byte_array(Name, Repetition, _Opts = #{}).
fixed_len_byte_array(Name, Repetition, Opts) ->
    type(Name, Repetition, Opts#{?primitive_type => ?FIXED_LEN_BYTE_ARRAY}).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

lt_string() -> #{?name => ?lt_string}.

lt_list() -> #{?name => ?lt_list}.

lt_map() -> #{?name => ?lt_map}.

lt_enum() -> #{?name => ?lt_enum}.

lt_decimal(#{?precision := Precision, ?scale := Scale}) ->
    #{?name => ?lt_decimal, ?precision => Precision, ?scale => Scale}.

lt_timestamp(#{?is_adjusted_to_utc := IsAdjustedToUTC, ?unit := Unit}) ->
    #{?name => ?lt_timestamp, ?is_adjusted_to_utc => IsAdjustedToUTC, ?unit => Unit}.

do_flatten(#{?fields := Fields} = Type, Context0) ->
    #{
        ?path := Path,
        ?max_definition_level := MaxDefLevel0,
        ?max_repetition_level := MaxRepLevel0
    } = Context0,
    #{
        ?name := Name,
        ?repetition := Repetition
    } = Type,
    Node0 = maps:with(
        [
            ?name,
            ?id,
            ?repetition,
            ?logical_type,
            ?primitive_type,
            ?converted_type
        ],
        Type
    ),
    NumChildren = length(Fields),
    Node = Node0#{?num_children => NumChildren, ?path => Path},
    MaxDefLevel = advance_max_definition_level(MaxDefLevel0, Repetition, Path),
    MaxRepLevel = advance_max_repetition_level(MaxRepLevel0, Repetition, Path),
    Context = Context0#{
        ?path := Path ++ [{Name, Repetition}],
        ?max_definition_level := MaxDefLevel,
        ?max_repetition_level := MaxRepLevel
    },
    [Node | lists:flatmap(fun(T) -> do_flatten(T, Context) end, Fields)];
do_flatten(Type, Context) ->
    #{
        ?path := Path,
        ?max_definition_level := MaxDefLevel0,
        ?max_repetition_level := MaxRepLevel0
    } = Context,
    #{?repetition := Repetition} = Type,
    MaxDefLevel = advance_max_definition_level(MaxDefLevel0, Repetition, Path),
    MaxRepLevel = advance_max_repetition_level(MaxRepLevel0, Repetition, Path),
    Column0 = maps:with(
        [
            ?name,
            ?id,
            ?repetition,
            ?logical_type,
            ?primitive_type,
            ?converted_type,
            ?type_length
        ],
        Type
    ),
    [
        Column0#{
            ?path => Path,
            ?max_definition_level => MaxDefLevel,
            ?max_repetition_level => MaxRepLevel
        }
    ].

advance_max_repetition_level(MaxRepLevel0, Repetition, Path) ->
    IsRoot = Path == [],
    case Repetition of
        ?REPETITION_REPEATED when not IsRoot ->
            MaxRepLevel0 + 1;
        _ ->
            MaxRepLevel0
    end.

advance_max_definition_level(MaxDefLevel0, Repetition, Path) ->
    IsRoot = Path == [],
    case Repetition of
        ?REPETITION_REQUIRED ->
            MaxDefLevel0;
        _ when IsRoot ->
            MaxDefLevel0;
        _ ->
            MaxDefLevel0 + 1
    end.
