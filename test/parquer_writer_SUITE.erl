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
-module(parquer_writer_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("parquer.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(F0, <<"f0">>).
-define(F1, <<"f1">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
  parquer_test_utils:all(?MODULE).

init_per_suite(Config) ->
  Dir = code:lib_dir(parquer),
  CD = filename:join([Dir, "test", "clj"]),
  Opts = #{
    program => os:find_executable("clojure"),
    args => ["-M", "-m", "writer-oracle"],
    cd => CD
  },
  ct:print("starting oracle"),
  {ok, Oracle} = parquer_oracle:start(Opts),
  ct:print("oracle started"),
  [{oracle, Oracle} | Config].

end_per_suite(Config) ->
  Oracle = ?config(oracle, Config),
  is_process_alive(Oracle) andalso parquer_oracle:stop(Oracle),
  ok.

init_per_testcase(TestCase, TCConfig) ->
  link(?config(oracle, TCConfig)),
  case erlang:function_exported(?MODULE, TestCase, 2) of
    true ->
      ?MODULE:TestCase(init, TCConfig);
    false ->
      TCConfig
  end.

end_per_testcase(TestCase, TCConfig) ->
  maybe
    true ?= erlang:function_exported(?MODULE, TestCase, 2),
    ?MODULE:TestCase('end', TCConfig)
  end,
  unlink(?config(oracle, TCConfig)),
  ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

query_oracle(Parquet, TCConfig) ->
  Oracle = ?config(oracle, TCConfig),
  Payload = [base64:encode(iolist_to_binary(Parquet)), $\n],
  ct:pal("querying oracle"),
  {ok, Data0} = parquer_oracle:command(Oracle, Payload),
  Data1 = iolist_to_binary(Data0),
  Data = json:decode(Data1),
  ct:pal("oracle responded:\n  ~p", [Data]),
  Data.

single_field_schema(Repetition, Type) when is_atom(Type) ->
  parquer_schema:root(<<"root">>, [parquer_schema:Type(?F0, Repetition)]).

write_and_close(Writer0, Records) ->
  {IOData0, Writer1} = parquer_writer:append_records(Writer0, Records),
  IOData1 = parquer_writer:close(Writer1),
  [IOData0, IOData1].

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_smoke_binary_optional(TCConfig) ->
  Schema = single_field_schema(?REPETITION_OPTIONAL, string),
  Opts = #{},
  Writer0 = parquer_writer:new(Schema, Opts),
  Records = [
    #{?F0 => <<"hello">>},
    #{},
    #{?F0 => <<"world">>}
  ],
  Parquet = write_and_close(Writer0, Records),
  Reference = query_oracle(Parquet, TCConfig),
  ?assertMatch(
     [ #{?F0 := <<"hello">>}
     , #{?F0 := null}
     , #{?F0 := <<"world">>}
     ],
     Reference),
  ok.

t_smoke_binary_required(TCConfig) ->
  Schema = single_field_schema(?REPETITION_REQUIRED, string),
  Opts = #{},
  Writer0 = parquer_writer:new(Schema, Opts),
  Records = [
    #{?F0 => <<"hello">>},
    #{?F0 => <<"world">>}
  ],
  Parquet = write_and_close(Writer0, Records),
  Reference = query_oracle(Parquet, TCConfig),
  ?assertMatch(
     [ #{?F0 := <<"hello">>}
     , #{?F0 := <<"world">>}
     ],
     Reference),
  ?assertError(
     #{reason := missing_required_value},
     write_and_close(Writer0, [#{}])),
  ok.

t_smoke_binary_repeated(TCConfig) ->
  %% N.B.: Java implementation does not like a repeated field that is not a group.
  %%
  %% e.g.:
  %%   Execution error (ClassCastException) at org.apache.parquet.schema.Type/asGroupType
  %%   (Type.java:247).  repeated binary f is not a group
  %%
  %% Also, it does not like nulls in repeated primitive fields (unless
  %% `parquet.avro.write-old-list-structure=false`, in which case the schema
  %% representation is different).
  %%
  %% This attempts to mimick the following Parquet schema:
  %%    message root {
  %%      repeated group f0 {
  %%        optional binary f1;
  %%      }
  %%    }
  %% ... which is produced by `MessageTypeParser/parseMessageType` followed by
  %% `AvroSchemaConverter/convert`
  Schema =
    parquer_schema:root(
      <<"root">>,
      [parquer_schema:group(
         ?F0, ?REPETITION_REQUIRED,
         #{?converted_type => ?CONVERTED_TYPE_LIST},
         [parquer_schema:group(
            <<"array">>, ?REPETITION_REPEATED,
            [parquer_schema:byte_array(?F1, ?REPETITION_OPTIONAL)])])]),
  Opts = #{},
  Writer0 = parquer_writer:new(Schema, Opts),
  Records = [
    #{?F0 => [#{?F1 => <<"hello">>}, #{?F1 => <<"world">>}]},
    #{?F0 => []},
    #{?F0 => ?undefined},
    #{?F0 => [#{?F1 => <<"!">>}, #{}]}
  ],
  Parquet = write_and_close(Writer0, Records),
  Reference = query_oracle(Parquet, TCConfig),
  ?assertMatch(
     [ #{?F0 := [#{?F1 := <<"hello">>}, #{?F1 := <<"world">>}]}
     , #{?F0 := []}
     , #{?F0 := []}
     , #{?F0 := [#{?F1 := <<"!">>}, #{?F1 := null}]}
     ],
     Reference),
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
