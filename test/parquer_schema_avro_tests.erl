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
-module(parquer_schema_avro_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parquer.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(write_old_list_structure, write_old_list_structure).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

read_example_avro_sc(Name) ->
  BaseDir = code:lib_dir(parquer),
  File = filename:join([BaseDir, "test", "sample_avro_schemas", Name]),
  {ok, Bin} = file:read_file(File),
  json:decode(Bin).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

write_new_list_structure_test_() ->
  Opts = #{?write_old_list_structure => false},
  [ { "non-null list, non-null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?fields := [
                   #{ ?name := <<"list">>
                    , ?repetition := ?REPETITION_REPEATED
                    , ?fields := [
                        #{ ?name := <<"element">>
                         , ?id := 2
                         , ?primitive_type := ?BYTE_ARRAY
                         , ?logical_type := #{?name := ?lt_string}
                         , ?repetition := ?REPETITION_REQUIRED
                         }
                      ]
                    }
                 ]
               }
            ]
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("non_null_list_non_null_elem.avsc"),
           Opts
         )
      )
    }
  ].

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
