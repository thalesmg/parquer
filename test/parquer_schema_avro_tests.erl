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

single_field_record(Type) ->
  #{ <<"name">> => <<"root">>
   , <<"type">> => <<"record">>
   , <<"fields">> => [
       #{ <<"field-id">> => 1
        , <<"name">> => <<"f1">>
        , <<"type">> => Type
        }
     ]
   }.

fmt(Template, Args) ->
  iolist_to_binary(io_lib:format(Template, Args)).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

primitive_type_test_() ->
  PlainTitle =
    fun(null, T) -> fmt("null ~s", [T]);
       (non_null, T) -> fmt("non-null ~s", [T])
    end,
  PlainType =
    fun(null, T) -> [<<"null">>, T];
       (non_null, T) -> T
    end,
  PlainRepetition =
    fun(null) -> ?REPETITION_OPTIONAL;
       (non_null) -> ?REPETITION_REQUIRED
    end,
  PlainCases = [
     { PlainTitle(Null, AvroType)
     , begin
         Repetition = PlainRepetition(Null),
         ?_assertMatch(
            #{ ?name := <<"root">>
             , ?fields :=
                 [#{ ?id := 1
                   , ?name := <<"f1">>
                   , ?primitive_type := PrimitiveType
                   , ?repetition := Repetition}]
             , ?repetition := ?REPETITION_REPEATED},
            parquer_schema_avro:from_avro(single_field_record(PlainType(Null, AvroType)))
           )
       end
     }
  || Null <- [null, non_null],
     {AvroType, PrimitiveType} <- [ {<<"int">>, ?INT32}
                                  , {<<"long">>, ?INT64}
                                  , {<<"float">>, ?FLOAT}
                                  , {<<"double">>, ?DOUBLE}
                                  , {<<"boolean">>, ?BOOLEAN}
                                  , {<<"bytes">>, ?BYTE_ARRAY}
                                  ]
  ],
  [ { "non-null string"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?fields :=
              [#{ ?id := 1
                , ?name := <<"f1">>
                , ?converted_type := ?CONVERTED_TYPE_UTF8
                , ?logical_type := #{?name := ?lt_string}
                , ?primitive_type := ?BYTE_ARRAY
                , ?repetition := ?REPETITION_REQUIRED}]
          , ?repetition := ?REPETITION_REPEATED},
         parquer_schema_avro:from_avro(single_field_record(<<"string">>))
        )}
  , { "null string"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?fields :=
              [#{ ?id := 1
                , ?name := <<"f1">>
                , ?converted_type := ?CONVERTED_TYPE_UTF8
                , ?logical_type := #{?name := ?lt_string}
                , ?primitive_type := ?BYTE_ARRAY
                , ?repetition := ?REPETITION_OPTIONAL}]
          , ?repetition := ?REPETITION_REPEATED},
         parquer_schema_avro:from_avro(single_field_record([<<"null">>, <<"string">>]))
        )}
  | PlainCases
  ].

write_new_list_structure_test_() ->
  Opts = #{?write_old_list_structure => false},
  [ { "non-null list, non-null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_REQUIRED
               , ?logical_type := #{?name := ?lt_list}
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
  , { "non-null list, null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_REQUIRED
               , ?logical_type := #{?name := ?lt_list}
               , ?fields := [
                   #{ ?name := <<"list">>
                    , ?repetition := ?REPETITION_REPEATED
                    , ?fields := [
                        #{ ?name := <<"element">>
                         , ?id := 2
                         , ?primitive_type := ?BYTE_ARRAY
                         , ?logical_type := #{?name := ?lt_string}
                         , ?repetition := ?REPETITION_OPTIONAL
                         }
                      ]
                    }
                 ]
               }
            ]
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("non_null_list_null_elem.avsc"),
           Opts
         )
      )
    }
  , { "null list, null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_OPTIONAL
               , ?logical_type := #{?name := ?lt_list}
               , ?fields := [
                   #{ ?name := <<"list">>
                    , ?repetition := ?REPETITION_REPEATED
                    , ?fields := [
                        #{ ?name := <<"element">>
                         , ?id := 2
                         , ?primitive_type := ?BYTE_ARRAY
                         , ?logical_type := #{?name := ?lt_string}
                         , ?repetition := ?REPETITION_OPTIONAL
                         }
                      ]
                    }
                 ]
               }
            ]
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("null_list_null_elem.avsc"),
           Opts
         )
      )
    }
  , { "null list, non-null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_OPTIONAL
               , ?logical_type := #{?name := ?lt_list}
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
           read_example_avro_sc("null_list_non_null_elem.avsc"),
           Opts
         )
      )
    }
  ].

write_old_list_structure_test_() ->
  Opts = #{?write_old_list_structure => true},
  [ { "non-null list, non-null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_REQUIRED
               , ?logical_type := #{?name := ?lt_list}
               , ?fields := [
                   #{ ?name := <<"array">>
                    , ?repetition := ?REPETITION_REPEATED
                    , ?id := 2
                    , ?primitive_type := ?BYTE_ARRAY
                    , ?logical_type := #{?name := ?lt_string}
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
  , { "null list, non-null elem"
    , ?_assertMatch(
         #{ ?name := <<"root">>
          , ?repetition := ?REPETITION_REPEATED
          , ?fields := [
              #{ ?name := <<"f1">>
               , ?id := 1
               , ?repetition := ?REPETITION_OPTIONAL
               , ?logical_type := #{?name := ?lt_list}
               , ?fields := [
                   #{ ?name := <<"array">>
                    , ?repetition := ?REPETITION_REPEATED
                    , ?id := 2
                    , ?primitive_type := ?BYTE_ARRAY
                    , ?logical_type := #{?name := ?lt_string}
                    }
                 ]
               }
            ]
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("null_list_non_null_elem.avsc"),
           Opts
         )
      )
    }
  , { "null list, null elem"
    , ?_assertThrow(
         #{ reason := unsupported_type
          , type := #{<<"items">> := [<<"null">> | _]}
          , hint := _
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("null_list_null_elem.avsc"),
           Opts
         )
      )
    }
  , { "non-null list, null elem"
    , ?_assertThrow(
         #{ reason := unsupported_type
          , type := #{<<"items">> := [<<"null">> | _]}
          , hint := _
          },
         parquer_schema_avro:from_avro(
           read_example_avro_sc("non_null_list_null_elem.avsc"),
           Opts
         )
      )
    }
  ].

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
