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
-module(parquer_test_utils).

-export([
  all/1,
  groups/1,
  matrix_cases/1,
  matrix_to_groups/2,
  get_matrix_params/3,
  group_path/1
]).

all(Module) ->
  All0 = all0(Module),
  All = All0 -- matrix_cases(Module),
  Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups(Module)),
  Groups ++ All.

groups(Module) ->
  matrix_to_groups(Module, matrix_cases(Module)).

matrix_cases(Module) ->
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                get_tc_prop(Module, TestCase, matrix, false);
            (TestCase) ->
                get_tc_prop(Module, TestCase, matrix, false)
        end,
        all0(Module)
    ).

get_matrix_params(Module, Group, Default) ->
  persistent_term:get({Module, Group}, Default).

get_tc_prop(Module, TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(Module, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, Module:TestCase()),
        Val
    else
        _ -> Default
    end.

%% Generate ct sub-groups from test-case's 'matrix' clause
%% NOTE: the test cases must have a root group name which
%% is unkonwn to this API.
%%
%% e.g.
%% all() -> [{group, g1}].
%%
%% groups() ->
%%   parquer_test_utils:groups(?MODULE, [case1, case2]).
%%
%% case1(matrix) ->
%%   {g1, [[tcp, no_auth],
%%         [ssl, no_auth],
%%         [ssl, basic_auth]
%%        ]};
%%
%% case2(matrix) ->
%%   {g1, ...}
%% ...
%%
%% Return:
%%
%%  [{g1, [],
%%     [ {tcp, [], [{no_auth,    [], [case1, case2]}
%%                 ]},
%%       {ssl, [], [{no_auth,    [], [case1, case2]},
%%                  {basic_auth, [], [case1, case2]}
%%                 ]}
%%     ]
%%   }
%%  ]
matrix_to_groups(Module, Cases) ->
    lists:foldr(
        fun(Case, Acc) ->
            add_case_matrix(Module, Case, Acc)
        end,
        [],
        Cases
    ).

add_case_matrix(Module, TestCase0, Acc0) ->
    TestCase =
        case TestCase0 of
            {testcase, TestCase1, _Opts} ->
                TestCase1;
            _ ->
                TestCase0
        end,
    {MaybeRootGroup, Matrix} =
        case Module:TestCase(matrix) of
            {RootGroup0, Matrix0} ->
                {RootGroup0, Matrix0};
            Matrix0 ->
                {undefined, Matrix0}
        end,
    lists:foldr(
        fun(Row0, Acc) ->
            Row = extract_params(Module, Row0),
            case MaybeRootGroup of
                undefined ->
                    add_group(Row, Acc, TestCase0);
                RootGroup ->
                    add_group([RootGroup | Row], Acc, TestCase0)
            end
        end,
        Acc0,
        Matrix
    ).

add_group([], Acc, TestCase) ->
    case lists:member(TestCase, Acc) of
        true ->
            Acc;
        false ->
            [TestCase | Acc]
    end;
add_group([Name | More], Acc, TestCases) ->
    case lists:keyfind(Name, 1, Acc) of
        false ->
            [{Name, [], add_group(More, [], TestCases)} | Acc];
        {Name, [], SubGroup} ->
            New = {Name, [], add_group(More, SubGroup, TestCases)},
            lists:keystore(Name, 1, Acc, New)
    end.

group_path(Config) ->
    try
        Current = proplists:get_value(tc_group_properties, Config),
        NameF = fun(Props) ->
            {name, Name} = lists:keyfind(name, 1, Props),
            Name
        end,
        Stack = proplists:get_value(tc_group_path, Config),
        lists:reverse(lists:map(NameF, [Current | Stack]))
    catch
        _:_ ->
            []
    end.

all0(Module) ->
  lists:usort([
    F
    || {F, 1} <- Module:module_info(exports),
       string:substr(atom_to_list(F), 1, 2) == "t_"
  ]).

extract_params(Module, Rows0) ->
  {Rows, Params} =
    lists:foldr(
      fun({Name0, Params}, {RowsAcc, ParamsAcc0}) ->
           N = erlang:unique_integer([positive]),
           Name1 = atom_to_binary(Name0),
           Name = binary_to_atom(iolist_to_binary([Name1, "_", integer_to_binary(N)])),
           ParamsAcc = [{Name, Params} | ParamsAcc0],
           {[Name | RowsAcc], ParamsAcc};
         (Name, {RowsAcc, ParamsAcc0}) when is_atom(Name) ->
           {[Name | RowsAcc], ParamsAcc0}
      end,
      {[], []},
      Rows0
     ),
  lists:foreach(
    fun({Name, ParamsIn}) -> persistent_term:put({Module, Name}, ParamsIn) end,
    Params),
  Rows.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
