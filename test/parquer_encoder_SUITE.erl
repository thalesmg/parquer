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
-module(parquer_encoder_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

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
        args => ["-M", "-m", "ble-oracle"],
        cd => CD
    },
    {ok, Oracle} = parquer_oracle:start(Opts),
    [{oracle, Oracle} | Config].

end_per_suite(Config) ->
    Oracle = ?config(oracle, Config),
    is_process_alive(Oracle) andalso parquer_oracle:stop(Oracle),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

query_oracle(TCConfig, BitWidth, Values) ->
    Oracle = ?config(oracle, TCConfig),
    Payload = [json:encode(#{<<"values">> => Values, <<"bit-width">> => BitWidth}), $\n],
    {ok, Data0} = parquer_oracle:command(Oracle, Payload),
    Data1 = iolist_to_binary(Data0),
    base64:decode(Data1).

encode(BitWidth, Values) ->
    Encoder0 = parquer_rle_bp_hybrid_encoder:new(BitWidth),
    Encoder = lists:foldl(
        fun(V, Acc) -> parquer_rle_bp_hybrid_encoder:write_int(Acc, V) end,
        Encoder0,
        Values
    ),
    iolist_to_binary(parquer_rle_bp_hybrid_encoder:to_bytes(Encoder)).

consult_oracle(TCConfig, BitWidth, Values) ->
    Result = encode(BitWidth, Values),
    Expected = query_oracle(TCConfig, BitWidth, Values),
    ?assertEqual(Expected, Result).

alternating([]) ->
    [];
alternating([{N, I} | Rest]) ->
    lists:duplicate(N, I) ++ alternating(Rest).

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

t_same_as_oracle(TCConfig) ->
    ?assert(
        proper:quickcheck(
            prop_same_as_oracle(TCConfig),
            [{numtests, 10_000}, {to_file, user}]
        )
    ).

prop_same_as_oracle(TCConfig) ->
    MaxInt = trunc(math:pow(2, 32)) - 1,
    ?FORALL(
        {BitWidth, Values},
        %% TODO: enarge ints
        {range(0, 32), list(range(0, MaxInt))},
        begin
            Expected = query_oracle(TCConfig, BitWidth, Values),
            Result = encode(BitWidth, Values),
            ?WHENFAIL(
                io:format(
                    user,
                    "Bit-width:\n  ~b\n"
                    "Values:\n  ~p\n"
                    "Expected:\n  ~p\n"
                    "Got:\n  ~p\n",
                    [
                        BitWidth,
                        Values,
                        Expected,
                        Result
                    ]
                ),
                Expected == Result
            )
        end
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_repeated_8_plus_times(TCConfig) ->
    BitWidth = 1,
    Values = lists:duplicate(9, 0),
    consult_oracle(TCConfig, BitWidth, Values),
    ok.

t_repeated_fewer_than_8_times(TCConfig) ->
    BitWidth = 1,
    Values = lists:duplicate(7, 0),
    consult_oracle(TCConfig, BitWidth, Values),
    ok.

t_repeated_8_plus_times_then_new(TCConfig) ->
    BitWidth = 1,
    Values0 = lists:duplicate(8, 0),
    Values = lists:append(Values0, [1]),
    consult_oracle(TCConfig, BitWidth, Values),
    ok.

t_run_63_groups(TCConfig) ->
    BitWidth = 1,
    Specs = lists:flatten(lists:duplicate(73, [{7, 0}, {7, 1}])),
    Values = alternating(Specs),
    consult_oracle(TCConfig, BitWidth, Values),
    ok.

%% Checks varint encoding for long runs (>= 16#80).
t_long_run(TCConfig) ->
    BitWidth = 1,
    Values = lists:duplicate(128, 0),
    consult_oracle(TCConfig, BitWidth, Values),
    ok.
