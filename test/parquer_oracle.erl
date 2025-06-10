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
-module(parquer_oracle).

%% API
-export([
    start/1,
    stop/1,

    command/2
]).

%% `gen_server` API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-record(command, {payload}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start(Opts) ->
    gen_server:start(?MODULE, Opts, []).

stop(Pid) ->
    gen_server:stop(Pid).

command(Pid, Payload) ->
    gen_server:call(Pid, #command{payload = Payload}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server` API
%%------------------------------------------------------------------------------

init(Opts) ->
    process_flag(trap_exit, true),
    #{
        program := Program,
        args := Args,
        cd := CD
    } = Opts,
    Port = open_port(
        {spawn_executable, Program},
        [
            binary,
            {line, 4096},
            {args, Args},
            {cd, CD}
        ]
    ),
    State = #{
        buffer => [],
        caller => undefined,
        port => Port
    },
    link(Port),
    {ok, State}.

terminate(_Reason, State) ->
    #{port := Port} = State,
    port_close(Port),
    ok.

handle_call(#command{payload = Payload}, From, State0) ->
    #{port := Port} = State0,
    Port ! {self(), {command, Payload}},
    State = State0#{caller := From},
    {noreply, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({Port, {data, Data0}}, #{port := Port} = State0) ->
    #{
        buffer := Buffer0,
        caller := From
    } = State0,
    State =
        case Data0 of
            {noeol, Data} ->
                State0#{buffer := [Buffer0 | Data]};
            {eol, Data} ->
                gen_server:reply(From, {ok, [Buffer0 | Data]}),
                State0#{buffer := [], caller := undefined}
        end,
    {noreply, State};
handle_info({'EXIT', Port, Reason}, #{port := Port} = _State) ->
    exit({port_closed, Reason});
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
