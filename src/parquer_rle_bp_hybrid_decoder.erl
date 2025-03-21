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
-module(parquer_rle_bp_hybrid_decoder).

%% API
-export([new/2, read/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(RLE, 0).
-define(PACKED, 1).

%% State
-record(s, {
  bit_width,
  current_count,
  current_value,
  mode,
  data
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

new(BitWidth, Bin) ->
  #s{
     bit_width = BitWidth,
     current_count = 0,
     data = Bin
    }.

read(#s{} = S0) ->
    do_read(S0).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_read(#s{current_count = 0} = S0) ->
  #s{data = Data0} = S0,
  <<Header:8/unsigned, Data1/binary>> = Data0,
  Mode = Header band 1,
  S1 = S0#s{data = Data1, mode = Mode},
  case Mode of
    ?RLE ->
      do_read_rle(S1, Header);
    ?PACKED ->
      do_read_packed(S1, Header)
  end;
do_read(#s{current_count = C, mode = ?RLE} = S0) when C > 0 ->
  #s{current_value = X} = S0,
  S1 = S0#s{current_count = C - 1},
  {X, S1};
do_read(#s{current_count = C, mode = ?PACKED} = S0) when C > 0 ->
  #s{current_value = [X | Rest]} = S0,
  S1 = S0#s{current_count = C - 1, current_value = Rest},
  {X, S1}.

do_read_rle(S0, Header) ->
  Count = Header bsr 1,
  S1 = S0#s{current_count = Count},
  read_next_int(S1).

do_read_packed(S0, Header) ->
  NumGroups = Header bsr 1,
  S1 = S0#s{current_count = NumGroups * 8},
  read_next_pack(S1, NumGroups).

read_next_int(S0) ->
  #s{ bit_width = BitWidth
    , current_count = CurrentCount0
    , data = Data0
    } = S0,
  BytesWidth = floor((BitWidth + 7) / 8),
  <<X:(BytesWidth * 8)/little-signed, Data1/binary>> = Data0,
  CurrentCount = max(CurrentCount0 - 1, 0),
  S1 = S0#s{
    current_count = CurrentCount,
    current_value = X,
    data = Data1
  },
  {X, S1}.

read_next_pack(S0, NumGroups) ->
  #s{bit_width = BitWidth
    , current_count = CurrentCount
    , data = Data0
    } = S0,
  {Values0, Data1} =
    lists:mapfoldl(
      fun(_, Acc0) ->
          <<Int:(BitWidth * 8)/little-signed, Acc1/binary>> = Acc0,
          Values = unpack8values(Int, BitWidth),
          {Values, Acc1}
      end,
      Data0,
      lists:seq(1, NumGroups)
     ),
  Values1 = lists:flatten(Values0),
  case Values1 of
    [] ->
      %% Impossible? Corrupt RLE?
      error({no_values_unpacked, CurrentCount, BitWidth, Data0});
    [V | Values2] ->
      S1 = S0#s{
        data = Data1,
        current_count = max(0, CurrentCount - 1),
        current_value = Values2
      },
      {V, S1}
  end.

unpack8values(Int, BitWidth) ->
  OnesMask = floor(math:pow(2, BitWidth) - 1),
  lists:map(
    fun(I) ->
        Shift = I * BitWidth,
        (Int band (OnesMask bsl Shift)) bsr Shift
    end,
    lists:seq(0, 7)
   ).

%% Enum.map_reduce(1..20, :parquer_rle_bp_hybrid_decoder.new(2, <<40, 4, 1>>), fn _, s -> :parquer_rle_bp_hybrid_decoder.read(s) end)

%% Enum.map_reduce(1..20, :parquer_rle_bp_hybrid_decoder.new(1, <<7, 170, 170, 10>>), fn _, s -> :parquer_rle_bp_hybrid_decoder.read(s) end)

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
