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
-module(parquer_rle_bp_hybrid_encoder).

%% API
-export([
    new/1,
    write_int/2,
    to_bytes/1,
    estimate_byte_size/1
]).

%% Debug only
-export([inspect/1]).

-export_type([t/0, int/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(MAX_INT, 4294967295).

%% State
-record(s, {
    bit_packed_group_count = 0,
    bit_width,
    buffered_values_count = 0,
    buffered_values,
    output_acc = [],
    previous_value = 0,
    repeat_count = 0,
    run_acc = []
}).

-type int() :: 0..?MAX_INT.
-type bit_width() :: 0..32.

-opaque t() :: #s{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec new(bit_width()) -> t().
new(BitWidth) when 0 =< BitWidth, BitWidth =< 32 ->
    #s{
        bit_width = BitWidth,
        buffered_values = new_buffered_values()
    }.

-doc """
Adds an integer to the encoder state.  The integer must be non-negative, and fit into 4
bytes.

See also:

  - https://github.com/apache/parquet-java/blob/859eac165b08f927fa14590c33bc5f476405fb68/parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridEncoder.java

  - https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3
""".
-spec write_int(t(), int()) -> t().
write_int(#s{} = S, V) when is_integer(V), V >= 0 ->
    do_write_int(S, V).

-doc """
Finalizes the encoder and returns the encoded values as iodata.
""".
-spec to_bytes(t()) -> iodata().
to_bytes(S0) ->
    #s{
        buffered_values_count = BufferedValuesCount0,
        repeat_count = RepeatCount0
    } = S0,
    HasRun = RepeatCount0 >= 8,
    HasBuffered = BufferedValuesCount0 > 0,
    %% write anything that is buffered / queued up for an rle-run
    S1 =
        case HasRun of
            true ->
                write_rle_run(S0);
            false when HasBuffered ->
                S01 = write_or_append_bit_packed_run(S0),
                end_previous_bit_packed_run(S01);
            false ->
                end_previous_bit_packed_run(S0)
        end,
    S1#s.output_acc.

-spec estimate_byte_size(t()) -> non_neg_integer().
estimate_byte_size(#s{output_acc = OutputAcc, run_acc = RunAcc}) ->
    iolist_size([OutputAcc, RunAcc]).

%%------------------------------------------------------------------------------
%% Debug only
%%------------------------------------------------------------------------------

inspect(#s{} = S) ->
    #s{
        bit_packed_group_count = BitPackedGroupCount,
        bit_width = BitWidth,
        buffered_values_count = BufferedValuesCount,
        buffered_values = BufferedValues,
        output_acc = OutputAcc,
        previous_value = PreviousValue,
        repeat_count = RepeatCount,
        run_acc = RunAcc
    } = S,
    #{
        bit_packed_group_count => BitPackedGroupCount,
        bit_width => BitWidth,
        buffered_values_count => BufferedValuesCount,
        buffered_values => array:to_list(BufferedValues),
        output_acc => OutputAcc,
        previous_value => PreviousValue,
        repeat_count => RepeatCount,
        run_acc => RunAcc
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% See
%% https://github.com/apache/parquet-java/blob/859eac165b08f927fa14590c33bc5f476405fb68/parquet-column/src/main/java/org/apache/parquet/column/values/rle/RunLengthBitPackingHybridEncoder.java
do_write_int(#s{previous_value = V} = S0, V) ->
    #s{repeat_count = RepeatCount0} = S0,
    %%  keep track of how many times we've seen this value consecutively
    RepeatCount = RepeatCount0 + 1,
    S1 = S0#s{repeat_count = RepeatCount},
    case RepeatCount >= 8 of
        true ->
            %% we've seen this at least 8 times, we're certainly going to write an rle-run, so
            %% just keep on counting repeats for now
            S1;
        false ->
            buffer_value(S1, V)
    end;
do_write_int(#s{} = S0, V) ->
    %% This is a new value, check if it signals the end of an rle-run
    S1 =
        case S0#s.repeat_count >= 8 of
            true ->
                %% it does! write an rle-run
                write_rle_run(S0);
            false ->
                S0
        end,
    S2 = S1#s{
        %% this is a new value so we've only seen it once
        repeat_count = 1,
        %% start tracking this value for repeats
        previous_value = V
    },
    buffer_value(S2, V).

buffer_value(S0, V) ->
    %% We have not seen enough repeats to justify an rle-run yet, so buffer this value
    %% in case we decide to write a bit-packed-run
    #s{
        buffered_values_count = BufferedValuesCount0,
        buffered_values = BufferedValues0
    } = S0,
    BufferedValues = array:set(BufferedValuesCount0, V, BufferedValues0),
    BufferedValuesCount = BufferedValuesCount0 + 1,
    S1 = S0#s{
        buffered_values_count = BufferedValuesCount,
        buffered_values = BufferedValues
    },
    case BufferedValuesCount == 8 of
        true ->
            %% we've encountered less than 8 repeated values, so either start a new
            %% bit-packed-run or append to the current bit-packed-run
            write_or_append_bit_packed_run(S1);
        false ->
            S1
    end.

write_or_append_bit_packed_run(S0) ->
    S1 =
        case S0#s.bit_packed_group_count >= 63 of
            true ->
                end_previous_bit_packed_run(S0);
            false ->
                S0
        end,
    #s{
        bit_width = BitWidth,
        bit_packed_group_count = BitPackedGroupCount1,
        buffered_values = BufferedValues,
        run_acc = RunAcc0
    } = S1,
    Packed = pack8values(BufferedValues, BitWidth),
    S1#s{
        run_acc = [RunAcc0, Packed],
        %% empty the buffer, they've all been written
        buffered_values_count = 0,
        buffered_values = new_buffered_values(),
        %% clear the repeat count, as some repeated values may have just been bit packed into
        %% this run
        repeat_count = 0,
        bit_packed_group_count = BitPackedGroupCount1 + 1
    }.

end_previous_bit_packed_run(#s{run_acc = []} = S0) ->
    %% we're not currently in a bit-packed-run
    S0;
end_previous_bit_packed_run(S0) ->
    %% create bit-packed-header, which needs to fit in 1 byte
    #s{
        bit_packed_group_count = BitPackedGroupCount,
        output_acc = OutputAcc0,
        run_acc = RunAcc0
    } = S0,
    BitPackedRunHeader0 = (BitPackedGroupCount bsl 1) bor 1,
    BitPackedRunHeader = <<BitPackedRunHeader0:8>>,
    S0#s{
        %% update this byte
        output_acc = [OutputAcc0 | [BitPackedRunHeader | RunAcc0]],
        %% mark that this run is over
        run_acc = [],
        %% reset the number of groups
        bit_packed_group_count = 0
    }.

-dialyzer([{no_improper_lists, write_rle_run/1}]).
write_rle_run(S0) ->
    %% we may have been working on a bit-packed-run so close that run if it exists before
    %% writing this rle-run
    S1 = end_previous_bit_packed_run(S0),
    #s{
        bit_width = BitWidth,
        output_acc = OutputAcc0,
        previous_value = PreviousValue,
        repeat_count = RepeatCount
    } = S1,
    %% write the rle-header (lsb of 0 signifies a rle run)
    RLEHeader = pack_unsigned_var_int(RepeatCount bsl 1),
    %% write the repeated-value
    EncV = bit_width_padded_value(PreviousValue, BitWidth),
    S1#s{
        output_acc = [OutputAcc0, [RLEHeader | EncV]],
        %% reset the repeat count
        repeat_count = 0,
        %% throw away all the buffered values, they were just repeats and they've been written
        buffered_values_count = 0
    }.

%% Adapted from the `org.apache.parquet.column.values.bitpacking.ByteBitPackingLE` class
%% generated by `ByteBasedBitPackingGenerator`.
pack8values(Ints, BitWidth) ->
    OnesMask = floor(math:pow(2, BitWidth) - 1),
    Res = array:foldl(
        fun(I, V, Acc) ->
            Shift = I * BitWidth,
            ((V band OnesMask) bsl Shift) bor Acc
        end,
        0,
        Ints
    ),
    <<Res:(8 * BitWidth)/little>>.

pack_unsigned_var_int(I) ->
    do_pack_unsigned_var_int(I, <<>>).

do_pack_unsigned_var_int(I, Acc0) ->
    case (I band 16#FFFFFF80) == 0 of
        true ->
            V = I band 16#7F,
            <<Acc0/binary, V>>;
        false ->
            V = (I band 16#7F) bor 16#80,
            Acc = <<Acc0/binary, V>>,
            J = I bsr 7,
            do_pack_unsigned_var_int(J, Acc)
    end.

bit_width_padded_value(V, BitWidth) ->
    BytesWidth = floor((BitWidth + 7) / 8),
    %% Should not be possible since we have a guard when creating a new encoder.
    maybe
        true ?= BytesWidth > 4,
        error({badarg, V, BitWidth})
    end,
    <<V:(BytesWidth * 8)/little>>.

new_buffered_values() ->
    array:new(8, {default, 0}).
