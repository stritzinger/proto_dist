-module(packet_codec).

%--- Includes ------------------------------------------------------------------

-include("types.hrl").


%--- Exports -------------------------------------------------------------------

-export([encode/1, encode/5]).
-export([decode/1]).


%--- Types ---------------------------------------------------------------------

-type flags() :: non_neg_integer().
-type packet() :: binary().
-type packet_tuple() :: {channel(), seq_num(), frag_idx(), binary(), flags()}.

-export_type([flags/0, packet/0, packet_tuple/0]).


%--- API Functions -------------------------------------------------------------

-spec encode(Ch, SeqNum, FragIdx, Data, Flags) -> Packet
  when Ch :: channel(), SeqNum :: seq_num(), FragIdx :: frag_idx(),
       Data :: binary(), Flags :: flags(), Packet :: packet().

encode(Ch, SeqNum, FragIdx, Data, Flags) ->
  ChBin = term_to_binary(Ch),
  ChBinSize = byte_size(ChBin),
  <<Flags:8/big-unsigned-integer,
    SeqNum:32/big-unsigned-integer,
    FragIdx:16/big-unsigned-integer,
    ChBinSize:8/big-unsigned-integer,
    ChBin/binary,
    Data/binary>>.


-spec encode(PacketTuple) -> Packet
  when PacketTuple :: packet_tuple(), Packet :: packet().

encode({Ch, SeqNum, FragIdx, Data, Flags}) ->
  encode(Ch, SeqNum, FragIdx, Data, Flags).


-spec decode(Packet) -> PacketTuple | <<>>
  when PacketTuple :: packet_tuple(), Packet :: packet().

decode(<<>>) -> <<>>;

decode(<<Flags:8/big-unsigned-integer,
         SeqNum:32/big-unsigned-integer,
         FragIdx:16/big-unsigned-integer,
         ChBinSize:8/big-unsigned-integer,
         ChBin:ChBinSize/binary,
         Data/binary>>) ->
  Ch = binary_to_term(ChBin),
  {Ch, SeqNum, FragIdx, Data, Flags}.
