-module(packet_receiver).

%--- Includes ------------------------------------------------------------------

-include("types.hrl").


%--- Exports -------------------------------------------------------------------

-export([new/0]).
-export([collect/2]).


%--- Macros --------------------------------------------------------------------

-define(ST, #?MODULE).

-define(DOP_LINK, 1).
-define(DOP_SEND, 2).
-define(DOP_EXIT, 3).
-define(DOP_UNLINK, 4).
-define(DOP_NODE_LINK, 5).
-define(DOP_REG_SEND, 6).
-define(DOP_GROUP_LEADER, 7).
-define(DOP_EXIT2, 8).
-define(DOP_SEND_TT, 12).
-define(DOP_EXIT_TT, 13).
-define(DOP_REG_SEND_TT, 16).
-define(DOP_EXIT2_TT, 18).
-define(DOP_MONITOR_P, 19).
-define(DOP_DEMONITOR_P, 20).
-define(DOP_MONITOR_P_EXIT, 21).
-define(DOP_SEND_SENDER, 22).
-define(DOP_SEND_SENDER_TT, 23).


%--- Records -------------------------------------------------------------------

-record(?MODULE, {
  packets = #{} :: #{channel() => packet_state()},
  fragments = #{} :: #{channel() => fragment_buffer()}
}).


%--- Types ---------------------------------------------------------------------

-type t() :: ?ST{}.
-type packet_buffer() :: #{seq_num() => packet_codec:packet_tuple()}.
-type packet_state() :: {Next :: seq_num(), Max :: seq_num(), packet_buffer()}.
-type fragment_buffer() :: list(binary()).


%--- API Functions -------------------------------------------------------------

-spec new() -> St when St :: t().

new() -> ?ST{}.


-spec collect(Packet, St) -> {list({Ch, Message}), St}
  when Packet :: binary(), Ch :: channel(), Message :: binary(), St :: t().

collect(Packet, St) ->
  case packet_codec:decode(Packet) of
    <<>> -> {[<<>>], St}; % Empty packet is allowed for ticks
    PacketTup ->
      % {Ch, SeqNum, FragIdx, Data, _} = PacketTup,
      % io:format(standard_error, "<.  ~p / ~5b ~4b ~3b~n", [Ch, SeqNum, FragIdx, byte_size(Data)]),
      {Packets, St1} = collect_packets(PacketTup, St),
      join_fragments(Packets, St1)
      % {X, St2} = join_fragments(Packets, St1),
      % lists:foreach(fun({C, Message}) ->
      %   io:format(standard_error, "<-- ~p / ~b~n", [C, byte_size(Message)])
      % end, X),
      % {X, St2}
  end.

%--- Internal Functions --------------------------------------------------------

-spec collect_packets(PacketTup, St) -> {[PacketTup], St}
  when PacketTup :: packet_codec:packet_tuple(), St :: t().

collect_packets({Ch, 1, _, _, _} = PacketTup, ?ST{packets = Buffs} = St) ->
  case maps:find(Ch, Buffs) of
    {ok, {1, Max, Buff}} ->
      {Packets, LastSeqNum, NewBuff} = take_packets(2, Max, Buff),
      NewMax = max(LastSeqNum, Max),
      St1 = St?ST{packets = Buffs#{Ch := {LastSeqNum + 1, NewMax, NewBuff}}},
      {[PacketTup | Packets], St1};
    error ->
      {[PacketTup], St?ST{packets = Buffs#{Ch => {2, 1, #{}}}}}
  end;

collect_packets({Ch, SeqNum, _, _, _} = PacketTup, ?ST{packets = Buffs} = St) ->
  case maps:find(Ch, Buffs) of
    {ok, {Next, Max, Buff}} when SeqNum =:= Next ->
      {Packets, LastSeqNum, NewBuff} = take_packets(SeqNum + 1, Max, Buff),
      NewMax = max(LastSeqNum, Max),
      St1 = St?ST{packets = Buffs#{Ch := {LastSeqNum + 1, NewMax, NewBuff}}},
      {[PacketTup | Packets], St1};
    {ok, {Next, Max, Buff}} ->
      NewMax = max(SeqNum, Max),
      NewBuff = Buff#{SeqNum => PacketTup},
      {[], St?ST{packets = Buffs#{Ch := {Next, NewMax, NewBuff}}}};
    error ->
      {[], St?ST{packets = Buffs#{Ch => {1, SeqNum, #{SeqNum => PacketTup}}}}}
  end.


-spec take_packets(From, To, Buff) -> {[PacketTup], Last, Buff}
  when From :: seq_num(), To :: seq_num(), Last :: seq_num(),
       PacketTup :: packet_codec:packet_tuple(), Buff :: packet_buffer().

take_packets(From, To, Buff) -> take_packets(From, To, Buff, []).


-spec take_packets(From, To, Buff, Acc) -> {[PacketTup], Last, Buff}
  when From :: seq_num(), To :: seq_num(), Last :: seq_num(),
       PacketTup :: packet_codec:packet_tuple(), Buff :: packet_buffer(),
       Acc :: list(PacketTup).

take_packets(From, To, Buff, Acc) when From =< To ->
  case maps:take(From, Buff) of
    {Packet, NewBuff} -> take_packets(From + 1, To, NewBuff, [Packet | Acc]);
    error -> {lists:reverse(Acc), From - 1, Buff}
  end;

take_packets(From, _To, Buff, Acc) ->
  {lists:reverse(Acc), From - 1, Buff}.


-spec join_fragments([PacketTup], St) -> {[MsgTup], St}
  when PacketTup :: packet_codec:packet_tuple(), MsgTup :: {Ch, Message},
       Ch :: channel(), Message :: binary(), St :: t().

join_fragments(Packets, St) -> join_fragments(Packets, St, []).


-spec join_fragments([PacketTup], St, Acc) -> {[MsgTup], St}
  when PacketTup :: packet_codec:packet_tuple(), MsgTup :: {Ch, Message},
       Ch :: channel(), Message :: binary(), St :: t(), Acc :: list(MsgTup).

join_fragments([], St, Acc) -> {lists:reverse(Acc), St};

join_fragments([{Ch, _, _, Data, ?PKT_FLAG_FINAL} | Rest],
               ?ST{fragments = Buffs} = St, Acc) ->
  case maps:take(Ch, Buffs) of
    {Buff, NewBuffs} ->
      Message = iolist_to_binary(lists:reverse([Data | Buff])),
      join_fragments(Rest, St?ST{fragments = NewBuffs}, [{Ch, Message} | Acc]);
    error ->
      join_fragments(Rest, St, [{Ch, Data} | Acc])
  end;

join_fragments([{Ch, _, _, Data, _} | Rest],
               ?ST{fragments = Buffs} = St, Acc) ->
  case maps:find(Ch, Buffs) of
    {ok, Buff} ->
      join_fragments(Rest, St?ST{fragments = Buffs#{Ch := [Data | Buff]}}, Acc);
    error ->
      join_fragments(Rest, St?ST{fragments = Buffs#{Ch => [Data]}}, Acc)
  end.
