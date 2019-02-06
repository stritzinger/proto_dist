-module(packet_sender).

%--- Includes ------------------------------------------------------------------

-include("types.hrl").


%--- Exports -------------------------------------------------------------------

-export([new/0, new/1]).
-export([schedule/3]).
-export([next/1]).


%--- Macros --------------------------------------------------------------------

-define(ST, #?MODULE).

-define(DEFAULT_PKT_SIZE, 448).


%--- Records -------------------------------------------------------------------

-record(?MODULE, {
  max_size = ?DEFAULT_PKT_SIZE :: pos_integer(),
  % Sequence numbers
  seq_nums = #{} :: #{channel() => seq_num()},
  % Round-robin index
  head = [] :: list(channel()),
  tail = [] :: list(channel()),
  % Pending packet buffers per channel
  buffs = #{} :: #{channel() => buffer()}
}).


%--- Types ---------------------------------------------------------------------

-type t() :: ?ST{}.
-type buffer() :: {frag_idx() | undefined, binary(), queue:queue()}.


%--- API Functions -------------------------------------------------------------

-spec new() -> St when St :: t().

new() -> ?ST{}.


-spec new(MaxSize) -> St
  when MaxSize :: pos_integer(), St :: t().

new(MaxSize) -> ?ST{max_size = MaxSize}.


-spec schedule(Ch, Message, St) -> St
  when Ch :: channel(), Message :: binary(), St :: t().

schedule(Ch, Message, ?ST{tail = Tail, buffs = Buffs} = St) ->
  % io:format(standard_error, "--> ~p / ~b~n", [Ch, byte_size(Message)]),
  case maps:find(Ch, Buffs) of
    error ->
      % If we add a new channel we need to add it to the scheduling tail
      St?ST{tail = [Ch | Tail], buffs = Buffs#{Ch => new_buffer(Message)}};
    {ok, Buff} ->
      St?ST{buffs = Buffs#{Ch := push_message(Message, Buff)}}
  end.


-spec next(St) -> {empty, St} | {Packet, St}
  when Packet :: binary(), St :: t().

next(?ST{head = [], tail = []} = St) -> {empty, St};

next(?ST{head = [], tail = Tail} = St) ->
  next(St?ST{head = lists:reverse(Tail), tail = []});

next(?ST{head = [Ch | Head], tail = Tail, buffs = Buffs} = St) ->
  case maps:find(Ch, Buffs) of
    error ->
      next(St?ST{head = Head});
    {ok, Buff} ->
      case next_fragment(St?ST.max_size, Buff) of
        {empty, _NewBuff} ->
          % No more data for this channel
          NewBuffs = maps:remove(Ch, Buffs),
          next(St?ST{head = Head, buffs = NewBuffs});
        {FragIdx, Data, Flags, NewBuff} ->
          {SeqNum, St2} = next_seq_num(Ch, St),
          St3 = St2?ST{head = Head, tail = [Ch | Tail],
                       buffs = Buffs#{Ch := NewBuff}},
          % io:format(standard_error, "> ~p / ~5b ~4b ~3b~n", [{Ch, SeqNum, FragIdx, byte_size(Data)}]),
          {packet_codec:encode(Ch, SeqNum, FragIdx, Data, Flags), St3}
      end
  end.


%--- Internal Functions --------------------------------------------------------

-spec new_buffer(Message) -> Buff
  when Message :: binary(), Buff :: buffer().

new_buffer(Message) ->
  {1, Message, queue:new()}.


-spec push_message(Message, Buff) -> Buff
  when Message :: binary(), Buff :: buffer().

push_message(Message, {FragIdx, Data, Queue}) ->
  {FragIdx, Data, queue:in(Message, Queue)}.


-spec next_fragment(Size, Buff) -> {empty, Buff} | {FragIdx, Data, Flags, Buff}
  when Size :: pos_integer(), Buff :: buffer(), FragIdx :: frag_idx(),
       Data :: binary(), Flags :: packet_codec:flags().

next_fragment(Size, {undefined, _, Queue}) ->
  case queue:out(Queue) of
    {{value, Packet}, NewQueue} ->
      next_fragment(Size, {1, Packet, NewQueue});
    {empty, NewQueue} ->
      {empty, {undefined, <<>>, NewQueue}}
  end;

next_fragment(Size, {FragIdx, Data, Queue}) ->
  case Data of
    <<Frag:Size/binary>> ->
      {FragIdx, Frag, ?PKT_FLAG_FINAL, {undefined, <<>>, Queue}};
    <<Frag:Size/binary, Rest/binary>> ->
      {FragIdx, Frag, 0, {FragIdx + 1, Rest, Queue}};
    Frag ->
      {FragIdx, Frag, ?PKT_FLAG_FINAL, {undefined, <<>>, Queue}}
  end.


-spec next_seq_num(Ch, St) -> {SeqNum, St}
  when Ch :: channel(), St :: t(), SeqNum :: seq_num().

next_seq_num(Ch, ?ST{seq_nums = SeqNums} = St) ->
  case maps:find(Ch, SeqNums) of
    error -> {1, St?ST{seq_nums = SeqNums#{Ch => 2}}};
    {ok, SeqNum} -> {SeqNum, St?ST{seq_nums = SeqNums#{Ch := SeqNum + 1}}}
  end.
