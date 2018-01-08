-module(packet_tests).

%--- Includes ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-include("types.hrl").


%--- Tests ---------------------------------------------------------------------

identity_test_() ->
  Messages = random_messages(100, [foo, bar, buz, biz, boz], 1, 20),
  [ fun() -> check_identity(3, Messages) end,
    fun() -> check_identity(5, Messages) end,
    fun() -> check_identity(10, Messages) end
  ].


randomized_identity_test_() ->
  Messages = random_messages(100, [foo, bar, buz, biz, boz], 1, 20),
  [ fun() -> check_randomized_identity(3, Messages) end,
    fun() -> check_randomized_identity(5, Messages) end,
    fun() -> check_randomized_identity(10, Messages) end
  ].


%--- Internal Functions --------------------------------------------------------

check_identity(Size, Messages) ->
  Sender = packet_sender:new(Size),
  Receiver = packet_receiver:new(),
  {Packets, Expected} = feed(Sender, Messages, [], #{}),
  {_, Result} = consume(Receiver, Packets, [], #{}),
  ?assertEqual(Expected, Result).


check_randomized_identity(Size, Messages) ->
  Sender = packet_sender:new(Size),
  Receiver = packet_receiver:new(),
  {Packets, Expected} = feed(Sender, Messages, [], #{}),
  Rand = [P || {_, P} <- lists:sort([{rand:uniform(), P} || P <- Packets])],
  {_, Result} = consume(Receiver, Rand, [], #{}),
  ?assertEqual(Expected, Result).


random_message(Min, Max) ->
  Size = rand:uniform(Max - Min + 1) + Min - 1,
  crypto:strong_rand_bytes(Size).


random_channel(Options) ->
  Idx = rand:uniform(length(Options)),
  lists:nth(Idx, Options).


random_messages(Count, ChOpts, Min, Max) ->
  random_messages(Count, ChOpts, Min, Max, []).


random_messages(0, _ChOpts, _Min, _Max, Acc) -> Acc;

random_messages(Count, ChOpts, Min, Max, Acc) ->
  Msg = {random_channel(ChOpts), random_message(Min, Max)},
  random_messages(Count - 1, ChOpts, Min, Max, [Msg | Acc]).


feed(_Sender, [], Acc, Expected) ->
  {lists:reverse(Acc), maps:map(fun(_, L) -> lists:reverse(L) end, Expected)};

feed(Sender, [{Ch, Msg} | Rest], Acc, Expected) ->
  Sender1 = packet_sender:schedule(Ch, Msg, Sender),
  {NewAcc, Sender2}  = gather_packets(Sender1, Acc),
  NewExpected = maps:update_with(Ch, fun(L) -> [Msg | L] end, [Msg], Expected),
  feed(Sender2, Rest, NewAcc, NewExpected).


gather_packets(Sender, Acc) ->
  case packet_sender:next(Sender) of
    {empty, NewSender} -> {Acc, NewSender};
    {Packet, NewSender} -> gather_packets(NewSender, [Packet | Acc])
  end.


consume(_Receiver, [], Acc, Result) ->
  {lists:reverse(Acc), maps:map(fun(_, L) -> lists:reverse(L) end, Result)};

consume(Receiver, [Packet | Rest], Acc, Result) ->
  {Messages, NewReceiver} = packet_receiver:collect(Packet, Receiver),
  {NewAcc, NewResult} = merge_messages(Messages, Acc, Result),
  consume(NewReceiver, Rest, NewAcc, NewResult).


merge_messages([], Acc, Result) -> {Acc, Result};

merge_messages([{Ch, Msg} | Rest], Acc, Result) ->
  NewAcc = [Msg | Acc],
  NewResult = maps:update_with(Ch, fun(L) -> [Msg | L] end, [Msg], Result),
  merge_messages(Rest, NewAcc, NewResult).
