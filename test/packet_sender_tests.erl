-module(packet_sender_tests).

%--- Includes ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-include("types.hrl").


%--- Tests ---------------------------------------------------------------------

simple_roundrobin_test() ->
  Sender = schedule(packet_sender:new(5), [
    {foo, <<"aaaaabbbbbccccc">>},
    {bar, <<"dddddeee">>},
    {buz, <<"fffff">>},
    {biz, <<"ggg">>}
  ]),
  {Result, _Sebnder} = produce(Sender),
  ?assertEqual([
    {foo, 1, 1, <<"aaaaa">>, 0},
    {bar, 1, 1, <<"ddddd">>, 0},
    {buz, 1, 1, <<"fffff">>, ?PKT_FLAG_FINAL},
    {biz, 1, 1, <<"ggg">>, ?PKT_FLAG_FINAL},
    {foo, 2, 2, <<"bbbbb">>, 0},
    {bar, 2, 2, <<"eee">>, ?PKT_FLAG_FINAL},
    {foo, 3, 3, <<"ccccc">>, ?PKT_FLAG_FINAL}
  ], Result),
  ok.


multiple_packets_roundrobin_test() ->
  Sender = schedule(packet_sender:new(5), [
    {foo, <<"aaaaabbbbbccc">>},
    {bar, <<"dddddeeeee">>},
    {foo, <<"fff">>},
    {foo, <<"ggggghhhhh">>},
    {bar, <<"">>},
    {bar, <<"iiiiijjjjj">>},
    {bar, <<"kkk">>}
  ]),
  {Result, _Sebnder} = produce(Sender),
  ?assertEqual([
    {foo, 1, 1, <<"aaaaa">>, 0},
    {bar, 1, 1, <<"ddddd">>, 0},
    {foo, 2, 2, <<"bbbbb">>, 0},
    {bar, 2, 2, <<"eeeee">>, ?PKT_FLAG_FINAL},
    {foo, 3, 3, <<"ccc">>, ?PKT_FLAG_FINAL},
    {bar, 3, 1, <<"">>, ?PKT_FLAG_FINAL},
    {foo, 4, 1, <<"fff">>, ?PKT_FLAG_FINAL},
    {bar, 4, 1, <<"iiiii">>, 0},
    {foo, 5, 1, <<"ggggg">>, 0},
    {bar, 5, 2, <<"jjjjj">>, ?PKT_FLAG_FINAL},
    {foo, 6, 2, <<"hhhhh">>, ?PKT_FLAG_FINAL},
    {bar, 6, 1, <<"kkk">>, ?PKT_FLAG_FINAL}
  ], Result),
  ok.


%--- Internal Functions --------------------------------------------------------

schedule(Sender, []) -> Sender;

schedule(Sender, [{Ch, Msg} | Rest]) ->
  schedule(packet_sender:schedule(Ch, Msg, Sender), Rest).


produce(Sender) -> produce(Sender, []).


produce(Sender, Acc) ->
  case packet_sender:next(Sender) of
    {empty, NewSender} -> {lists:reverse(Acc), NewSender};
    {Packet, NewSender} ->
      produce(NewSender, [packet_codec:decode(Packet) | Acc])
  end.