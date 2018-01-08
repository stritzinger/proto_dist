-module(packet_codec_tests).

%--- Includes ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-include("types.hrl").


%--- Tests ---------------------------------------------------------------------

identity_test() ->
  assert_identity(foo, 1, 1, <<"spam">>, 0),
  assert_identity({self(), self()}, 4218, 42, <<"spam">>, ?PKT_FLAG_FINAL),
  ok.

empty_packet_test() ->
  ?assertEqual(<<>> , packet_codec:decode(<<>>)).


%--- Internal Functions --------------------------------------------------------

assert_identity(Ch, SeqNum, FragIdx, Data, Flags) ->
  PacketTup = {Ch, SeqNum, FragIdx, Data, Flags},
  Packet = packet_codec:encode(PacketTup),
  ?assertEqual(PacketTup, packet_codec:decode(Packet)).
