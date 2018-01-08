-module(packet_channel).

%--- Includes ------------------------------------------------------------------

-include("types.hrl").


%--- Exports -------------------------------------------------------------------

-export([channel/1]).


%--- Macros --------------------------------------------------------------------

-define(DEFAULT_CHANNEL, oob).

-define(DOP_LINK, 1).
-define(DOP_SEND, 2).
-define(DOP_EXIT, 3).
-define(DOP_UNLINK, 4).
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


%--- API Functions -------------------------------------------------------------

-spec channel(Message) -> Ch
  when Message :: binary(), Ch :: channel().

channel(<<112, Data/binary>>) ->
  CtrlMsg = binary_to_term(Data),
  ctrl_to_channel(CtrlMsg);

channel(_Message) -> ?DEFAULT_CHANNEL.


%--- Internal Functions --------------------------------------------------------

ctrl_to_channel({?DOP_SEND, _, _}) ->
  exit({dist_message_not_supported, ?DOP_SEND});

ctrl_to_channel({?DOP_SEND_TT, _, _, _, _}) ->
  exit({dist_message_not_supported, ?DOP_SEND_TT});

ctrl_to_channel({?DOP_LINK, FromPid, ToPid}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_EXIT, FromPid, ToPid, _}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_UNLINK, FromPid, ToPid}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_GROUP_LEADER, FromPid, ToPid}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_EXIT2, FromPid, ToPid, _}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_EXIT_TT, FromPid, ToPid, _, _}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_EXIT2_TT, FromPid, ToPid, _, _}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_MONITOR_P, FromPid, ToProc, _}) -> {FromPid, ToProc};

ctrl_to_channel({?DOP_DEMONITOR_P, FromPid, ToProc, _}) -> {FromPid, ToProc};

ctrl_to_channel({?DOP_MONITOR_P_EXIT, FromProc, ToPid, _}) -> {FromProc, ToPid};

ctrl_to_channel({?DOP_SEND_SENDER, FromPid, ToPid}) -> {FromPid, ToPid};

ctrl_to_channel({?DOP_SEND_SENDER_TT, FromPid, ToPid, _}) -> {FromPid, ToPid};

ctrl_to_channel(_) -> ?DEFAULT_CHANNEL.
