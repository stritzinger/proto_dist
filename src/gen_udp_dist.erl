-module(gen_udp_dist).

-behaviour(gen_dist).

% Callbacks
-export([acceptor_init/0]).
-export([acceptor_info/2]).
-export([acceptor_controller_spawned/3]).
-export([acceptor_controller_approved/3]).
-export([acceptor_terminate/1]).
-export([controller_init/1]).
-export([controller_send/2]).
-export([controller_recv/3]).

-define(time,
    erlang:convert_time_unit(erlang:monotonic_time()-erlang:system_info(start_time), native, microsecond)
).
% -define(display(Term), ok).
-define(display(Term), erlang:display({?time, self(), ?MODULE, ?FUNCTION_NAME, Term})).

-define(DEBUG(Args, Body),
    ?display({'CALL', Args}),
    VVVVVValue = Body,
    ?display({'RETURN', VVVVVValue}),
    VVVVVValue
).

%--- Callbacks -----------------------------------------------------------------

% Acceptor

acceptor_init() ->
    ?DEBUG([], begin
    {ok, ListenSocket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, _Port} = inet:port(ListenSocket),
    ?display({socket, ListenSocket, _Port}),
    {ok, Address} = inet:sockname(ListenSocket),
    {ok, {udp, inet, Address}, ListenSocket}
    end).

acceptor_info({udp, Socket, SrcAddress, SrcPort, <<"hello\n">>} = Msg, Socket) ->
    ?DEBUG([Msg, Socket], begin
    ?display({acceptor, {got_hello, SrcAddress, SrcPort}}),
    ID = {SrcAddress, SrcPort},
    {ok, CtrlSocket} = gen_udp:open(0, [binary, {active, false}]),
    {spawn_controller, {ID, CtrlSocket}, Socket}
    end);
acceptor_info(_Other, Socket) ->
    ?display({unknown_msg, _Other}),
    {ok, Socket}.

acceptor_controller_spawned({_ID, CtrlSocket} = _State, Pid, ListenSocket) ->
    ?DEBUG([_State, Pid, ListenSocket], begin
    ok = gen_udp:controlling_process(CtrlSocket, Pid),

    Pid ! {self(), {socket, CtrlSocket}},
    receive {Pid, ok} -> ok end,

    ok
    end).

acceptor_controller_approved({ID, CtrlSocket} = _State, _Pid, ListenSocket) ->
    ?DEBUG([_State, _Pid, ListenSocket], begin
    {ok, {_IP, Port}} = inet:sockname(CtrlSocket),
    send(ListenSocket, ID, <<Port:16>>),
    ok
    end).

acceptor_terminate(Socket) ->
    ?DEBUG([Socket], begin
    ok = gen_udp:close(Socket)
    end).

% Controller

controller_init({ID, Socket} = Arg) ->
    ?DEBUG([Arg], begin

    receive
        {Acceptor, {socket, Socket}} ->
            Acceptor ! {self(), ok}
    end,

    TickFun = fun() -> send(Socket, ID, <<"tick\n">>) end,
    {ok, TickFun, Arg}
    end).

controller_send(Packet, {ID, Socket} = State) ->
    ?DEBUG([Packet, State], begin
    send(Socket, ID, Packet),
    {ok, State}
    end).

controller_recv(Length, Timeout, {{IP, Port}, Socket} = State) ->
    ?DEBUG([Length, Timeout, State], begin
    {ok, {IP, Port, Data}} = gen_udp:recv(Socket, Length, Timeout),
    {ok, Data, State}
    end).

%--- Internal ------------------------------------------------------------------

send(Socket, {IP, Port}, Data) ->
    ?display({send, Socket, {IP, Port}, Data}),
    ok = gen_udp:send(Socket, IP, Port, Data).
