-module(gen_udp_dist).

-behaviour(gen_dist).

% Callbacks
-export([acceptor_init/0]).
-export([acceptor_info/2]).
-export([acceptor_controller_approved/3]).
-export([acceptor_terminate/1]).
-export([setup/2]).
-export([controller_init/1]).
-export([controller_send/2]).
-export([controller_recv/3]).
-export([controller_done/2]).
-export([output_init/1]).
-export([output_send/2]).
-export([input_init/1]).
-export([input_info/2]).
-export([tick_init/1]).
-export([tick_trigger/1]).

-define(time,
    erlang:convert_time_unit(erlang:monotonic_time()-erlang:system_info(start_time), native, microsecond)
).
-define(display(Term), ok).
% -define(display(Term), erlang:display({?time, self(), ?MODULE, ?FUNCTION_NAME, Term})).

%--- Callbacks -----------------------------------------------------------------

% Acceptor

acceptor_init() ->
    {ok, ListenSocket} = gen_udp:open(0, [binary, {active, true}]),
    {ok, _Port} = inet:port(ListenSocket),
    ?display({socket, ListenSocket, _Port}),
    {ok, Address} = inet:sockname(ListenSocket),
    {ok, {udp, inet, Address}, ListenSocket}.

acceptor_info({udp, Socket, SrcAddress, SrcPort, <<"hello\n">>}, Socket) ->
    ?display({acceptor, {got_hello, SrcAddress, SrcPort}}),
    ID = {SrcAddress, SrcPort},
    {spawn_controller, {connect, ID}, Socket};
acceptor_info(_Other, Socket) ->
    ?display({unknown_msg, _Other}),
    {ok, Socket}.

acceptor_controller_approved({connect, ID}, Port, ListenSocket) ->
    ?display({controller_approved, ID, Port, ListenSocket}),
    send(ListenSocket, ID, <<Port:16>>),
    ok.

acceptor_terminate(Socket) -> ok = gen_udp:close(Socket).

% Outgoing

setup(Name, RemoteHost) ->
    RemotePort = port(Name, ip(RemoteHost)),
    {spawn_controller, {setup, RemoteHost, RemotePort}}.

% Controller

controller_init({setup, RemoteHost, RemotePort}) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    ok = gen_udp:send(Socket, RemoteHost, RemotePort, <<"hello\n">>),
    {ok, {_IP, _MyPort}} = inet:sockname(Socket),
    ?display({waiting_for_port, Socket, _IP, _MyPort}),
    ID = case gen_udp:recv(Socket, 2, 5000) of
        {ok, {SrcAddress, RemotePort, <<ControllerPort:16>>}} ->
            {SrcAddress, ControllerPort};
        Error ->
            ?display({error, Error}),
            error({could_not_get_remote_port, Error})
    end,
    {reply, ok, {ID, Socket}};
controller_init({connect, ID}) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    {ok, {_IP, Port}} = inet:sockname(Socket),
    {reply, Port, {ID, Socket}}.

controller_send(Packet, {ID, Socket} = State) ->
    send(Socket, ID, Packet),
    {ok, State}.

controller_recv(Length, Timeout, {{IP, Port}, Socket} = State) ->
    ?display({recv, Length, Timeout, State}),
    {ok, {IP, Port, Data}} = gen_udp:recv(Socket, Length, Timeout),
    {ok, Data, State}.

controller_done(InputHandler, {_ID, Socket} = State) ->
    ?display({done, InputHandler, State}),
    ok = gen_udp:controlling_process(Socket, InputHandler),
    {ok, State}.

% Output

output_init({ID, Socket}) ->
    ?display({output_init, {ID, Socket}}),
    Snd = packet_sender:new(),
    {ok, {ID, Socket, Snd}}.

output_send(Data, {ID, Socket, Snd0}) ->
    ?display({output_send, byte_size(Data)}),
    Ch = packet_channel:channel(Data),
    Snd1 = packet_sender:schedule(Ch, Data, Snd0),
    Snd2 = send_all(Socket, ID, Snd1),
    {ok, {ID, Socket, Snd2}}.

% Input

input_init({ID, Socket}) ->
    Rcv = packet_receiver:new(),
    ?display({input_init, ID, Socket, Rcv}),
    ok = inet:setopts(Socket, [{active, true}]),
    {ok, {ID, Socket, Rcv}}.

input_info({udp, Socket, _SrcAddress, _SrcPort, <<"tick\n">>}, {_, Socket, _} = State) ->
    ?display({got_tick, _SrcAddress, _SrcPort, <<"tick\n">>}),
    {noreply, State};
input_info({udp, Socket, _SrcAddress, _SrcPort, Data}, {ID, Socket, Rcv}) ->
    case packet_receiver:collect(Data, Rcv) of
        {[], R} ->
            {noreply, {ID, Socket, R}};
        {Messages, R} ->
            AllData = lists:map(fun({_, D}) -> D end, Messages),
            {reply, AllData, {ID, Socket, R}}
    end.

tick_init({ID, Socket}) ->
    {ok, {ID, Socket}}.

tick_trigger({ID, Socket}) ->
    send(Socket, ID, <<"tick\n">>),
    {ok, {ID, Socket}}.

%--- Internal ------------------------------------------------------------------

ip(Host) ->
    case inet:getaddr(Host, inet) of
        {ok, Result} -> Result;
        Error        -> error({inet_getaddr, Error})
    end.

port(Name, IP) ->
    Epmd = net_kernel:epmd_module(),
    case Epmd:port_please(Name, IP) of
        {port, TcpPort, 5} ->
            ?display({port_please, Name, TcpPort}),
            TcpPort;
        Error ->
            error({epmd_error, Error})
    end.

send(Socket, {IP, Port}, Data) ->
    ?display({send, Socket, {IP, Port}, byte_size(iolist_to_binary(Data))}),
    ok = gen_udp:send(Socket, IP, Port, Data).

send_all(Socket, ID, Snd) ->
    case packet_sender:next(Snd) of
        {empty, S} ->
            ?display({send_all, empty, 0}),
            S;
        {ToSend, S} ->
            ?display({send_all, byte_size(ToSend)}),
            send(Socket, ID, ToSend),
            send_all(Socket, ID, S)
    end.
