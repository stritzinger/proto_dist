-module(proto_udp_dist).

% Callbacks
-export([listen/1]).
-export([accept/1]).
-export([accept_connection/5]).
-export([setup/5]).
-export([close/1]).
-export([select/1]).
% OPTIONAL:
% -export([setopts/2]).
% -export([getopts/2]).

% Includes
-include_lib("kernel/include/net_address.hrl").
-include_lib("kernel/include/dist_util.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%--- Macros --------------------------------------------------------------------

-define(time,
    erlang:convert_time_unit(erlang:monotonic_time()-erlang:system_info(start_time), native, microsecond)
).
% -define(display(Term), ok).
-define(display(Term), erlang:display({?time, node(), self(), ?FUNCTION_NAME, Term})).

%% In order to avoid issues with lingering signal binaries
%% we enable off-heap message queue data as well as fullsweep
%% after 0. The fullsweeps will be cheap since we have more
%% or less no live data.
-define(CONTROLLER_SPAWN_OPTS, [
    {message_queue_data, off_heap},
    {fullsweep_after, 0}
]).
-define(CONTROLLER_ACTIVE_BUF, 10).

%--- Callbacks -----------------------------------------------------------------


-spec listen(atom()) -> {ok, {term(), #net_address{}, 1..3}} | {error, term()}.
listen(Name) ->
    ?display({enter, [Name]}),
    Acceptor = acceptor_spawn(),
    {_IP, Port} = Address = acceptor_get_address(Acceptor),
    {ok, Host} = inet:gethostname(),
    {ok, Creation} = (net_kernel:epmd_module()):register_node(Name, Port),
    NetAddress = #net_address{
        address = Address,
        host = Host,
        protocol = udp,
        family = inet
    },
    {ok, {Acceptor, NetAddress, Creation}}.

-spec accept(term()) -> pid().
accept(Acceptor) ->
    ?display({enter, [Acceptor]}),
    acceptor_listen(Acceptor),
    Acceptor.

-spec accept_connection(pid(), term(), node(), term(), term()) -> pid().
accept_connection(AcceptPid, DistCtrl, MyNode, Allowed, SetupTime) ->
    ?display({enter, [AcceptPid, DistCtrl, MyNode, Allowed, SetupTime]}),
    Kernel = self(),
    Pid = spawn_opt(fun() ->
        do_accept(Kernel, AcceptPid, DistCtrl, MyNode, Allowed, SetupTime)
    end, [link, {priority, max}]),
    dbg:tracer(),
    dbg:p(all, messages),
    Pid.

-spec setup(node(), term(), node(), longnames | shortnames, term()) -> pid().
setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    Kernel = self(),
    spawn_opt(fun() ->
        do_setup(Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime)
    end, [link, {priority, max}]).

-spec close(term()) -> no_return().
close(Socket) ->
    gen_udp:close(Socket).

-spec select(node()) -> boolean().
select(_NodeName) ->
    % TODO: Implement a real select check. Use what's in gen_tcp_dist?
    true.

do_setup(Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    ?display({enter, [Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime]}),
    [Name, Host] = split_node(Node ,LongOrShortNames),
    case inet:getaddr(Host, inet) of
        {ok, IP} ->
            ?display({inet, getaddr, [Host, inet], {ok, IP}}),
            Timer = dist_util:start_timer(SetupTime),
            Epmd = net_kernel:epmd_module(),
            case Epmd:port_please(Name, IP) of
                {port, TcpPort, Version} ->
                    ?display({port_please, Node, #{tcp_port => TcpPort, version => Version}}),
                    dist_util:reset_timer(Timer),
                    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
                    ok = gen_udp:send(Socket, Host, TcpPort, <<"hello\n">>),
                    {Address, Port} = case gen_udp:recv(Socket, 2, 5000) of
                        {ok, {SrcAddress, TcpPort, <<PortNumber:16>>}} ->
                            {SrcAddress, PortNumber};
                        Error ->
                            ?display({error, Error}),
                            death_row(Error)
                    end,
                    ID = {Address, Port},
                    Controller = controller_spawn(ID, Socket),
                    _ControllerPort = controller_set_supervisor(Controller, self()),
                    HSData0 = hs_data(Controller),
                    HSData = HSData0#hs_data{
                        kernel_pid = Kernel,
                        other_node = Node,
                        this_node = MyNode,
                        socket = Controller, % socket == controlling process
                        timer = Timer,
                        this_flags = 0,
                        other_version = Version,
                        request_type = Type
                    },
                    dist_util:handshake_we_started(HSData);
                _Error ->
                    ?display({error, {Epmd, port_please, [Name, IP], _Error}}),
                    ?shutdown(Node)
            end;
        _Other ->
            ?display({error, {inet, getaddr, [Host, inet], _Other}}),
            ?shutdown(Node)
    end.

do_accept(Kernel, Acceptor, Controller, MyNode, Allowed, SetupTime) ->
    ?display({enter, [Kernel, Acceptor, Controller, MyNode, Allowed, SetupTime]}),
    receive
        {Acceptor, controller} ->
            ?display({do_accept, controller}),
            Timer = dist_util:start_timer(SetupTime),
            HSData0 = hs_data(Controller),
            HSData = HSData0#hs_data{
                kernel_pid = Kernel,
                this_node = MyNode,
                socket = Controller,
                timer = Timer,
                this_flags = 0, % ?
                allowed = Allowed
            },
            dist_util:handshake_other_started(HSData);
        {false, _IP} ->
            ?display({do_accept, {error, {false, _IP}}}),
            ?shutdown(no_node)
    end.

split_node(Node, LongOrShortNames) ->
    case dist_util:split_node(Node) of
        {node, Name, Host} ->
            case string:split(Host, ".", all) of
                [_] when LongOrShortNames =:= longnames ->
                    case inet:parse_address(Host) of
                        {ok, _} ->
                            [Name, Host];
                        _ ->
                            error_logger:error_msg(
                                "** System running to use fully qualified "
                                "hostnames **~n"
                                "** Hostname ~ts is illegal **~n",
                                [Host]
                            ),
                            ?shutdown(Node)
                    end;
                L when length(L) > 1, LongOrShortNames =:= shortnames ->
                    error_logger:error_msg(
                        "** System NOT running to use fully qualified "
                        "hostnames **~n"
                        "** Hostname ~ts is illegal **~n",
                        [Host]
                    );
                _ ->
                    [Name, Host]
            end;
        _ ->
            error_logger:error_msg("** Nodename ~p illegal **~n", [Node]),
            ?shutdown(Node)
    end.
    % case split_node(atom_to_list(Node), $@, []) of
    %     [Name|Tail] when Tail =/= [] ->
    %         Host = lists:append(Tail),
    %         case split_node(Host, $., []) of
    %             [_] when LongOrShortNames =:= longnames ->
    %                 case inet:parse_address(Host) of
    %                     {ok, _} ->
    %                         [Name, Host];
    %                     _ ->
    %                         error_msg("** System running to use "
    %                                 "fully qualified "
    %                                 "hostnames **~n"
    %                                 "** Hostname ~ts is illegal **~n",
    %                                 [Host]),
    %                         ?shutdown(Node)
    %                 end;
    %             L when length(L) > 1, LongOrShortNames =:= shortnames ->
    %                 error_msg("** System NOT running to use fully qualified "
    %                     "hostnames **~n"
    %                     "** Hostname ~ts is illegal **~n",
    %                     [Host]),
    %                 ?shutdown(Node);
    %             _ ->
    %                 [Name, Host]
    %         end;
    %     [_] ->
    %         error_msg("** Nodename ~p illegal, no '@' character **~n",
    %             [Node]),
    %         ?shutdown(Node);
    %     _ ->
    %         error_msg("** Nodename ~p illegal **~n", [Node]),
    %         ?shutdown(Node)
    % end.

hs_data(DistController) ->
    TickHandler = request(DistController, tick_handler),
    Socket = request(DistController, socket),
    #hs_data{
        f_send = fun(Controller, Packet) ->
            ?display({f_send, Controller, Packet}),
            request(Controller, {send, Packet})
        end,
        f_recv = fun(Controller, Length, Timeout) ->
            ?display({f_recv, Controller, Length, Timeout}),
            case request(Controller, {recv, Length, Timeout}, infinity) of
                {ok, Bin} when is_binary(Bin) ->
                    ?display({f_recv, Bin}),
                    {ok, binary_to_list(Bin)};
                Other ->
                    ?display({f_recv, Other}),
                    Other
            end
        end,
        f_setopts_pre_nodeup = fun(Controller) ->
            ?display({f_setopts_pre_nodeup, Controller}),
            request(Controller, pre_nodeup)
        end,
        f_setopts_post_nodeup = fun(Controller) ->
            ?display({f_setopts_post_nodeup, Controller}),
            request(Controller, post_nodeup)
        end,
        f_getll = fun(Controller) ->
            ?display({f_getll, Controller}),
            request(Controller, getll)
        end,
        f_handshake_complete = fun(Controller, Node, DHandle) ->
            ?display({f_handshake_complete, Controller, Node, DHandle}),
            request(Controller, {handshake_complete, Node, DHandle})
        end,
        f_address = fun(Controller, Node) ->
            ?display({f_address, Controller, Node}),
            case request(Controller, {address, Node}) of
                {error, no_node} ->
                    ?shutdown(no_node);
                Reply ->
                    Reply
            end
        end,
        mf_setopts = fun(_Controller, _Opts) ->
            ?display({mf_setopts, _Controller, _Opts})
        end,
        mf_getopts = fun(Controller, Opts) ->
            ?display({mf_getopts, Controller, Opts}),
            request(Controller, {getopts, Opts})
        end,
        mf_getstat = fun(_Controller) ->
            ?display({mf_getstat, _Controller, Socket}),
            Res = case inet:getstat(Socket, [recv_cnt, send_cnt, send_pend]) of
                {ok, Stat} ->
                    split_stat(Stat, 0, 0, 0);
                Error ->
                    Error
            end,
            ?display({mf_getstat, Res}),
            Res
        end,
        mf_tick = fun(_Controller) ->
            ?display({mf_tick, _Controller, TickHandler}),
            TickHandler ! tick
        end
    }.

request(Pid, Req) -> request(Pid, Req, 5000).

request(Pid, Req, Timeout) when is_pid(Pid) ->
    Ref = erlang:monitor(process, Pid),
    try
        Pid ! {self(), Ref, Req},
        receive
            {Ref, Reply} ->
                Reply;
            {'DOWN', Ref, process, Pid, Reason} ->
                exit({dist_process_exit, Reason})
        after
            Timeout ->
                Error = {dist_process_request_timeout, Pid, Req},
                ?display(Error),
                exit(Error)
        end
    after
        erlang:demonitor(Ref, [flush])
    end.

reply(Pid, Ref, Reply) ->
    Pid ! {Ref, Reply}.

acceptor_spawn() ->
    ?display({enter, []}),
    Kernel = self(),
    spawn_opt(fun() -> acceptor_init(Kernel) end, [link, {priority, max}]).

acceptor_get_address(Pid) -> request(Pid, get_address).

acceptor_listen(Pid) -> request(Pid, listen).

acceptor_init(Kernel) ->
    {ok, ListenSocket} = gen_udp:open(0, [
        binary,
        {active, true},
        {reuseaddr, true}
    ]),
    {ok, _Port} = inet:port(ListenSocket),
    ?display({socket, ListenSocket, _Port}),
    receive
        {From, Ref, get_address} ->
            {ok, Address} = inet:sockname(ListenSocket),
            reply(From, Ref, Address)
    end,
    receive
        {From2, Ref2, listen} ->
            reply(From2, Ref2, ok),
            acceptor_loop(Kernel, ListenSocket)
    end.

acceptor_loop(Kernel, ListenSocket) ->
    receive
        {udp, ListenSocket, SrcAddress, SrcPort, <<"hello\n">>} ->
            ?display({acceptor, {got_hello, SrcAddress, SrcPort}}),
            ID = {SrcAddress, SrcPort},
            {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
            Controller = controller_spawn(ID, Socket),
            Kernel ! {accept, self(), Controller, inet, udp},
            receive
                {Kernel, controller, SupervisorPid} ->
                    Port = controller_set_supervisor(Controller, SupervisorPid),
                    % We unlink to avoid crashing acceptor if controller process
                    % dies:
                    unlink(Controller),
                    ?display({acceptor, {reply_port, {SrcAddress, SrcPort}, Port}}),
                    ok = gen_udp:send(ListenSocket, SrcAddress, SrcPort, <<Port:16>>),
                    SupervisorPid ! {self(), controller};
                {Kernel, unsupported_protocol} ->
                    exit(unsupported_protocol)
            end,
            acceptor_loop(Kernel, ListenSocket);
        _Other ->
            ?display({unknown_msg, _Other}),
            acceptor_loop(Kernel, ListenSocket)
    end.

controller_spawn(ID, Socket) ->
    ?display({enter, [ID]}),
    Pid = spawn_opt(fun() ->
        controller_init(ID)
    end, [link, {priority, max}] ++ ?CONTROLLER_SPAWN_OPTS),
    gen_udp:controlling_process(Socket, Pid),
    request(Pid, {socket, Socket}),
    Pid.

controller_set_supervisor(Pid, SupervisorPid) ->
    request(Pid, {supervisor, SupervisorPid}).

controller_init(ID) ->
    Socket = receive
        {From, Ref, {socket, S}} ->
            reply(From, Ref, ok),
            S
    end,
    {ok, _Port} = inet:port(Socket),
    ?display({Socket, ID, _Port}),
    TickHandler = spawn_opt(fun() ->
        controller_tick_loop(ID, Socket)
    end, [link, {priority, max}] ++ ?CONTROLLER_SPAWN_OPTS),
    controller_setup_loop(ID, Socket, TickHandler, undefined).

controller_setup_loop({IP, Port} = ID, Socket, TickHandler, Supervisor) ->
    receive
        {From, Ref, {supervisor, Pid}} ->
            link(Pid),
            {ok, {_OwnIP, OwnPort}} = inet:sockname(Socket),
            reply(From, Ref, OwnPort),
            controller_setup_loop(ID, Socket, TickHandler, Pid);
        {From, Ref, tick_handler} ->
            reply(From, Ref, TickHandler),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, socket} ->
            reply(From, Ref, Socket),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, {send, Packet}} ->
            ok = gen_udp:send(Socket, IP, Port, Packet),
            ?display({send, Socket, IP, Port, Packet}),
            reply(From, Ref, ok),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, {recv, Length, Timeout}} ->
            case gen_udp:recv(Socket, Length, Timeout) of
                {ok, {IP, Port, Data}} ->
                    ?display({recv, IP, Port, Data}),
                    reply(From, Ref, {ok, Data});
                Other ->
                    ?display({recv, {error, Other}}),
                    reply(From, Ref, {error, Other})
            end,
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, {address, Node}} ->
            {node, _Name, Host} = dist_util:split_node(Node),
            reply(From, Ref, #net_address{
                address = ID,
                host = Host,
                protocol = udp,
                family = inet
            }),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, pre_nodeup} ->
            Res = inet:setopts(Socket, [{active, false}]),
            reply(From, Ref, Res),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, post_nodeup} ->
            Res = inet:setopts(Socket, [{active, false}]),
            reply(From, Ref, Res),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, getll} ->
            reply(From, Ref, {ok, self()}),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, {getopts, Opts}} ->
            reply(From, Ref, inet:getopts(Socket, Opts)),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor);
        {From, Ref, {handshake_complete, _Node, DHandle}} ->
            reply(From, Ref, ok),
            InputHandler = controller_input_spawn(ID, DHandle, Supervisor),
            ok = erlang:dist_ctrl_input_handler(DHandle, InputHandler),
            ok = gen_udp:controlling_process(Socket, InputHandler),
            request(InputHandler, {socket, Socket}),
            process_flag(priority, normal),
            erlang:dist_ctrl_get_data_notification(DHandle),
            ?display({output_init, ID, Socket}),
            controller_output_loop(ID, Socket, DHandle);
        _Other ->
            ?display({controller, {msg, _Other}}),
            controller_setup_loop(ID, Socket, TickHandler, Supervisor)
    end.

controller_input_spawn(ID, DHandle, Supervisor) ->
    spawn_opt(fun() ->
        controller_input_init(ID, DHandle, Supervisor)
    end, [link] ++ ?CONTROLLER_SPAWN_OPTS).

controller_input_init(ID, DHandle, Supervisor) ->
    link(Supervisor),
    % Ensure we don't put data before registered as input handler:
    receive {From, Ref, {socket, Socket}} -> reply(From, Ref, ok) end,
    ?display({input_init, ID, Socket}),
    ok = inet:setopts(Socket, [{active, true}]),
    controller_input_loop(ID, Socket, DHandle, 0).

% controller_input_loop(ID, Socket, DHandle, N) when N =< ?CONTROLLER_ACTIVE_BUF / 2 ->
%     ok = inet:setopts(Socket, [{active, ?CONTROLLER_ACTIVE_BUF - N}]),
%     controller_input_loop(ID, Socket, DHandle, ?CONTROLLER_ACTIVE_BUF);
controller_input_loop(ID, Socket, DHandle, N) ->
    NewN = receive
        {udp, Socket, _SrcAddress, _SrcPort, <<"tick">>} ->
            ?display({got_tick, _SrcAddress, _SrcPort, <<"tick">>}),
            N;
        {udp, Socket, _SrcAddress, _SrcPort, Data} ->
            ?display({udp, Socket, _SrcAddress, _SrcPort, Data}),
            try
                erlang:dist_ctrl_put_data(DHandle, Data)
            catch
                _C:_R:_ST ->
                    ?display({error, _C, _R, _ST}),
                    death_row()
            end,
            N - 1;
        _Other ->
            ?display({msg, _Other}),
            N
    end,
    controller_input_loop(ID, Socket, DHandle, NewN).

controller_send_data({Address, Port} = ID, Socket, DHandle) ->
    case erlang:dist_ctrl_get_data(DHandle) of
        none ->
            erlang:dist_ctrl_get_data_notification(DHandle);
        Data ->
            ?display({send, Socket, Address, Port, Data}),
            ok = gen_udp:send(Socket, Address, Port, Data),
            controller_send_data(ID, Socket, DHandle)
    end.

controller_output_loop(ID, Socket, DHandle) ->
    receive
        dist_data ->
            try
                controller_send_data(ID, Socket, DHandle)
            catch
                _C:_R:_ST ->
                    ?display({error, _C, _R, _ST}),
                    death_row()
            end,
            controller_output_loop(ID, Socket, DHandle);
        _Other ->
            ?display({msg, _Other}),
            controller_output_loop(ID, Socket, DHandle)
    end.


controller_tick_loop({IP, Port} = ID, Socket) ->
    receive
        tick ->
            ?display({sending_tick, Socket, IP, Port, <<"tick">>}),
            ok = gen_udp:send(Socket, IP, Port, <<"tick">>);
        _ ->
            ok
    end,
    controller_tick_loop(ID, Socket).

death_row() -> death_row(connection_closed).

death_row(normal) -> death_row();
death_row(Reason) ->
    receive after 5000 -> exit(Reason) end.

split_stat([{recv_cnt, R}|Stat], _, W, P) ->
    split_stat(Stat, R, W, P);
split_stat([{send_cnt, W}|Stat], R, _, P) ->
    split_stat(Stat, R, W, P);
split_stat([{send_pend, P}|Stat], R, W, _) ->
    split_stat(Stat, R, W, P);
split_stat([], R, W, P) ->
    {ok, R, W, P}.
