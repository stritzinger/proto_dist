-module(gen_dist).

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
-include_lib("kernel/include/dist.hrl").
-include_lib("kernel/include/dist_util.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%--- Types ---------------------------------------------------------------------

-type state() :: term().

%--- Behaviour -----------------------------------------------------------------

-callback acceptor_init() -> {ok, {
    inet:socket_protocol(),
    inet:address_family(),
    {inet:ip_address(), inet:port_number()},
    state()}
}.

-callback acceptor_terminate(state()) -> no_return().

-callback valid_hostname(inet:hostname()) -> boolean().

-callback controller_init(term()) -> {ok, state()}.

-optional_callbacks([valid_hostname/1]).

%--- Records -------------------------------------------------------------------

-record(acc_state, {
    mod       :: module(),
    mod_state :: state(),
    acceptor  :: pid(),
    kernel    :: pid(),
    family    :: inet:address_family(),
    protocol  :: inet:socket_protocol()
}).

-record(ctrl_state, {
    mod          :: module(),
    mod_state    :: state(),
    tick_handler :: pid(),
    supervisor   :: pid()
}).

%--- Macros --------------------------------------------------------------------

-define(time,
    erlang:convert_time_unit(erlang:monotonic_time()-erlang:system_info(start_time), native, microsecond)
).
-define(display(Term), ok).
% -define(display(Term), erlang:display({?time, self(), ?MODULE, ?FUNCTION_NAME, Term})).

%% In order to avoid issues with lingering signal binaries
%% we enable off-heap message queue data as well as fullsweep
%% after 0. The fullsweeps will be cheap since we have more
%% or less no live data.
-define(CONTROLLER_SPAWN_OPTS, [
    {message_queue_data, off_heap},
    {fullsweep_after, 0}
]).
-define(CONTROLLER_ACTIVE_BUF, 10).

-define(CALL(Rec, Func, Args),
    erlang:apply(Rec.mod, Func, Args ++ [Rec.mod_state])
).

%--- Callbacks -----------------------------------------------------------------


-spec listen(atom()) -> {ok, {term(), #net_address{}, 1..3}} | {error, term()}.
listen(Name) ->
    ?display({enter, [Name]}),
    Mod = callback_module(),
    State0 = #acc_state{mod = Mod},
    State1 = acceptor_spawn(State0),
    {Protocol, Family, {_IP, Port} = Address} = acceptor_get_meta(State1#acc_state.acceptor),
    {ok, Host} = inet:gethostname(),
    {ok, Creation} = (net_kernel:epmd_module()):register_node(Name, Port),
    NetAddress = #net_address{
        address = Address,
        host = Host,
        protocol = Protocol,
        family = Family
    },
    {ok, {State1, NetAddress, Creation}}.

-spec accept(term()) -> pid().
accept(State) ->
    ?display({'CALL', [State]}),
    acceptor_listen(State#acc_state.acceptor),
    State.

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
    ?display({'CALL', Node, Type, MyNode, LongOrShortNames, SetupTime}),
    Kernel = self(),
    spawn_opt(fun() ->
        do_setup(Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime)
    end, [link, {priority, max}]).

-spec close(term()) -> no_return().
close(State) ->
    acceptor_close(State).

-spec select(node()) -> boolean().
select(_NodeName) ->
    % TODO: Implement a real select check. Use what's in gen_tcp_dist?
    true.

%--- Internal ------------------------------------------------------------------

callback_module() ->
    case application:get_env(kernel, gen_dist) of
        {ok, Module} when is_list(Module) ->
            list_to_atom(Module);
        {ok, Module} ->
            Module;
        undefined ->
            error(gen_dist_module_not_configured)
    end.

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
                    Module = gen_udp_dist, % FIXME: Get real module
                    Controller = controller_spawn({ID, Socket}, Module),
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
        end,
        add_flags = ?DFLAG_SEND_SENDER,
        reject_flags = dist_util:strict_order_flags()
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

% Acceptor

acceptor_spawn(State) ->
    ?display({enter, []}),
    Kernel = self(),
    Pid = spawn_opt(fun() -> acceptor_init(State#acc_state{kernel = Kernel}) end, [
        link,
        {priority, max}
    ]),
    State#acc_state{acceptor = Pid}.

acceptor_get_meta(Pid) -> request(Pid, get_meta).

acceptor_listen(Pid) -> request(Pid, listen).

acceptor_init(State) ->
    {ok, {Protocol, Family, Address}, ModState} = (State#acc_state.mod):acceptor_init(),
    receive
        {From, Ref, get_meta} ->
            reply(From, Ref, {Protocol, Family, Address})
    end,
    receive
        {From2, Ref2, listen} ->
            reply(From2, Ref2, ok),
            acceptor_loop(State#acc_state{
                mod_state = ModState,
                family = Family,
                protocol = Protocol
            })
    end.

acceptor_loop(#acc_state{kernel = Kernel, family = Family, protocol = Protocol} = State) ->
    ?display({'CALL', [State]}),
    receive
        {From, Ref, close} ->
            ?CALL(State#acc_state, acceptor_terminate, []),
            reply(From, Ref, ok);
        Msg ->
            case ?CALL(State#acc_state, acceptor_info, [Msg]) of
                {spawn_controller, Arg, NewModState} ->
                    CtrlPid = controller_spawn(Arg, State#acc_state.mod),
                    ok = ?CALL(State#acc_state, acceptor_controller_spawned, [Arg, CtrlPid]),
                    Kernel ! {accept, self(), CtrlPid, Family, Protocol},
                    ?display(kernel_notified),
                    receive
                        {Kernel, controller, SupervisorPid} ->
                            ?display(kernel_happy),
                            controller_set_supervisor(CtrlPid, SupervisorPid),
                            ok = ?CALL(State#acc_state, acceptor_controller_approved, [Arg, CtrlPid]),
                            SupervisorPid ! {self(), controller};
                        {Kernel, unsupported_protocol} ->
                            ?display(kernel_sad),
                            exit(unsupported_protocol)
                    end,
                    acceptor_loop(State#acc_state{mod_state = NewModState});
                {ok, NewModState} ->
                    acceptor_loop(State#acc_state{mod_state = NewModState})
        end
    end.

acceptor_close(#acc_state{acceptor = Pid}) ->
    request(Pid, close).

% Controller

controller_spawn(Arg, Module) ->
    % TODO: Use behaviour here!
    ?display({enter, [Arg, Module]}),
    Pid = spawn_opt(fun() ->
        controller_init(#ctrl_state{mod = Module, mod_state = Arg})
    end, [link, {priority, max}] ++ ?CONTROLLER_SPAWN_OPTS),
    Pid.

controller_set_supervisor(Pid, SupervisorPid) ->
    ok = request(Pid, {supervisor, SupervisorPid}),
    % We unlink to avoid crashing acceptor if controller process
    % dies:
    unlink(Pid),
    ok.

controller_init(#ctrl_state{mod_state = {ID, Socket}} = State) ->
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
    controller_setup_loop(State#ctrl_state{tick_handler = TickHandler}).

controller_setup_loop(#ctrl_state{mod_state = {{IP, Port} = ID, Socket}} = State) ->
    receive
        {From, Ref, {supervisor, Pid}} ->
            link(Pid),
            reply(From, Ref, ok),
            controller_setup_loop(State#ctrl_state{supervisor = Pid});
        {From, Ref, tick_handler} ->
            reply(From, Ref, State#ctrl_state.tick_handler),
            controller_setup_loop(State);
        {From, Ref, socket} ->
            reply(From, Ref, Socket),
            controller_setup_loop(State);
        {From, Ref, {send, Packet}} ->
            ok = gen_udp:send(Socket, IP, Port, Packet),
            % ?display({send, Socket, IP, Port, Packet}),
            reply(From, Ref, ok),
            controller_setup_loop(State);
        {From, Ref, {recv, Length, Timeout}} ->
            case gen_udp:recv(Socket, Length, Timeout) of
                {ok, {IP, Port, Data}} ->
                    ?display({recv, IP, Port, Data}),
                    reply(From, Ref, {ok, Data});
                Other ->
                    ?display({recv, {error, Other}}),
                    reply(From, Ref, {error, Other})
            end,
            controller_setup_loop(State);
        {From, Ref, {address, Node}} ->
            {node, _Name, Host} = dist_util:split_node(Node),
            reply(From, Ref, #net_address{
                address = ID,
                host = Host,
                protocol = udp,
                family = inet
            }),
            controller_setup_loop(State);
        {From, Ref, pre_nodeup} ->
            Res = inet:setopts(Socket, [{active, false}]),
            reply(From, Ref, Res),
            controller_setup_loop(State);
        {From, Ref, post_nodeup} ->
            Res = inet:setopts(Socket, [{active, false}]),
            reply(From, Ref, Res),
            controller_setup_loop(State);
        {From, Ref, getll} ->
            reply(From, Ref, {ok, self()}),
            controller_setup_loop(State);
        {From, Ref, {getopts, Opts}} ->
            reply(From, Ref, inet:getopts(Socket, Opts)),
            controller_setup_loop(State);
        {From, Ref, {handshake_complete, _Node, DHandle}} ->
            reply(From, Ref, ok),
            InputHandler = controller_input_spawn(ID, DHandle, State#ctrl_state.supervisor),
            ok = erlang:dist_ctrl_input_handler(DHandle, InputHandler),
            ok = gen_udp:controlling_process(Socket, InputHandler),
            request(InputHandler, {socket, Socket}),
            process_flag(priority, normal),
            erlang:dist_ctrl_get_data_notification(DHandle),
            ?display({output_init, ID, Socket}),
            Snd = packet_sender:new(),
            controller_output_loop(ID, Socket, DHandle, Snd);
        _Other ->
            ?display({controller, {msg, _Other}}),
            controller_setup_loop(State)
    end.

controller_input_spawn(ID, DHandle, Supervisor) ->
    spawn_opt(fun() ->
        controller_input_init(ID, DHandle, Supervisor)
    end, [link] ++ ?CONTROLLER_SPAWN_OPTS).

controller_input_init(ID, DHandle, Supervisor) ->
    link(Supervisor),
    % Ensure we don't put data before registered as input handler:
    receive {From, Ref, {socket, Socket}} -> reply(From, Ref, ok) end,
    Rcv = packet_receiver:new(),
    ?display({input_init, ID, Socket, Rcv}),
    ok = inet:setopts(Socket, [{active, true}]),
    controller_input_loop(ID, Socket, DHandle, Rcv).

controller_input_loop(ID, Socket, DHandle, Rcv) ->
    try
        receive
            {udp, Socket, _SrcAddress, _SrcPort, <<"tick\n">>} ->
                % ?display({got_tick, _SrcAddress, _SrcPort, <<"tick\n">>}),
                controller_input_loop(ID, Socket, DHandle, Rcv);
            {udp, Socket, _SrcAddress, _SrcPort, Data} ->
                % ?display({udp, Socket, _SrcAddress, _SrcPort, Data}),
                NewRcv = case packet_receiver:collect(Data, Rcv) of
                    {[], R} ->
                        erlang:dist_ctrl_put_data(DHandle, <<>>),
                        R;
                    {Messages, R} ->
                        lists:foreach(fun({_, M}) ->
                            erlang:dist_ctrl_put_data(DHandle, M)
                        end, Messages),
                        R
                end,
                controller_input_loop(ID, Socket, DHandle, NewRcv);
            _Other ->
                ?display({msg, _Other}),
                controller_input_loop(ID, Socket, DHandle, Rcv)
        end
    catch
        _C:_R:_ST ->
            ?display({error, _C, _R, _ST}),
            death_row()
    end.

controller_gather_data(ID, Socket, DHandle, Snd) ->
    case erlang:dist_ctrl_get_data(DHandle) of
        none ->
            erlang:dist_ctrl_get_data_notification(DHandle),
            Snd;
        Data ->
            NewSnd = controller_add_data(Data, Snd),
            NewNewSnd = controller_send_once(ID, Socket, NewSnd),
            controller_gather_data(ID, Socket, DHandle, NewNewSnd)
    end.

controller_add_data(Data, Snd) ->
    Ch = packet_channel:channel(Data),
    packet_sender:schedule(Ch, Data, Snd).

controller_send_once({Address, Port}, Socket, Snd) ->
    case packet_sender:next(Snd) of
        {empty, NewSnd} ->
            NewSnd;
        {Data, NewSnd} ->
            % ?display({send, Socket, Address, Port, Data}),
            ok = gen_udp:send(Socket, Address, Port, Data),
            NewSnd
    end.

controller_send_all({Address, Port} = ID, Socket, Snd) ->
    case packet_sender:next(Snd) of
        {empty, NewSnd} ->
            NewSnd;
        {Data, NewSnd} ->
            ?display({send, Socket, Address, Port, Data}),
            ok = gen_udp:send(Socket, Address, Port, Data),
            controller_send_all(ID, Socket, NewSnd)
    end.

controller_output_loop(ID, Socket, DHandle, Snd) ->
    try
        receive
            dist_data ->
                NewSnd = controller_gather_data(ID, Socket, DHandle, Snd),
                NewNewSnd = controller_send_all(ID, Socket, NewSnd),
                controller_output_loop(ID, Socket, DHandle, NewNewSnd);
            _Other ->
                ?display({msg, _Other}),
                controller_output_loop(ID, Socket, DHandle, Snd)
        end
    catch
        _C:_R:_ST ->
            ?display({error, _C, _R, _ST}),
            death_row()
    end.

controller_tick_loop({IP, Port} = ID, Socket) ->
    receive
        tick ->
            ?display({sending_tick, Socket, IP, Port, <<"tick\n">>}),
            ok = gen_udp:send(Socket, IP, Port, <<"tick\n">>);
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
