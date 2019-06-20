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
-type arg() :: term().
-type reply() :: term().

%--- Behaviour -----------------------------------------------------------------

-callback acceptor_init() -> {ok, {
    inet:socket_protocol(),
    inet:address_family(),
    {inet:ip_address(), inet:port_number()},
    state()}
}.

-callback acceptor_info(term(), state()) ->
    {spawn_controller, arg(), state()} | {ok, state()}.

-callback acceptor_controller_approved(arg(), reply(), state()) ->
    state().

-callback acceptor_terminate(state()) ->
    no_return().

-callback controller_init(term()) ->
    {ok, state()}.

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
    supervisor   :: pid(),
    address      :: #net_address{}
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
    Mod = callback_module(),
    Timer = dist_util:start_timer(SetupTime),
    try
        {spawn_controller, Arg} = Mod:setup(Name, Host), % TODO: Error handling?
        dist_util:reset_timer(Timer),
        Controller = controller_spawn(Arg, Mod),
        ok = controller_finalize(Controller, self()),
        HSData0 = hs_data(Controller),
        HSData = HSData0#hs_data{
            kernel_pid = Kernel,
            other_node = Node,
            this_node = MyNode,
            % socket refers to the controller process pid:
            socket = Controller,
            timer = Timer,
            this_flags = 0,
            % Distribution protocol, version 5 since R6:
            % http://erlang.org/doc/man/erl_epmd.html#port_please-3
            other_version = 5,
            request_type = Type
        },
        dist_util:handshake_we_started(HSData)
    catch
        _Class:_Reason ->
        ?display({setup_failed, _Class, _Reason}),
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
        f_setopts_pre_nodeup = fun(_Controller) ->
            ?display({f_setopts_pre_nodeup, _Controller}),
            ok
        end,
        f_setopts_post_nodeup = fun(_Controller) ->
            ?display({f_setopts_post_nodeup, _Controller}),
            ok
        end,
        f_getll = fun(Controller) ->
            ?display({f_getll, Controller}),
            {ok, Controller}
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
        mf_getstat = fun(_Controller) ->
            % ?display({mf_getstat, Controller}),
            % Stats are used by the kernel to determine if ticks should be sent.
            % If stats does not change (i.e. no data has flowed on the wire),
            % the kernel issues a tick command to the tick handler. Because
            % we're always returning 0's here, we fool the kernel to always send
            % ticks. To never send ticks, we would need to increment these
            % values every time stats are asked for.
            {ok, 0, 0, 0}
        end,
        mf_tick = fun(_Controller) ->
            % ?display({mf_tick, _Controller, TickHandler}),
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
                Stack = try error(foo) catch error:foo:ST -> ST end,
                Error = {dist_process_request_timeout, Pid, Req, Stack},
                ?display(Error),
                error(Error)
        end
    after
        erlang:demonitor(Ref, [flush])
    end.

respond(Fun) ->
    receive {From, Ref, Msg} ->
        {reply, Reply, Result} = Fun(Msg),
        reply(From, Ref, Reply),
        Result
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
    respond(fun(get_meta) -> {reply, {Protocol, Family, Address}, ok} end),
    respond(fun(listen) -> {reply, ok, ok} end),
    acceptor_loop(State#acc_state{
        mod_state = ModState,
        family = Family,
        protocol = Protocol
    }).

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
                    Kernel ! {accept, self(), CtrlPid, Family, Protocol},
                    ?display(kernel_notified),
                    receive
                        {Kernel, controller, SupervisorPid} ->
                            ?display(kernel_happy),
                            Reply = controller_finalize(CtrlPid, SupervisorPid),
                            ok = ?CALL(State#acc_state, acceptor_controller_approved, [Arg, Reply]),
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
    ?display({enter, [Arg, Module]}),
    Pid = spawn_opt(fun() ->
        controller_init(#ctrl_state{mod = Module}, Arg)
    end, [link, {priority, max}] ++ ?CONTROLLER_SPAWN_OPTS),
    Pid.

controller_finalize(Pid, SupervisorPid) ->
    ok = request(Pid, {supervisor, SupervisorPid}),
    % We unlink to avoid crashing acceptor if controller process
    % dies:
    unlink(Pid),
    request(Pid, get_reply).

controller_init(#ctrl_state{mod = Module} = State, Arg) ->
    Supervisor = respond(fun({supervisor, Supervisor}) ->
        link(Supervisor),
        {reply, ok, Supervisor}
    end),
    {reply, Reply, Address, ModState} = Module:controller_init(Arg),
    ok = respond(fun(get_reply) -> {reply, Reply, ok} end),
    TickHandler = spawn_opt(fun() ->
        controller_tick_init(State#ctrl_state{mod_state = ModState})
    end, [link, {priority, max}] ++ ?CONTROLLER_SPAWN_OPTS),
    controller_setup_loop(State#ctrl_state{
        supervisor = Supervisor,
        tick_handler = TickHandler,
        mod_state = ModState,
        address = Address
    }).

controller_setup_loop(State) ->
    receive
        {From, Ref, tick_handler} ->
            reply(From, Ref, State#ctrl_state.tick_handler),
            controller_setup_loop(State);
        {From, Ref, {send, Packet}} ->
            % TODO: Error handling
            {ok, NewModState} = ?CALL(State#ctrl_state, controller_send, [Packet]),
            reply(From, Ref, ok),
            controller_setup_loop(State#ctrl_state{mod_state = NewModState});
        {From, Ref, {recv, Length, Timeout}} ->
            % TODO: Error handling
            {ok, Data, NewModState} = ?CALL(State#ctrl_state, controller_recv, [Length, Timeout]),
            reply(From, Ref, {ok, Data}),
            controller_setup_loop(State#ctrl_state{mod_state = NewModState});
        {From, Ref, {address, Node}} ->
            {node, _Name, Host} = dist_util:split_node(Node),
            reply(From, Ref, (State#ctrl_state.address)#net_address{
                host = Host
            }),
            controller_setup_loop(State);
        {From, Ref, {handshake_complete, _Node, DHandle}} ->
            reply(From, Ref, ok),
            InputHandler = controller_input_spawn(State, DHandle, State#ctrl_state.supervisor),
            ok = erlang:dist_ctrl_input_handler(DHandle, InputHandler),
            {ok, NewModState} = ?CALL(State#ctrl_state, controller_done, [InputHandler]),
            request(InputHandler, {activate, NewModState}),
            controller_output_init(State, DHandle);
        _Other ->
            ?display({controller, {msg, _Other}}),
            controller_setup_loop(State)
    end.

controller_input_spawn(State, DHandle, Supervisor) ->
    spawn_opt(fun() ->
        controller_input_init(State, DHandle, Supervisor)
    end, [link] ++ ?CONTROLLER_SPAWN_OPTS).

controller_input_init(#ctrl_state{mod = Module} = State, DHandle, Supervisor) ->
    link(Supervisor),
    % Ensure we don't start before registered as input handler:
    CtrlModState = respond(fun({activate, S}) ->
        {reply, ok, S}
    end),
    {ok, InputModState} = Module:input_init(CtrlModState),
    controller_input_loop(State#ctrl_state{mod_state = InputModState}, DHandle).

controller_input_loop(State, DHandle) ->
    try
        receive
            Msg ->
                NewModState = case ?CALL(State#ctrl_state, input_info, [Msg]) of
                    {reply, Data, S} ->
                        erlang:dist_ctrl_put_data(DHandle, Data),
                        S;
                    {noreply, S} ->
                        S
                end,
                controller_input_loop(State#ctrl_state{mod_state = NewModState}, DHandle)
        end
    catch
        _C:_R:_ST ->
            ?display({error, _C, _R, _ST}),
            death_row()
    end.

controller_output_init(#ctrl_state{mod = Module, mod_state = ModState} = State, DHandle) ->
    process_flag(priority, normal),
    {ok, OutputModState} = Module:output_init(ModState),
    erlang:dist_ctrl_get_data_notification(DHandle),
    controller_output_loop(State#ctrl_state{mod_state = OutputModState}, DHandle).

controller_output_loop(State, DHandle) ->
    try
        receive
            dist_data ->
                NewState = controller_output_gather(State, DHandle),
                controller_output_loop(NewState, DHandle);
            _Other ->
                ?display({msg, _Other}),
                controller_output_loop(State, DHandle)
        end
    catch
        _C:_R:_ST ->
            ?display({error, _C, _R, _ST}),
            death_row()
    end.

controller_output_gather(State, DHandle) ->
    case erlang:dist_ctrl_get_data(DHandle) of
        none ->
            erlang:dist_ctrl_get_data_notification(DHandle),
            State;
        Data ->
            {ok, NewModState} = ?CALL(State#ctrl_state, output_send, [Data]),
            controller_output_gather(State#ctrl_state{mod_state = NewModState}, DHandle)
    end.

controller_tick_init(#ctrl_state{mod = Module, mod_state = CtrlModState} = State) ->
    {ok, TickModState} = Module:tick_init(CtrlModState),
    controller_tick_loop(State#ctrl_state{mod_state = TickModState}).

controller_tick_loop(State) ->
    NewTickModState = receive
        tick ->
            {ok, S} = ?CALL(State#ctrl_state, tick_trigger, []),
            S;
        _ ->
            State#ctrl_state.mod_state
    end,
    controller_tick_loop(State#ctrl_state{mod_state = NewTickModState}).

death_row() -> death_row(connection_closed).

death_row(normal) -> death_row();
death_row(Reason) ->
    receive after 5000 -> exit(Reason) end.
