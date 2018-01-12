-module(benchmark_server).


%--- Exports -------------------------------------------------------------------

% Server-Side API Exports
-export([start/0]).

% Client-Side API Exports
-export([rtt/2]).
-export([prepare_pingpong/2]).
-export([start_pingpong/1]).
-export([stop_pingpong/1]).


%--- Macros --------------------------------------------------------------------

-define(SERVER, benchmark_server).
-define(TIMEOUT, 5000).


%--- Server-Side API Functions -------------------------------------------------

start() -> spawn_link_benchmark_server().


%--- Client-Side API Functions -------------------------------------------------

rtt(Node, true) ->
  T1 = os:timestamp(),
  case call_server(Node, ping) of
    {ok, pong} ->
      T2 = os:timestamp(),
      {ok, timer:now_diff(T2, T1)};
    Error -> Error
  end;

rtt(Node, false) ->
  T1 = os:timestamp(),
  case unmonitored_call_server(Node, ping) of
    {ok, pong} ->
      T2 = os:timestamp(),
      {ok, timer:now_diff(T2, T1)};
    Error -> Error
  end.


prepare_pingpong(Node, Size) ->
  {ok, spawn_link_ping(Node, Size)}.


start_pingpong(Pid) ->
  call(Pid, start).


stop_pingpong(Pid) ->
  case call(Pid, stop) of
    {ok, _} -> ok;
    Error -> Error
  end.


%--- Internal Functions --------------------------------------------------------

call(Pid, Msg) ->
  Ref = erlang:monitor(process, Pid),
  Pid ! {rpc, self(), Ref, Msg},
  receive
    {Ref, Result} ->
      erlang:demonitor(Ref, [flush]),
      {ok, Result};
    {'DOWN', Ref, process, _, Reason} ->
      {error, Reason}
  after
    ?TIMEOUT ->
      {error, timeout}
  end.


unmonitored_call(Pid, Msg) ->
  Ref = make_ref(),
  Pid ! {rpc, self(), Ref, Msg},
  receive
    {Ref, Result} -> {ok, Result}
  after
    ?TIMEOUT ->
      {error, timeout}
  end.


call_server(Node, Msg) ->
  call({?SERVER, Node}, Msg).


unmonitored_call_server(Node, Msg) ->
  unmonitored_call({?SERVER, Node}, Msg).


reply(From, Ref, Result) ->
  From ! {Ref, Result},
  ok.


spawn_link_benchmark_server() ->
  spawn_link(fun() -> benchmark_server_setup() end).


benchmark_server_setup() ->
  register(?SERVER, self()),
  io:format("Benchmarking server started...~n"),
  benchmark_server_loop().


benchmark_server_loop() ->
  receive
    {rpc, From, Ref, ping} ->
      reply(From, Ref, pong);
    {rpc, From, Ref, start_pong} ->
      reply(From, Ref, spawn_pong(From));
    _ -> ok
  end,
  benchmark_server_loop().


spawn_link_ping(Node, Size) ->
  spawn_link(fun() -> ping_setup(Node, Size) end).


spawn_pong(ForPid) ->
  spawn(fun() -> pong_setup(ForPid) end).


ping_setup(Node, Size) ->
  {ok, PongPid} = call_server(Node, start_pong),
  true = link(PongPid),
  Data = crypto:strong_rand_bytes(Size),
  receive
    {rpc, From, Ref, start} -> reply(From, Ref, self())
  end,
  PongPid ! {echo, self(), Data},
  pingpong_loop(PongPid).


pong_setup(PingPid) ->
  pingpong_loop(PingPid).


pingpong_loop(ForPid) ->
  receive
    stop -> ok;
    {rpc, From, Ref, stop} ->
      ForPid ! stop,
      reply(From, Ref, done);
    {echo, ForPid, Data} ->
      ForPid ! {echo, self(), Data},
      pingpong_loop(ForPid);
    _ ->
      pingpong_loop(ForPid)
  end.
