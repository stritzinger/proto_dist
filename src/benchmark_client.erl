-module(benchmark_client).


%--- Exports -------------------------------------------------------------------

-export([start/0]).


%--- Macros --------------------------------------------------------------------

-define(RTT_CHECK_COUNT, 50).
-define(SETTLING_SLEEP, 3000).

-define(BENCHMARKS, [
% {MONITORING, PINGPONG_COUNT, PINGPONG_SIZE,  DUMP}
  {     false,              0,             0, false},

  {     false,             10,        1*1024, false},
  {     false,             10,        2*1024, false},
  {     false,             10,        3*1024, false},
  {     false,             10,        5*1024, false},
  {     false,             10,        8*1024, false},
  {     false,             10,       13*1024, false},
  {     false,             10,       21*1024, false},
  {     false,             10,       34*1024, false},
  {     false,             10,       55*1024, false},
  {     false,             10,       89*1024, false},
  {     false,             10,      144*1024, false},

  {     false,              0,             0, false}
]).


%--- API Functions -------------------------------------------------------------

start() ->
  [ServerHost] = init:get_plain_arguments(),
  ServerNode = list_to_atom("server@" ++ ServerHost),
  case net_adm:ping(ServerNode) of
    pang -> fatal("Server node ~p not responding", [ServerNode]);
    pong ->
      Results = benchmark(?BENCHMARKS, ServerNode),
      print_report(Results),
      dump_samples(Results)
  end,
  halt().


%--- Internal Functions --------------------------------------------------------

fatal(Msg, Params) ->
  io:format(standard_error, "ERROR: " ++ Msg ++ "~n", Params),
  halt().


benchmark(Benchmarks, Node) ->
  io:format("==============================================================~n"),
  benchmark(Benchmarks, Node, []).


benchmark([], _Node, Acc) -> lists:reverse(Acc);

benchmark([{Monitoring, Count, Size, Dump} | Rest], Node, Acc) ->
  io:format("~9w KB x ~2w : ", [trunc(Size / 1024), Count]),
  Pids = start_pingpong(Node, Count, Size),
  timer:sleep(?SETTLING_SLEEP),
  Stats = measure_rtt(Node, Monitoring, ?RTT_CHECK_COUNT),
  stop_pingpong(Pids),
  Result = {Count, Size, Dump, Stats},
  benchmark(Rest, Node, [Result | Acc]).


print_report(Results) ->
  io:format("==============================================================~n"),
  io:format("Count ; Size (KB) ; RTT Med (ms) ; RTT Avg (ms) ; RTT Dev (ms)~n"),
  lists:foreach(fun({Count, Size, _, {_, Avg, _, Dev, Med}}) ->
    io:format("~5w ; ~9w ; ~12.2f ; ~12.2f ; ~12.4f~n",
              [Count, trunc(Size / 1024), Med / 1000, Avg / 1000, Dev / 1000])
  end, Results),
  io:format("==============================================================~n"),
  ok.


dump_samples([]) -> ok;

dump_samples([{Count, Size, true, {Values, _, _, _, _}} | Rest]) ->
  io:format("Samples for ~4w KB x ~2w:~n", [trunc(Size / 1024), Count]),
  lists:foreach(fun(V) -> io:format("~12.2f~n", [V / 1000]) end, Values),
  io:format("==============================================================~n"),
  dump_samples(Rest);

dump_samples([_ | Rest]) ->
  dump_samples(Rest).


measure_rtt(Node, Monitoring, Count) ->
  {_, Rtt, _, _, _} = Result = measure_rtt(Node, Monitoring, Count, []),
  io:format(" ~8.2f ms~n", [Rtt / 1000]),
  Result.


measure_rtt(_Node, _Monitoring, 0, Acc) ->
  Average = lists:sum(Acc) / length(Acc),
  F = fun(X, Sum) -> Sum + (X - Average) * (X - Average) end,
  Variance = lists:foldl(F, 0.0, Acc) / length(Acc),
  StdDev = math:sqrt(Variance),
  Median = lists:nth(ceil(length(Acc) / 2), lists:sort(Acc)),
  {lists:reverse(Acc), Average, Variance, StdDev, Median};

measure_rtt(Node, Monitoring, Count, Acc) ->
  io:format("."),
  case benchmark_server:rtt(Node, Monitoring) of
    {error, Reason} ->
      fatal("failed to measure RTT: ~p", [Reason]);
    {ok, Rtt} ->
      measure_rtt(Node, Monitoring, Count - 1, [Rtt | Acc])
  end.


start_pingpong(Node, Count, Size) ->
  start_pingpong(Node, Count, Size, []).


start_pingpong(_Node, 0, _Size, Acc) -> Acc;

start_pingpong(Node, Count, Size, Acc) ->
  case benchmark_server:start_pingpong(Node, Size) of
    {error, Reason} ->
      stop_pingpong(Acc),
      fatal("failed to start pingpong processes: ~p", [Reason]);
    {ok, Pid} ->
      start_pingpong(Node, Count - 1, Size, [Pid | Acc])
  end.


stop_pingpong(Pids) ->
  lists:foreach(fun benchmark_server:stop_pingpong/1, Pids).
