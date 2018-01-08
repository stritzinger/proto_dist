-module(benchmark_client).


%--- Exports -------------------------------------------------------------------

-export([start/0]).


%--- Macros --------------------------------------------------------------------

-define(RTT_CHECK_COUNT, 30).
-define(SETTLING_SLEEP, 2000).

-define(BENCHMARKS, [
% {PINGPONG_COUNT, PINGPONG_SIZE}
  {             0,             0},

  {             1,        1*1024},
  {             2,        1*1024},
  {             3,        1*1024},
  {             5,        1*1024},
  {             8,        1*1024},
  {            13,        1*1024},

  {             1,       10*1024},
  {             2,       10*1024},
  {             3,       10*1024},
  {             5,       10*1024},
  {             8,       10*1024},
  {            13,       10*1024},

  {             1,       20*1024},
  {             2,       20*1024},
  {             3,       20*1024},
  {             5,       20*1024},
  {             8,       20*1024},
  {            13,       20*1024},

  {             1,       30*1024},
  {             2,       30*1024},
  {             3,       30*1024},
  {             5,       30*1024},
  {             8,       30*1024},
  {            13,       30*1024},

  {             1,       50*1024},
  {             2,       50*1024},
  {             3,       50*1024},
  {             5,       50*1024},
  {             8,       50*1024},
  {            13,       50*1024},

  {             1,       80*1024},
  {             2,       80*1024},
  {             3,       80*1024},
  {             5,       80*1024},
  {             8,       80*1024},
  {            13,       80*1024},

  {             1,      130*1024},
  {             2,      130*1024},
  {             3,      130*1024},
  {             5,      130*1024},
  {             8,      130*1024},
  {            13,      130*1024},

  {             0,             0}
]).


%--- API Functions -------------------------------------------------------------

start() ->
  [ServerHost] = init:get_plain_arguments(),
  ServerNode = list_to_atom("server@" ++ ServerHost),
  case net_adm:ping(ServerNode) of
    pang -> fatal("Server node ~p not responding", [ServerNode]);
    pong ->
      Results = benchmark(?BENCHMARKS, ServerNode),
      print_report(Results)
  end,
  halt().


%--- Internal Functions --------------------------------------------------------

fatal(Msg, Params) ->
  io:format(standard_error, "ERROR: " ++ Msg ++ "~n", Params),
  halt().


benchmark(Benchmarks, Node) ->
  io:format("===============================================~n"),
  benchmark(Benchmarks, Node, []).


benchmark([], _Node, Acc) -> lists:reverse(Acc);

benchmark([{Count, Size} | Rest], Node, Acc) ->
  io:format("~9w KB x ~2w : ", [trunc(Size / 1024), Count]),
  Pids = start_pingpong(Node, Count, Size),
  timer:sleep(?SETTLING_SLEEP),
  Stats = measure_rtt(Node, ?RTT_CHECK_COUNT),
  stop_pingpong(Pids),
  Result = {Count, Size, Stats},
  benchmark(Rest, Node, [Result | Acc]).


print_report(Results) ->
  io:format("===============================================~n"),
  io:format("Count ; Size (KB) ; RTT Avg (ms) ; RTT Dev (ms)~n"),
  lists:foreach(fun({Count, Size, {Avg, _, Dev}}) ->
    io:format("~5w ; ~9w ; ~12.2f ; ~12.4f~n",
              [Count, trunc(Size / 1024), Avg / 1000, Dev / 1000])
  end, Results),
  io:format("===============================================~n"),
  ok.


measure_rtt(Node, Count) ->
  {Rtt, _, _} = Result = measure_rtt(Node, Count, []),
  io:format(" ~8.2f ms~n", [Rtt / 1000]),
  Result.


measure_rtt(_Node, 0, Acc) ->
  Average = lists:sum(Acc) / length(Acc),
  F = fun(X, Sum) -> Sum + (X - Average) * (X - Average) end,
  Variance = lists:foldl(F, 0.0, Acc) / length(Acc),
  StdDev = math:sqrt(Variance),
  {Average, Variance, StdDev};

measure_rtt(Node, Count, Acc) ->
  io:format("."),
  case benchmark_server:rtt(Node) of
    {error, Reason} ->
      fatal("failed to measure RTT: ~p", [Reason]);
    {ok, Rtt} ->
      measure_rtt(Node, Count - 1, [Rtt | Acc])
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
