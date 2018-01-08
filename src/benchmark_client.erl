-module(benchmark_client).


%--- Exports -------------------------------------------------------------------

-export([start/0]).


%--- Macros --------------------------------------------------------------------

-define(RTT_CHECK_COUNT, 50).
-define(SETTLING_SLEEP, 3000).

-define(BENCHMARKS, [
% {PINGPONG_COUNT, PINGPONG_SIZE}
  {             0,             0},

  {             10,        1*1024},
  {             10,        2*1024},
  {             10,        3*1024},
  {             10,        5*1024},
  {             10,        8*1024},
  {             10,       13*1024},
  {             10,       21*1024},
  {             10,       34*1024},
  {             10,       55*1024},
  {             10,       89*1024},
  {             10,      144*1024},

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
