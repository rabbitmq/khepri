%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(heavy_load_SUITE).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         concurrent_writes_to_same_key/1
        ]).

all() ->
    [
     {group, single_node},
     {group, cluster}
    ].

groups() ->
    [
     {single_node, [],
      [
       concurrent_writes_to_same_key
      ]},
     {cluster, [],
      [
       concurrent_writes_to_same_key
      ]}
    ].

init_per_suite(Config) ->
    helpers:basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    ok = helpers:start_epmd(),
    {ok, _} = cover:start([node()]),
    case net_kernel:start(?MODULE, #{name_domain => shortnames}) of
        {ok, _} ->
            [{started_net_kernel, true} | Config];
        _ ->
            ?assertNotEqual(nonode@nohost, node()),
            [{started_net_kernel, false} | Config]
    end.

end_per_suite(Config) ->
    _ = case ?config(started_net_kernel, Config) of
            true  -> net_kernel:stop();
            false -> ok
        end,
    ok.

init_per_group(single_node, Config) ->
    Config1 = [{cluster_size, 1} | Config],
    Config1;
init_per_group(cluster, Config) ->
    Config1 = [{cluster_size, 3} | Config],
    Config1.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config) ->
    ClusterSize = ?config(cluster_size, Config),
    Nodes = helpers:start_n_nodes(?MODULE, Testcase, ClusterSize),
    PropsPerNode0 = [begin
                         {ok, _} = peer:call(
                                     Peer, application, ensure_all_started,
                                     [khepri], infinity),
                         Props = peer:call(
                                   Peer, helpers, start_ra_system,
                                   [Testcase], infinity),
                         {Node, #{peer => Peer, props => Props}}
                     end || {Node, Peer} <- Nodes],
    PropsPerNode = maps:from_list(PropsPerNode0),
    [{ra_system_props, PropsPerNode}, {peer_nodes, Nodes} | Config].

end_per_testcase(_Testcase, Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    maps:foreach(
      fun
          (Node, #{peer := Peer, props := Props}) ->
              _ = catch peer:call(
                          Peer, helpers, stop_ra_system, [Props], infinity),
              _ = catch helpers:stop_erlang_node(Node, Peer);
          (_Node, #{props := Props}) ->
              helpers:stop_ra_system(Props)
      end, PropsPerNode),
    ok.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

concurrent_writes_to_same_key(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [FirstNode | RemainingNodes] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = helpers:get_ra_system_name(Config),
    StoreId = RaSystem,

    WorkerCount = 10,
    TxCount = 4,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 helpers:call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 helpers:call(Config, Node, khepri_cluster, join, [StoreId, FirstNode]))
      end, RemainingNodes),

    {StoreId, LeaderNode} = helpers:get_leader_in_store(Config, StoreId, Nodes),

    Key = [?FUNCTION_NAME, same, key, for, everyone],
    Tx = fun() ->
                 {ok, Count} = khepri_tx:get_or(Key, 0),
                 Count1 = Count + 1,
                 ok = khepri_tx:put(Key, Count1),
                 ok
         end,

    Parent = self(),
    Workers = [spawn_link(
                 fun() ->
                         tx_worker(
                           Config, Parent, LeaderNode, StoreId, Tx, TxCount)
                 end)
               || _ <- lists:seq(1, WorkerCount)],

    wait_for_workers_and_print_stats(?FUNCTION_NAME, Workers, Config),

    ?assertEqual(
       {ok, WorkerCount * TxCount},
       helpers:call(Config, LeaderNode, khepri, get, [StoreId, Key])),
    ok.

tx_worker(Config, Parent, Node, StoreId, Tx, TxCount) ->
    Times = lists:map(
              fun(_) ->
                      {Time, Ret} = timer:tc(
                                      fun() ->
                                              helpers:call(
                                                Config, Node,
                                                khepri, transaction,
                                                [StoreId, Tx, #{timeout => 60000}])
                                      end),
                      ?assertEqual({ok, ok}, Ret),
                      Time
              end, lists:seq(1, TxCount)),

    erlang:unlink(Parent),
    Parent ! {self(), done, Times}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

wait_for_workers_and_print_stats(Testcase, Workers, Config) ->
    {ok, Times} = wait_for_workers(Workers, []),
    print_stats(Testcase, Times, Config).

wait_for_workers([], Times) ->
    {ok, Times};
wait_for_workers(Workers, Times) ->
    receive
        {Worker, done, WorkerTimes} ->
            ?assert(lists:member(Worker, Workers)),
            Workers1 = Workers -- [Worker],
            Times1 = Times ++ WorkerTimes,
            wait_for_workers(Workers1, Times1)
    end.

print_stats(Testcase, Times, Config) ->
    Statistics = bear:get_statistics(Times),
    ct:pal("Statistics:~n~p", [Statistics]),

    case os:find_executable("ministat") of
        Executable when is_list(Executable) ->
            MinistatInput = [io_lib:format("~b~n", [Time]) || Time <- Times],
            PrivDir = ?config(priv_dir, Config),
            InputFilename = lists:flatten(
                              io_lib:format("times-~s.txt", [Testcase])),
            InputPath = filename:join(PrivDir, InputFilename),
            ok = file:write_file(InputPath, MinistatInput),

            Args = ["-w", "150",
                    InputFilename],
            Port = erlang:open_port(
                     {spawn_executable, Executable},
                     [stream, in, binary, use_stdio, stderr_to_stdout,
                      exit_status,
                      {cd, PrivDir},
                      {args, Args}]),
            receive_ministat_output(Port, <<>>);
        false ->
            ok
    end.

receive_ministat_output(Port, Output) ->
    receive
        {Port, {data, Data}} ->
            receive_ministat_output(Port, [Output, Data]);
        {Port, {exit_status, _}} ->
            ct:pal("Ministat:~n~n~s", [Output]),
            ok
    end.
