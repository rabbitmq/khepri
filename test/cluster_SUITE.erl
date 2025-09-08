%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(cluster_SUITE).

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

         setup_node/0,

         can_start_a_single_node/1,
         can_restart_a_single_node_with_ra_server_config/1,
         handle_timeout_during_recovery/1,
         can_query_members_with_a_single_node/1,
         can_wait_for_leader_with_a_single_node/1,
         fail_to_start_with_bad_ra_server_config/1,
         initial_members_are_ignored/1,
         can_start_a_three_node_cluster/1,
         can_join_several_times_a_three_node_cluster/1,
         can_rejoin_after_a_reset_in_a_three_node_cluster/1,
         can_restart_nodes_in_a_three_node_cluster/1,
         can_reset_a_cluster_member/1,
         can_query_members_with_a_three_node_cluster/1,
         can_wait_for_leader_with_a_three_node_cluster/1,
         fail_to_join_if_not_started/1,
         fail_to_join_non_existing_node/1,
         fail_to_join_non_existing_store/1,
         can_use_default_store_on_single_node/1,
         can_start_store_in_specified_data_dir_on_single_node/1,
         handle_leader_down_on_three_node_cluster_command/1,
         handle_leader_down_on_three_node_cluster_response/1,
         can_set_snapshot_interval/1,
         projections_are_consistent_on_three_node_cluster/1,
         projections_are_updated_when_a_snapshot_is_installed/1,
         async_command_leader_change_in_three_node_cluster/1,
         spam_txs_during_election/1,
         spam_changes_during_unregister_projections/1,
         trigger_based_on_process_event_works/1,
         process_event_is_delayed_during_node_down_1/1,
         process_event_is_delayed_during_node_down_2/1,
         process_event_is_delayed_during_node_down_3/1,
         trigger_runs_on_leader/1,
         trigger_runs_on_follower/1,
         trigger_runs_on_local_member/1,
         trigger_runs_on_all_members/1,
         trigger_runs_on_leader_if_non_member_target/1,
         trigger_options_rejected_before_v3/1,

         start_monitored_proc/0]).

all() ->
    [
     {group, all}
    ].

groups() ->
    [
     {all, [parallel],
      [
       {single_node, [],
        [
         {non_parallel, [],
          [
           can_use_default_store_on_single_node,
           can_start_store_in_specified_data_dir_on_single_node,
           trigger_options_rejected_before_v3
          ]},
         {parallel, [parallel],
          [
           can_start_a_single_node,
           can_restart_a_single_node_with_ra_server_config,
           handle_timeout_during_recovery,
           can_query_members_with_a_single_node,
           can_wait_for_leader_with_a_single_node,
           fail_to_start_with_bad_ra_server_config,
           initial_members_are_ignored,
           fail_to_join_non_existing_node,
           can_set_snapshot_interval,
           trigger_based_on_process_event_works
          ]}
        ]},
       {cluster, [],
        [
         {group1, [parallel],
          [
           can_start_a_three_node_cluster,
           can_join_several_times_a_three_node_cluster,
           can_rejoin_after_a_reset_in_a_three_node_cluster,
           can_restart_nodes_in_a_three_node_cluster,
           can_reset_a_cluster_member,
           can_query_members_with_a_three_node_cluster,
           can_wait_for_leader_with_a_three_node_cluster,
           fail_to_join_if_not_started,
           fail_to_join_non_existing_store,
           handle_leader_down_on_three_node_cluster_command
          ]},
         {group2, [parallel],
          [
           handle_leader_down_on_three_node_cluster_response,
           projections_are_consistent_on_three_node_cluster,
           projections_are_updated_when_a_snapshot_is_installed,
           async_command_leader_change_in_three_node_cluster,
           spam_txs_during_election,
           spam_changes_during_unregister_projections
          ]},
         {group3, [parallel],
          [
           process_event_is_delayed_during_node_down_1,
           process_event_is_delayed_during_node_down_2,
           process_event_is_delayed_during_node_down_3,
           trigger_runs_on_leader,
           trigger_runs_on_follower,
           trigger_runs_on_local_member,
           trigger_runs_on_all_members,
           trigger_runs_on_leader_if_non_member_target
          ]}
        ]}
      ]}
    ].

init_per_suite(Config) ->
    basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    ok = helpers:start_epmd(),
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
    setup_node(),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_single_node orelse
       Testcase =:= can_restart_a_single_node_with_ra_server_config orelse
       Testcase =:= handle_timeout_during_recovery orelse
       Testcase =:= can_query_members_with_a_single_node orelse
       Testcase =:= can_wait_for_leader_with_a_single_node orelse
       Testcase =:= fail_to_start_with_bad_ra_server_config orelse
       Testcase =:= initial_members_are_ignored orelse
       Testcase =:= fail_to_join_non_existing_node orelse
       Testcase =:= can_set_snapshot_interval orelse
       Testcase =:= trigger_based_on_process_event_works orelse
       Testcase =:= trigger_options_rejected_before_v3 ->
    {ok, _} = application:ensure_all_started(khepri),
    Props = helpers:start_ra_system(Testcase),
    [{ra_system_props, #{node() => #{props => Props}}} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_three_node_cluster orelse
       Testcase =:= can_join_several_times_a_three_node_cluster orelse
       Testcase =:= can_rejoin_after_a_reset_in_a_three_node_cluster orelse
       Testcase =:= can_restart_nodes_in_a_three_node_cluster orelse
       Testcase =:= can_reset_a_cluster_member orelse
       Testcase =:= can_query_members_with_a_three_node_cluster orelse
       Testcase =:= can_wait_for_leader_with_a_three_node_cluster orelse
       Testcase =:= fail_to_join_if_not_started orelse
       Testcase =:= fail_to_join_non_existing_store orelse
       Testcase =:= handle_leader_down_on_three_node_cluster_command orelse
       Testcase =:= handle_leader_down_on_three_node_cluster_response orelse
       Testcase =:= projections_are_consistent_on_three_node_cluster orelse
       Testcase =:= projections_are_updated_when_a_snapshot_is_installed orelse
       Testcase =:= async_command_leader_change_in_three_node_cluster orelse
       Testcase =:= spam_changes_during_unregister_projections orelse
       Testcase =:= process_event_is_delayed_during_node_down_1 orelse
       Testcase =:= process_event_is_delayed_during_node_down_2 orelse
       Testcase =:= process_event_is_delayed_during_node_down_3 orelse
       Testcase =:= trigger_runs_on_leader orelse
       Testcase =:= trigger_runs_on_follower orelse
       Testcase =:= trigger_runs_on_local_member orelse
       Testcase =:= trigger_runs_on_all_members orelse
       Testcase =:= trigger_runs_on_leader_if_non_member_target->
    Nodes = start_n_nodes(Testcase, 3),
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
    [{ra_system_props, PropsPerNode}, {peer_nodes, Nodes} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= spam_txs_during_election ->
    Nodes = start_n_nodes(Testcase, 2),
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
    [{ra_system_props, PropsPerNode}, {peer_nodes, Nodes} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= can_use_default_store_on_single_node orelse
       Testcase =:= can_start_store_in_specified_data_dir_on_single_node ->
    Config.

end_per_testcase(Testcase, _Config)
  when Testcase =:= can_use_default_store_on_single_node orelse
       Testcase =:= can_start_store_in_specified_data_dir_on_single_node ->
    ok;
end_per_testcase(_Testcase, Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    maps:foreach(
      fun
          (Node, #{peer := Peer, props := Props}) ->
              _ = catch peer:call(
                          Peer, helpers, stop_ra_system, [Props], infinity),
              _ = catch stop_erlang_node(Node, Peer);
          (_Node, #{props := Props}) ->
              helpers:stop_ra_system(Props)
      end, PropsPerNode),
    ok.

can_start_a_single_node(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Use database before starting it"),
    ?assertEqual(
       {error, noproc},
       khepri:put(StoreId, [foo], value1)),
    ?assertEqual(
       {error, noproc},
       khepri:get(StoreId, [foo])),

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, StoreId)),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value2)),
    ?assertEqual({ok, value2}, khepri:get(StoreId, [foo])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    %% TODO: Verify that the server process exited.

    ct:pal("Use database after stopping it"),
    ?assertEqual(
       {error, noproc},
       khepri:put(StoreId, [foo], value3)),
    ?assertEqual(
       {error, noproc},
       khepri:get(StoreId, [foo])),

    ok.

can_restart_a_single_node_with_ra_server_config(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    RaServerConfig = #{cluster_name => StoreId},
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig)),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),
    ?assertEqual({ok, value1}, khepri:get(StoreId, [foo])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ct:pal("Restart database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig, infinity)),

    ct:pal("Use database after restarting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value2)),
    ?assertEqual({ok, value2}, khepri:get(StoreId, [foo])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    %% TODO: Verify that the server process exited.

    ct:pal("Use database after stopping it"),
    ?assertEqual(
       {error, noproc},
       khepri:put(StoreId, [foo], value3)),
    ?assertEqual(
       {error, noproc},
       khepri:get(StoreId, [foo])),

    ok.

handle_timeout_during_recovery(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    RaServerConfig = #{cluster_name => StoreId},
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig)),

    TxCount = 40,
    ct:pal("Execute slow transactions:"),
    lists:foreach(
      fun(I) ->
              ct:pal("- transaction ~b/~b", [I, TxCount]),
              ?assertEqual(
                 {ok, ok},
                 khepri:transaction(
                   StoreId,
                   fun() -> timer:sleep(1000) end, rw))
      end, lists:seq(1, TxCount)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ct:pal("Restart database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig, infinity)),

    ct:pal("Wait for leader for 60 seconds"),
    ?assertEqual(
       ok,
       khepri_cluster:wait_for_leader(StoreId, 60000)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ct:pal("Restart database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig, infinity)),

    ct:pal("Wait for leader for 30 seconds"),
    ?assertEqual(
       {error, timeout},
       khepri_cluster:wait_for_leader(StoreId, 30000)),

    ok.

can_query_members_with_a_single_node(Config) ->
    Node = node(),
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Query members before starting database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId, #{timeout => 10000})),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId, #{favor => low_latency})),

    ct:pal("Query nodes before starting database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, #{timeout => 10000})),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, #{favor => low_latency})),

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, StoreId)),

    ct:pal("Query members after starting database"),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:members(StoreId)),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:members(StoreId, #{timeout => 10000})),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:members(StoreId, #{favor => low_latency})),

    ct:pal("Query nodes after starting database"),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:nodes(StoreId, #{timeout => 10000})),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:nodes(StoreId, #{favor => low_latency})),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ct:pal("Query members after stopping database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId, #{favor => low_latency})),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId, #{favor => low_latency})),

    ct:pal("Query nodes after stopping database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, #{timeout => 10000})),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, #{favor => low_latency})),

    ok.

can_wait_for_leader_with_a_single_node(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Wait for leader before starting database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:wait_for_leader(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:wait_for_leader(StoreId, 2000)),

    ct:pal("Start database and wait for it in parallel"),
    Parent = self(),
    _ = spawn_link(fun() ->
                           timer:sleep(2000),
                           ?assertEqual(
                              {ok, StoreId},
                              khepri:start(RaSystem, StoreId)),
                           erlang:unlink(Parent)
                   end),
    ?assertEqual(
       ok,
       khepri_cluster:wait_for_leader(StoreId, 40000)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ct:pal("Wait for leader after stopping database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:wait_for_leader(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:wait_for_leader(StoreId, 2000)),

    ok.

fail_to_start_with_bad_ra_server_config(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    Ret1 = (catch khepri:start(RaSystem, #{cluster_name => StoreId,
                                           tick_timeout => not_a_timeout})),
    ct:pal("Return value of khepri:start/2: ~p", [Ret1]),
    ?assert(
       case Ret1 of
           {'EXIT',
            {{{bad_action_from_state_function,
               {{timeout, tick}, not_a_timeout, tick_timeout}},
              _},
             _}} ->
               true;
           {'EXIT',
            {noproc, _}} ->
               true;
           _ ->
               ct:pal("Unexpected return value:~n~p", [Ret1]),
               false
       end),

    ThisMember = khepri_cluster:this_member(StoreId),
    ok = khepri_cluster:wait_for_ra_server_exit(ThisMember),

    %% The process is restarted by its supervisor. Depending on the timing, we
    %% may get a `noproc' or an exception.
    ct:pal("Database unusable after failing to start it"),
    Ret2 = (catch khepri:get(StoreId, [foo])),
    ct:pal("Return value of khepri:get/2: ~p", [Ret2]),
    ?assert(
       case Ret2 of
           {'EXIT',
            {{{bad_action_from_state_function,
               {{timeout, tick}, not_a_timeout, tick_timeout}},
              _},
             _}} ->
               true;
           {error,
            {{bad_action_from_state_function,
               {{timeout, tick}, not_a_timeout, tick_timeout}},
             _}} ->
               true;
           {error, noproc} ->
               true;
           _ ->
               ct:pal("Unexpected return value:~n~p", [Ret2]),
               false
       end),

    ok.

initial_members_are_ignored(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, #{cluster_name => StoreId,
                                initial_members => [{StoreId, a},
                                                    {StoreId, b},
                                                    {StoreId, c}]})),

    ct:pal("This member is alone in the \"cluster\""),
    ThisMember = khepri_cluster:this_member(StoreId),
    ?assertEqual(
       {ok, [ThisMember]},
       khepri_cluster:members(StoreId)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ok.

can_start_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Use database before starting it"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:put() from node ~s", [Node]),
              ?assertEqual(
                 {error, noproc},
                 call(Config, Node, khepri, put, [StoreId, [foo], value1])),
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {error, noproc},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:members() from node ~s", [Node]),
              ExpectedMembers = lists:sort([{StoreId, N} || N <- Nodes]),
              ?assertEqual(
                 ExpectedMembers,
                 begin
                     {ok, M} = call(Config,
                                 Node,
                                 khepri_cluster, members, [StoreId]),
                     lists:sort(M)
                 end),
              ?assertEqual(
                 ExpectedMembers,
                 begin
                     {ok, LKM} = call(Config,
                                   Node,
                                   khepri_cluster, members,
                                   [StoreId, #{favor => low_latency}]),
                     lists:sort(LKM)
                 end),

              ExpectedNodes = lists:sort(Nodes),
              ?assertEqual(
                 ExpectedNodes,
                 begin
                     {ok, N} = call(Config,
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end),
              ?assertEqual(
                 ExpectedNodes,
                 begin
                     {ok, LKN} = call(Config,
                                   Node,
                                   khepri_cluster, nodes,
                                   [StoreId, #{favor => low_latency}]),
                     lists:sort(LKN)
                 end)
      end, Nodes),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value2},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal("Stop database on leader node (quorum is maintained)"),
    ?assertEqual(
       ok,
       call(Config, StoppedLeaderNode1, khepri, stop, [StoreId])),

    ct:pal("Use database having it running on 2 out of 3 nodes"),
    %% We try a put from the stopped leader and it should fail because the
    %% leaderboard on that node is stale.
    ?assertEqual(
       {error, noproc},
       call(
         Config, StoppedLeaderNode1, khepri, put, [StoreId, [foo], value3])),
    ?assertEqual(
       {error, noproc},
       call(Config, StoppedLeaderNode1, khepri, get, [StoreId, [foo]])),

    %% Querying running nodes should be fine however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       ok,
       call(Config, hd(RunningNodes1), khepri, put, [StoreId, [foo], value4])),

    %% The stopped leader should still fail to respond because it is stopped
    %% and again, the leaderboard is stale on this node.
    ?assertEqual(
       {error, noproc},
       call(Config, StoppedLeaderNode1, khepri, get, [StoreId, [foo]])),

    %% Running nodes should see the updated value however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:fence() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, fence, [StoreId])),
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value4},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),

    LeaderId2 = get_leader_in_store(Config, StoreId, RunningNodes1),
    {StoreId, StoppedLeaderNode2} = LeaderId2,
    RunningNodes2 = RunningNodes1 -- [StoppedLeaderNode2],

    ct:pal("Stop database on the new leader node (quorum is lost)"),
    ?assertEqual(
       ok,
       call(Config, StoppedLeaderNode2, khepri, stop, [StoreId])),

    ct:pal("Use database having it running on 1 out of 3 nodes"),
    %% We try a put from the second old leader and it should fail.
    ?assertEqual(
       {error, noproc},
       call(
         Config, StoppedLeaderNode2, khepri, put, [StoreId, [foo], value5])),

    %% The last running node should fail to respond as well because the quorum
    %% is lost.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {error, timeout},
                 call(Config,
                   Node, khepri, get,
                   [StoreId, [foo], #{favor => consistency}]))
      end, RunningNodes2),

    ok.

can_join_several_times_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode   -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    {StoreId, LeaderNode1} = LeaderId1,
    OtherNodes1 = Nodes -- [LeaderNode1],

    ct:pal("Make leader node join the cluster again"),
    ?assertEqual(
       ok,
       call(Config,
         LeaderNode1, khepri_cluster, join, [StoreId, hd(OtherNodes1)])),

    ct:pal("Use database after recreating the cluster"),
    ?assertEqual(
       ok,
       call(Config, LeaderNode1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 call(Config,
                   Node, khepri, get,
                   [StoreId, [foo], #{favor => consistency}]))
      end, Nodes),

    ok.

can_rejoin_after_a_reset_in_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    {StoreId, LeaderNode1} = LeaderId1,
    OtherNodes1 = Nodes -- [LeaderNode1],

    ct:pal("Stop and reset leader node"),
    ?assertEqual(
       ok,
       call(Config,
         LeaderNode1, khepri, stop, [StoreId])),
    #{props := Props} = maps:get(LeaderNode1, PropsPerNode),
    ?assertMatch(
       ok,
       call(Config, LeaderNode1, helpers, stop_ra_system, [Props])),

    %% Wait for a new leader to be elected among the remaining members.
    NewLeader = get_leader_in_store(Config, StoreId, OtherNodes1),
    ?assertNotEqual(LeaderId1, NewLeader),

    %% The following call removes the existing data directory.
    ?assertMatch(
       #{},
       call(Config, LeaderNode1, helpers, start_ra_system, [RaSystem])),
    ?assertEqual(
       {ok, StoreId},
       call(Config, LeaderNode1, khepri, start, [RaSystem, StoreId])),

    ct:pal("Make leader node join the cluster again"),
    ?assertEqual(
       ok,
       call(Config,
         LeaderNode1, khepri_cluster, join, [StoreId, hd(OtherNodes1)])),

    ct:pal("Use database after recreating the cluster"),
    ?assertEqual(
       ok,
       call(Config, LeaderNode1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:fence() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, fence, [StoreId])),
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    ok.

can_restart_nodes_in_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    %% This (gratuitous) restart of Khepri stores is to ensure the remembered
    %% `RaServerConfig' contains everything we need in `join()' later. Indeed,
    %% `join()' calls `do_start_server()' which assumes the presence of some
    %% keys in `RaServerConfig'.
    lists:foreach(
      fun(Node) ->
              ct:pal("- Restart Khepri store on node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode   -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    %% Stop the current leader.
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal(
      "Stop database on leader node ~s (quorum is maintained)",
      [StoppedLeaderNode1]),
    ?assertEqual(
       ok,
       call(Config, StoppedLeaderNode1, khepri, stop, [StoreId])),

    %% Stop the next elected leader.
    LeaderId2 = get_leader_in_store(Config, StoreId, RunningNodes1),
    ?assertNotEqual(LeaderId1, LeaderId2),
    {StoreId, StoppedLeaderNode2} = LeaderId2,
    RunningNodes2 = RunningNodes1 -- [StoppedLeaderNode2],

    ct:pal(
      "Stop database on the new leader node ~s (quorum is lost)",
      [StoppedLeaderNode2]),
    ?assertEqual(
       ok,
       call(Config, StoppedLeaderNode2, khepri, stop, [StoreId])),

    ct:pal(
      "Restart database on node ~s (quorum is restored)",
      [StoppedLeaderNode1]),
    ?assertEqual(
       {ok, StoreId},
       call(Config,
         StoppedLeaderNode1, khepri, start, [RaSystem, RaServerConfig])),
    RunningNodes3 = RunningNodes2 ++ [StoppedLeaderNode1],

    ct:pal("Use database after having it running on 2 out of 3 nodes"),
    %% We try a put from the stopped leader and it should fail because the
    %% leaderboard on that node is stale.
    ?assertEqual(
       {error, noproc},
       call(
         Config, StoppedLeaderNode2, khepri, put, [StoreId, [foo], value2])),
    ?assertEqual(
       {error, noproc},
       call(Config, StoppedLeaderNode2, khepri, get, [StoreId, [foo]])),

    %% Querying running nodes should be fine however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       ok,
       call(Config, hd(RunningNodes3), khepri, put, [StoreId, [foo], value3])),

    %% The stopped leader should still fail to respond because it is stopped
    %% and again, the leaderboard is stale on this node.
    ?assertEqual(
       {error, noproc},
       call(Config, StoppedLeaderNode2, khepri, get, [StoreId, [foo]])),

    %% Running nodes should see the updated value however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:fence() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, fence, [StoreId])),
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value3},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

    ok.

handle_leader_down_on_three_node_cluster_command(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    %% This (gratuitous) restart of Khepri stores is to ensure the remembered
    %% `RaServerConfig' contains everything we need in `join()' later. Indeed,
    %% `join()' calls `do_start_server()' which assumes the presence of some
    %% keys in `RaServerConfig'.
    lists:foreach(
      fun(Node) ->
              ct:pal("- Restart Khepri store on node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    %% Stop the current leader.
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],
    Peer = proplists:get_value(StoppedLeaderNode1, PeerPerNode),

    ct:pal(
      "Stop database on leader node ~s (quorum is maintained)",
      [StoppedLeaderNode1]),
    ?assertEqual(ok, stop_erlang_node(StoppedLeaderNode1, Peer)),

    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:put() in node ~s", [Node]),
              ?assertMatch(
                 ok,
                 call(Config, Node, khepri, put, [StoreId, [foo], value1]))
      end, RunningNodes1),
    ok.

handle_leader_down_on_three_node_cluster_response(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    %% This (gratuitous) restart of Khepri stores is to ensure the remembered
    %% `RaServerConfig' contains everything we need in `join()' later. Indeed,
    %% `join()' calls `do_start_server()' which assumes the presence of some
    %% keys in `RaServerConfig'.
    lists:foreach(
      fun(Node) ->
              ct:pal("- Restart Khepri store on node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId1,
    [FollowerNode | _] = Nodes -- [LeaderNode],
    ct:pal("- khepri:put() from node ~s", [FollowerNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              Options = case Node of
                            LeaderNode -> #{};
                            FollowerNode -> #{};
                            _            -> #{favor => consistency}
                        end,
              ct:pal(
                "- khepri:get() from node ~s; options: ~0p", [Node, Options]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo], Options]))
      end, Nodes),

    %% Stop the current leader.
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],
    Peer = proplists:get_value(StoppedLeaderNode1, PeerPerNode),

    ct:pal(
      "Stop database on leader node ~s (quorum is maintained)",
      [StoppedLeaderNode1]),
    ?assertEqual(ok, stop_erlang_node(StoppedLeaderNode1, Peer)),

    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 call(Config, Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),
    ok.

can_reset_a_cluster_member(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Check membership on all nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:nodes() from node ~s", [Node]),
              ?assertEqual(
                 lists:sort(Nodes),
                 begin
                     {ok, N} = call(Config,
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end)
      end, Nodes),

    %% Reset the current leader.
    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal(
      "Reset database on leader node ~s",
      [StoppedLeaderNode1]),
    ?assertEqual(
       ok,
       call(Config, StoppedLeaderNode1, khepri_cluster, reset, [StoreId])),

    ct:pal("Check membership on remaining nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:nodes() from node ~s", [Node]),
              ?assertEqual(
                 lists:sort(RunningNodes1),
                 begin
                     {ok, N} = call(Config,
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end)
      end, RunningNodes1),

    ok.

can_query_members_with_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Query members before starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 call(Config, Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node,
                   khepri_cluster, members, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node,
                   khepri_cluster, members,
                   [StoreId, #{favor => low_latency}]))
      end, Nodes),

    ct:pal("Query nodes before starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 call(Config, Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{favor => low_latency}]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Query members after starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 call(Config, Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 call(Config,
                   Node,
                   khepri_cluster, members, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 call(Config,
                   Node,
                   khepri_cluster, members,
                   [StoreId, #{favor => low_latency}]))
      end, Nodes),

    ct:pal("Query nodes after starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, Nodes},
                 call(Config, Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {ok, Nodes},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {ok, Nodes},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{favor => low_latency}]))
      end, Nodes),

    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode1} = LeaderId1,
    ct:pal("Stop node ~s", [LeaderNode1]),
    LeaderPeer1 = proplists:get_value(LeaderNode1, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(LeaderNode1, LeaderPeer1)),

    ct:pal("Query members after stopping node ~s", [LeaderNode1]),
    LeftNodes1 = Nodes -- [LeaderNode1],
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, Nodes},
                 call(Config, Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {ok, Nodes},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {ok, Nodes},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{favor => low_latency}]))
      end, LeftNodes1),

    LeaderId2 = get_leader_in_store(Config, StoreId, LeftNodes1),
    {StoreId, LeaderNode2} = LeaderId2,
    ct:pal("Stop node ~s", [LeaderNode2]),
    LeaderPeer2 = proplists:get_value(LeaderNode2, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(LeaderNode2, LeaderPeer2)),

    ct:pal("Query members after stopping node ~s", [LeaderNode2]),
    LeftNodes2 = LeftNodes1 -- [LeaderNode2],
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, timeout},
                 call(Config, Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {error, timeout},
                 call(Config,
                   Node,
                   khepri_cluster, members, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 call(Config,
                   Node,
                   khepri_cluster, members,
                   [StoreId, #{favor => low_latency}]))
      end, LeftNodes2),

    ct:pal("Query nodes after stopping node ~s", [LeaderNode2]),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, timeout},
                 call(Config, Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {error, timeout},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{timeout => 10000}])),
              ?assertEqual(
                 {ok, Nodes},
                 call(Config,
                   Node,
                   khepri_cluster, nodes, [StoreId, #{favor => low_latency}]))
      end, LeftNodes2),

    ok.

can_wait_for_leader_with_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Wait for leader before starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId, 2000]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Wait for leader after starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId])),
              ?assertEqual(
                 ok,
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId, 2000]))
      end, Nodes),

    LeaderId1 = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode1} = LeaderId1,
    ct:pal("Stop node ~s", [LeaderNode1]),
    LeaderPeer1 = proplists:get_value(LeaderNode1, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(LeaderNode1, LeaderPeer1)),

    ct:pal("Wait for leader after stopping leader"),
    LeftNodes1 = Nodes -- [LeaderNode1],
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId])),
              ?assertEqual(
                 ok,
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId, 2000]))
      end, LeftNodes1),

    lists:foreach(
      fun(Node) ->
              ct:pal("Stop node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, stop, [StoreId]))
      end, LeftNodes1),

    ct:pal("Wait for leader after stopping database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 call(Config,
                   Node, khepri_cluster, wait_for_leader, [StoreId, 2000]))
      end, LeftNodes1),

    ok.

fail_to_join_if_not_started(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, _Node3] = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Cluster node"),
    ?assertEqual(
       {error, ?khepri_error(
                  not_a_khepri_store,
                  #{store_id => StoreId})},
       call(Config,
         Node1, khepri_cluster, join, [StoreId, Node2])),

    ok.

fail_to_join_non_existing_node(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, StoreId)),

    ct:pal("Cluster node"),
    RemoteNode = non_existing@localhost,
    ?assertEqual(
       {error, ?khepri_error(
                  failed_to_join_remote_khepri_node,
                  #{store_id => StoreId,
                    node => RemoteNode})},
       khepri_cluster:join(StoreId, RemoteNode)),

    ThisMember = khepri_cluster:this_member(StoreId),
    ?assertEqual(
       {ok, [ThisMember]},
       khepri_cluster:members(StoreId)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ok.

fail_to_join_non_existing_store(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, _Node3] = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       call(Config, Node1, khepri, start, [RaSystem, StoreId])),

    ct:pal("Cluster node"),
    ?assertEqual(
       {error, noproc},
       call(Config,
         Node1, khepri_cluster, join, [StoreId, Node2])),

    ?assertEqual(
       {ok, [khepri_cluster:node_to_member(StoreId, Node1)]},
       call(Config,
         Node1, khepri_cluster, members, [StoreId])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       call(Config, Node1, khepri, stop, [StoreId])),

    ok.

can_use_default_store_on_single_node(_Config) ->
    Node = node(),
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),
    DataDir = khepri_cluster:get_default_ra_system_or_data_dir(),
    ?assertNot(filelib:is_dir(DataDir)),

    ?assertEqual({error, noproc}, khepri:is_empty()),

    ?assertEqual({error, noproc}, khepri:get([foo])),
    ?assertEqual({error, noproc}, khepri:exists([foo])),
    ?assertEqual({error, noproc}, khepri:has_data([foo])),
    ?assertEqual({error, noproc}, khepri:is_sproc([foo])),

    ?assertEqual({error, noproc}, khepri_cluster:members()),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(#{favor => low_latency})),
    ?assertEqual({error, noproc}, khepri_cluster:nodes()),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(#{favor => low_latency})),

    ?assertEqual({error, noproc}, khepri_cluster:wait_for_leader()),

    {ok, StoreId} = khepri:start(),
    ?assert(filelib:is_dir(DataDir)),

    ?assertEqual(true, khepri:is_empty()),

    ?assertEqual(ok, khepri:create([foo], value1)),
    ?assertEqual(ok, khepri:put([foo], value2)),
    ?assertEqual(ok, khepri:put_many([foo], value2)),
    ?assertEqual(ok, khepri:update([foo], value3)),
    ?assertEqual(ok, khepri:compare_and_swap([foo], value3, value4)),

    ?assertEqual(false, khepri:is_empty(#{})),

    ?assertMatch(
       {error, ?khepri_error(mismatching_node, _)},
        khepri_adv:create([foo], value1)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 5}}},
       khepri_adv:put([foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 5}}},
       khepri_adv:put_many([foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 6}}},
       khepri_adv:update([foo], value3)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value3,
                         payload_version => 7}}},
       khepri_adv:compare_and_swap([foo], value3, value4)),

    ?assertEqual(true, khepri:exists([foo])),
    ?assertEqual({ok, value4}, khepri:get([foo])),
    ?assertEqual({ok, value4}, khepri:get([foo], #{})),
    ?assertEqual({ok, value4}, khepri:get_or([foo], default)),
    ?assertEqual({ok, value4}, khepri:get_or([foo], default, #{})),
    ?assertEqual({ok, #{[foo] => value4}}, khepri:get_many([foo])),
    ?assertEqual({ok, #{[foo] => value4}}, khepri:get_many([foo], #{})),
    ?assertEqual({ok, #{[foo] => value4}}, khepri:get_many_or([foo], default)),
    ?assertEqual(
       {ok, #{[foo] => value4}},
       khepri:get_many_or([foo], default, #{})),

    ?assertEqual(
       {ok, #{[foo] => ok}},
       khepri:fold([foo], fun(P, _NP, Acc) -> Acc#{P => ok} end, #{})),
    ?assertEqual(
       {ok, #{[foo] => ok}},
       khepri:fold([foo], fun(P, _NP, Acc) -> Acc#{P => ok} end, #{}, #{})),

    ?assertEqual(
       ok,
       khepri:foreach([foo], fun(_P, _NP) -> ok end)),
    ?assertEqual(
       ok,
       khepri:foreach([foo], fun(_P, _NP) -> ok end, #{})),

    ?assertEqual(
       {ok, #{[foo] => ok}},
       khepri:map([foo], fun(_P, _NP) -> ok end)),
    ?assertEqual(
       {ok, #{[foo] => ok}},
       khepri:map([foo], fun(_P, _NP) -> ok end, #{})),

    ?assertEqual(
       {ok, #{[foo] => value4}},
       khepri:filter([foo], fun(_P, _NP) -> true end)),
    ?assertEqual(
       {ok, #{[foo] => value4}},
       khepri:filter([foo], fun(_P, _NP) -> true end, #{})),

    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get([foo])),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get([foo], #{})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get_many([foo])),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get_many([foo], #{})),

    ?assertNot(khepri_machine:does_api_comply_with(some_behaviour, StoreId)),

    ?assertEqual({ok, [{StoreId, Node}]}, khepri_cluster:members()),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:members(#{favor => low_latency})),
    ?assertEqual({ok, [Node]}, khepri_cluster:nodes()),
    ?assertEqual({ok, [Node]}, khepri_cluster:nodes(#{favor => low_latency})),

    ?assertEqual(ok, khepri_cluster:wait_for_leader()),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual({error, noproc}, khepri:get([foo])),
    ?assertEqual({error, noproc}, khepri:exists([foo])),
    ?assertEqual({error, noproc}, khepri:has_data([foo])),
    ?assertEqual({error, noproc}, khepri:is_sproc([foo])),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertEqual({ok, value4}, khepri:get([foo])),
    ?assertEqual(true, khepri:has_data([foo])),
    ?assertEqual(false, khepri:is_sproc([foo])),
    ?assertThrow(
       ?khepri_exception(
          denied_execution_of_non_sproc_node,
          #{path := [foo],
            args := [],
            node_props := #{data := value4,
                            payload_version := 7}}),
       khepri:run_sproc([foo], [])),
    ?assertEqual({ok, 1}, khepri:count("**")),
    ?assertEqual(
       ok,
       khepri:register_trigger(
         trigger_id,
         [foo],
         [sproc])),
    ?assertEqual(
       ok,
       khepri:register_trigger(
         trigger_id,
         [foo],
         [sproc], #{})),

    ProjectionName1 = projection1,
    ?assertEqual(false, khepri:has_projection(ProjectionName1)),
    ?assertEqual(
       false,
       khepri:has_projection(ProjectionName1, #{favor => consistency})),
    Projection1 = khepri_projection:new(
                    ProjectionName1,
                    fun(Path, Data) -> {Path, Data} end),
    ?assertEqual(ok, khepri:register_projection("/**", Projection1)),
    ?assertEqual(
       {error, ?khepri_error(
                 projection_already_exists,
                 #{name => ProjectionName1})},
       khepri:register_projection("/**", Projection1)),
    ?assertEqual(
       true,
       khepri:has_projection(ProjectionName1, #{favor => consistency})),

    ProjectionName2 = projection2,
    ?assertEqual(false, khepri:has_projection(ProjectionName2)),
    Projection2 = khepri_projection:new(
                    ProjectionName2,
                    fun(Path, Data) -> {Data, Path} end,
                    #{read_concurrency => true, keypos => 2}),
    ?assertEqual(ok, khepri:register_projection("/**", Projection2, #{})),
    ?assertEqual(true, khepri:has_projection(ProjectionName2)),

    ?assertEqual(ok, khepri:unregister_projections([ProjectionName1])),
    ?assertEqual(
       {ok, #{}},
       khepri_adv:unregister_projections([ProjectionName1])),
    ?assertEqual(ok, khepri:unregister_projections([ProjectionName2], #{})),
    ?assertEqual(
       {ok, #{}},
       khepri_adv:unregister_projections([ProjectionName2], #{})),

    ?assertEqual({ok, ok}, khepri:transaction(fun() -> ok end)),
    ?assertEqual({ok, ok}, khepri:transaction(fun() -> ok end, ro)),
    ?assertEqual({ok, ok}, khepri:transaction(fun() -> ok end, ro, #{})),

    ?assertEqual(ok, khepri:create([bar], value1)),
    ?assertEqual(ok, khepri:clear_payload([bar])),
    ?assertEqual(ok, khepri:clear_many_payloads([bar])),
    ?assertEqual(ok, khepri:delete([bar])),
    ?assertEqual(ok, khepri:delete([bar], #{})),
    ?assertEqual(ok, khepri:delete_many([bar])),
    ?assertEqual(ok, khepri:delete_many([bar], #{})),

    ?assertEqual(ok, khepri:create([bar], value1)),
    ?assertEqual(
       {ok, #{[bar] => #{data => value1,
                         payload_version => 2}}},
       khepri_adv:clear_payload([bar])),
    ?assertEqual(
       {ok, #{[bar] => #{payload_version => 2}}},
       khepri_adv:clear_many_payloads([bar])),
    ?assertEqual(
       {ok, #{[bar] => #{payload_version => 2,
                         delete_reason => explicit}}},
       khepri_adv:delete([bar])),
    ?assertMatch(
       {ok, #{}},
       khepri_adv:delete([bar], #{})),
    ?assertEqual(
       {ok, #{}},
       khepri_adv:delete_many([bar])),
    ?assertEqual(
       {ok, #{}},
       khepri_adv:delete_many([bar], #{})),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertEqual(ok, khepri:reset()),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertMatch(
       {error, ?khepri_error(node_not_found, _)},
       khepri:get([foo])),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual(ok, application:stop(khepri)),
    ?assertEqual(ok, application:stop(ra)),

    helpers:remove_store_dir(DataDir),
    ?assertNot(filelib:is_dir(DataDir)).

can_start_store_in_specified_data_dir_on_single_node(_Config) ->
    DataDir = atom_to_list(?FUNCTION_NAME),
    ?assertNot(filelib:is_dir(DataDir)),

    ?assertEqual({error, noproc}, khepri:get([foo])),

    {ok, StoreId} = khepri:start(DataDir),
    ?assert(filelib:is_dir(DataDir)),

    ?assertEqual(ok, khepri:create(StoreId, [foo], value1, #{})),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value2, #{})),
    ?assertEqual(ok, khepri:update(StoreId, [foo], value3, #{})),
    ?assertEqual(
       ok,
       khepri:compare_and_swap(StoreId, [foo], value3, value4, #{})),

    ?assertEqual(true, khepri:exists([foo], #{})),
    ?assertEqual({ok, value4}, khepri:get([foo], #{})),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start(list_to_binary(DataDir))),
    ?assertEqual({ok, value4}, khepri:get([foo])),
    ?assertEqual({ok, value4}, khepri:get_or([foo], no_data)),
    ?assertEqual(true, khepri:has_data([foo], #{})),
    ?assertEqual(false, khepri:is_sproc([foo], #{})),
    ?assertThrow(
       ?khepri_exception(
          denied_execution_of_non_sproc_node,
          #{path := [foo],
            args := [],
            node_props := #{data := value4,
                            payload_version := 4}}),
       khepri:run_sproc([foo], [], #{})),
    ?assertEqual({ok, 1}, khepri:count("**", #{})),

    ?assertEqual(ok, khepri:create([bar], value1)),
    ?assertEqual(ok, khepri:clear_payload([bar])),
    ?assertEqual(ok, khepri:delete([bar], #{})),

    ?assertEqual({ok, StoreId}, khepri:start(DataDir)),
    ?assertEqual(ok, khepri:reset(10000)),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start(DataDir)),
    ?assertMatch(
       {error, ?khepri_error(node_not_found, _)},
       khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start(DataDir)),
    ?assertEqual(ok, khepri:reset(StoreId, 10000)),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual(ok, application:stop(khepri)),
    ?assertEqual(ok, application:stop(ra)),

    helpers:remove_store_dir(DataDir),
    ?assertNot(filelib:is_dir(DataDir)).

can_set_snapshot_interval(Config) ->
    Node = node(),
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    RaServerConfig = #{cluster_name => StoreId,
                       machine_config => #{snapshot_interval => 5}},
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig)),

    RaServer = khepri_cluster:node_to_member(StoreId, Node),
    ?assertMatch(
      {ok, #{log := #{snapshot_index := undefined}}, RaServer},
      ra:member_overview(RaServer)),

    ?assertEqual(ok, khepri:fence(StoreId)),

    %% The first two second "invisible" commands are `machine_version' and
    %% `#cache_members_list{}' which is appended by the aux handler.
    await_applied_command_count(RaServer, 2),

    ct:pal("Verify applied command count is 2"),
    ?assertEqual(
       #{applied_command_count => 2},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Submit command 2 (`put`)"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

    ct:pal("Verify applied command count is 3"),
    ?assertEqual(
       #{applied_command_count => 3},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Submit command 3 (`put`)"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

    ct:pal("Verify applied command count is 4"),
    ?assertEqual(
       #{applied_command_count => 4},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ?assertMatch(
      {ok, #{log := #{snapshot_index := undefined}}, RaServer},
      ra:member_overview(RaServer)),

    ct:pal("Submit command 4 (`put`)"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

    await_snapshot_index(RaServer, 5),

    ct:pal("Verify applied command count is 0"),
    ?assertEqual(
       #{},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    ok.

await_applied_command_count(RaServer, ExpectedCount) ->
    await_applied_command_count(RaServer, ExpectedCount, 10).

await_applied_command_count({StoreId, _} = RaServer, ExpectedCount, Retries) ->
    #{applied_command_count := Count} = khepri_machine:process_query(
                                          StoreId,
                                          fun khepri_machine:get_metrics/1,
                                          #{}),
    case Count of
       ExpectedCount ->
          ok;
       _ ->
          case Retries of
              0 ->
                 erlang:error({?FUNCTION_NAME,
                               [{expected, ExpectedCount},
                                {value, Count}]});
              _ ->
                 timer:sleep(10),
                 await_applied_command_count(
                   RaServer, ExpectedCount, Retries - 1)
          end
    end.

await_snapshot_index(RaServer, ExpectedIndex) ->
    await_snapshot_index(RaServer, ExpectedIndex, 10).

await_snapshot_index(RaServer, ExpectedIndex, Retries) ->
    {ok, #{log := #{snapshot_index := ActualIndex}}, RaServer} =
      ra:member_overview(RaServer),
    case ActualIndex of
       ExpectedIndex ->
          ok;
       _ ->
          case Retries of
              0 ->
                 erlang:error({await_snapshot_index,
                               [{expected, ExpectedIndex},
                                {value, ActualIndex}]});
              _ ->
                 timer:sleep(10),
                 await_snapshot_index(RaServer, ExpectedIndex, Retries - 1)
          end
    end.

projections_are_consistent_on_three_node_cluster(Config) ->
    ProjectionName = ?MODULE,

    %% We call `khepri_projection:new/2' on the local node and thus need
    %% Khepri.
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),

    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Projection table does not exist before registering"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- ets:info/1 from node ~s", [Node]),
              ?assertEqual(
                 undefined,
                 call(Config, Node, ets, info, [ProjectionName]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Register projection on node ~s", [Node1]),
    Projection = khepri_projection:new(
                   ProjectionName, fun(Path, Payload) -> {Path, Payload} end),
    call(Config, Node1,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection]),

    ct:pal("The projection table exists on node ~s", [Node1]),
    ?assertNotEqual(
       undefined,
       call(Config, Node1, ets, info, [ProjectionName])),

    ct:pal("Wait for the projection table to exist on all nodes"),
    %% `khepri:register_projection/4' uses a `reply_mode' of `local' which
    %% blocks until the projection is registered by the cluster member on
    %% `Node1'. The projection is also guaranteed to exist on the leader
    %% member but the remaining cluster members are eventually consistent.
    %% This function polls for the remaining cluster members.
    ok = wait_for_projection_on_nodes(Config, [Node2, Node3], ProjectionName),

    ct:pal("An update by a member looks consistent to that member "
           "when using the `local' `reply_mode'"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:put/4 from node ~s", [Node]),
              %% `#{reply_from => local}' makes the `put' immediately
              %% consistent on the caller node.
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri, put, [StoreId, [foo], value1,
                                              #{reply_from => local}])),

              ct:pal("- ets:lookup() from node ~s", [Node]),
              ?assertEqual(
                 [{[foo], value1}],
                 call(Config, Node, ets, lookup, [ProjectionName, [foo]]))
      end, Nodes),

    LeaderId = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId,
    [FollowerNode | _] = Nodes -- [LeaderNode],

    ct:pal("An update by a follower (~s) is immediately consistent on the "
           "leader (~s) with a `local' `reply_mode'",
           [FollowerNode, LeaderNode]),
    ?assertEqual(
       ok,
       call(Config, FollowerNode, khepri, put, [StoreId, [foo], value2,
                                            #{reply_from => local}])),
    ?assertEqual(
       [{[foo], value2}],
       call(Config, LeaderNode, ets, lookup, [ProjectionName, [foo]])),

    ok.

wait_for_projection_on_nodes(_Config, [], _ProjectionName) ->
   ok;
wait_for_projection_on_nodes(Config, [Node | Rest] = Nodes, ProjectionName) ->
   case call(Config, Node, ets, info, [ProjectionName]) of
      undefined ->
         timer:sleep(10),
         wait_for_projection_on_nodes(Config, Nodes, ProjectionName);
      _Info ->
         ct:pal("- projection ~s exists on node ~s", [ProjectionName, Node]),
         wait_for_projection_on_nodes(Config, Rest, ProjectionName)
   end.

projections_are_updated_when_a_snapshot_is_installed(Config) ->
    %% When a cluster member falls behind on log entries, the leader tries to
    %% catch it up with a snapshot. Specifically: if a cluster member's current
    %% latest raft index is older than the leader's snapshot index, the leader
    %% catches up that member by sending it a snapshot and then any log entries
    %% that follow.
    %%
    %% When this happens the member doesn't see the changes as regular
    %% commands (i.e. handled in `ra_machine:apply/3'). Instead the machine
    %% state is replaced entirely. So when a snapshot is installed we must
    %% restore projections. The machine is alive at this point though so we
    %% can't restore projections the same way as when we restart a member
    %% though. We need to diff the old and new state first to handle any newly
    %% registered or unregistered projections and then to update any existing
    %% projections with updated or deleted records. In `khepri_machine' this is
    %% done in the `snapshot_installed/3' callback implementation.
    %%
    %% To test this we stop a member, apply enough commands to cause the leader
    %% to take a snapshot, and then restart the member and assert that the
    %% projection contents are as expected.

    %% We call `khepri_projection:new/2' on the local node and thus need
    %% Khepri.
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),

    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    %% Set the snapshot interval low so that we can trigger a snapshot by
    %% sending a few commands.
    RaServerConfig = #{cluster_name => StoreId,
                       machine_config => #{snapshot_interval => 20}},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ProjectionName1 = projection_1,
    Projection1 = khepri_projection:new(
                    ProjectionName1,
                    fun(Path, Payload) -> {Path, Payload} end),
    ProjectionName2 = projection_2,
    Projection2 = khepri_projection:new(
                    ProjectionName2,
                    fun(Path, Payload) -> {Path, Payload} end),
    ProjectionName3 = projection_3,
    Projection3 = khepri_projection:new(
                    ProjectionName3,
                    fun(Path, Payload) -> {Path, Payload} end),

    ct:pal("Register projection ~ts on node ~s", [ProjectionName1, Node1]),
    call(Config, Node1,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection1]),
    ok = wait_for_projection_on_nodes(
           Config, [Node2, Node3], ProjectionName1),

    ct:pal("Register projection ~ts on node ~s", [ProjectionName2, Node1]),
    call(Config, Node1,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection2]),
    ok = wait_for_projection_on_nodes(
           Config, [Node2, Node3], ProjectionName2),

    ?assertEqual(
       ok,
       call(Config, Node3, khepri, put, [StoreId, [key1], value1v1,
                                     #{reply_from => local}])),
    ?assertEqual(
       value1v1,
       call(Config, Node3, ets, lookup_element, [ProjectionName1, [key1], 2])),
    %% This key will be deleted.
    ?assertEqual(
       ok,
       call(Config, Node3, khepri, put, [StoreId, [key2], value2v1,
                                     #{reply_from => local}])),
    %% So far there isn't a snapshot.
    ?assertMatch(
      {ok, #{log := #{snapshot_index := undefined}}, _},
      ra:member_overview(khepri_cluster:node_to_member(StoreId, Node3))),

    ct:pal(
      "Stop cluster member ~s (quorum is maintained)", [Node1]),
    ok = call(Config, Node1, khepri, stop, [StoreId]),

    ct:pal("Modify paths which are watched by projections"),
    ct:pal("- set key1:value1v2"),
    ok = call(Config, Node3, khepri, put, [StoreId, [key1], value1v2]),
    ct:pal("- delete key2"),
    ok = call(Config, Node3, khepri, delete, [StoreId, [key2]]),
    ct:pal("- set key3:value3v1"),
    ok = call(Config, Node3, khepri, put, [StoreId, [key3], value3v1]),
    ct:pal("- set key4:value4v1"),
    ok = call(Config, Node3, khepri, put, [StoreId, [key4], value4v1]),

    ct:pal("Register projection ~ts on node ~s", [ProjectionName3, Node3]),
    call(Config, Node3,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection3]),
    ct:pal("Unregister projection ~ts on node ~s", [ProjectionName2, Node3]),
    call(Config, Node3,
      khepri, unregister_projections, [StoreId, [ProjectionName2]]),

    ct:pal("Send many commands to ensure a snapshot is triggered"),
    [ok = call(Config, Node3, khepri, put, [StoreId, [key5], value5v1])
     || _ <- lists:seq(1, 20)],

    {ok, #{log := #{snapshot_index := SnapshotIndex}}, _} =
       ra:member_overview(khepri_cluster:node_to_member(StoreId, Node3)),
    ct:pal("New snapshot index: ~p", [SnapshotIndex]),
    ?assert(is_number(SnapshotIndex) andalso SnapshotIndex > 20),

    ct:pal("Restart cluster member ~s", [Node1]),
    {ok, StoreId} = call(
                      Config, Node1,
                      khepri, start, [RaSystem, RaServerConfig]),

    %% Execute a command with local-reply from Node1 - this will ensure that
    %% we block until Node1 has caught up with the latest changes before we
    %% check its projection table. We have to retry the command a few times
    %% if it times out to deal with CI runners with few schedulers.
    ok = put_with_retry(
           Config, StoreId, Node1,
           [key5], value5v1, #{reply_from => local}),
    ?assertEqual(
      value5v1,
      call(Config, Node1, ets, lookup_element, [ProjectionName1, [key5], 2])),

    ct:pal("Contents of projection table '~ts'", [ProjectionName1]),
    [begin
         Contents = call(Config, Node, ets, tab2list, [ProjectionName1]),
         ct:pal("- node ~ts:~n~p", [Node, Contents])
     end || Node <- Nodes],

    [begin
         ?assertEqual(
           value1v2,
           call(
             Config, Node, ets, lookup_element, [ProjectionName1, [key1], 2])),
         ?assertEqual(
           false,
           call(Config, Node, ets, member, [ProjectionName1, [key2]])),
         ?assertEqual(
           value3v1,
           call(
             Config, Node, ets, lookup_element, [ProjectionName1, [key3], 2])),
         ?assertEqual(
           value4v1,
           call(
             Config, Node, ets, lookup_element, [ProjectionName1, [key4], 2]))
     end || Node <- Nodes],

    %% Ensure that the projections themselves are also updated on Node1.
    %% ProjectionName2 was unregistered and ProjectionName3 was registered.
    ?assertEqual(
      undefined,
      call(Config, Node1, ets, info, [ProjectionName2])),
    ?assertEqual(
      value1v2,
      call(Config, Node1, ets, lookup_element, [ProjectionName3, [key1], 2])),

    ok.

put_with_retry(Config, StoreId, Node, Key, Value, Options) ->
   put_with_retry(Config, StoreId, Node, Key, Value, Options, 10).

put_with_retry(Config, StoreId, Node, Key, Value, Options, Retries) ->
   ct:pal("- put (~p) '~p':'~p' (try ~b)", [Node, Key, Value, 10 - Retries]),
   case call(Config, Node, khepri, put, [StoreId, Key, Value, Options]) of
      {error, timeout} = Err ->
         case Retries of
            0 ->
               erlang:error({?FUNCTION_NAME, [{actual, Err}]});
            _ ->
               timer:sleep(10),
               put_with_retry(
                 Config, StoreId, Node, Key, Value, Options, Retries - 1)
         end;
      Ret ->
         Ret
   end.

async_command_leader_change_in_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    LeaderId = get_leader_in_store(Config, StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId,

    ct:pal("Send an async command from the leader node ~s", [LeaderNode]),
    ok = erpc:call(
           LeaderNode,
           fun() ->
                 %% The async call will succeed because this member is the
                 %% leader.
                 CorrelationId = 1,
                 Extra = #{async => CorrelationId},
                 ok = khepri:put(StoreId, [foo], ?NO_PAYLOAD, Extra),
                 RaEvent = receive
                              {ra_event, _, _} = Event ->
                                 Event
                           after
                              5_000 ->
                                 throw(timeout)
                           end,
                 ?assertMatch(
                   [{CorrelationId, {ok, _}}],
                   khepri:handle_async_ret(StoreId, RaEvent))
           end),

    [FollowerNode, _] = Nodes -- [LeaderNode],
    ct:pal("Send async commands from a follower node ~s", [FollowerNode]),
    ok = erpc:call(
           FollowerNode,
           fun() ->
                 CorrelationId = 1,
                 Extra2 = #{async => CorrelationId},
                 ok = khepri:put(StoreId, [foo], ?NO_PAYLOAD, Extra2),
                 RaEvent2 = receive
                               {ra_event, _, _} = Event2 ->
                                  Event2
                            after
                               1_000 ->
                                  throw(timeout)
                            end,
                 ?assertMatch(
                   [{CorrelationId, {ok, _}}],
                   khepri:handle_async_ret(StoreId, RaEvent2))
           end),
    ok.

%% This testcase tries to reproduce a bug discovered while working on the
%% following patch:
%% https://github.com/rabbitmq/rabbitmq-server/pull/10472
%%
%% `ra:process_command()' may return an error but the command may still be
%% appended to the log. The current retry mechanism calls
%% `ra:process_command()' again after some specific errors. Therefore, the
%% command may be appended twice and thus executed twice.
%%
%% For transactions in particular, this can be a problem because the caller
%% will receive the return value of the second run, not the first.
%%
%% In this testcase, we reproduce the situation by starting a process that
%% spams the Khepri store with transactions that bump a counter. Concurrently,
%% we add and remove a cluster member to trigger an election. At the end, we
%% compare the number of transactions to the counter value. They must be the
%% same.

spam_txs_during_election(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database on each node"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    %% The first process to spam the Khepri store is here to make sure the test
    %% base is ok. We don't expect any failure at this stage.
    Parent = self(),
    {Pid1, MRef1} = spawn_monitor(
                      fun() ->
                              bump_counter_proc(
                                Config, Parent, Node1, StoreId, 0)
                      end),
    timer:sleep(500),

    ct:pal("Node ~s joins node ~s", [Node2, Node1]),
    ?assertEqual(
       ok,
       call(Config, Node2, khepri_cluster, join, [StoreId, Node1])),

    ct:pal("Asking spammer process #1 ~0p to stop", [Pid1]),
    Pid1 ! stop,
    Runs1 = receive
                {'DOWN', MRef1, _, Pid1, Reason1} ->
                    ?assertEqual(normal, Reason1),
                    receive {runs, R1} -> R1 end
            end,
    ct:pal("Spammer process #1 ~0p ran transaction ~b times", [Pid1, Runs1]),

    %% The real test starts here: we have a cluster of two nodes and we are
    %% about to remove the initial one which must be the leader. This will stop
    %% that leader process that answers transactions and it should return an
    %% `{error, shutdown}' error. The retry mechanism will submit the
    %% transaction again.
    %%
    %% The spammer process is started on the second node, the one that will
    %% become the leader, just because the first one will go away. The
    %% important part is that its requests will first go to the leader on node
    %% 1 first, then at some point will go to the new leader on node 2.
    {Pid2, MRef2} = spawn_monitor(
                      fun() ->
                              bump_counter_proc(
                                Config, Parent, Node2, StoreId, Runs1)
                      end),
    timer:sleep(500),

    ct:pal("Node ~s leaves node ~s", [Node1, Node2]),
    ?assertEqual(
       ok,
       call(Config, Node1, khepri_cluster, reset, [StoreId])),
    timer:sleep(500),

    ct:pal("Asking spammer process #2 ~0p to stop", [Pid2]),
    Pid2 ! stop,
    _Runs = receive
                {'DOWN', MRef2, _, Pid2, Reason2} ->
                    ?assertEqual(normal, Reason2),
                    receive {runs, R2} -> R2 end
            end,

    ok.

bump_counter_proc(Config, Parent, Node, StoreId, Runs) ->
    receive
        stop ->
            ct:pal(
              "Spammer process ~0p exiting after ~b runs",
              [self(), Runs]),
            Parent ! {runs, Runs},
            ok
    after 0 ->
              NewRuns = Runs + 1,
              ?LOG_INFO(
                 "Transaction run #~b on node ~0p",
                 [NewRuns, Node]),
              {ok, Ret} = call(Config,
                            Node, khepri, transaction,
                            [StoreId, fun bump_counter_tx/0, rw,
                             #{reply_from => leader}]),
              ?LOG_INFO(
                 "Transaction returned ~p after ~b runs",
                 [Ret, NewRuns]),
              ?assertEqual({counter, NewRuns}, Ret),
              bump_counter_proc(Config, Parent, Node, StoreId, NewRuns)
    end.

bump_counter_tx() ->
    Path = [counter],
    {ok, Counter} = khepri_tx:get_or(Path, 0),
    NewCounter = Counter + 1,
    ok = khepri_tx:put(Path, NewCounter),
    {counter, NewCounter}.

spam_changes_during_unregister_projections(Config) ->
    %% We call `khepri_projection:new/2' on the local node and thus need
    %% Khepri.
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),
    ProjectionName = ?MODULE,

    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Register projection on node ~s", [Node1]),
    Projection = khepri_projection:new(
                   ProjectionName, fun(Path, Payload) -> {Path, Payload} end),
    call(Config, Node1,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection]),

    ct:pal("Start the spammer process"),
    Path = [key],
    Parent = self(),
    {Pid, MRef} = spawn_monitor(
                    fun() ->
                          spam_async_changes(
                            Config, Parent, Node1, StoreId, Path, 0)
                    end),
    timer:sleep(50),

    ct:pal("Unregistering the projection"),
    call(Config,
      Node1, khepri, unregister_projections, [StoreId, [ProjectionName]]),

    ?assertEqual(
      undefined,
      call(Config, Node1, ets, info, [ProjectionName])),

    timer:sleep(50),

    ct:pal("Asking spammer process ~0p to stop", [Pid]),
    Pid ! stop,
    Runs = receive
               {'DOWN', MRef, _, Pid, Reason1} ->
                   ?assertEqual(normal, Reason1),
                   receive {runs, R} -> R end
           end,
    ct:pal("Spammer process ~0p sent ~b async commands", [Pid, Runs]),

    ok.

spam_async_changes(Config, Parent, Node, StoreId, Path, Runs) ->
    receive
        stop ->
            ct:pal(
              "Spammer process ~0p exiting after ~b runs",
              [self(), Runs]),
            Parent ! {runs, Runs},
            ok
    after 0 ->
              NewRuns = Runs + 1,
              ?LOG_INFO("Update run #~b on node ~0p", [NewRuns, Node]),
              ok = call(Config,
                     Node, khepri, put,
                     [StoreId, Path, NewRuns, #{async => true}]),
              spam_async_changes(
                Config, Parent, Node, StoreId, Path, NewRuns)
    end.

trigger_based_on_process_event_works(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, StoreId)),

    TriggerId = ?FUNCTION_NAME,
    Pid = start_monitored_proc(),
    EventFilter = khepri_evf:process(Pid),
    StoredProcPath = [sproc],

    ct:pal("Put store procedure"),
    SprocRet = ?FUNCTION_NAME,
    ?assertEqual(
       ok,
       khepri:put(StoreId, StoredProcPath, make_sproc(self(), SprocRet))),

    ct:pal("Register trigger"),
    ?assertEqual(
       ok,
       khepri:register_trigger(
         StoreId, TriggerId, EventFilter, StoredProcPath)),

    ct:pal("Terminate process to trigger the stored procedure"),
    Pid ! stop,

    ct:pal("Wait for the trigger message from the leader node"),
    receive
        {sproc, SprocRet, Node, _Props} when Node =:= node() ->
            ok
    after 60000 ->
              ct:pal(
                "Messages in inbox:~n~p",
                [erlang:process_info(self(), messages)]),
              ct:fail("Did not receive the expected message from the trigger")
    end,

    ct:pal("Stop database"),
    ?assertEqual(ok, khepri:stop(StoreId)),

    ok.

process_event_is_delayed_during_node_down_1(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, _Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node1]))
      end, Nodes -- [Node1]),

    TriggerId = ?FUNCTION_NAME,
    MonitoredProcNode = Node3,
    Pid = call(Config, MonitoredProcNode, ?MODULE, start_monitored_proc, []),
    EventFilter = khepri_evf:process(Pid),
    StoredProcPath = [sproc],

    ct:pal("Put store procedure"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, put,
         [StoreId, StoredProcPath, make_sproc(self(), TriggerId)])),

    ct:pal("Register trigger"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, register_trigger,
         [StoreId, TriggerId, EventFilter, StoredProcPath])),

    ct:pal(
      "Stop node ~s to trigger the process monitor",
      [MonitoredProcNode]),
    Peer = proplists:get_value(MonitoredProcNode, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(MonitoredProcNode, Peer)),

    ct:pal(
      "Restart node ~s to trigger the node monitor",
      [MonitoredProcNode]),
    {ok, NewPeer, _Node} = peer:start(#{name => MonitoredProcNode,
                                        wait_boot => infinity,
                                        connection => standard_io}),
    ?assertEqual(pong, peer:call(NewPeer, net_adm, ping, [Node1])),
    peer:stop(NewPeer),

    {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),

    ct:pal("Wait for the trigger message from the leader node"),
    receive
        {sproc, TriggerId, LeaderNode, Event} ->
            ct:pal("Process event: ~p", [Event]),
            ?assertMatch(
               #khepri_trigger{event = #{pid := Pid,
                                         change := {'DOWN', Reason}}}
                 when Reason =:= noproc orelse
                      Reason =:= noconnection,
               Event),
            ok
    after 60000 ->
              ct:pal(
                "Messages in inbox:~n~p",
                [erlang:process_info(self(), messages)]),
              ct:fail(
                "Did not receive the expected message from the "
                "trigger from ~s",
                [LeaderNode])
    end,

    ok.

process_event_is_delayed_during_node_down_2(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, _Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node1]))
      end, Nodes -- [Node1]),

    TriggerId = ?FUNCTION_NAME,
    MonitoredProcNode = Node3,
    Pid = call(Config, MonitoredProcNode, ?MODULE, start_monitored_proc, []),
    EventFilter = khepri_evf:process(Pid),
    StoredProcPath = [sproc],

    ct:pal("Put store procedure"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, put,
         [StoreId, StoredProcPath, make_sproc(self(), TriggerId)])),

    ct:pal("Register trigger"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, register_trigger,
         [StoreId, TriggerId, EventFilter, StoredProcPath])),

    ct:pal(
      "Stop node ~s to trigger the process monitor",
      [MonitoredProcNode]),
    Peer = proplists:get_value(MonitoredProcNode, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(MonitoredProcNode, Peer)),

    receive
        {sproc, TriggerId, _LeaderNode, _Event} ->
            ct:fail(
              "Received the stored proc message before the monitored node "
              "came back up")
    after 2000 ->
              ok
    end,

    ct:pal("Remove node ~s from cluster", [MonitoredProcNode]),
    ?assertMatch(
       {ok, _, _},
       call(
         Config, Node1,
         ra, remove_member,
         [{StoreId, Node1}, {StoreId, MonitoredProcNode}, infinity])),

    {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),

    ct:pal("Wait for the trigger message from the leader node"),
    receive
        {sproc, TriggerId, LeaderNode, Event} ->
            ct:pal("Process event: ~p", [Event]),
            ?assertMatch(
               #khepri_trigger{event = #{pid := Pid,
                                         change := {'DOWN', Reason}}}
                 when Reason =:= noproc orelse
                      Reason =:= noconnection,
               Event),
            ok
    after 60000 ->
              ct:pal(
                "Messages in inbox:~n~p",
                [erlang:process_info(self(), messages)]),
              ct:fail(
                "Did not receive the expected message from the "
                "trigger from ~s",
                [LeaderNode])
    end,

    ok.

process_event_is_delayed_during_node_down_3(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = maps:keys(PropsPerNode),

    %% Compared to `process_event_is_delayed_during_node_down_1', we only
    %% cluster the first two nodes. Therefore, the monitored process will run
    %% on a node that is outside of the Khepri cluster.
    Nodes = [Node1, Node2],

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node1]))
      end, Nodes -- [Node1]),

    TriggerId = ?FUNCTION_NAME,
    MonitoredProcNode = Node3,
    Pid = call(Config, MonitoredProcNode, ?MODULE, start_monitored_proc, []),
    EventFilter = khepri_evf:process(Pid),
    StoredProcPath = [sproc],

    ct:pal("Put store procedure"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, put,
         [StoreId, StoredProcPath, make_sproc(self(), TriggerId)])),

    ct:pal("Register trigger"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, register_trigger,
         [StoreId, TriggerId, EventFilter, StoredProcPath])),

    ct:pal(
      "Stop database on leader node ~s (quorum is maintained)",
      [Node1]),
    Peer = proplists:get_value(MonitoredProcNode, PeerPerNode),
    ?assertEqual(ok, stop_erlang_node(MonitoredProcNode, Peer)),

    {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),

    ct:pal("Wait for the trigger message from the leader node"),
    receive
        {sproc, TriggerId, LeaderNode, Event} ->
            ct:pal("Process event: ~p", [Event]),
            ?assertMatch(
               #khepri_trigger{event = #{pid := Pid,
                                         change := {'DOWN', noconnection}}},
               Event),
            ok
    after 60000 ->
              ct:pal(
                "Messages in inbox:~n~p",
                [erlang:process_info(self(), messages)]),
              ct:fail(
                "Did not receive the expected message from the "
                "trigger from ~s",
                [LeaderNode])
    end,

    ok.

trigger_runs_on_leader(Config) ->
    trigger_runs_on_where(Config, ?FUNCTION_NAME, leader).

trigger_runs_on_follower(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = maps:keys(PropsPerNode),
    trigger_runs_on_where(Config, ?FUNCTION_NAME, {member, Node1}).

trigger_runs_on_local_member(Config) ->
    trigger_runs_on_where(Config, ?FUNCTION_NAME, local).

trigger_runs_on_all_members(Config) ->
    trigger_runs_on_where(Config, ?FUNCTION_NAME, all_members).

trigger_runs_on_leader_if_non_member_target(Config) ->
    trigger_runs_on_where(Config, ?FUNCTION_NAME, {member, node()}).

trigger_runs_on_where(Config, Testcase, Where) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 call(Config, Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 call(Config, Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    Path = [foo],
    EventFilter = khepri_evf:tree(Path),
    StoredProcPath = [sproc],

    ct:pal("Put store procedure"),
    ?assertEqual(
       ok,
       call(
         Config, Node1,
         khepri, put,
         [StoreId, StoredProcPath, make_sproc(self(), Testcase)])),

    ct:pal("Register trigger"),
    RegisterNode = Node2,
    ?assertEqual(
       ok,
       call(
         Config, RegisterNode,
         khepri, register_trigger,
         [StoreId, Testcase, EventFilter, StoredProcPath,
          #{where => Where}])),

    ct:pal("Put a value in the store to trigger the stored procedure"),
    ?assertEqual(
       ok,
       call(Config, Node1, khepri, put, [StoreId, Path, value])),

    case Where of
        leader ->
            {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),

            ct:pal("Wait for the trigger message from the leader node"),
            receive
                {sproc, Testcase, LeaderNode, _Props1} ->
                    ok
            after 60000 ->
                      ct:pal(
                        "Messages in inbox:~n~p",
                        [erlang:process_info(self(), messages)]),
                      ct:fail(
                        "Did not receive the expected message from the "
                        "trigger from ~s",
                        [LeaderNode])
            end,
            receive
                {sproc, Testcase, _MemberNode, _Props2} = Message ->
                    ct:fail(
                      "Received an unexpected message from the trigger:~n~p",
                      [Message])
            after 1000 ->
                      ok
            end;
        {member, MemberNode}
          when MemberNode =:= Node1 orelse
               MemberNode =:= Node2 orelse
               MemberNode =:= Node3 ->
            %% We ensure the target member is not the leader.
            {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),
            [FutureLeader | _] = Nodes -- [MemberNode, LeaderNode],
            ct:pal(
              "Transter leadership from ~s to ~s",
              [LeaderNode, FutureLeader]),
            ?assertEqual(
               ok,
               call(
                 Config, Node1, ra, transfer_leadership,
                 [{StoreId, LeaderNode}, {StoreId, FutureLeader}])),

            WantedLeaderId = {StoreId, FutureLeader},
            wait_for_leader_to_be(Config, WantedLeaderId),
            lists:foreach(
              fun(Node) ->
                      ?assertEqual(
                         WantedLeaderId,
                         get_leader_in_store(Config, StoreId, [Node])),
                      ?assertNotEqual(FutureLeader, MemberNode)
              end, Nodes),

            ct:pal("Wait for the trigger message from a follower node"),
            receive
                {sproc, Testcase, MemberNode, _Props1} ->
                    ok
            after 60000 ->
                      ct:pal(
                        "Messages in inbox:~n~p",
                        [erlang:process_info(self(), messages)]),
                      ct:fail(
                        "Did not receive the expected message from the "
                        "trigger from ~s",
                        [MemberNode])
            end,
            receive
                {sproc, Testcase, _MemberNode, _Props2} = Message ->
                    ct:fail(
                      "Received an unexpected message from the trigger:~n~p",
                      [Message])
            after 1000 ->
                      ok
            end;
        {member, _NonMemberNode} ->
            %% The target node is not part of the cluster. In this case, the
            %% message comes from the leader.
            {_, LeaderNode} = get_leader_in_store(Config, StoreId, Nodes),

            ct:pal("Wait for the trigger message from the leader node"),
            receive
                {sproc, Testcase, LeaderNode, _Props1} ->
                    ok
            after 60000 ->
                      ct:pal(
                        "Messages in inbox:~n~p",
                        [erlang:process_info(self(), messages)]),
                      ct:fail(
                        "Did not receive the expected message from the "
                        "trigger from ~s",
                        [LeaderNode])
            end,
            receive
                {sproc, Testcase, _MemberNode, _Props2} = Message ->
                    ct:fail(
                      "Received an unexpected message from the trigger:~n~p",
                      [Message])
            after 1000 ->
                      ok
            end;
        local ->
            ct:pal("Wait for the trigger message from the local node"),
            receive
                {sproc, Testcase, RegisterNode, _Props1} ->
                    ok
            after 60000 ->
                      ct:pal(
                        "Messages in inbox:~n~p",
                        [erlang:process_info(self(), messages)]),
                      ct:fail(
                        "Did not receive the expected message from the "
                        "trigger from ~s",
                        [RegisterNode])
            end,
            receive
                {sproc, Testcase, _MemberNode, _Props2} = Message ->
                    ct:fail(
                      "Received an unexpected message from the trigger:~n~p",
                      [Message])
            after 1000 ->
                      ok
            end;
        all_members ->
            ct:pal("Wait for the trigger message from members"),
            lists:foreach(
              fun(Node) ->
                      receive
                          {sproc, Testcase, Node, _Props1} ->
                              ok
                      after 60000 ->
                                ct:pal(
                                  "Messages in inbox:~n~p",
                                  [erlang:process_info(self(), messages)]),
                                ct:fail(
                                  "Did not receive the expected message "
                                  "from the trigger from ~s",
                                  [Node])
                      end
              end, Nodes),
            receive
                {sproc, Testcase, _MemberNode, _Props2} = Message ->
                    ct:fail(
                      "Received an unexpected message from the trigger:~n~p",
                      [Message])
            after 1000 ->
                      ok
            end
    end,
    ok.

make_sproc(Pid, Testcase) ->
    fun(Props) ->
            Pid ! {sproc, Testcase, node(), Props}
    end.

start_monitored_proc() ->
    spawn(fun() ->
                  receive
                      _ ->
                          ok
                  end
          end).

wait_for_leader_to_be(Config, WantedLeaderId) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = maps:keys(PropsPerNode),
    wait_for_leader_to_be(Config, WantedLeaderId, Nodes, 60000).

wait_for_leader_to_be(
  Config,
  {StoreId, _LeaderNode} = WantedLeaderId,
  [Node | Rest] = Nodes,
  TimeLeft) when TimeLeft >= 0 ->
    LeaderId = call(Config, Node, ra_leaderboard, lookup_leader, [StoreId]),
    case LeaderId of
        WantedLeaderId ->
            wait_for_leader_to_be(Config, WantedLeaderId, Rest, TimeLeft);
        _ ->
            Sleep = 500,
            timer:sleep(Sleep),
            TimeLeft1 = TimeLeft - Sleep,
            wait_for_leader_to_be(Config, WantedLeaderId, Nodes, TimeLeft1)
    end;
wait_for_leader_to_be(_Config, _WantedLeaderId, [], _TimeLeft) ->
    ok;
wait_for_leader_to_be(_Config, WantedLeaderId, [Node | _], TimeLeft)
  when TimeLeft < 0 ->
    ct:fail(
      "Timeout while waiting for leader to become ~0p on node ~p",
      [WantedLeaderId, Node]).

trigger_options_rejected_before_v3(Config) ->
    RaSystem = get_ra_system_name(Config),
    StoreId = RaSystem,

    MacVer = 2,
    meck:new(khepri_machine, [passthrough, no_link]),
    meck:expect(khepri_machine, version, fun() -> MacVer end),

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, StoreId)),

    TriggerId = ?FUNCTION_NAME,
    Path = [foo],
    EventFilter = khepri_evf:tree(Path),
    StoredProcPath = [sproc],

    TriggerOptions = #{where => {member, node()}},
    ?assertError(
       ?khepri_exception(
          unsupported_trigger_options,
          #{trigger_id := TriggerId,
            event_filter := EventFilter,
            options := TriggerOptions}),
       khepri:register_trigger(
         StoreId, TriggerId, EventFilter, StoredProcPath, TriggerOptions)),

    ?assertEqual(
       ok,
       khepri:register_trigger(
         StoreId, TriggerId, EventFilter, StoredProcPath)),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       khepri:stop(StoreId)),

    meck:unload(khepri_machine),
    ok.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

setup_node() ->
    basic_logger_config(),

    %% We use an additional logger handler for messages tagged with a non-OTP
    %% domain because by default, `cth_log_redirect' drops them.
    GL = erlang:group_leader(),
    GLNode = node(GL),
    Ret = logger:add_handler(
            cth_log_redirect_any_domains, cth_log_redirect_any_domains,
            #{config => #{group_leader => GL,
                          group_leader_node => GLNode}}),
    case Ret of
        ok                          -> ok;
        {error, {already_exist, _}} -> ok
    end,
    ok = logger:set_handler_config(
           cth_log_redirect_any_domains, formatter,
           {logger_formatter, ?LOGFMT_CONFIG}),
    ?LOG_INFO(
       "Extended logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),

    ok.

basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),

    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              ok = logger:update_handler_config(
                    HandlerId, config, #{burst_limit_enable => false}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ?LOG_INFO(
       "Basic logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok.

start_n_nodes(NamePrefix, Count) ->
    ct:pal("Start ~b Erlang nodes:", [Count]),
    Nodes = [begin
                 Name = lists:flatten(
                          io_lib:format(
                            "~s-~s-~b", [?MODULE, NamePrefix, I])),
                 ct:pal("- ~s", [Name]),
                 start_erlang_node(Name)
             end || I <- lists:seq(1, Count)],
    ct:pal("Started nodes: ~p", [[Node || {Node, _Peer} <- Nodes]]),

    %% We add all nodes to the test coverage report.
    CoveredNodes = [Node || {Node, _Peer} <- Nodes],
    {ok, _} = cover:start([node() | CoveredNodes]),

    CodePath = code:get_path(),
    lists:foreach(
      fun({_Node, Peer}) ->
              peer:call(Peer, code, add_pathsz, [CodePath], infinity),
              ok = peer:call(Peer, ?MODULE, setup_node, [], infinity)
      end, Nodes),
    Nodes.

start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, Peer, Node} = peer:start(#{name => Name1,
                                    wait_boot => infinity,
                                    connection => standard_io}),
    {Node, Peer}.

stop_erlang_node(_Node, Peer) ->
    ok = peer:stop(Peer).

get_ra_system_name(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node | _] = maps:keys(PropsPerNode),
    #{props := #{ra_system := RaSystem}} = maps:get(Node, PropsPerNode),
    RaSystem.

call(Config, Node, Module, Func, Args) ->
    PropsPerNode = ?config(ra_system_props, Config),
    #{peer := Peer} = maps:get(Node, PropsPerNode),
    peer:call(Peer, Module, Func, Args, infinity).

get_leader_in_store(Config, StoreId, [Node | _] = _RunningNodes) ->
    %% Query members; this is used to make sure there is an elected leader.
    ct:pal("Trying to figure out who the leader is in \"~s\"", [StoreId]),
    {ok, Members} = call(Config, Node, khepri_cluster, members, [StoreId]),
    Pids = [[Member, catch call(Config, N, erlang, whereis, [RegName])]
            || {RegName, N} = Member <- Members],
    LeaderId = call(Config, Node, ra_leaderboard, lookup_leader, [StoreId]),
    ?assertNotEqual(undefined, LeaderId),
    ct:pal(
      "Leader: ~0p~n"
      "Members:~n" ++
      string:join([" - ~0p -> ~0p" || _ <- Pids], "\n"),
      [LeaderId] ++ lists:flatten(Pids)),
    LeaderId.
