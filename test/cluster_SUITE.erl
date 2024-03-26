%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
         can_query_members_with_a_single_node/1,
         fail_to_start_with_bad_ra_server_config/1,
         initial_members_are_ignored/1,
         can_start_a_three_node_cluster/1,
         can_join_several_times_a_three_node_cluster/1,
         can_rejoin_after_a_reset_in_a_three_node_cluster/1,
         can_restart_nodes_in_a_three_node_cluster/1,
         can_reset_a_cluster_member/1,
         can_query_members_with_a_three_node_cluster/1,
         fail_to_join_if_not_started/1,
         fail_to_join_non_existing_node/1,
         fail_to_join_non_existing_store/1,
         can_use_default_store_on_single_node/1,
         can_start_store_in_specified_data_dir_on_single_node/1,
         handle_leader_down_on_three_node_cluster_command/1,
         handle_leader_down_on_three_node_cluster_response/1,
         can_set_snapshot_interval/1,
         projections_are_consistent_on_three_node_cluster/1,
         async_command_leader_change_in_three_node_cluster/1]).

all() ->
    [can_start_a_single_node,
     can_restart_a_single_node_with_ra_server_config,
     can_query_members_with_a_single_node,
     fail_to_start_with_bad_ra_server_config,
     initial_members_are_ignored,
     can_start_a_three_node_cluster,
     can_join_several_times_a_three_node_cluster,
     can_rejoin_after_a_reset_in_a_three_node_cluster,
     can_restart_nodes_in_a_three_node_cluster,
     can_reset_a_cluster_member,
     can_query_members_with_a_three_node_cluster,
     fail_to_join_if_not_started,
     fail_to_join_non_existing_node,
     fail_to_join_non_existing_store,
     can_use_default_store_on_single_node,
     can_start_store_in_specified_data_dir_on_single_node,
     handle_leader_down_on_three_node_cluster_command,
     handle_leader_down_on_three_node_cluster_response,
     can_set_snapshot_interval,
     projections_are_consistent_on_three_node_cluster,
     async_command_leader_change_in_three_node_cluster].

groups() ->
    [].

init_per_suite(Config) ->
    basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_single_node orelse
       Testcase =:= can_restart_a_single_node_with_ra_server_config orelse
       Testcase =:= can_query_members_with_a_single_node orelse
       Testcase =:= fail_to_start_with_bad_ra_server_config orelse
       Testcase =:= initial_members_are_ignored orelse
       Testcase =:= fail_to_join_non_existing_node orelse
       Testcase =:= can_set_snapshot_interval ->
    setup_node(),
    {ok, _} = application:ensure_all_started(khepri),
    Props = helpers:start_ra_system(Testcase),
    [{ra_system_props, #{node() => Props}} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_three_node_cluster orelse
       Testcase =:= can_join_several_times_a_three_node_cluster orelse
       Testcase =:= can_rejoin_after_a_reset_in_a_three_node_cluster orelse
       Testcase =:= can_restart_nodes_in_a_three_node_cluster orelse
       Testcase =:= can_reset_a_cluster_member orelse
       Testcase =:= can_query_members_with_a_three_node_cluster orelse
       Testcase =:= fail_to_join_if_not_started orelse
       Testcase =:= fail_to_join_non_existing_store orelse
       Testcase =:= handle_leader_down_on_three_node_cluster_command orelse
       Testcase =:= handle_leader_down_on_three_node_cluster_response orelse
       Testcase =:= projections_are_consistent_on_three_node_cluster orelse
       Testcase =:= async_command_leader_change_in_three_node_cluster ->
    Nodes = start_n_nodes(Testcase, 3),
    PropsPerNode0 = [begin
                         {ok, _} = rpc:call(
                                     Node, application, ensure_all_started,
                                     [khepri]),
                         Props = rpc:call(
                                   Node, helpers, start_ra_system,
                                   [Testcase]),
                         {Node, Props}
                     end || {Node, _Peer} <- Nodes],
    PropsPerNode = maps:from_list(PropsPerNode0),
    [{ra_system_props, PropsPerNode}, {peer_nodes, Nodes} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= can_use_default_store_on_single_node orelse
       Testcase =:= can_start_store_in_specified_data_dir_on_single_node ->
    setup_node(),
    Config.

end_per_testcase(Testcase, _Config)
  when Testcase =:= can_use_default_store_on_single_node orelse
       Testcase =:= can_start_store_in_specified_data_dir_on_single_node ->
    ok;
end_per_testcase(_Testcase, Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    maps:fold(
      fun(Node, Props, Acc) ->
              _ = rpc:call(Node, helpers, stop_ra_system, [Props]),
              Acc
      end, ok, PropsPerNode),
    ok.

can_start_a_single_node(Config) ->
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
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
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
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

can_query_members_with_a_single_node(Config) ->
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
    StoreId = RaSystem,

    ct:pal("Query members before starting database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:members(StoreId, 10000)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_members(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_members(StoreId, 10000)),

    ct:pal("Query nodes before starting database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, 10000)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_nodes(StoreId, 10000)),

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
       khepri_cluster:members(StoreId, 10000)),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:locally_known_members(StoreId)),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:locally_known_members(StoreId, 10000)),

    ct:pal("Query nodes after starting database"),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:nodes(StoreId, 10000)),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:locally_known_nodes(StoreId)),
    ?assertEqual(
       {ok, [Node]},
       khepri_cluster:locally_known_nodes(StoreId, 10000)),

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
       khepri_cluster:members(StoreId, 10000)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_members(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_members(StoreId, 10000)),

    ct:pal("Query nodes after stopping database"),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:nodes(StoreId, 10000)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_nodes(StoreId)),
    ?assertEqual(
       {error, noproc},
       khepri_cluster:locally_known_nodes(StoreId, 10000)),

    ok.

fail_to_start_with_bad_ra_server_config(Config) ->
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertExit(
       {{{bad_action_from_state_function,
          {{timeout, tick}, not_a_timeout, tick_timeout}},
         _},
        _},
       khepri:start(RaSystem, #{cluster_name => StoreId,
                                tick_timeout => not_a_timeout})),

    ThisMember = khepri_cluster:this_member(StoreId),
    ok = khepri_cluster:wait_for_ra_server_exit(ThisMember),

    %% The process is restarted by its supervisor. Depending on the timing, we
    %% may get a `noproc' or an exception.
    ct:pal("Database unusable after failing to start it"),
    Ret = (catch khepri:get(StoreId, [foo])),
    ct:pal("Return value of khepri:get/2: ~p", [Ret]),
    ?assert(
       case Ret of
           {'EXIT',
            {{{bad_action_from_state_function,
               {{timeout, tick}, not_a_timeout, tick_timeout}},
              _},
             _}} ->
               true;
           {error, noproc} ->
               true;
           _ ->
               false
       end),

    ok.

initial_members_are_ignored(Config) ->
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
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
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Use database before starting it"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:put() from node ~s", [Node]),
              ?assertEqual(
                 {error, noproc},
                 rpc:call(Node, khepri, put, [StoreId, [foo], value1])),
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {error, noproc},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:members() from node ~s", [Node]),
              ExpectedMembers = lists:sort([{StoreId, N} || N <- Nodes]),
              ?assertEqual(
                 ExpectedMembers,
                 begin
                     {ok, M} = rpc:call(
                                 Node,
                                 khepri_cluster, members, [StoreId]),
                     lists:sort(M)
                 end),
              ?assertEqual(
                 ExpectedMembers,
                 begin
                     {ok, LKM} = rpc:call(
                                   Node,
                                   khepri_cluster, locally_known_members,
                                   [StoreId]),
                     lists:sort(LKM)
                 end),

              ExpectedNodes = lists:sort(Nodes),
              ?assertEqual(
                 ExpectedNodes,
                 begin
                     {ok, N} = rpc:call(
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end),
              ?assertEqual(
                 ExpectedNodes,
                 begin
                     {ok, LKN} = rpc:call(
                                   Node,
                                   khepri_cluster, locally_known_nodes,
                                   [StoreId]),
                     lists:sort(LKN)
                 end)
      end, Nodes),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    LeaderId1 = get_leader_in_store(StoreId, Nodes),
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal("Stop database on leader node (quorum is maintained)"),
    ?assertEqual(
       ok,
       rpc:call(StoppedLeaderNode1, khepri, stop, [StoreId])),

    ct:pal("Use database having it running on 2 out of 3 nodes"),
    %% We try a put from the stopped leader and it should fail because the
    %% leaderboard on that node is stale.
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode1, khepri, put, [StoreId, [foo], value3])),
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode1, khepri, get, [StoreId, [foo]])),

    %% Querying running nodes should be fine however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       ok,
       rpc:call(hd(RunningNodes1), khepri, put, [StoreId, [foo], value4])),

    %% The stopped leader should still fail to respond because it is stopped
    %% and again, the leaderboard is stale on this node.
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode1, khepri, get, [StoreId, [foo]])),

    %% Running nodes should see the updated value however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value4},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),

    LeaderId2 = get_leader_in_store(StoreId, RunningNodes1),
    {StoreId, StoppedLeaderNode2} = LeaderId2,
    RunningNodes2 = RunningNodes1 -- [StoppedLeaderNode2],

    ct:pal("Stop database on the new leader node (quorum is lost)"),
    ?assertEqual(
       ok,
       rpc:call(StoppedLeaderNode2, khepri, stop, [StoreId])),

    ct:pal("Use database having it running on 1 out of 3 nodes"),
    %% We try a put from the second old leader and it should fail.
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode2, khepri, put, [StoreId, [foo], value5])),

    %% The last running node should fail to respond as well because the quorum
    %% is lost.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              Member = khepri_cluster:node_to_member(StoreId, Node),
              ?assertEqual(
                 {error, {timeout, Member}},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes2),

    ok.

can_join_several_times_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    LeaderId1 = get_leader_in_store(StoreId, Nodes),
    {StoreId, LeaderNode1} = LeaderId1,
    OtherNodes1 = Nodes -- [LeaderNode1],

    ct:pal("Make leader node join the cluster again"),
    ?assertEqual(
       ok,
       rpc:call(
         LeaderNode1, khepri_cluster, join, [StoreId, hd(OtherNodes1)])),

    ct:pal("Use database after recreating the cluster"),
    ?assertEqual(
       ok,
       rpc:call(LeaderNode1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    ok.

can_rejoin_after_a_reset_in_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    LeaderId1 = get_leader_in_store(StoreId, Nodes),
    {StoreId, LeaderNode1} = LeaderId1,
    OtherNodes1 = Nodes -- [LeaderNode1],

    ct:pal("Stop and reset leader node"),
    ?assertEqual(
       ok,
       rpc:call(
         LeaderNode1, khepri, stop, [StoreId])),
    Props = maps:get(LeaderNode1, PropsPerNode),
    ?assertMatch(
       ok,
       rpc:call(LeaderNode1, helpers, stop_ra_system, [Props])),

    %% Wait for a new leader to be elected among the remaining members.
    NewLeader = get_leader_in_store(StoreId, OtherNodes1),
    ?assertNotEqual(LeaderId1, NewLeader),

    %% The following call removes the existing data directory.
    ?assertMatch(
       #{},
       rpc:call(LeaderNode1, helpers, start_ra_system, [RaSystem])),
    ?assertEqual(
       {ok, StoreId},
       rpc:call(LeaderNode1, khepri, start, [RaSystem, StoreId])),

    ct:pal("Make leader node join the cluster again"),
    ?assertEqual(
       ok,
       rpc:call(
         LeaderNode1, khepri_cluster, join, [StoreId, hd(OtherNodes1)])),

    ct:pal("Use database after recreating the cluster"),
    ?assertEqual(
       ok,
       rpc:call(LeaderNode1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value2},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    ok.

can_restart_nodes_in_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
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
                 rpc:call(Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    %% Stop the current leader.
    LeaderId1 = get_leader_in_store(StoreId, Nodes),
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal(
      "Stop database on leader node ~s (quorum is maintained)",
      [StoppedLeaderNode1]),
    ?assertEqual(
       ok,
       rpc:call(StoppedLeaderNode1, khepri, stop, [StoreId])),

    %% Stop the next elected leader.
    LeaderId2 = get_leader_in_store(StoreId, RunningNodes1),
    ?assertNotEqual(LeaderId1, LeaderId2),
    {StoreId, StoppedLeaderNode2} = LeaderId2,
    RunningNodes2 = RunningNodes1 -- [StoppedLeaderNode2],

    ct:pal(
      "Stop database on the new leader node ~s (quorum is lost)",
      [StoppedLeaderNode2]),
    ?assertEqual(
       ok,
       rpc:call(StoppedLeaderNode2, khepri, stop, [StoreId])),

    ct:pal(
      "Restart database on node ~s (quorum is restored)",
      [StoppedLeaderNode1]),
    ?assertEqual(
       {ok, StoreId},
       rpc:call(
         StoppedLeaderNode1, khepri, start, [RaSystem, RaServerConfig])),
    RunningNodes3 = RunningNodes2 ++ [StoppedLeaderNode1],

    ct:pal("Use database after having it running on 2 out of 3 nodes"),
    %% We try a put from the stopped leader and it should fail because the
    %% leaderboard on that node is stale.
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode2, khepri, put, [StoreId, [foo], value2])),
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode2, khepri, get, [StoreId, [foo]])),

    %% Querying running nodes should be fine however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       ok,
       rpc:call(hd(RunningNodes3), khepri, put, [StoreId, [foo], value3])),

    %% The stopped leader should still fail to respond because it is stopped
    %% and again, the leaderboard is stale on this node.
    ?assertEqual(
       {error, noproc},
       rpc:call(StoppedLeaderNode2, khepri, get, [StoreId, [foo]])),

    %% Running nodes should see the updated value however.
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value3},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

    ok.

handle_leader_down_on_three_node_cluster_command(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),
    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
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
                 rpc:call(Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    %% Stop the current leader.
    LeaderId1 = get_leader_in_store(StoreId, Nodes),
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
                 rpc:call(Node, khepri, put, [StoreId, [foo], value1]))
      end, RunningNodes1),
    ok.

handle_leader_down_on_three_node_cluster_response(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    PeerPerNode = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),
    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    RaServerConfig = #{cluster_name => StoreId},

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
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
                 rpc:call(Node, khepri, stop, [StoreId])),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, RaServerConfig]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, value1},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, Nodes),

    %% Stop the current leader.
    LeaderId1 = get_leader_in_store(StoreId, Nodes),
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
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),
    ok.

can_reset_a_cluster_member(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Check membership on all nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:nodes() from node ~s", [Node]),
              ?assertEqual(
                 lists:sort(Nodes),
                 begin
                     {ok, N} = rpc:call(
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end)
      end, Nodes),

    %% Reset the current leader.
    LeaderId1 = get_leader_in_store(StoreId, Nodes),
    {StoreId, StoppedLeaderNode1} = LeaderId1,
    RunningNodes1 = Nodes -- [StoppedLeaderNode1],

    ct:pal(
      "Reset database on leader node ~s",
      [StoppedLeaderNode1]),
    ?assertEqual(
       ok,
       rpc:call(StoppedLeaderNode1, khepri_cluster, reset, [StoreId])),

    ct:pal("Check membership on remaining nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:nodes() from node ~s", [Node]),
              ?assertEqual(
                 lists:sort(RunningNodes1),
                 begin
                     {ok, N} = rpc:call(
                                 Node,
                                 khepri_cluster, nodes, [StoreId]),
                     lists:sort(N)
                 end)
      end, RunningNodes1),

    ok.

can_query_members_with_a_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Query members before starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, members, [StoreId, 10000])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId, 10000]))
      end, Nodes),

    ct:pal("Query nodes before starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId, 10000])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId, 10000]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Query members after starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 erpc:call(Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 erpc:call(Node, khepri_cluster, members, [StoreId, 10000])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId])),
              ?assertEqual(
                 {ok, [{StoreId, N} || N <- Nodes]},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId, 10000]))
      end, Nodes),

    ct:pal("Query nodes after starting database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, Nodes},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {ok, Nodes},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId, 10000])),
              ?assertEqual(
                 {ok, Nodes},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId])),
              ?assertEqual(
                 {ok, Nodes},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId, 10000]))
      end, Nodes),

    ct:pal("Stop database"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:stop() from node ~s", [Node]),
              ?assertEqual(ok, rpc:call(Node, khepri, stop, [StoreId]))
      end, Nodes),

    ct:pal("Query members after stopping database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, members, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, members, [StoreId, 10000])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_members, [StoreId, 10000]))
      end, Nodes),

    ct:pal("Query nodes after stopping database"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(Node, khepri_cluster, nodes, [StoreId, 10000])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId])),
              ?assertEqual(
                 {error, noproc},
                 erpc:call(
                   Node,
                   khepri_cluster, locally_known_nodes, [StoreId, 10000]))
      end, Nodes),

    ok.

fail_to_join_if_not_started(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, _Node3] = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Cluster node"),
    ?assertEqual(
       {error, ?khepri_error(
                  not_a_khepri_store,
                  #{store_id => StoreId})},
       rpc:call(
         Node1, khepri_cluster, join, [StoreId, Node2])),

    ok.

fail_to_join_non_existing_node(Config) ->
    Node = node(),
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
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
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Start database"),
    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    ct:pal("Cluster node"),
    ?assertEqual(
       {error, noproc},
       rpc:call(
         Node1, khepri_cluster, join, [StoreId, Node2])),

    ?assertEqual(
       {ok, [khepri_cluster:node_to_member(StoreId, Node1)]},
       rpc:call(
         Node1, khepri_cluster, members, [StoreId])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       rpc:call(Node1, khepri, stop, [StoreId])),

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
    ?assertEqual({error, noproc}, khepri_cluster:locally_known_members()),
    ?assertEqual({error, noproc}, khepri_cluster:nodes()),
    ?assertEqual({error, noproc}, khepri_cluster:locally_known_nodes()),

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
       {ok, #{data => value4,
              payload_version => 5}},
       khepri_adv:put([foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 5}}},
       khepri_adv:put_many([foo], value2)),
    ?assertEqual(
       {ok, #{data => value2,
              payload_version => 6}},
       khepri_adv:update([foo], value3)),
    ?assertEqual(
       {ok, #{data => value3,
              payload_version => 7}},
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
       {ok, #{data => value4,
              payload_version => 7}},
       khepri_adv:get([foo])),
    ?assertEqual(
       {ok, #{data => value4,
              payload_version => 7}},
       khepri_adv:get([foo], #{})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get_many([foo])),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 7}}},
       khepri_adv:get_many([foo], #{})),

    ?assertEqual({ok, [{StoreId, Node}]}, khepri_cluster:members()),
    ?assertEqual(
       {ok, [{StoreId, Node}]},
       khepri_cluster:locally_known_members()),
    ?assertEqual({ok, [Node]}, khepri_cluster:nodes()),
    ?assertEqual({ok, [Node]}, khepri_cluster:locally_known_nodes()),

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
    Projection1 = khepri_projection:new(ProjectionName1, copy),
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
                    ProjectionName2, copy,
                    #{read_concurrency => true, keypos => 2}),
    ?assertEqual(ok, khepri:register_projection("/**", Projection2, #{})),
    ?assertEqual(true, khepri:has_projection(ProjectionName2)),

    ?assertEqual(ok, khepri:unregister_projection(ProjectionName1)),
    ?assertEqual(
       {error, ?khepri_error(
                 projection_not_found,
                 #{name => ProjectionName1})},
       khepri:unregister_projection(ProjectionName1)),
    ?assertEqual(ok, khepri:unregister_projection(ProjectionName2, #{})),

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
       {ok, #{data => value1,
              payload_version => 2}},
       khepri_adv:clear_payload([bar])),
    ?assertEqual(
       {ok, #{[bar] => #{payload_version => 2}}},
       khepri_adv:clear_many_payloads([bar])),
    ?assertEqual(
       {ok, #{payload_version => 2}},
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
    #{Node := #{ra_system := RaSystem}} = ?config(ra_system_props, Config),
    StoreId = RaSystem,

    ct:pal("Start database"),
    RaServerConfig = #{cluster_name => StoreId,
                       machine_config => #{snapshot_interval => 3}},
    ?assertEqual(
       {ok, StoreId},
       khepri:start(RaSystem, RaServerConfig)),

    ct:pal("Verify applied command count is 0"),
    ?assertEqual(
       #{},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

    ct:pal("Verify applied command count is 1"),
    ?assertEqual(
       #{applied_command_count => 1},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

    ct:pal("Verify applied command count is 2"),
    ?assertEqual(
       #{applied_command_count => 2},
       khepri_machine:process_query(
         StoreId,
         fun khepri_machine:get_metrics/1,
         #{})),

    ct:pal("Use database after starting it"),
    ?assertEqual(ok, khepri:put(StoreId, [foo], value1)),

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

projections_are_consistent_on_three_node_cluster(Config) ->
    ProjectionName = ?MODULE,

    %% We call `khepri_projection:new/2 on the local node and thus need
    %% Khepri.
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),

    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Projection table does not exist before registering"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- ets:info/1 from node ~s", [Node]),
              ?assertEqual(
                 undefined,
                 rpc:call(Node, ets, info, [ProjectionName]))
      end, Nodes),

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    ct:pal("Register projection on node ~s", [Node1]),
    Projection = khepri_projection:new(
                   ProjectionName, fun(Path, Payload) -> {Path, Payload} end),
    rpc:call(Node1,
      khepri, register_projection,
      [StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Projection]),

    ct:pal("The projection table exists on node ~s", [Node1]),
    ?assertNotEqual(undefined, rpc:call(Node1, ets, info, [ProjectionName])),

    ct:pal("Wait for the projection table to exist on all nodes"),
    %% `khepri:register_projection/4' uses a `reply_mode' of `local' which
    %% blocks until the projection is registered by the cluster member on
    %% `Node1'. The projection is also guaranteed to exist on the leader
    %% member but the remaining cluster members are eventually consistent.
    %% This function polls for the remaining cluster members.
    ok = wait_for_projection_on_nodes([Node2, Node3], ProjectionName),

    ct:pal("An update by a member looks consistent to that member "
           "when using the `local' `reply_mode'"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:put/4 from node ~s", [Node]),
              %% `#{reply_from => local}' makes the `put' immediately
              %% consistent on the caller node.
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri, put, [StoreId, [foo], value1,
                                              #{reply_from => local}])),

              ct:pal("- ets:lookup() from node ~s", [Node]),
              ?assertEqual(
                 [{[foo], value1}],
                 rpc:call(Node, ets, lookup, [ProjectionName, [foo]]))
      end, Nodes),

    LeaderId = get_leader_in_store(StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId,
    [FollowerNode | _] = Nodes -- [LeaderNode],

    ct:pal("An update by a follower (~s) is immediately consistent on the "
           "leader (~s) with a `local' `reply_mode'",
           [FollowerNode, LeaderNode]),
    ?assertEqual(
       ok,
       rpc:call(FollowerNode, khepri, put, [StoreId, [foo], value2,
                                            #{reply_from => local}])),
    ?assertEqual(
       [{[foo], value2}],
       rpc:call(LeaderNode, ets, lookup, [ProjectionName, [foo]])),

    ok.

wait_for_projection_on_nodes([], _ProjectionName) ->
   ok;
wait_for_projection_on_nodes([Node | Rest] = Nodes, ProjectionName) ->
   case rpc:call(Node, ets, info, [ProjectionName]) of
      undefined ->
         timer:sleep(10),
         wait_for_projection_on_nodes(Nodes, ProjectionName);
      _Info ->
         ct:pal("- projection ~s exists on node ~s", [ProjectionName, Node]),
         wait_for_projection_on_nodes(Rest, ProjectionName)
   end.

async_command_leader_change_in_three_node_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3] = Nodes = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Start database + cluster nodes"),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:start() from node ~s", [Node]),
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri_cluster:join() from node ~s", [Node]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node3]))
      end, [Node1, Node2]),

    LeaderId = get_leader_in_store(StoreId, Nodes),
    {StoreId, LeaderNode} = LeaderId,

    ct:pal("Send an async command from the leader node ~s", [LeaderNode]),
    ok = erpc:call(
           LeaderNode,
           fun() ->
                 %% This member hasn't sent any commands so the leader isn't
                 %% cached yet. The async call will succeed though because this
                 %% member is the leader.
                 ?assertEqual(
                    undefined,
                    khepri_cluster:get_cached_leader(StoreId)),
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
                 %% This member hasn't sent any commands so the leader isn't
                 %% cached yet. This member is not the leader so the async
                 %% command will fail.
                 ?assertEqual(
                    undefined,
                    khepri_cluster:get_cached_leader(StoreId)),
                 CorrelationId1 = 1,
                 Extra1 = #{async => CorrelationId1},
                 ok = khepri:put(StoreId, [foo], ?NO_PAYLOAD, Extra1),
                 RaEvent1 = receive
                               {ra_event, _, _} = Event1 ->
                                  Event1
                            after
                               1_000 ->
                                  throw(timeout)
                            end,
                 ?assertEqual(
                   [{CorrelationId1, {error, not_leader}}],
                   khepri:handle_async_ret(StoreId, RaEvent1)),

                 %% `khepri:handle_async_ret/2' updated the cached leader so
                 %% the async call will now send the command to the leader.
                 ?assertNotEqual(
                    undefined,
                    khepri_cluster:get_cached_leader(StoreId)),
                 CorrelationId2 = 2,
                 Extra2 = #{async => CorrelationId2},
                 ok = khepri:put(StoreId, [foo], ?NO_PAYLOAD, Extra2),
                 RaEvent2 = receive
                               {ra_event, _, _} = Event2 ->
                                  Event2
                            after
                               1_000 ->
                                  throw(timeout)
                            end,
                 ?assertMatch(
                   [{CorrelationId2, {ok, _}}],
                   khepri:handle_async_ret(StoreId, RaEvent2))
           end),
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
      fun({Node, _Peer}) ->
              rpc:call(Node, code, add_pathsz, [CodePath]),
              ok = rpc:call(Node, ?MODULE, setup_node, [])
      end, Nodes),
    Nodes.

-if(?OTP_RELEASE >= 25).
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, Peer, Node} = peer:start(#{name => Name1,
                                    wait_boot => infinity}),
    {Node, Peer}.
stop_erlang_node(_Node, Peer) ->
    ok = peer:stop(Peer).
-else.
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    Options = [{monitor_master, true}],
    {ok, Node} = ct_slave:start(Name1, Options),
    {Node, Node}.
stop_erlang_node(_Node, Node) ->
    {ok, _} = ct_slave:stop(Node),
    ok.
-endif.

get_leader_in_store(StoreId, [Node | _] = _RunningNodes) ->
    %% Query members; this is used to make sure there is an elected leader.
    ct:pal("Trying to figure who the leader is in \"~s\"", [StoreId]),
    {ok, Members} = rpc:call(Node, khepri_cluster, members, [StoreId]),
    Pids = [[Member, rpc:call(N, erlang, whereis, [RegName])]
            || {RegName, N} = Member <- Members],
    LeaderId = rpc:call(Node, ra_leaderboard, lookup_leader, [StoreId]),
    ?assertNotEqual(undefined, LeaderId),
    ct:pal(
      "Leader: ~0p~n"
      "Members:~n" ++
      string:join([" - ~0p -> ~0p" || _ <- Pids], "\n"),
      [LeaderId] ++ lists:flatten(Pids)),
    LeaderId.
