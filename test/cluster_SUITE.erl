%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(cluster_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("include/khepri.hrl").

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
         fail_to_start_with_bad_ra_server_config/1,
         initial_members_are_ignored/1,
         can_start_a_three_node_cluster/1,
         can_restart_nodes_in_a_three_node_cluster/1,
         can_reset_a_cluster_member/1,
         fail_to_join_if_not_started/1,
         fail_to_join_non_existing_node/1,
         fail_to_join_non_existing_store/1,
         can_use_default_store_on_single_node/1,
         can_start_store_in_specified_data_dir_on_single_node/1]).

all() ->
    [can_start_a_single_node,
     fail_to_start_with_bad_ra_server_config,
     initial_members_are_ignored,
     can_start_a_three_node_cluster,
     can_restart_nodes_in_a_three_node_cluster,
     can_reset_a_cluster_member,
     fail_to_join_if_not_started,
     fail_to_join_non_existing_node,
     fail_to_join_non_existing_store,
     can_use_default_store_on_single_node,
     can_start_store_in_specified_data_dir_on_single_node].

groups() ->
    [].

init_per_suite(Config) ->
    setup_node(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_single_node orelse
       Testcase =:= fail_to_start_with_bad_ra_server_config orelse
       Testcase =:= initial_members_are_ignored orelse
       Testcase =:= fail_to_join_non_existing_node ->
    {ok, _} = application:ensure_all_started(khepri),
    Props = helpers:start_ra_system(Testcase),
    [{ra_system_props, #{node() => Props}} | Config];
init_per_testcase(Testcase, Config)
  when Testcase =:= can_start_a_three_node_cluster orelse
       Testcase =:= can_restart_nodes_in_a_three_node_cluster orelse
       Testcase =:= can_reset_a_cluster_member orelse
       Testcase =:= fail_to_join_if_not_started orelse
       Testcase =:= fail_to_join_non_existing_store ->
    Nodes = start_n_nodes(Testcase, 3),
    PropsPerNode0 = [begin
                         {ok, _} = rpc:call(
                                     Node, application, ensure_all_started,
                                     [khepri]),
                         Props = rpc:call(
                                   Node, helpers, start_ra_system,
                                   [Testcase]),
                         {Node, Props}
                     end || Node <- Nodes],
    PropsPerNode = maps:from_list(PropsPerNode0),
    [{ra_system_props, PropsPerNode} | Config];
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
    maps:fold(
      fun(Node, Props, Acc) ->
              ok = rpc:call(Node, helpers, stop_ra_system, [Props]),
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
    ?assertEqual(
       {ok, #{[foo] => #{}}},
       khepri:put(StoreId, [foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:get(StoreId, [foo])),

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
       [ThisMember],
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
                 lists:sort(
                   rpc:call(Node, khepri_cluster, members, [StoreId]))),
              ?assertEqual(
                 ExpectedMembers,
                 lists:sort(
                   rpc:call(
                     Node, khepri_cluster, locally_known_members,
                     [StoreId]))),

              ExpectedNodes = lists:sort(Nodes),
              ?assertEqual(
                 ExpectedNodes,
                 lists:sort(
                   rpc:call(Node, khepri_cluster, nodes, [StoreId]))),
              ?assertEqual(
                 ExpectedNodes,
                 lists:sort(
                   rpc:call(
                     Node, khepri_cluster, locally_known_nodes,
                     [StoreId])))
      end, Nodes),

    ct:pal("Use database after starting it"),
    ?assertEqual(
       {ok, #{[foo] => #{}}},
       rpc:call(Node1, khepri, put, [StoreId, [foo], value2])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, #{[foo] => #{data => value2,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
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
                 {ok, #{[foo] => #{data => value2,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes1),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
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
                 {ok, #{[foo] => #{data => value4,
                                   payload_version => 2,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
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

can_restart_nodes_in_a_three_node_cluster(Config) ->
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
    ?assertEqual(
       {ok, #{[foo] => #{}}},
       rpc:call(Node1, khepri, put, [StoreId, [foo], value1])),
    lists:foreach(
      fun(Node) ->
              ct:pal("- khepri:get() from node ~s", [Node]),
              ?assertEqual(
                 {ok, #{[foo] => #{data => value1,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
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
       rpc:call(StoppedLeaderNode1, khepri, start, [RaSystem, StoreId])),
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
                 {ok, #{[foo] => #{data => value1,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

    %% Likewise, a put from a running node should succeed.
    ?assertEqual(
       {ok, #{[foo] => #{data => value1,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
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
                 {ok, #{[foo] => #{data => value3,
                                   payload_version => 2,
                                   child_list_version => 1,
                                   child_list_length => 0}}},
                 rpc:call(Node, khepri, get, [StoreId, [foo]]))
      end, RunningNodes3),

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
                 lists:sort(rpc:call(
                              Node, khepri_cluster, nodes, [StoreId])))
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
                 lists:sort(rpc:call(
                              Node, khepri_cluster, nodes, [StoreId])))
      end, RunningNodes1),

    ok.

fail_to_join_if_not_started(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, _Node3] = maps:keys(PropsPerNode),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ct:pal("Cluster node"),
    ?assertEqual(
       {error, {not_a_khepri_store, StoreId}},
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
       {error, {nodedown, RemoteNode}},
       khepri_cluster:join(StoreId, RemoteNode)),

    ThisMember = khepri_cluster:this_member(StoreId),
    ?assertEqual(
       [ThisMember],
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
       [khepri_cluster:node_to_member(StoreId, Node1)],
       rpc:call(
         Node1, khepri_cluster, members, [StoreId])),

    ct:pal("Stop database"),
    ?assertEqual(
       ok,
       rpc:call(Node1, khepri, stop, [StoreId])),

    ok.

can_use_default_store_on_single_node(_Config) ->
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),
    DataDir = khepri_cluster:get_default_ra_system_or_data_dir(),
    ?assertNot(filelib:is_dir(DataDir)),

    ?assertEqual({error, noproc}, khepri:get([foo])),

    {ok, StoreId} = khepri:start(),
    ?assert(filelib:is_dir(DataDir)),

    ?assertEqual(
       {ok, #{[foo] => #{}}},
       khepri:create([foo], value1)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value1,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:put([foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 2,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:update([foo], value3)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value3,
                         payload_version => 3,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:compare_and_swap([foo], value3, value4)),

    ?assertEqual(true, khepri:exists([foo])),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 4,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:get([foo])),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 4,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:get([foo])),
    ?assertEqual(
       #{data => value4,
         payload_version => 4,
         child_list_version => 1,
         child_list_length => 0},
       khepri:get_node_props([foo])),
    ?assertEqual(true, khepri:has_data([foo])),
    ?assertEqual(value4, khepri:get_data([foo])),
    ?assertEqual(value4, khepri:get_data_or([foo], no_data)),
    ?assertEqual(false, khepri:has_sproc([foo])),
    ?assertThrow(
       {invalid_sproc_fun,
        {no_sproc, [foo], #{data := value4,
                            payload_version := 4,
                            child_list_version := 1,
                            child_list_length := 0}}},
       khepri:run_sproc([foo], [])),
    ?assertEqual({ok, 1}, khepri:count("**")),
    ?assertEqual({ok, #{}}, khepri:list([bar])),
    ?assertEqual({ok, #{}}, khepri:find([bar], ?STAR)),

    ?assertEqual(
       {ok, #{[bar] => #{}}},
       khepri:create([bar], value1)),
    ?assertEqual(
       {ok, #{[bar] => #{data => value1,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:clear_payload([bar])),
    ?assertEqual(
       {ok, #{[bar] => #{payload_version => 2,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:delete([bar])),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertEqual(ok, khepri:reset()),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start()),
    ?assertEqual({ok, #{}}, khepri:get([foo])),

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

    ?assertEqual(
       {ok, #{[foo] => #{}}},
       khepri:create(StoreId, [foo], value1, #{})),
    ?assertMatch(
       {error, {mismatching_node, _}},
       khepri:create(StoreId, [foo], value1, #{keep_while => #{}})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value1,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:put(StoreId, [foo], value2)),
    ?assertEqual(
       {ok, #{[foo] => #{data => value2,
                         payload_version => 2,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:update(StoreId, [foo], value3, #{})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value3,
                         payload_version => 3,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:update(StoreId, [foo], value3, #{keep_while => #{}})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value3,
                         payload_version => 3,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:compare_and_swap([foo], value3, value4)),

    ?assertEqual(true, khepri:exists([foo], #{})),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 4,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:get([foo], #{})),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start(list_to_binary(DataDir))),
    ?assertEqual(
       {ok, #{[foo] => #{data => value4,
                         payload_version => 4,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:get([foo])),
    ?assertEqual(
       #{data => value4,
         payload_version => 4,
         child_list_version => 1,
         child_list_length => 0},
       khepri:get_node_props([foo], #{})),
    ?assertEqual(true, khepri:has_data([foo], #{})),
    ?assertEqual(value4, khepri:get_data([foo], #{})),
    ?assertEqual(value4, khepri:get_data_or([foo], no_data, #{})),
    ?assertEqual(false, khepri:has_sproc([foo], #{})),
    ?assertThrow(
       {invalid_sproc_fun,
        {no_sproc, [foo], #{data := value4,
                            payload_version := 4,
                            child_list_version := 1,
                            child_list_length := 0}}},
       khepri:run_sproc([foo], [], #{})),
    ?assertEqual({ok, 1}, khepri:count("**", #{})),
    ?assertEqual({ok, #{}}, khepri:list([bar], #{})),
    ?assertEqual({ok, #{}}, khepri:find([bar], ?STAR, #{})),

    ?assertEqual(
       {ok, #{[bar] => #{}}},
       khepri:create([bar], value1)),
    ?assertEqual(
       {ok, #{[bar] => #{data => value1,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:clear_payload([bar])),
    ?assertEqual(
       {ok, #{[bar] => #{payload_version => 2,
                         child_list_version => 1,
                         child_list_length => 0}}},
       khepri:delete([bar], #{})),

    ?assertEqual({ok, StoreId}, khepri:start(DataDir)),
    ?assertEqual(ok, khepri:reset(10000)),
    ?assertEqual({error, noproc}, khepri:get([foo])),

    ?assertEqual({ok, StoreId}, khepri:start(DataDir)),
    ?assertEqual({ok, #{}}, khepri:get([foo])),

    ?assertEqual(ok, khepri:stop()),
    ?assertEqual(ok, application:stop(khepri)),
    ?assertEqual(ok, application:stop(ra)),

    helpers:remove_store_dir(DataDir),
    ?assertNot(filelib:is_dir(DataDir)).

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

setup_node() ->
    _ = logger:set_primary_config(level, debug),

    %% We use an additional logger handler for messages tagged with a non-OTP
    %% domain because by default, `cth_log_redirect' drops them.
    ok = logger:add_handler(
           cth_log_redirect_any_domains, cth_log_redirect_any_domains,
           #{}),

    HandlerIds = [default, cth_log_redirect, cth_log_redirect_any_domains],
    lists:foreach(
      fun(HandlerId) ->
              _ = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, #{single_line => true}}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop})
      end, HandlerIds),
    ct:pal("Logger configuration: ~p", [logger:get_config()]),

    ok = application:set_env(
           khepri, default_timeout, 2000, [{persistent, true}]),

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
    ct:pal("Started nodes: ~p", [Nodes]),
    CodePath = code:get_path(),
    lists:foreach(
      fun(Node) ->
              rpc:call(Node, code, add_pathsz, [CodePath]),
              ok = rpc:call(Node, ?MODULE, setup_node, [])
      end, Nodes),
    Nodes.

-if(?OTP_RELEASE >= 25).
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, _, Node} = peer:start(#{name => Name1,
                                 wait_boot => infinity}),
    Node.
-else.
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    Options = [{monitor_master, true}],
    {ok, Node} = ct_slave:start(Name1, Options),
    Node.
-endif.

get_leader_in_store(StoreId, [Node | _] = _RunningNodes) ->
    %% Query members; this is used to make sure there is an elected leader.
    ct:pal("Trying to figure who the leader is in \"~s\"", [StoreId]),
    [_ | _] = rpc:call(Node, khepri_cluster, members, [StoreId]),
    LeaderId = rpc:call(Node, ra_leaderboard, lookup_leader, [StoreId]),
    ?assertNotEqual(undefined, LeaderId),
    LeaderId.
