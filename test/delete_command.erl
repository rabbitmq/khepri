%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(delete_command).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

delete_non_existing_node_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #delete{path = [foo]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).

delete_non_existing_node_under_non_existing_parent_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #delete{path = [foo, bar, baz]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_data_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [foo]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes = #{}},
       Root),
    ?assertEqual({ok, #{[foo] => #{data => foo_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_data_using_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [foo, ?THIS_NODE]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes = #{}},
       Root),
    ?assertEqual({ok, #{[foo] => #{data => foo_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_child_nodes_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [foo]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes = #{}},
       Root),
    ?assertEqual({ok, #{[foo] => #{payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 1}}}, Ret),
    ?assertEqual([], SE).

delete_a_node_deep_into_the_tree_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [foo, bar, baz]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               stat = ?INIT_NODE_STAT,
               child_nodes =
               #{bar =>
                 #node{
                    stat = #{payload_version => 1,
                             child_list_version => 2},
                    child_nodes = #{}}}}}},
       Root),
    ?assertEqual({ok, #{[foo, bar, baz] => #{payload_version => 1,
                                             child_list_version => 1,
                                             child_list_length => 1}}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_condition_true_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [#if_data_matches{pattern = bar_value}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 4},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{[bar] => #{data => bar_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_condition_false_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [#if_data_matches{pattern = other_value}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)},
            bar =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(bar_value)}}},
       Root),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_condition_true_using_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path =
                      [bar,
                       #if_all{conditions =
                               [?THIS_NODE,
                                #if_data_matches{pattern = bar_value}]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 4},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{[bar] => #{data => bar_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_existing_node_with_condition_false_using_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path =
                      [bar,
                       #if_all{conditions =
                               [?THIS_NODE,
                                #if_data_matches{pattern = other_value}]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)},
            bar =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(bar_value)}}},
       Root),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).

delete_many_nodes_at_once_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [#if_name_matches{regex = "a"}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 5},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{[bar] => #{data => bar_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0},
                        [baz] => #{data => baz_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_command_bumps_applied_command_count_test() ->
    Commands = [#delete{path = [foo]}],
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               member => khepri_cluster:this_member(
                                           ?FUNCTION_NAME),
                               snapshot_interval => 3,
                               commands => Commands}),

    ?assertEqual(#{}, S0#khepri_machine.metrics),

    Command1 = #delete{path = [bar]},
    {S1, _, SE1} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual([], SE1),

    Command2 = #delete{path = [baz]},
    {S2, _, SE2} = khepri_machine:apply(?META, Command2, S1),

    ?assertEqual(#{applied_command_count => 2}, S2#khepri_machine.metrics),
    ?assertEqual([], SE2),

    Command3 = #delete{path = [qux]},
    Meta = ?META,
    {S3, _, SE3} = khepri_machine:apply(Meta, Command3, S2),

    ?assertEqual(#{}, S3#khepri_machine.metrics),
    ?assertEqual([{release_cursor, maps:get(index, Meta), S3}], SE3).
