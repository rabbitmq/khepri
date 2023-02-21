%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(root_node).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

query_root_node_implicitly_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_explicitly_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [?KHEPRI_ROOT_NODE],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_using_dot_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [?THIS_KHEPRI_NODE],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_above_root_node_using_dot_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),

    Ret = khepri_tree:find_matching_nodes(
            Tree, [?PARENT_KHEPRI_NODE, ?PARENT_KHEPRI_NODE],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret),

    Ret = khepri_tree:find_matching_nodes(
            Tree, [?THIS_KHEPRI_NODE, ?PARENT_KHEPRI_NODE],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret),

    Ret = khepri_tree:find_matching_nodes(
            Tree, [foo, ?PARENT_KHEPRI_NODE],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret).

query_root_node_with_conditions_true_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [?KHEPRI_ROOT_NODE,
                                   #if_child_list_length{count = 0}]}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_with_conditions_false_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [?KHEPRI_ROOT_NODE,
                                   #if_child_list_length{count = 1}]}],
            #{}),

    ?assertEqual(
       {ok, #{}},
       Ret).

store_data_in_root_node_using_empty_path_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #put{path = [],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_root_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #put{path = [?KHEPRI_ROOT_NODE],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_dot_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #put{path = [?THIS_KHEPRI_NODE],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_dot_dot_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #put{path = [?PARENT_KHEPRI_NODE],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_true_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #put{path = [#if_all{conditions =
                                   [?KHEPRI_ROOT_NODE, Compiled]}],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_true_using_dot_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #put{path = [#if_all{conditions =
                                   [?THIS_KHEPRI_NODE, Compiled]}],
                   payload = khepri_payload:data(value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_false_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #put{path = [#if_all{conditions =
                                   [?KHEPRI_ROOT_NODE, Compiled]}],
                   payload = khepri_payload:data(value),
                   options = #{expect_specific_node => true}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 1}},
       Root),
    ?assertEqual({error,
                  ?khepri_error(
                     mismatching_node,
                     #{node_name => ?KHEPRI_ROOT_NODE,
                       node_path => [],
                       node_is_target => true,
                       node_props => #{payload_version => 1},
                       condition => Compiled})}, Ret),
    ?assertEqual([], SE).

delete_empty_root_node_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Command = #delete{path = [],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 1}},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_using_empty_path_test() ->
    Commands = [#put{path = [],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 3,
            child_list_version => 1}},
       Root),
    ?assertEqual({ok, #{[] => #{data => value,
                                payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_using_root_test() ->
    Commands = [#put{path = [],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [?KHEPRI_ROOT_NODE],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 3,
            child_list_version => 1}},
       Root),
    ?assertEqual({ok, #{[] => #{data => value,
                                payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_using_dot_test() ->
    Commands = [#put{path = [],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [?THIS_KHEPRI_NODE],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 3,
            child_list_version => 1}},
       Root),
    ?assertEqual({ok, #{[] => #{data => value,
                                payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_using_dot_dot_test() ->
    Commands = [#put{path = [],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [?PARENT_KHEPRI_NODE],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 3,
            child_list_version => 1}},
       Root),
    ?assertEqual({ok, #{[] => #{data => value,
                                payload_version => 2,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_with_child_nodes_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [baz, qux],
                     payload = khepri_payload:data(qux_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Command = #delete{path = [],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 4}},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 3,
                                child_list_length => 2}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_with_condition_true_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #delete{path = [#if_all{conditions =
                                      [?KHEPRI_ROOT_NODE, Compiled]}],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 3}},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 2,
                                child_list_length => 1}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_with_condition_true_using_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #delete{path = [#if_all{conditions =
                                      [?THIS_KHEPRI_NODE, Compiled]}],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 3}},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 2,
                                child_list_length => 1}}}, Ret),
    ?assertEqual([], SE).

delete_root_node_with_condition_false_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #delete{path = [#if_all{conditions =
                                      [?KHEPRI_ROOT_NODE, Compiled]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{props = ?INIT_NODE_PROPS,
                  payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).
