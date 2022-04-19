%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(root_node).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

query_root_node_implicitly_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Root = khepri_machine:get_root(S0),
    Ret = khepri_machine:find_matching_nodes(Root, [], #{}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_explicitly_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Root = khepri_machine:get_root(S0),
    Ret = khepri_machine:find_matching_nodes(Root, [?ROOT_NODE], #{}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_using_dot_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Root = khepri_machine:get_root(S0),
    Ret = khepri_machine:find_matching_nodes(Root, [?THIS_NODE], #{}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_above_root_node_using_dot_dot_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Root = khepri_machine:get_root(S0),

    Ret = khepri_machine:find_matching_nodes(
            Root, [?PARENT_NODE, ?PARENT_NODE], #{}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret),

    Ret = khepri_machine:find_matching_nodes(
            Root, [?THIS_NODE, ?PARENT_NODE], #{}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret),

    Ret = khepri_machine:find_matching_nodes(Root, [foo, ?PARENT_NODE], #{}),
    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 2,
                      child_list_length => 1}}},
       Ret).

query_root_node_with_conditions_true_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Root = khepri_machine:get_root(S0),
    Ret = khepri_machine:find_matching_nodes(
            Root,
            [#if_all{conditions = [?ROOT_NODE,
                                   #if_child_list_length{count = 0}]}],
            #{}),

    ?assertEqual(
       {ok, #{[] => #{payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}}},
       Ret).

query_root_node_with_conditions_false_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Root = khepri_machine:get_root(S0),
    Ret = khepri_machine:find_matching_nodes(
            Root,
            [#if_all{conditions = [?ROOT_NODE,
                                   #if_child_list_length{count = 1}]}],
            #{}),

    ?assertEqual(
       {ok, #{}},
       Ret).

store_data_in_root_node_using_empty_path_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Command = #put{path = [],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_root_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Command = #put{path = [?ROOT_NODE],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_dot_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Command = #put{path = [?THIS_NODE],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_using_dot_dot_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Command = #put{path = [?PARENT_NODE],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_true_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #put{path = [#if_all{conditions = [?ROOT_NODE, Compiled]}],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_true_using_dot_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #put{path = [#if_all{conditions = [?THIS_NODE, Compiled]}],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 2,
            child_list_version => 1},
          payload = khepri_payload:data(value)},
       Root),
    ?assertEqual({ok, #{[] => #{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

store_data_in_root_node_with_condition_false_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #put{path = [#if_all{conditions = [?ROOT_NODE, Compiled]}],
                   payload = khepri_payload:data(value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 1}},
       Root),
    ?assertEqual({error,
                  {mismatching_node,
                   #{node_name => ?ROOT_NODE,
                     node_path => [],
                     node_is_target => true,
                     node_props => #{payload_version => 1,
                                         child_list_version => 1,
                                         child_list_length => 0},
                     condition => Compiled}}}, Ret),
    ?assertEqual([], SE).

delete_empty_root_node_test() ->
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME}),
    Command = #delete{path = []},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Command = #delete{path = []},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Command = #delete{path = [?ROOT_NODE]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Command = #delete{path = [?THIS_NODE]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Command = #delete{path = [?PARENT_NODE]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Command = #delete{path = []},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #delete{path = [#if_all{conditions = [?ROOT_NODE, Compiled]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 1}),
    Command = #delete{path = [#if_all{conditions = [?THIS_NODE, Compiled]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
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
    S0 = khepri_machine:init(#{store_id => ?FUNCTION_NAME,
                               commands => Commands}),
    Compiled = khepri_condition:compile(#if_child_list_length{count = 0}),
    Command = #delete{path = [#if_all{conditions = [?ROOT_NODE, Compiled]}]},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{stat = ?INIT_NODE_STAT,
                  payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{}}, Ret),
    ?assertEqual([], SE).
