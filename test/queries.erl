%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(queries).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

query_a_non_existing_node_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(Tree, [foo], #{}),

    ?assertEqual(
       {ok, #{}},
       Ret).

query_an_existing_node_with_no_value_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [foo],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 1}}},
       Ret).

query_an_existing_node_with_value_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [foo, bar],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, bar] => #{data => value,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0}}},
       Ret).

query_a_node_with_matching_condition_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [foo,
                                   #if_data_matches{pattern = value}]}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{data => value,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       Ret).

query_a_node_with_non_matching_condition_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [foo,
                                   #if_data_matches{pattern = other}]}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{}},
       Ret).

query_child_nodes_of_a_specific_node_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_name_matches{regex = any}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 1},
              [baz] => #{data => baz_value,
                         payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0}}},
       Ret).

query_child_nodes_of_a_specific_node_with_condition_on_leaf_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [#if_name_matches{regex = any},
                                   #if_child_list_length{count = {ge, 1}}]}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 1}}},
       Ret).

query_many_nodes_with_condition_on_parent_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_child_list_length{count = {gt, 1}},
             #if_name_matches{regex = any}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, bar] => #{data => bar_value,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0},
              [foo, youpi] => #{data => youpi_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}},
       Ret).

query_many_nodes_recursively_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret1 = khepri_tree:find_matching_nodes(
             Tree,
             [#if_path_matches{regex = any}],
             #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 2,
                         child_list_length => 2},
              [foo, bar] => #{data => bar_value,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0},
              [foo, youpi] => #{data => youpi_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
              [baz] => #{data => baz_value,
                         payload_version => 1,
                         child_list_version => 2,
                         child_list_length => 1},
              [baz, pouet] => #{data => pouet_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}},
       Ret1),

    Ret2 = khepri_tree:find_matching_nodes(
             Tree,
             [#if_path_matches{regex = any}],
             #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),
    ?assertEqual(Ret1, Ret2).

query_many_nodes_recursively_using_regex_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_path_matches{regex = "o"}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 2,
                         child_list_length => 2},
              [foo, youpi] => #{data => youpi_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
              [baz, pouet] => #{data => pouet_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}},
       Ret).

query_many_nodes_recursively_with_condition_on_leaf_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_path_matches{regex = any}, #if_name_matches{regex = "o"}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, youpi] => #{data => youpi_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
              [baz, pouet] => #{data => pouet_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}},
       Ret).

query_many_nodes_recursively_with_condition_on_self_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_all{conditions = [#if_path_matches{regex = any},
                                   #if_data_matches{pattern = '_'}]}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, bar] => #{data => bar_value,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0},
              [foo, youpi] => #{data => youpi_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
              [baz] => #{data => baz_value,
                         payload_version => 1,
                         child_list_version => 2,
                         child_list_length => 1},
              [baz, pouet] => #{data => pouet_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0}}},
       Ret).

query_many_nodes_recursively_with_several_star_star_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [#if_path_matches{regex = any},
             baz,
             #if_path_matches{regex = any}],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, bar, baz, qux] => #{data => qux_value,
                                        payload_version => 1,
                                        child_list_version => 1,
                                        child_list_length => 0}}},
       Ret).

query_a_node_using_relative_path_components_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree, [?THIS_KHEPRI_NODE, foo, ?PARENT_KHEPRI_NODE, foo, bar],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_list_length]}),

    ?assertEqual(
       {ok, #{[foo, bar] => #{data => value,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0}}},
       Ret).

include_child_names_in_query_response_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [foo, quux],
                     payload = khepri_payload:data(quux_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    Ret = khepri_tree:find_matching_nodes(
            Tree,
            [foo],
            #{props_to_return => [payload,
                                  payload_version,
                                  child_list_version,
                                  child_names]}),

    ?assertEqual(
       {ok, #{[foo] => #{payload_version => 1,
                         child_list_version => 2,
                         child_names => [bar, quux]}}},
       Ret).
