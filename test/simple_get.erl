%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_get).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

get_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:get(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri:get(
           ?FUNCTION_NAME, [foo], #{expect_specific_node => true}))]}.

get_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

get_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[] => #{payload_version => 1,
                        child_list_version => 3,
                        child_list_length => 2},
                [foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 1},
                [baz] => #{data => baz_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(
           ?FUNCTION_NAME, [?THIS_NODE, #if_name_matches{regex = any}])),
      ?_assertEqual(
         {error, matches_many_nodes},
         khepri:get(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}],
           #{expect_specific_node => true}))]}.

check_node_exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assert(khepri:exists(?FUNCTION_NAME, [foo])),
      ?_assertNot(khepri:exists(?FUNCTION_NAME, [bar]))]}.

check_node_has_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertNot(khepri:has_data(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:has_data(?FUNCTION_NAME, [baz]))]}.

list_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:list(?FUNCTION_NAME, [foo]))]}.

list_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[] => #{payload_version => 1,
                        child_list_version => 3,
                        child_list_length => 2},
                [foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 1},
                [baz] => #{data => baz_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:list(?FUNCTION_NAME, []))]}.

find_node_by_name_in_empty_db_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:find(?FUNCTION_NAME, [], foo))]}.

find_node_by_name_in_filled_db_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 1}}},
         khepri:find(?FUNCTION_NAME, [], foo))]}.

find_node_by_condition_in_filled_db_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo, bar] => #{data => bar_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
                [baz] => #{data => baz_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:find(?FUNCTION_NAME, [], #if_name_matches{regex = "b"}))]}.

find_node_starting_from_subnode_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, qux], qux_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo, bar, baz] => #{data => baz_value,
                                     payload_version => 1,
                                     child_list_version => 1,
                                     child_list_length => 0}}},
         khepri:find(
           ?FUNCTION_NAME, [foo, bar], #if_name_matches{regex = "b"}))]}.
