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
         {ok, #{[foo] => #{}}},
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
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
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
         {error,
          {possibly_matching_many_nodes_denied,
           #if_name_matches{regex = any}}},
         khepri:get(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}],
           #{expect_specific_node => true}))]}.

check_node_exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assert(khepri:exists(?FUNCTION_NAME, [foo])),
      ?_assertNot(khepri:exists(?FUNCTION_NAME, [bar]))]}.

check_node_has_data_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertNot(khepri:has_data(?FUNCTION_NAME, [foo]))]}.

check_node_has_data_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertNot(khepri:has_data(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:has_data(?FUNCTION_NAME, [baz]))]}.

check_node_has_sproc_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertNot(khepri:has_sproc(?FUNCTION_NAME, [foo]))]}.

check_node_has_sproc_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], fun() -> bar_value end)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], fun() -> baz_value end)),
      ?_assertNot(khepri:has_sproc(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:has_sproc(?FUNCTION_NAME, [baz]))]}.

get_node_props_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {error, {node_not_found, #{node_name := foo,
                                    node_path := [foo],
                                    node_is_target := true}}},
         khepri:get_node_props(?FUNCTION_NAME, [foo]))]}.

get_node_props_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         #{data => foo_value,
           payload_version => 1,
           child_list_version => 1,
           child_list_length => 0},
         khepri:get_node_props(?FUNCTION_NAME, [foo]))]}.

get_node_props_on_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertThrow(
         {error,
          {possibly_matching_many_nodes_denied,
           #if_name_matches{regex = any}}},
         khepri:get_node_props(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}]))]}.

get_data_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {error, {node_not_found, #{node_name := foo,
                                    node_path := [foo],
                                    node_is_target := true}}},
         khepri:get_data(?FUNCTION_NAME, [foo]))]}.

get_data_on_existing_node_with_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         foo_value,
         khepri:get_data(?FUNCTION_NAME, [foo]))]}.

get_data_on_existing_node_without_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], khepri_payload:none())),
      ?_assertThrow(
         {error, {no_data, #{payload_version := 1,
                             child_list_version := 1,
                             child_list_length := 0}}},
         khepri:get_data(?FUNCTION_NAME, [foo]))]}.

get_data_on_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertThrow(
         {error,
          {possibly_matching_many_nodes_denied,
           #if_name_matches{regex = any}}},
         khepri:get_data(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}]))]}.

get_data_or_default_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         default,
         khepri:get_data_or(?FUNCTION_NAME, [foo], default))]}.

get_data_or_default_on_existing_node_with_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         foo_value,
         khepri:get_data_or(?FUNCTION_NAME, [foo], default))]}.

get_data_or_default_on_existing_node_without_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], khepri_payload:none())),
      ?_assertEqual(
         default,
         khepri:get_data_or(?FUNCTION_NAME, [foo], default))]}.

get_data_or_default_on_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertThrow(
         {error,
          {possibly_matching_many_nodes_denied,
           #if_name_matches{regex = any}}},
         khepri:get_data_or(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}],
           default))]}.

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
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
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
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
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
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
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
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[foo, bar, baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo, bar, qux] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar, qux], qux_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo, bar, baz] => #{data => baz_value,
                                     payload_version => 1,
                                     child_list_version => 1,
                                     child_list_length => 0}}},
         khepri:find(
           ?FUNCTION_NAME, [foo, bar], #if_name_matches{regex = "b"}))]}.

count_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, 0},
         khepri:count(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, 0},
         khepri:count(
           ?FUNCTION_NAME, [foo], #{expect_specific_node => true}))]}.

count_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, 1},
         khepri:count(?FUNCTION_NAME, [foo]))]}.

count_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, 2},
         khepri:count(
           ?FUNCTION_NAME, [?THIS_NODE, #if_name_matches{regex = any}])),
      ?_assertEqual(
         {ok, 3},
         khepri:count(
           ?FUNCTION_NAME, [#if_path_matches{regex = any}])),
      ?_assertEqual(
         {error,
          {possibly_matching_many_nodes_denied,
           #if_name_matches{regex = any}}},
         khepri:count(
           ?FUNCTION_NAME,
           [?THIS_NODE, #if_name_matches{regex = any}],
           #{expect_specific_node => true}))]}.
