%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_put).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

create_non_existing_node_test_() ->
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

create_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {error,
          {mismatching_node,
           #{condition => #if_node_exists{exists = false},
             node_name => foo,
             node_path => [foo],
             node_is_target => true,
             node_props => #{data => foo_value1,
                                 payload_version => 1,
                                 child_list_version => 1,
                                 child_list_length => 0}}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value2))]}.

insert_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:put(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

insert_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value1,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:put(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value2,
                           payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

update_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error,
          {node_not_found,
           #{condition => #if_all{conditions =
                                  [foo,
                                   #if_node_exists{exists = true}]},
             node_name => foo,
             node_path => [foo],
             node_is_target => true}}},
         khepri:update(?FUNCTION_NAME, [foo], foo_value))]}.

update_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value1,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:update(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value2,
                           payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

compare_and_swap_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertMatch(
         {error,
          {node_not_found,
           #{condition := #if_all{conditions =
                                  [foo,
                                   #if_data_matches{pattern = foo_value1}]},
             node_name := foo,
             node_path := [foo],
             node_is_target := true}}},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2))]}.

compare_and_swap_matching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value1,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value2,
                           payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

compare_and_swap_mismatching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertMatch(
         {error,
          {mismatching_node,
           #{condition := #if_data_matches{pattern = foo_value2},
             node_name := foo,
             node_path := [foo],
             node_is_target := true,
             node_props := #{data := foo_value1,
                                 payload_version := 1,
                                 child_list_version := 1,
                                 child_list_length := 0}}}},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value2, foo_value3))]}.

compare_and_swap_with_keep_while_or_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value1,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2,
           #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value2,
                           payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value2, foo_value3,
           #{async => false})),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value3,
                           payload_version => 3,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_from_existing_node_test_() ->
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
         khepri:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_keep_while_test_() ->
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
         khepri:clear_payload(?FUNCTION_NAME, [foo], #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_options_test_() ->
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
         khepri:clear_payload(?FUNCTION_NAME, [foo], #{async => false})),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 2,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.
