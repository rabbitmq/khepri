%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(advanced_put).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

create_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 1}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

create_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {error,
          {mismatching_node,
           #{condition => #if_node_exists{exists = false},
             node_name => foo,
             node_path => [foo],
             node_is_target => true,
             node_props => #{data => foo_value1,
                             payload_version => 1}}}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {error,
          {mismatching_node,
           #{condition => #if_node_exists{exists = false},
             node_name => foo,
             node_path => [foo],
             node_is_target => true,
             node_props => #{data => foo_value1,
                             payload_version => 1}}}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value2, #{})),
      ?_assertEqual(
         {error,
          {mismatching_node,
           #{condition => #if_node_exists{exists = false},
             node_name => foo,
             node_path => [foo],
             node_is_target => true,
             node_props => #{data => foo_value1,
                             payload_version => 1}}}},
         khepri_adv:create(
           ?FUNCTION_NAME, [foo], foo_value2, #{keep_while => #{}}))]}.

invalid_create_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_adv:create/5:\n"
          "Called with a path pattern which could match many nodes:\n"
          ++ _},
         khepri_adv:create(?FUNCTION_NAME, [?STAR], foo_value))]}.

insert_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:put(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 1}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

insert_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{data => foo_value1,
                payload_version => 2}},
         khepri_adv:put(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:put(?FUNCTION_NAME, [foo], foo_value2, #{})),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:put(
           ?FUNCTION_NAME, [foo], foo_value2, #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

invalid_put_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_adv:put/5:\n"
          "Called with a path pattern which could match many nodes:\n"
          ++ _},
         khepri_adv:put(?FUNCTION_NAME, [?STAR], foo_value))]}.

insert_many_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [a], ?NO_PAYLOAD)),
      ?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [b], ?NO_PAYLOAD)),
      ?_assertEqual(
         {ok, #{[a, foo] => #{payload_version => 1},
                [b, foo] => #{payload_version => 1}}},
         khepri_adv:put_many(?FUNCTION_NAME, [?STAR, foo], foo_value)),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value,
                              payload_version => 1},
                [b, foo] => #{data => foo_value,
                              payload_version => 1}}},
         khepri_adv:get_many(?FUNCTION_NAME, [?STAR, foo]))]}.

insert_many_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [a, foo], foo_value_a)),
      ?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [b, foo], foo_value_b)),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value_a,
                              payload_version => 2},
                [b, foo] => #{data => foo_value_b,
                              payload_version => 2}}},
         khepri_adv:put_many(?FUNCTION_NAME, [?STAR, foo], foo_value_all)),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value_all,
                              payload_version => 2},
                [b, foo] => #{data => foo_value_all,
                              payload_version => 2}}},
         khepri_adv:put_many(
           ?FUNCTION_NAME, [?STAR, foo], foo_value_all, #{})),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value_all,
                              payload_version => 2},
                [b, foo] => #{data => foo_value_all,
                              payload_version => 2}}},
         khepri_adv:put_many(
           ?FUNCTION_NAME, [?STAR, foo], foo_value_all,
           #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value_all,
                              payload_version => 2},
                [b, foo] => #{data => foo_value_all,
                              payload_version => 2}}},
         khepri_adv:get_many(?FUNCTION_NAME, [?STAR, foo]))]}.

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
         khepri_adv:update(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {error,
          {node_not_found,
           #{condition => #if_all{conditions =
                                  [foo,
                                   #if_node_exists{exists = true}]},
             node_name => foo,
             node_path => [foo],
             node_is_target => true}}},
         khepri_adv:update(?FUNCTION_NAME, [foo], foo_value, #{})),
      ?_assertEqual(
         {error,
          {node_not_found,
           #{condition => #if_all{conditions =
                                  [foo,
                                   #if_node_exists{exists = true}]},
             node_name => foo,
             node_path => [foo],
             node_is_target => true}}},
         khepri_adv:update(
           ?FUNCTION_NAME, [foo], foo_value, #{keep_while => #{}}))]}.

update_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{data => foo_value1,
                payload_version => 2}},
         khepri_adv:update(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

invalid_update_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_adv:update/5:\n"
          "Called with a path pattern which could match many nodes:\n"
          ++ _},
         khepri_adv:update(?FUNCTION_NAME, [?STAR], foo_value))]}.

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
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2))]}.

compare_and_swap_matching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{data => foo_value1,
                payload_version => 2}},
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2)),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

compare_and_swap_mismatching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertMatch(
         {error,
          {mismatching_node,
           #{condition := #if_data_matches{pattern = foo_value2},
             node_name := foo,
             node_path := [foo],
             node_is_target := true,
             node_props := #{data := foo_value1,
                             payload_version := 1}}}},
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value2, foo_value3))]}.

compare_and_swap_with_keep_while_or_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, #{data => foo_value1,
                payload_version => 2}},
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2,
           #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 3}},
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value2, foo_value3,
           #{async => false})),
      ?_assertEqual(
         {ok, #{data => foo_value3,
                payload_version => 3}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

invalid_compare_and_swap_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_adv:compare_and_swap/6:\n"
          "Called with a path pattern which could match many nodes:\n"
          ++ _},
         khepri_adv:compare_and_swap(
           ?FUNCTION_NAME, [?STAR], foo_value1, foo_value2))]}.
