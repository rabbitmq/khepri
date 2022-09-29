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
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

create_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

create_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {error,
          ?khepri_error(
             mismatching_node,
             #{condition => #if_node_exists{exists = false},
               node_name => foo,
               node_path => [foo],
               node_is_target => true,
               node_props => #{data => foo_value1,
                               payload_version => 1}})},
         khepri:create(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {error,
          ?khepri_error(
             mismatching_node,
             #{condition => #if_node_exists{exists = false},
               node_name => foo,
               node_path => [foo],
               node_is_target => true,
               node_props => #{data => foo_value1,
                               payload_version => 1}})},
         khepri:create(?FUNCTION_NAME, [foo], foo_value2, #{}))]}.

invalid_create_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:create(?FUNCTION_NAME, [?STAR], foo_value))]}.

insert_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

insert_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [foo], foo_value2, #{})),
      ?_assertEqual(
         {ok, foo_value2},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

invalid_put_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:put(?FUNCTION_NAME, [?STAR], foo_value))]}.

insert_many_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [a], ?NO_PAYLOAD)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [b], ?NO_PAYLOAD)),
      ?_assertEqual(
         ok,
         khepri:put_many(?FUNCTION_NAME, [?STAR, foo], foo_value)),
      ?_assertEqual(
         {ok, #{[a, foo] => foo_value,
                [b, foo] => foo_value}},
         khepri:get_many(?FUNCTION_NAME, [?STAR, foo]))]}.

insert_many_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [a, foo], foo_value_a)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [b, foo], foo_value_b)),
      ?_assertEqual(
         ok,
         khepri:put_many(?FUNCTION_NAME, [?STAR, foo], foo_value_all)),
      ?_assertEqual(
         ok,
         khepri:put_many(?FUNCTION_NAME, [?STAR, foo], foo_value_all, #{})),
      ?_assertEqual(
         {ok, #{[a, foo] => foo_value_all,
                [b, foo] => foo_value_all}},
         khepri:get_many(?FUNCTION_NAME, [?STAR, foo]))]}.

update_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error,
          ?khepri_error(
             node_not_found,
             #{condition => #if_all{conditions =
                                    [foo,
                                     #if_node_exists{exists = true}]},
               node_name => foo,
               node_path => [foo],
               node_is_target => true})},
         khepri:update(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {error,
          ?khepri_error(
             node_not_found,
             #{condition => #if_all{conditions =
                                    [foo,
                                     #if_node_exists{exists = true}]},
               node_name => foo,
               node_path => [foo],
               node_is_target => true})},
         khepri:update(?FUNCTION_NAME, [foo], foo_value, #{}))]}.

update_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         ok,
         khepri:update(?FUNCTION_NAME, [foo], foo_value2)),
      ?_assertEqual(
         {ok, foo_value2},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

invalid_update_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:update(?FUNCTION_NAME, [?STAR], foo_value))]}.

compare_and_swap_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertMatch(
         {error,
          ?khepri_error(
             node_not_found,
             #{condition := #if_all{conditions =
                                    [foo,
                                     #if_data_matches{pattern = foo_value1}]},
               node_name := foo,
               node_path := [foo],
               node_is_target := true})},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2))]}.

compare_and_swap_matching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         ok,
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2)),
      ?_assertEqual(
         {ok, foo_value2},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

compare_and_swap_mismatching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertMatch(
         {error,
          ?khepri_error(
             mismatching_node,
             #{condition := #if_data_matches{pattern = foo_value2},
               node_name := foo,
               node_path := [foo],
               node_is_target := true,
               node_props := #{data := foo_value1,
                               payload_version := 1}})},
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value2, foo_value3))]}.

compare_and_swap_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         ok,
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [foo], foo_value1, foo_value2,
           #{async => false})),
      ?_assertEqual(
         {ok, foo_value2},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

invalid_compare_and_swap_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:compare_and_swap(
           ?FUNCTION_NAME, [?STAR], foo_value1, foo_value2))]}.
