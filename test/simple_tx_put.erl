%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(simple_tx_put).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_payload.hrl").
-include("test/helpers.hrl").

create_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:create([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:create([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
         {ok,
          {error,
           ?khepri_error(
              mismatching_node,
              #{condition => #if_node_exists{exists = false},
                node_name => foo,
                node_path => [foo],
                node_is_target => true,
                node_props => #{data => foo_value1,
                                payload_version => 1}})}},
         begin
             Fun = fun() ->
                           khepri_tx:create([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {error,
           ?khepri_error(
              mismatching_node,
              #{condition => #if_node_exists{exists = false},
                node_name => foo,
                node_path => [foo],
                node_is_target => true,
                node_props => #{data => foo_value1,
                                payload_version => 1}})}},
         begin
             Fun = fun() ->
                           khepri_tx:create([foo], foo_value2, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_create_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:create([?KHEPRI_WILDCARD_STAR], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

insert_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:put([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:put([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put([foo], foo_value2, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
         begin
             Fun = fun() ->
                           khepri_tx:put([?KHEPRI_WILDCARD_STAR], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[a, foo] => foo_value,
                [b, foo] => foo_value}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR, foo]))]}.

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
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[a, foo] => foo_value_all,
                [b, foo] => foo_value_all}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR, foo]))]}.

update_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok,
          {error,
           ?khepri_error(
              node_not_found,
              #{condition => #if_all{conditions =
                                     [foo,
                                      #if_node_exists{exists = true}]},
                node_name => foo,
                node_path => [foo],
                node_is_target => true})}},
         begin
             Fun = fun() ->
                           khepri_tx:update([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {error,
           ?khepri_error(
              node_not_found,
              #{condition => #if_all{conditions =
                                     [foo,
                                      #if_node_exists{exists = true}]},
                node_name => foo,
                node_path => [foo],
                node_is_target => true})}},
         begin
             Fun = fun() ->
                           khepri_tx:update([foo], foo_value, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

update_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:update([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:update([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
         begin
             Fun = fun() ->
                           khepri_tx:update([?KHEPRI_WILDCARD_STAR], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertMatch(
         {ok,
          {error,
           ?khepri_error(
              node_not_found,
              #{condition := #if_all{conditions =
                                     [foo,
                                      #if_data_matches{pattern = foo_value1}]},
                node_name := foo,
                node_path := [foo],
                node_is_target := true})}},
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_matching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
         {ok,
          {error,
           ?khepri_error(
              mismatching_node,
              #{condition := #if_data_matches{pattern = foo_value2},
                node_name := foo,
                node_path := [foo],
                node_is_target := true,
                node_props := #{data := foo_value1,
                                payload_version := 1}})}},
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], foo_value2, foo_value3)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], foo_value1, foo_value2,
                             #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [?KHEPRI_WILDCARD_STAR], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.
