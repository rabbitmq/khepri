%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(advanced_tx_put).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

create_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx_adv:create([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, #{payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:create([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
                           khepri_tx_adv:create([foo], foo_value2)
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
                           khepri_tx_adv:create([foo], foo_value2, #{})
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
                           khepri_tx_adv:create(
                             [?KHEPRI_WILDCARD_STAR], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

insert_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put([foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx_adv:put([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value1,
                 payload_version => 2}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value2,
                 payload_version => 2}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put([foo], foo_value2, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

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
                           khepri_tx_adv:put([?KHEPRI_WILDCARD_STAR], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
         {ok,
          {ok, #{[a, foo] => #{payload_version => 1},
                 [b, foo] => #{payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value,
                              payload_version => 1},
                [b, foo] => #{data => foo_value,
                              payload_version => 1}}},
         khepri_adv:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR, foo]))]}.

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
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx_adv:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[a, foo] => #{data => foo_value_a,
                               payload_version => 2},
                 [b, foo] => #{data => foo_value_b,
                               payload_version => 2}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[a, foo] => #{data => foo_value_all,
                               payload_version => 2},
                 [b, foo] => #{data => foo_value_all,
                               payload_version => 2}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:put_many(
                             [?KHEPRI_WILDCARD_STAR, foo], foo_value_all, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[a, foo] => #{data => foo_value_all,
                              payload_version => 2},
                [b, foo] => #{data => foo_value_all,
                              payload_version => 2}}},
         khepri_adv:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR, foo]))]}.

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
                           khepri_tx_adv:update([foo], foo_value)
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
                           khepri_tx_adv:update([foo], foo_value, #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

update_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx_adv:update([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value1,
                 payload_version => 2}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:update([foo], foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

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
                           khepri_tx_adv:update(
                             [?KHEPRI_WILDCARD_STAR], foo_value)
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
                           khepri_tx_adv:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_matching_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx_adv:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value1,
                 payload_version => 2}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:compare_and_swap(
                             [foo], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
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
                           khepri_tx_adv:compare_and_swap(
                             [foo], foo_value2, foo_value3)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value1)),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value1,
                 payload_version => 2}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:compare_and_swap(
                             [foo], foo_value1, foo_value2,
                             #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{data => foo_value2,
                payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

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
                           khepri_tx_adv:compare_and_swap(
                             [?KHEPRI_WILDCARD_STAR], foo_value1, foo_value2)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end
         )]}.
