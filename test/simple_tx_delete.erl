%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_tx_delete).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

delete_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

invalid_delete_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:delete([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

delete_many_on_non_existing_node_with_condition_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_true_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_false_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [#if_name_matches{regex = "bar"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_many_recursively_1_test_() ->
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
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [#if_path_matches{regex = any}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{}},
         khepri:get_many(?FUNCTION_NAME, [#if_path_matches{regex = any}]))]}.

delete_many_recursively_2_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many(
                             [foo, #if_path_matches{regex = any}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo] => foo_value}},
         khepri:get_many(?FUNCTION_NAME, [#if_path_matches{regex = any}]))]}.

delete_payload_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_with_keep_while_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_payload(
                             [foo], #{keep_while => #{}})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_payload(
                             [foo], #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_many_payloads_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{}},
         khepri:get_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}]))]}.

delete_many_payloads_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo1], foo1_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo2, bar], bar_value)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Fun = fun() ->
                           khepri_tx:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo1] => undefined,
                [foo2] => undefined,
                [foo2, bar] => bar_value}},
         khepri:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}], #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, ok},
         begin
             Fun = fun() ->
                           khepri_tx:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}],
                             #{keep_while => #{}})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.
