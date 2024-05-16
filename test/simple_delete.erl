%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(simple_delete).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

delete_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:delete(?FUNCTION_NAME, [foo])),
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
      ?_assertEqual(
         ok,
         khepri:delete(?FUNCTION_NAME, [foo])),
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
         khepri:delete(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

delete_many_on_non_existing_node_with_condition_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
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
      ?_assertEqual(
         ok,
         khepri:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
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
         ok,
         khepri:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "bar"}])),
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
         ok,
         khepri:delete_many(
           ?FUNCTION_NAME, [#if_path_matches{regex = any}])),
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
         ok,
         khepri:delete_many(
           ?FUNCTION_NAME, [foo, #if_path_matches{regex = any}])),
      ?_assertEqual(
         {ok, #{[foo] => foo_value}},
         khepri:get_many(?FUNCTION_NAME, [#if_path_matches{regex = any}]))]}.

clear_payload_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertMatch(
         {error, ?khepri_error(node_not_found, _)},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_keep_while_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:clear_payload(?FUNCTION_NAME, [foo], #{keep_while => #{}})),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:clear_payload(?FUNCTION_NAME, [foo], #{async => false})),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_many_payloads_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{}},
         khepri:get_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}]))]}.

clear_many_payloads_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo1], foo1_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo2, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{[foo1] => undefined,
                [foo2] => undefined,
                [foo2, bar] => bar_value}},
         khepri:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         ok,
         khepri:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}],
           #{})),
      ?_assertEqual(
         ok,
         khepri:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}],
           #{keep_while => #{}}))]}.
