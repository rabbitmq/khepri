%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(advanced_delete).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

delete_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:delete(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 1}},
         khepri_adv:delete(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

invalid_delete_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri_adv:delete(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

delete_many_on_non_existing_node_with_condition_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_true_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1}}},
         khepri_adv:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_false_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{}},
         khepri_adv:delete_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "bar"}])),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 1}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_return_keep_while_expirations_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [a, b, c], val1)),
      ?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [d, e], val2,
           #{keep_while => #{[a, b, c] => #if_node_exists{exists = true}}})),
      ?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [f, g], val3,
           #{keep_while => #{[d, e] => #if_node_exists{exists = true}}})),
      ?_assertMatch(
         {ok, #{[a, b, c] := #{data := val1},
                [d, e] := #{data := val2},
                [f, g] := #{data := val3}}},
         khepri_adv:delete_many(
           ?FUNCTION_NAME, [a, b, c],
           #{return_keep_while_expirations => true})),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => a,
                                                 node_path => [a],
                                                 node_is_target => false})},
         khepri_adv:get(?FUNCTION_NAME, [a, b, c])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => d,
                                                 node_path => [d],
                                                 node_is_target => false})},
         khepri_adv:get(?FUNCTION_NAME, [d, e])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => f,
                                                 node_path => [f],
                                                 node_is_target => false})},
         khepri_adv:get(?FUNCTION_NAME, [f, g]))]}.

clear_payload_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 2}},
         khepri_adv:clear_payload(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_keep_while_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 2}},
         khepri_adv:clear_payload(
           ?FUNCTION_NAME, [foo], #{keep_while => #{}})),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

clear_payload_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 2}},
         khepri_adv:clear_payload(?FUNCTION_NAME, [foo], #{async => false})),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

clear_many_payloads_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{}},
         khepri_adv:get_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}]))]}.

clear_many_payloads_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo1], foo1_value)),
      ?_assertEqual(
         {ok, #{payload_version => 1}},
         khepri_adv:create(?FUNCTION_NAME, [foo2, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[foo1] => #{data => foo1_value,
                            payload_version => 2},
                [foo2] => #{payload_version => 1}}},
         khepri_adv:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{[foo1] => #{payload_version => 2},
                [foo2] => #{payload_version => 1},
                [foo2, bar] => #{data => bar_value,
                                 payload_version => 1}}},
         khepri_adv:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         {ok, #{[foo1] => #{payload_version => 2},
                [foo2] => #{payload_version => 1}}},
         khepri_adv:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}],
           #{})),
      ?_assertEqual(
         {ok, #{[foo1] => #{payload_version => 2},
                [foo2] => #{payload_version => 1}}},
         khepri_adv:clear_many_payloads(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}],
           #{keep_while => #{}}))]}.
