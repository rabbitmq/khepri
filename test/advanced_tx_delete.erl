%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(advanced_tx_delete).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

delete_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         {khepri, denied_update_in_readonly_tx,
          "Updates to the tree are denied in a read-only transaction"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value,
                 payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

invalid_delete_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_tx_adv:delete/2:\n"
          "Called with a path pattern which could match many nodes:\n"
          "[{if_name_matches,any,undefined}]"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

delete_many_on_non_existing_node_with_condition_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_true_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         {khepri, denied_update_in_readonly_tx,
          "Updates to the tree are denied in a read-only transaction"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => #{data => foo_value,
                            payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_on_existing_node_with_condition_false_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many(
                             [#if_name_matches{regex = "bar"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{data => foo_value,
                payload_version => 1}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {error, {node_not_found, #{node_name => foo,
                                    node_path => [foo],
                                    node_is_target => true}}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         {khepri, denied_update_in_readonly_tx,
          "Updates to the tree are denied in a read-only transaction"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value,
                 payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_with_keep_while_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value,
                 payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_payload(
                             [foo], #{keep_while => #{}})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_payload_with_options_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value,
                 payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_payload(
                             [foo], #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{payload_version => 2}},
         khepri_adv:get(?FUNCTION_NAME, [foo]))]}.

delete_many_payloads_from_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{}},
         khepri_adv:get_many(
           ?FUNCTION_NAME, [#if_name_matches{regex = "foo"}]))]}.

delete_many_payloads_from_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo1], foo1_value)),
      ?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo2, bar], bar_value)),
      ?_assertError(
         {khepri, denied_update_in_readonly_tx,
          "Updates to the tree are denied in a read-only transaction"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo1] => #{data => foo1_value,
                             payload_version => 1},
                 [foo2] => #{payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo1] => #{payload_version => 2},
                [foo2] => #{payload_version => 1},
                [foo2, bar] => #{data => bar_value,
                                 payload_version => 1}}},
         khepri_adv:get_many(
           ?FUNCTION_NAME, [?STAR_STAR])),
      ?_assertEqual(
         {ok,
          {ok, #{[foo1] => #{payload_version => 2},
                 [foo2] => #{payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}], #{})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo1] => #{payload_version => 2},
                 [foo2] => #{payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:delete_many_payloads(
                             [#if_name_matches{regex = "foo"}],
                             #{keep_while => #{}})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.