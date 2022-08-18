%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(advanced_tx_get).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_fun.hrl").
-include("test/helpers.hrl").

get_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok,
          {error, {node_not_found, #{node_name => foo,
                                     node_path => [foo],
                                     node_is_target => true}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_test_() ->
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
                           khepri_tx_adv:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{data => foo_value,
                 payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_with_sproc_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], fun() -> ok end)),
      ?_assertMatch(
         {ok, {ok, #{sproc := #standalone_fun{},
                     payload_version := 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok,
          {ok, #{payload_version => 1}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_get_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {khepri,
          invalid_call,
          "Invalid use of khepri_tx_adv:get/2:\n"
          "Called with a path pattern which could match many nodes:\n"
          "[{if_name_matches,any,undefined}]"},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{}},
         khepri_adv:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => #{payload_version => 1},
                 [baz] => #{data => baz_value,
                            payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => #{payload_version => 1},
                 [baz] => #{data => baz_value,
                            payload_version => 1}}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok,
          {error,
           {possibly_matching_many_nodes_denied,
            [?STAR]}}},
         begin
             Fun = fun() ->
                           khepri_tx_adv:get_many(
                             [?STAR],
                             #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.