%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_delete).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

delete_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:delete(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, #{}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_test_() ->
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
         khepri:delete(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, #{}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_non_existing_node_with_condition_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:delete(?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_with_condition_true_test_() ->
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
         khepri:delete(?FUNCTION_NAME, [#if_name_matches{regex = "foo"}])),
      ?_assertEqual(
         {ok, #{}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

delete_existing_node_with_condition_false_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{}},
         khepri:delete(?FUNCTION_NAME, [#if_name_matches{regex = "bar"}])),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

clear_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[] => #{payload_version => 1,
                        child_list_version => 3,
                        child_list_length => 2},
                [foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 1},
                [foo, bar] => #{data => bar_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0},
                [baz] => #{data => baz_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [?STAR_STAR])),
      %% FIXME: SHould it return child nodes of `/:foo'?
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 1},
                [baz] => #{data => baz_value,
                           payload_version => 1,
                           child_list_version => 1,
                           child_list_length => 0}}},
         khepri:clear_store(?FUNCTION_NAME)),
      ?_assertEqual(
         {ok, #{[] => #{payload_version => 1,
                        child_list_version => 4,
                        child_list_length => 0}}},
         khepri:get(?FUNCTION_NAME, [?STAR_STAR]))]}.
