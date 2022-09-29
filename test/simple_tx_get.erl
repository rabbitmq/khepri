%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_tx_get).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_fun.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

get_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok,
          {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                  node_path => [foo],
                                                  node_is_target => true})}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
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
         {ok, {ok, #standalone_fun{}}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, {ok, undefined}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_get_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:get([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, default}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_on_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, {ok, default}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_get_or_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:get_or([?STAR], default)
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
                           khepri_tx:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => undefined,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => undefined,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?STAR]}),
         begin
             Fun = fun() ->
                           khepri_tx:get_many(
                             [?STAR], #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_or_default_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or([?STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_or_default_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => default,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or([?STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => default,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or([?STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?STAR]}),
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or(
                             [?STAR], default,
                             #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:exists([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_exists_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:exists([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_has_data_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_has_data_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_has_data_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:has_data([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_is_sproc_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_is_sproc_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], fun() -> bar_value end)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], fun() -> baz_value end)),
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_is_sproc_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

count_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, 0}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, {ok, 0}},
         begin
             Fun = fun() ->
                           khepri_tx:count(
                             [foo], #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

count_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, 1}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, 1}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

count_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, {ok, 2}},
         begin
             Fun = fun() ->
                           khepri_tx:count([?THIS_NODE, ?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, {ok, 3}},
         begin
             Fun = fun() ->
                           khepri_tx:count([?STAR_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?STAR]}),
         begin
             Fun = fun() ->
                           khepri_tx:count(
                             [?THIS_NODE, ?STAR],
                             #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.
