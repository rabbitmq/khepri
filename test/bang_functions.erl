%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(bang_functions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

get_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [foo]),
         {ok, khepri:'get!'(?FUNCTION_NAME, [foo])}),
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [foo], #{}),
         {ok, khepri:'get!'(?FUNCTION_NAME, [foo], #{})}),

      %% Get non-existing node.
      ?_assertError(
         ?khepri_error(node_not_found, _),
         khepri:'get!'(?FUNCTION_NAME, [bar])),
      ?_assertError(
         ?khepri_error(node_not_found, _),
         khepri:'get!'(?FUNCTION_NAME, [bar], #{})),

      ?_assertError(noproc, khepri:'get!'([foo]))
     ]}.

get_or_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:get_or(?FUNCTION_NAME, [foo], default),
         {ok, khepri:'get_or!'(?FUNCTION_NAME, [foo], default)}),
      ?_assertEqual(
         khepri:get_or(?FUNCTION_NAME, [foo], default, #{}),
         {ok, khepri:'get_or!'(?FUNCTION_NAME, [foo], default, #{})}),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:get_or(?FUNCTION_NAME, [bar], default),
         {ok, khepri:'get_or!'(?FUNCTION_NAME, [bar], default)}),
      ?_assertEqual(
         khepri:get_or(?FUNCTION_NAME, [bar], default, #{}),
         {ok, khepri:'get_or!'(?FUNCTION_NAME, [bar], default, #{})}),

      ?_assertError(noproc, khepri:'get_or!'([foo], default))
     ]}.

get_many_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:get_many(?FUNCTION_NAME, [foo]),
         {ok, khepri:'get_many!'(?FUNCTION_NAME, [foo])}),
      ?_assertEqual(
         khepri:get_many(?FUNCTION_NAME, [foo], #{}),
         {ok, khepri:'get_many!'(?FUNCTION_NAME, [foo], #{})}),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:get_many(?FUNCTION_NAME, [bar]),
         {ok, khepri:'get_many!'(?FUNCTION_NAME, [bar])}),
      ?_assertEqual(
         khepri:get_many(?FUNCTION_NAME, [bar], #{}),
         {ok, khepri:'get_many!'(?FUNCTION_NAME, [bar], #{})}),

      ?_assertError(noproc, khepri:'get_many!'([foo]))
     ]}.

get_many_or_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:get_many_or(?FUNCTION_NAME, [foo], default),
         {ok, khepri:'get_many_or!'(?FUNCTION_NAME, [foo], default)}),
      ?_assertEqual(
         khepri:get_many_or(?FUNCTION_NAME, [foo], default, #{}),
         {ok, khepri:'get_many_or!'(?FUNCTION_NAME, [foo], default, #{})}),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:get_many_or(?FUNCTION_NAME, [bar], default),
         {ok, khepri:'get_many_or!'(?FUNCTION_NAME, [bar], default)}),
      ?_assertEqual(
         khepri:get_many_or(?FUNCTION_NAME, [bar], default, #{}),
         {ok, khepri:'get_many_or!'(?FUNCTION_NAME, [bar], default, #{})}),

      ?_assertError(noproc, khepri:'get_many_or!'([foo], default))
     ]}.

exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:exists(?FUNCTION_NAME, [foo]),
         khepri:'exists!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         khepri:exists(?FUNCTION_NAME, [foo], #{}),
         khepri:'exists!'(?FUNCTION_NAME, [foo], #{})),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:exists(?FUNCTION_NAME, [bar]),
         khepri:'exists!'(?FUNCTION_NAME, [bar])),
      ?_assertEqual(
         khepri:exists(?FUNCTION_NAME, [bar], #{}),
         khepri:'exists!'(?FUNCTION_NAME, [bar], #{})),

      ?_assertError(noproc, khepri:'exists!'([foo]))
     ]}.

has_data_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:has_data(?FUNCTION_NAME, [foo]),
         khepri:'has_data!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         khepri:has_data(?FUNCTION_NAME, [foo], #{}),
         khepri:'has_data!'(?FUNCTION_NAME, [foo], #{})),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:has_data(?FUNCTION_NAME, [bar]),
         khepri:'has_data!'(?FUNCTION_NAME, [bar])),
      ?_assertEqual(
         khepri:has_data(?FUNCTION_NAME, [bar], #{}),
         khepri:'has_data!'(?FUNCTION_NAME, [bar], #{})),

      ?_assertError(noproc, khepri:'has_data!'([foo]))
     ]}.

is_sproc_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:is_sproc(?FUNCTION_NAME, [foo]),
         khepri:'is_sproc!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         khepri:is_sproc(?FUNCTION_NAME, [foo], #{}),
         khepri:'is_sproc!'(?FUNCTION_NAME, [foo], #{})),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:is_sproc(?FUNCTION_NAME, [bar]),
         khepri:'is_sproc!'(?FUNCTION_NAME, [bar])),
      ?_assertEqual(
         khepri:is_sproc(?FUNCTION_NAME, [bar], #{}),
         khepri:'is_sproc!'(?FUNCTION_NAME, [bar], #{})),

      ?_assertError(noproc, khepri:'is_sproc!'([foo]))
     ]}.

count_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:count(?FUNCTION_NAME, [foo]),
         {ok, khepri:'count!'(?FUNCTION_NAME, [foo])}),
      ?_assertEqual(
         khepri:count(?FUNCTION_NAME, [foo], #{}),
         {ok, khepri:'count!'(?FUNCTION_NAME, [foo], #{})}),

      %% Get non-existing node.
      ?_assertEqual(
         0,
         khepri:'count!'(?FUNCTION_NAME, [bar])),
      ?_assertEqual(
         0,
         khepri:'count!'(?FUNCTION_NAME, [bar], #{})),

      ?_assertError(noproc, khepri:'count!'([foo]))
     ]}.

put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:'put!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'put!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertEqual(
         ok,
         khepri:'put!'(?FUNCTION_NAME, [foo], value3, #{async => true})),
      ?_assertError(noproc, khepri:'put!'([foo], value))
     ]}.

put_many_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:'put_many!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'put_many!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertEqual(
         ok,
         khepri:'put_many!'(?FUNCTION_NAME, [foo], value3, #{async => true})),
      ?_assertError(noproc, khepri:'put_many!'([foo], value))
     ]}.

create_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertError(
         ?khepri_error(
            mismatching_node,
            #{condition := #if_node_exists{exists = false},
              node_name := foo,
              node_path := [foo],
              node_is_target := true,
              node_props := #{data := value1,
                              payload_version := 1}}),
         khepri:'create!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertError(noproc, khepri:'create!'([foo], value))
     ]}.

update_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_error(
            node_not_found,
            #{condition := #if_all{conditions =
                                   [foo,
                                    #if_node_exists{exists = true}]},
              node_name := foo,
              node_path := [foo],
              node_is_target := true}),
         khepri:'update!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'update!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertError(noproc, khepri:'update!'([foo], value))
     ]}.

compare_and_swap_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_error(
            node_not_found,
            #{condition := #if_all{conditions =
                                   [foo,
                                    #if_data_matches{pattern = value0}]},
              node_name := foo,
              node_path := [foo],
              node_is_target := true}),
         khepri:'compare_and_swap!'(?FUNCTION_NAME, [foo], value0, value1)),
      ?_assertEqual(
         ok,
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'compare_and_swap!'(
                  ?FUNCTION_NAME, [foo], value1, value2, #{})),
      ?_assertError(
         noproc,
         khepri:'compare_and_swap!'([foo], old_value, new_value))
     ]}.

delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'delete!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         ok,
         khepri:'delete!'(?FUNCTION_NAME, [foo], #{})),
      ?_assertError(noproc, khepri:'delete!'([foo]))
     ]}.

delete_many_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'delete_many!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         ok,
         khepri:'delete_many!'(?FUNCTION_NAME, [foo], #{})),
      ?_assertError(noproc, khepri:'delete_many!'([foo]))
     ]}.

clear_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'clear_payload!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         ok,
         khepri:'clear_payload!'(?FUNCTION_NAME, [foo], #{})),
      ?_assertEqual(
         ok,
         khepri:'clear_payload!'(?FUNCTION_NAME, [foo], #{async => true})),
      ?_assertError(noproc, khepri:'clear_payload!'([foo]))
     ]}.

clear_many_payloads_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         ok,
         khepri:'clear_many_payloads!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         ok,
         khepri:'clear_many_payloads!'(?FUNCTION_NAME, [foo], #{})),
      ?_assertEqual(
         ok,
         khepri:'clear_many_payloads!'(?FUNCTION_NAME, [foo], #{async => true})),
      ?_assertError(noproc, khepri:'clear_many_payloads!'([foo]))
     ]}.
