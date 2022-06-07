%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(bang_functions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

get_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),

      %% Get existing node.
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [foo]),
         {ok, khepri:'get!'(?FUNCTION_NAME, [foo])}),
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [foo], #{}),
         {ok, khepri:'get!'(?FUNCTION_NAME, [foo], #{})}),

      %% Get non-existing node.
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [bar]),
         {ok, khepri:'get!'(?FUNCTION_NAME, [bar])}),
      ?_assertEqual(
         khepri:get(?FUNCTION_NAME, [bar], #{}),
         {ok, khepri:'get!'(?FUNCTION_NAME, [bar], #{})}),
      ?_assertError(noproc, khepri:'get!'([foo]))
     ]}.

put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         #{[foo] => #{}},
         khepri:'put!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         #{[foo] => #{data => value1,
                      payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'put!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertEqual(
         #{[foo] => #{data => value2,
                      payload_version => 2,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'put!'(?FUNCTION_NAME, [foo], value3, #{}, #{})),
      ?_assertEqual(
         ok,
         khepri:'put!'(?FUNCTION_NAME, [foo], value4, #{async => true})),
      ?_assertError(noproc, khepri:'put!'([foo], value))
     ]}.

create_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         #{[foo] => #{}},
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertError(
         {mismatching_node,
          #{condition := #if_node_exists{exists = false},
            node_name := foo,
            node_path := [foo],
            node_is_target := true,
            node_props := #{data := value1,
                            payload_version := 1,
                            child_list_version := 1,
                            child_list_length := 0}}},
         khepri:'create!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertError(
         {mismatching_node,
          #{condition := #if_node_exists{exists = false},
            node_name := foo,
            node_path := [foo],
            node_is_target := true,
            node_props := #{data := value1,
                            payload_version := 1,
                            child_list_version := 1,
                            child_list_length := 0}}},
         khepri:'create!'(?FUNCTION_NAME, [foo], value3, #{}, #{})),
      ?_assertError(noproc, khepri:'create!'([foo], value))
     ]}.

update_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {node_not_found,
          #{condition := #if_all{conditions =
                                 [foo,
                                  #if_node_exists{exists = true}]},
            node_name := foo,
            node_path := [foo],
            node_is_target := true}},
         khepri:'update!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         #{[foo] => #{}},
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         #{[foo] => #{data => value1,
                      payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'update!'(?FUNCTION_NAME, [foo], value2, #{})),
      ?_assertEqual(
         #{[foo] => #{data => value2,
                      payload_version => 2,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'update!'(?FUNCTION_NAME, [foo], value3, #{}, #{})),
      ?_assertError(noproc, khepri:'update!'([foo], value))
     ]}.

compare_and_swap_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {node_not_found,
          #{condition := #if_all{conditions =
                                 [foo,
                                  #if_data_matches{pattern = value0}]},
            node_name := foo,
            node_path := [foo],
            node_is_target := true}},
         khepri:'compare_and_swap!'(?FUNCTION_NAME, [foo], value0, value1)),
      ?_assertEqual(
         #{[foo] => #{}},
         khepri:'create!'(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         #{[foo] => #{data => value1,
                      payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'compare_and_swap!'(
                  ?FUNCTION_NAME, [foo], value1, value2, #{})),
      ?_assertError(
         {mismatching_node,
          #{condition := #if_data_matches{pattern = value1},
            node_name := foo,
            node_path := [foo],
            node_is_target := true,
            node_props := #{data := value2,
                            payload_version := 2,
                            child_list_version := 1,
                            child_list_length := 0}}},
         khepri:'compare_and_swap!'(
                  ?FUNCTION_NAME, [foo], value1, value3, #{}, #{})),
      ?_assertError(
         noproc,
         khepri:'compare_and_swap!'([foo], old_value, new_value))
     ]}.

delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo], value1)),
      ?_assertEqual(
         #{[foo] => #{data => value1,
                      payload_version => 1,
                      child_list_version => 1,
                      child_list_length => 0}},
         khepri:'delete!'(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         #{},
         khepri:'delete!'(?FUNCTION_NAME, [foo], #{})),
      ?_assertError(noproc, khepri:'delete!'([foo]))
     ]}.
