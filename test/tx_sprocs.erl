%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(tx_sprocs).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_payload.hrl").
-include("test/helpers.hrl").

use_valid_sproc_as_tx_function_test_() ->
    Fun = fun() -> ok end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], Fun)),
      ?_assertEqual(
         ok,
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {ok, ok},
         khepri:transaction(?FUNCTION_NAME, [tx], []))]}.

call_ro_sproc_test_() ->
    Fun = fun() -> khepri_tx:get([]) end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], Fun)),
      ?_assertError(
         ?khepri_exception(invalid_use_of_khepri_tx_outside_transaction, #{}),
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {ok, {ok, undefined}},
         khepri:transaction(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {ok, {ok, undefined}},
         khepri:transaction(?FUNCTION_NAME, [tx], [], auto)),
      ?_assertEqual(
         {ok, {ok, undefined}},
         khepri:transaction(?FUNCTION_NAME, [tx], [], ro)),
      ?_assertEqual(
         {ok, {ok, undefined}},
         khepri:transaction(?FUNCTION_NAME, [tx], [], rw))]}.

call_rw_sproc_test_() ->
    Fun = fun() -> khepri_tx:put([foo], value) end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], Fun)),
      ?_assertError(
         ?khepri_exception(invalid_use_of_khepri_tx_outside_transaction, #{}),
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {ok, ok},
         khepri:transaction(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {ok, ok},
         khepri:transaction(?FUNCTION_NAME, [tx], [], auto)),
      ?_assertEqual(
         {error, ?khepri_error(sproc_invalid_as_tx_fun, #{path => [tx]})},
         khepri:transaction(?FUNCTION_NAME, [tx], [], ro)),
      ?_assertEqual(
         {ok, ok},
         khepri:transaction(?FUNCTION_NAME, [tx], [], rw))]}.

abort_tx_sproc_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], fun abort_tx/0)),
      ?_assertError(
         ?khepri_exception(invalid_use_of_khepri_tx_outside_transaction, #{}),
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {error, abort_transaction},
         khepri:transaction(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {error, abort_transaction},
         khepri:transaction(?FUNCTION_NAME, [tx], [], auto)),
      ?_assertEqual(
         {error, abort_transaction},
         khepri:transaction(?FUNCTION_NAME, [tx], [], ro)),
      ?_assertEqual(
         {error, abort_transaction},
         khepri:transaction(?FUNCTION_NAME, [tx], [], rw))]}.

-spec abort_tx() -> no_return().

abort_tx() ->
    khepri_tx:abort(abort_transaction).

forbidden_tx_sproc_test_() ->
    Fun = fun() -> node() end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], Fun)),
      ?_assertEqual(
         node(),
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {error, ?khepri_error(sproc_invalid_as_tx_fun, #{path => [tx]})},
         khepri:transaction(?FUNCTION_NAME, [tx], [])),
      ?_assertEqual(
         {error, ?khepri_error(sproc_invalid_as_tx_fun, #{path => [tx]})},
         khepri:transaction(?FUNCTION_NAME, [tx], [], auto)),
      ?_assertEqual(
         {error, ?khepri_error(sproc_invalid_as_tx_fun, #{path => [tx]})},
         khepri:transaction(?FUNCTION_NAME, [tx], [], ro)),
      ?_assertEqual(
         {error, ?khepri_error(sproc_invalid_as_tx_fun, #{path => [tx]})},
         khepri:transaction(?FUNCTION_NAME, [tx], [], rw))]}.

crashing_tx_sproc_test_() ->
    Fun = fun() -> erlang:throw("Expected crash") end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [tx], Fun)),
      ?_assertThrow(
         "Expected crash",
         khepri:run_sproc(?FUNCTION_NAME, [tx], [])),
      ?_assertThrow(
         "Expected crash",
         khepri:transaction(?FUNCTION_NAME, [tx], [])),
      ?_assertThrow(
         "Expected crash",
         khepri:transaction(?FUNCTION_NAME, [tx], [], auto)),
      ?_assertThrow(
         "Expected crash",
         khepri:transaction(?FUNCTION_NAME, [tx], [], ro)),
      ?_assertThrow(
         "Expected crash",
         khepri:transaction(?FUNCTION_NAME, [tx], [], rw))]}.
