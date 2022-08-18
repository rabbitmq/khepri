%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(timeout).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

can_specify_timeout_in_milliseconds_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [foo], foo_value, #{timeout => 60000})),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo], #{timeout => 60000})),
      ?_assertEqual(
         ok,
         khepri:delete(?FUNCTION_NAME, [foo], #{timeout => 60000}))]}.

can_specify_infinity_timeout_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [foo], foo_value, #{timeout => infinity})),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo], #{timeout => infinity})),
      ?_assertEqual(
         ok,
         khepri:delete(?FUNCTION_NAME, [foo], #{timeout => infinity}))]}.
