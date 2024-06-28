%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(fence).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_payload.hrl").
-include("test/helpers.hrl").

fence_after_startup_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:fence(?FUNCTION_NAME))
         end)
     ]}.

fence_after_sync_updates_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             List = [a, b, c, d, e, f, g, h, i, j],
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {error,
                           ?khepri_error(
                              node_not_found, #{node_name => I,
                                                node_path => [I],
                                                node_is_target => true})},
                          khepri:get(?FUNCTION_NAME, [I]))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(?FUNCTION_NAME, [I], I))
               end, List),

             ?assertEqual(
                ok,
                khepri:fence(?FUNCTION_NAME)),

             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok, I},
                          khepri:get(?FUNCTION_NAME, [I]))
               end, List)
         end)
     ]}.

fence_after_async_updates_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             List = [a, b, c, d, e, f, g, h, i, j],
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {error,
                           ?khepri_error(
                              node_not_found, #{node_name => I,
                                                node_path => [I],
                                                node_is_target => true})},
                          khepri:get(?FUNCTION_NAME, [I]))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(
                            ?FUNCTION_NAME, [I], I, #{async => true}))
               end, List),

             ?assertEqual(
                ok,
                khepri:fence(?FUNCTION_NAME)),

             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok, I},
                          khepri:get(?FUNCTION_NAME, [I]))
               end, List)
         end)
     ]}.

fence_with_default_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             DefaultStoreId = khepri_cluster:get_default_store_id(),
             ok = application:set_env(
                    khepri, default_store_id, ?FUNCTION_NAME),

             ?assertEqual(
                ok,
                khepri:fence()),

             ok = application:set_env(
                    khepri, default_store_id, DefaultStoreId)
         end)
     ]}.

fence_with_default_store_and_a_timeout_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                {error, noproc},
                khepri:fence(10000)),
             ?assertEqual(
                {error, noproc},
                khepri:fence(infinity))
         end)
     ]}.

fence_with_a_timeout_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:fence(?FUNCTION_NAME, 10000))
         end)
     ]}.
