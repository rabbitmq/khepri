%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(favor_option).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

favor_consistency_in_get_test_() ->
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
                          khepri:get(
                            ?FUNCTION_NAME, [I], #{favor => consistency}))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(
                            ?FUNCTION_NAME, [I], I, #{async => true}))
               end, List),

             ?assertEqual(
                {ok, hd(List)},
                khepri:get(
                  ?FUNCTION_NAME, [hd(List)], #{favor => consistency})),

             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok, I},
                          khepri:get(?FUNCTION_NAME, [I]))
               end, tl(List))
         end)
     ]}.

favor_low_latency_in_get_test_() ->
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
                          khepri:get(
                            ?FUNCTION_NAME, [I], #{favor => low_latency}))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(
                            ?FUNCTION_NAME, [I], I, #{async => true}))
               end, List),

             lists:foreach(
               fun(I) ->
                       try
                           ?assertEqual(
                              {ok, I},
                              khepri:get(?FUNCTION_NAME, [I]))
                       catch
                           error:{assertEqual, Props} ->
                               ?assertEqual(
                                  {error,
                                   ?khepri_error(
                                      node_not_found,
                                      #{node_name => I,
                                        node_path => [I],
                                        node_is_target => true})},
                                  proplists:get_value(value, Props))
                       end
               end, List)
         end)
     ]}.

favor_consistency_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             List = [a, b, c, d, e, f, g, h, i, j],
             Fun = fun(I) -> khepri_tx:get([I]) end,
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok,
                           {error,
                            ?khepri_error(
                               node_not_found, #{node_name => I,
                                                 node_path => [I],
                                                 node_is_target => true})}},
                          khepri:transaction(
                            ?FUNCTION_NAME, Fun, [I], #{favor => consistency}))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(
                            ?FUNCTION_NAME, [I], I, #{async => true}))
               end, List),

             ?assertEqual(
                {ok, {ok, hd(List)}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, [hd(List)], #{favor => consistency})),

             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok, {ok, I}},
                          khepri:transaction(
                            ?FUNCTION_NAME, Fun, [I], #{favor => consistency}))
               end, tl(List))
         end)
     ]}.

favor_low_latency_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             List = [a, b, c, d, e, f, g, h, i, j],
             Fun = fun(I) -> khepri_tx:get([I]) end,
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          {ok,
                           {error,
                            ?khepri_error(
                               node_not_found, #{node_name => I,
                                                 node_path => [I],
                                                 node_is_target => true})}},
                          khepri:transaction(
                            ?FUNCTION_NAME, Fun, [I], #{favor => low_latency}))
               end, List),
             lists:foreach(
               fun(I) ->
                       ?assertEqual(
                          ok,
                          khepri:put(
                            ?FUNCTION_NAME, [I], I, #{async => true}))
               end, List),

             lists:foreach(
               fun(I) ->
                       try
                           ?assertEqual(
                              {ok, {ok, I}},
                              khepri:transaction(
                                ?FUNCTION_NAME, Fun, [I],
                                #{favor => low_latency}))
                       catch
                           error:{assertEqual, Props} ->
                               ?assertEqual(
                                  {ok,
                                   {error,
                                    ?khepri_error(
                                       node_not_found,
                                       #{node_name => I,
                                         node_path => [I],
                                         node_is_target => true})}},
                                  proplists:get_value(value, Props))
                       end
               end, List)
         end)
     ]}.

condition_option_takes_precedence_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], value)),

             ?assertEqual(
                {error, timeout},
                khepri:get(
                  ?FUNCTION_NAME, [foo],
                  #{condition => {applied, {10000, 5}},
                    favor => consistency,
                    timeout => 1000}))
         end)
     ]}.
