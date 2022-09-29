%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(async_option).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

async_unset_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_false_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD, #{async => false})),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_true_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD, #{async => true})),
             _ = lists:foldl(
                   fun
                       (_, false) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, true = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:put(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD,
                  #{async => Correlation})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                {ok, #{[foo] => #{}}},
                Ret),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_priority_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD, #{async => low})),
             _ = lists:foldl(
                   fun
                       (_, false) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, true = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_and_priority_in_put_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:put(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD,
                  #{async => {Correlation, low}})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                {ok, #{[foo] => #{}}},
                Ret),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_unset_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             ?assertEqual(
                ok,
                khepri:delete(?FUNCTION_NAME, [foo])),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_false_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             ?assertEqual(
                ok,
                khepri:delete(
                  ?FUNCTION_NAME, [foo], #{async => false})),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_true_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             ?assertEqual(
                ok,
                khepri:delete(
                  ?FUNCTION_NAME, [foo], #{async => true})),
             _ = lists:foldl(
                   fun
                       (_, true) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, false = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:delete(
                  ?FUNCTION_NAME, [foo], #{async => Correlation})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                {ok, #{[foo] => #{}}},
                Ret),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_priority_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             ?assertEqual(
                ok,
                khepri:delete(
                  ?FUNCTION_NAME, [foo], #{async => low})),
             _ = lists:foldl(
                   fun
                       (_, true) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, false = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_and_priority_in_delete_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                ok,
                khepri:put(?FUNCTION_NAME, [foo], ?NO_PAYLOAD)),
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri_adv:delete(
                  ?FUNCTION_NAME, [foo], #{async => {Correlation, low}})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                {ok, #{[foo] => #{payload_version => 1}}},
                Ret),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_unset_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             ?assertEqual(
                {ok, ok},
                khepri:transaction(?FUNCTION_NAME, Fun)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_false_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             ?assertEqual(
                {ok, ok},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{async => false})),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_true_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             ?assertEqual(
                ok,
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{async => true})),
             _ = lists:foldl(
                   fun
                       (_, false) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, true = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{async => Correlation})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                ok,
                Ret),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_in_aborted_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() ->
                           case khepri_tx:exists([non_existent]) of
                               true -> ok;
                               _    -> khepri_tx:abort(abort_reason)
                           end
                   end,
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, rw, #{async => Correlation})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                {error, abort_reason},
                Ret),
             ?assertEqual(
                {error,
                 ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_priority_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             ?assertEqual(
                ok,
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{async => low})),
             _ = lists:foldl(
                   fun
                       (_, false) ->
                           timer:sleep(500),
                           khepri:exists(?FUNCTION_NAME, [foo]);
                       (_, true = Ret) ->
                           Ret
                   end, khepri:exists(?FUNCTION_NAME, [foo]), lists:seq(1, 60)),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

async_with_correlation_and_priority_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:put([foo], ?NO_PAYLOAD) end,
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:transaction(
                  ?FUNCTION_NAME, Fun,
                  #{async => {Correlation, low}})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertEqual(
                ok,
                Ret),
             ?assertEqual(
                {ok, #{payload_version => 1}},
                khepri_adv:get(?FUNCTION_NAME, [foo]))
         end)
     ]}.

wait_for_async_error_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Correlation = 1,
             ?assertEqual(
                ok,
                khepri:update(
                  ?FUNCTION_NAME, [foo], ?NO_PAYLOAD,
                  #{async => Correlation})),
             Ret = khepri:wait_for_async_ret(Correlation),
             ?assertMatch(
                {error, ?khepri_error(node_not_found, _)},
                Ret)
         end)
     ]}.

wait_for_async_ret_but_no_async_call_test() ->
    ?assertEqual(
       {error, timeout},
       khepri:wait_for_async_ret(1, 100)).
