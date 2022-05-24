%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(favor_option).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

favor_compromise_in_get_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             Ref = khepri_machine:get_last_consistent_call_atomics(
                     ?FUNCTION_NAME),
             TS1 = atomics:get(Ref, 1),
             ?assertNotEqual(0, TS1),

             timer:sleep(1000),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS2 = atomics:get(Ref, 1),
             ?assertEqual(TS1, TS2),

             timer:sleep(2000),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS3 = atomics:get(Ref, 1),
             ?assertNotEqual(TS1, TS3),

             ok
         end)
     ]}.

favor_consistency_in_get_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => consistency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             Ref = khepri_machine:get_last_consistent_call_atomics(
                     ?FUNCTION_NAME),
             TS1 = atomics:get(Ref, 1),
             ?assertNotEqual(0, TS1),

             timer:sleep(1000),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => consistency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS2 = atomics:get(Ref, 1),
             ?assertNotEqual(TS1, TS2),

             ok
         end)
     ]}.

favor_low_latency_in_get_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => low_latency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {ok, #{}},
                khepri:get(
                  ?FUNCTION_NAME, [foo], #{favor => low_latency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ok
         end)
     ]}.

favor_compromise_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:get([foo]) end,

             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             Ref = khepri_machine:get_last_consistent_call_atomics(
                     ?FUNCTION_NAME),
             TS1 = atomics:get(Ref, 1),
             ?assertNotEqual(0, TS1),

             timer:sleep(1000),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS2 = atomics:get(Ref, 1),
             ?assertEqual(TS1, TS2),

             timer:sleep(2000),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => compromise})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS3 = atomics:get(Ref, 1),
             ?assertNotEqual(TS1, TS3),

             ok
         end)
     ]}.

favor_consistency_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:get([foo]) end,

             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => consistency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             Ref = khepri_machine:get_last_consistent_call_atomics(
                     ?FUNCTION_NAME),
             TS1 = atomics:get(Ref, 1),
             ?assertNotEqual(0, TS1),

             timer:sleep(1000),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => consistency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             TS2 = atomics:get(Ref, 1),
             ?assertNotEqual(TS1, TS2),

             ok
         end)
     ]}.

favor_low_latency_in_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_test(
         begin
             Fun = fun() -> khepri_tx:get([foo]) end,

             ?assertEqual(
                undefined,
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => low_latency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ?assertEqual(
                {atomic, {ok, #{}}},
                khepri:transaction(
                  ?FUNCTION_NAME, Fun, #{favor => low_latency})),

             ?assertEqual(
                {?FUNCTION_NAME, node()},
                khepri_cluster:get_cached_leader(?FUNCTION_NAME)),
             ?assertEqual(
                undefined,
                khepri_machine:get_last_consistent_call_atomics(
                  ?FUNCTION_NAME)),

             ok
         end)
     ]}.
