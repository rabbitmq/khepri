%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers
%% to Broadcom Inc. and/or its subsidiaries.
%%

-module(cached_leader).

-include_lib("eunit/include/eunit.hrl").

cache_leader_test() ->
    StoreId = ?FUNCTION_NAME,
    LeaderId = {name, node},
    ?assertEqual(
       undefined,
       khepri_cluster:get_cached_leader(StoreId)),
    ?assertEqual(
       ok,
       khepri_cluster:cache_leader(StoreId, LeaderId)),
    ?assertEqual(
       LeaderId,
       khepri_cluster:get_cached_leader(StoreId)),
    ?assertEqual(
       ok,
       khepri_cluster:clear_cached_leader(StoreId)).

cache_unknown_leader_test() ->
    StoreId = ?FUNCTION_NAME,
    ?assertEqual(
       undefined,
       khepri_cluster:get_cached_leader(StoreId)),
    ?assertEqual(
       ok,
       khepri_cluster:cache_leader(StoreId, not_known)),
    ?assertEqual(
       undefined,
       khepri_cluster:get_cached_leader(StoreId)).

clear_cached_leader_test() ->
    StoreId = ?FUNCTION_NAME,
    LeaderId = {name, node},
    ?assertEqual(
       ok,
       khepri_cluster:cache_leader(StoreId, LeaderId)),
    ?assertEqual(
       LeaderId,
       khepri_cluster:get_cached_leader(StoreId)),
    ?assertEqual(
       ok,
       khepri_cluster:clear_cached_leader(StoreId)),
    ?assertEqual(
       undefined,
       khepri_cluster:get_cached_leader(StoreId)).
