%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(snapshots).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

last_snapshot_request_timestamp_too_far_in_the_future_is_ignored_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS([])),
    S1 = khepri_machine:convert_state(S0, 0, ?LATEST_MACVER),
    StoreId = khepri_machine:get_store_id(S1),
    khepri_machine:cache_effective_machine_version(StoreId, ?LATEST_MACVER),
    ?assert(khepri_machine:does_api_comply_with(request_snapshot, StoreId)),

    Metrics0 = khepri_machine:get_metrics(S1),
    Timestamp = erlang:monotonic_time(second) + 10_000_000,
    Metrics1 = Metrics0#{last_snapshot_request_timestamp => Timestamp},
    S2 = khepri_machine:set_metrics(S1, Metrics1),
    ?assertEqual(continue, khepri_machine:need_snapshot_after_some_time(S2)).
