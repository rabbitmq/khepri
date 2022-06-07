%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(test_ra_server_helpers).

-include_lib("stdlib/include/assert.hrl").

-export([setup/1,
         cleanup/1]).

setup(Testcase) ->
    _ = logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(khepri),
    ok = application:set_env(
           khepri,
           consistent_query_interval_in_compromise,
           2),

    #{ra_system := RaSystem} = Props = helpers:start_ra_system(Testcase),
    {ok, StoreId} = khepri:start(RaSystem, Testcase),
    Props#{store_id => StoreId}.

cleanup(#{store_id := StoreId} = Props) ->
    Nodes = case khepri_cluster:nodes(StoreId) of
                %% If the list is empty, assumed it was running on this node
                %% but the store was stopped. This is the case in
                %% app_starts_workers_test_() for instance.
                [] -> [node()];
                L  -> L
            end,
    lists:foreach(
      fun(Node) ->
              _ = rpc:call(Node, khepri, stop, [StoreId]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, helpers, stop_ra_system, [Props]))
      end, Nodes),
    ok.
