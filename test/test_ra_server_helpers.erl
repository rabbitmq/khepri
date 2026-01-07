%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(test_ra_server_helpers).

-include_lib("stdlib/include/assert.hrl").

-export([setup/1,
         setup/2,
         cleanup/1]).

setup(Testcase) ->
    setup(Testcase, #{}).

setup(Testcase, CustomConfig) ->
    _ = logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(khepri),
    khepri_utils:init_list_of_modules_to_skip(),

    #{ra_system := RaSystem} = Props = helpers:start_ra_system(Testcase),
    RaServerConfig = maps:put(cluster_name, Testcase, CustomConfig),
    {ok, StoreId} = khepri:start(RaSystem, RaServerConfig),
    Props#{store_id => StoreId}.

cleanup(#{store_id := StoreId} = Props) ->
    Nodes = case khepri_cluster:nodes(StoreId) of
                %% If the list is empty, assumed it was running on this node
                %% but the store was stopped. This is the case in
                %% app_starts_workers_test_() for instance.
                {ok, L}    -> L;
                {error, _} -> [node()]
            end,
    lists:foreach(
      fun(Node) ->
              _ = rpc:call(Node, khepri, stop, [StoreId]),
              ?assertEqual(
                 ok,
                 rpc:call(Node, helpers, stop_ra_system, [Props]))
      end, Nodes),
    ok.
