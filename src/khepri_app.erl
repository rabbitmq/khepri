%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_app).
-behaviour(application).

-export([start/2,
         stop/1,
         config_change/3]).

start(normal, []) ->
    khepri_sup:start_link().

stop(_) ->
    StoreIds = khepri:get_store_ids(),
    lists:foreach(
      fun(StoreId) -> khepri_machine:clear_cached_leader(StoreId) end,
      StoreIds),
    khepri:forget_store_ids(),
    ok.

config_change(_Changed, _New, _Removed) ->
    ok.
