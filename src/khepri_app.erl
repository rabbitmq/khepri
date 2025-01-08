%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @hidden

-module(khepri_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").

-export([get_default_timeout/0]).

-export([start/2,
         stop/1,
         config_change/3]).

start(normal, []) ->
    ok = khepri_utils:init_list_of_modules_to_skip(),
    khepri_sup:start_link().

stop(_) ->
    StoreIds = khepri:get_store_ids(),
    lists:foreach(
      fun(StoreId) -> _ = khepri_cluster:stop(StoreId) end,
      StoreIds),
    khepri_utils:clear_list_of_modules_to_skip(),
    ok.

config_change(_Changed, _New, _Removed) ->
    ok.

get_default_timeout() ->
    Timeout = application:get_env(khepri, default_timeout, infinity),
    if
        ?IS_TIMEOUT(Timeout) ->
            ok;
        true ->
            ?LOG_ERROR(
               "Invalid timeout set in `default_timeout` "
               "application environment: ~p",
               [Timeout]),
            ?khepri_misuse(
               invalid_default_timeout_value,
               #{default_timeout => Timeout})
    end,
    Timeout.
