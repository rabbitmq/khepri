%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(khepri_app).
-behaviour(application).

-export([start/2,
         stop/1,
         config_change/3]).

start(normal, []) ->
    khepri_sup:start_link().

stop(_) ->
    ok.

config_change(_Changed, _New, _Removed) ->
    ok.
