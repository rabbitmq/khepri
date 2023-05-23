%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_sup).
-behaviour(supervisor).

-export([start_link/0,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_) ->
    SupFlags = #{strategy => one_for_one},
    BatchProxiesSup = #{id => batch_proxies_sup,
                        start => {khepri_batch_proxies_sup, start_link, []},
                        type => supervisor},
    EventHandlerSpec = #{id => event_handler,
                         start => {khepri_event_handler, start_link, []},
                         type => worker},
    ChildSpecs = [BatchProxiesSup, EventHandlerSpec],
    {ok, {SupFlags, ChildSpecs}}.
