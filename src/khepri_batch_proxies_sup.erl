%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_batch_proxies_sup).
-behaviour(supervisor).

-export([start_link/0,
         start_proxy/1,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_proxy(StoreId) ->
    case supervisor:start_child(?MODULE, [StoreId]) of
        {ok, _}    -> ok;
        {ok, _, _} -> ok;
        Error      -> Error
    end.

init(_) ->
    SupFlags = #{strategy => simple_one_for_one},
    BatchProxy = #{id => batch_proxy,
                   start => {khepri_batch_proxy, start_link, []},
                   type => worker},
    ChildSpecs = [BatchProxy],
    {ok, {SupFlags, ChildSpecs}}.
