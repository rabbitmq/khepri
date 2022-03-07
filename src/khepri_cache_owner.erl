%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_cache_owner).
-behaviour(gen_server).

-include("include/khepri.hrl").

-export([start_link/0,
         new_store/1,
         reset/1,
         name/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(?MODULE, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

new_store(StoreId) ->
    gen_server:call(?SERVER, {new_store, StoreId}).

reset(StoreId) ->
    ets:delete_all_objects(name(StoreId)).

name(StoreId) ->
    persistent_term:get({?MODULE, StoreId}).

name_from_store_id(StoreId) ->
    list_to_atom(io_lib:format("~s_~s", [?MODULE, StoreId])).

init(_) ->
    State = #?MODULE{},
    {ok, State}.

handle_call({new_store, StoreId}, _From, State) ->
    Name = name_from_store_id(StoreId),
    case ets:info(Name) of
        undefined ->
            ets:new(Name, [public, named_table, {read_concurrency, true}]),
            persistent_term:put({?MODULE, StoreId}, Name);
        _ ->
            ok
    end,
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
