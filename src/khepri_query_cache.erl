%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc An `ets' based cache for query results.
%%
%% @hidden

-module(khepri_query_cache).

-opaque cache() :: atom().
%% An identifier for a query cache.

-export_type([cache/0]).

-export([init/1,
         lookup/2,
         lookup_remote/3,
         evict/2,
         reset/1,
         from_store_id/1]).

-spec init(khepri:store_id()) -> cache().
%% @doc Creates a new ETS table to act as a query cache for the store.
%% If the cache already exists, it is not recreated.

init(StoreId) ->
    Name = from_store_id(StoreId),
    _ = case ets:info(Name) of
            undefined ->
                ets:new(Name, [public, named_table, {read_concurrency, true}]);
            _ ->
                ok
        end,
    Name.

-spec lookup(cache(), khepri_path:native_pattern()) ->
    khepri:ok(term()) | error.
%% @doc Looks up the value of `PathPattern' in the `Cache', returning
%% `error' if not present in the cache.

lookup(Cache, PathPattern) ->
    case ets:lookup(Cache, PathPattern) of
        [{PathPattern, Value}] ->
            {ok, Value};
        _ ->
            error
    end.

-spec lookup_remote(ra:server_id(), khepri_path:native_pattern(), timeout()) ->
    khepri:ok(term()) | error.
%% @doc Looks up the value for `PathPattern' in the {@link cache()} on
%% `ServerId''s node.

lookup_remote({StoreId, Node}, PathPattern, _Timeout) when Node =:= node() ->
    lookup(from_store_id(StoreId), PathPattern);
lookup_remote({StoreId, Node}, PathPattern, Timeout) ->
    Cache = from_store_id(StoreId),
    case rpc:call(Node, ?MODULE, lookup, [Cache, PathPattern], Timeout) of
        {badrpc, _Reason} ->
            error;
        Value ->
            Value
    end.

-spec evict(cache(), khepri_path:native_pattern()) -> ok.
%% @doc Removes the `PathPattern' entry from the `Cache'.

evict(Cache, PathPattern) ->
    ets:delete(Cache, PathPattern),
    ok.

-spec reset(cache()) -> ok.
%% @doc Resets the `Cache' by deleting all objects.

reset(Cache) ->
    ets:delete_all_objects(Cache),
    ok.

from_store_id(StoreId) ->
    case persistent_term:get({?MODULE, StoreId}, undefined) of
        undefined ->
            Name = list_to_atom(io_lib:format("~s_~s", [?MODULE, StoreId])),
            persistent_term:put({?MODULE, StoreId}, Name),
            Name;
        Name ->
            Name
    end.
