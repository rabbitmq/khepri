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

-type key() :: {khepri_path:native_path(), khepri:query_options()}.

-export_type([cache/0, key/0]).

-export([init/1,
         lookup/2,
         lookup_remote/3,
         store/3,
         store_remote/4,
         evict/2,
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

-spec lookup(cache(), key()) -> khepri:ok(term()) | error.
%% @doc Looks up the value of `PathPattern' in the `Cache', returning
%% `error' if not present in the cache.

lookup(Cache, Key) ->
    case ets:lookup(Cache, Key) of
        [{Key, Value}] ->
            {ok, Value};
        _ ->
            error
    end.

-spec lookup_remote(ra:server_id(), key(), timeout()) ->
    khepri:ok(term()) | error.
%% @doc Looks up the value for `PathPattern' in the {@link cache()} on
%% `ServerId''s node.

lookup_remote({StoreId, Node}, Key, _Timeout) when Node =:= node() ->
    lookup(from_store_id(StoreId), Key);
lookup_remote({StoreId, Node}, Key, Timeout) ->
    Cache = from_store_id(StoreId),
    case rpc:call(Node, ?MODULE, lookup, [Cache, Key], Timeout) of
        {badrpc, _Reason} ->
            error;
        Value ->
            Value
    end.

-spec store(cache(), key(), term()) -> ok.
%% @doc Stores `Value' in the given `Cache' for the given `PathPattern'.

store(Cache, Key, Value) ->
    ets:insert(Cache, {Key, Value}),
    ok.

-spec store_remote(RaServer, Key, Value, timeout()) -> ok when
    RaServer :: ra:server_id(),
    Key :: key(),
    Value :: term().
%% @doc Stores `Value' in `RaServer''s {@link cache()}.

store_remote({StoreId, Node}, Key, Value, _Timeout) when Node =:= node() ->
    store(from_store_id(StoreId), Key, Value);
store_remote({StoreId, Node}, Key, Value, Timeout) ->
    Cache = from_store_id(StoreId),
    rpc:call(Node, ?MODULE, store, [Cache, Key, Value], Timeout),
    ok.

-spec evict(cache(), key()) -> ok.
%% @doc Removes the `Key' entry from the `Cache'.

evict(Cache, Key) ->
    ets:delete(Cache, Key),
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
