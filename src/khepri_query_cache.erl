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
-include("khepri.hrl").

-opaque cache() :: atom().
%% An identifier for a query cache.

-type key() :: khepri_path:native_pattern().

-export_type([cache/0, key/0]).

-export([init/1,
         lookup/2,
         store/3,
         evict/2,
         from_store_id/1]).

-spec init(khepri:store_id()) -> cache().
%% @doc Creates a new ETS table to act as a query cache for the store.
%% If the cache already exists, it is not recreated.

init(StoreId) ->
    Name = from_store_id(StoreId),
    _ = case ets:info(Name) of
            undefined ->
                ets:new(Name, [public, named_table]);
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

-spec store(cache(), key(), term()) -> ok.
%% @doc Stores `Value' in the given `Cache' for the given `PathPattern'.

store(Cache, Key, Value) ->
    ets:insert(Cache, {Key, Value}),
    ok.

-spec evict(cache(), key()) -> ok.
%% @doc Removes the `Key' entry from the `Cache'.

evict(Cache, PathPattern) ->
    KeyPattern = path_pattern_to_pattern_match(PathPattern),
    ets:match_delete(Cache, {KeyPattern, '_'}),
    ok.

path_pattern_to_pattern_match(PathPattern) ->
    path_pattern_to_pattern_match(lists:reverse(PathPattern), []).

path_pattern_to_pattern_match([], Acc) ->
    Acc;
path_pattern_to_pattern_match([?STAR_STAR | Rest], _Acc) ->
    %% Note: this creates an improper list with `` '_' '' at the tail.
    %% This is equivalent to a pattern match such as
    %%     [component1, component2 | _]
    %% and describes that the tail can match anything.
    path_pattern_to_pattern_match(Rest, '_');
path_pattern_to_pattern_match([Condition | Rest], Acc)
  when ?IS_CONDITION(Condition) ->
    path_pattern_to_pattern_match(Rest, ['_' | Acc]);
path_pattern_to_pattern_match([Component | Rest], Acc) ->
    path_pattern_to_pattern_match(Rest, [Component | Acc]).

-spec from_store_id(khepri:store_id()) -> cache().
%% @doc Determines the name of the query cache from the store ID.

from_store_id(StoreId) ->
    case persistent_term:get({?MODULE, StoreId}, undefined) of
        undefined ->
            Name = list_to_atom(io_lib:format("~s_~s", [?MODULE, StoreId])),
            persistent_term:put({?MODULE, StoreId}, Name),
            Name;
        Name ->
            Name
    end.
