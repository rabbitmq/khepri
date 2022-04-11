%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc A cache interface for transaction functions
%%
%% The default cache implementation is an `ets'-based LRU cache.
%% The capacity of the cache can be configured by setting
%% `application:set_env(khepri, fun_cache_capacity, Capacity)'.
%% By default, the capacity is `10_000' elements.

-module(khepri_fun_cache).

-export([get/2, put/2]).

%% Exported for testing
-export([clear/0]).

-define(CACHE, '$__ETS_LRU_CACHE__').
-define(RANKS, '$__ETS_LRU_RANKS__').

-ifdef(TEST).
-define(CAPACITY, 3).
-else.
-define(CAPACITY, 10_000).
-endif.

-callback get(Key :: term(), Default :: term()) -> term().
-callback put(Key :: term(), Fun :: term()) -> ok.

get(Key, Default) ->
    case ets:whereis(?CACHE) of
        undefined ->
            setup_tables(),
            Default;
        _Tid ->
            get1(Key, Default)
    end.

get1(Key, Default) ->
    case ets:lookup(?CACHE, Key) of
        [{Key, Rank, Value}] ->
            update_rank(Rank, Key),
            Value;
        [] ->
            Default
    end.

put(Key, Value) ->
    case ets:whereis(?CACHE) of
        undefined ->
            setup_tables(),
            insert_key(Key, Value),
            ok;
        _Tid ->
            put1(Key, Value)
    end.

put1(Key, Value) ->
    case ets:lookup(?CACHE, Key) of
        [{Key, _Rank, Value}] ->
            ok;
        [{Key, Rank, _OtherValue}] ->
            update_rank(Rank, Key),
            ets:update_element(?CACHE, Key, {3, Value}),
            ok;
        [] ->
            insert_key(Key, Value),
            ok
    end.

clear() ->
    ets:delete(?CACHE),
    ets:delete(?RANKS),
    ok.

setup_tables() ->
    _ = ets:new(?CACHE, [public, named_table, set]),
    _ = ets:new(?RANKS, [public, named_table, ordered_set]),
    ok.

update_rank(Rank, Key) ->
    NextRank = next_rank(),
    ets:delete(?RANKS, Rank),
    ets:insert(?RANKS, {NextRank, Key}),
    ets:update_element(?CACHE, Key, {2, NextRank}),
    ok.

insert_key(Key, Value) ->
    case ets:info(?CACHE, size) >= capacity() of
        true ->
            %% The cache is full so we discard the least-recently used item.
            OldestRank = ets:first(?RANKS),
            [{OldestRank, OldestKey}] = ets:take(?RANKS, OldestRank),
            ets:delete(?CACHE, OldestKey);
        _ ->
            ok
    end,
    NextRank = next_rank(),
    ets:insert(?CACHE, {Key, NextRank, Value}),
    ets:insert(?RANKS, {NextRank, Key}),
    ok.

capacity() ->
    application:get_env(khepri, fun_cache_capacity, ?CAPACITY).

next_rank() ->
    erlang:unique_integer([monotonic]).
