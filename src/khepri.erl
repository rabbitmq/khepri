%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Khepri database API.
%%
%% This module exposes the database API to manipulate data.
%%
%% As a convenience, some functions of the service and cluster management API
%% are also repeated in this module. See the {@link khepri_cluster} module
%% for the documentation.

-module(khepri).

-include_lib("kernel/include/logger.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").

-export([start/0,
         start/1,
         start/3,
         reset/2,
         get_store_ids/0,

         create/2, create/3,
         insert/2, insert/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         clear_payload/1, clear_payload/2,
         delete/1, delete/2,

         get/1, get/2, get/3,
         exists/1, exists/2,
         has_data/1, has_data/2,
         list/1, list/2,
         find/2, find/3,

         transaction/1, transaction/2, transaction/3,
         run_sproc/2, run_sproc/3,

         clear_store/0, clear_store/1,

         no_payload/0,
         data_payload/1,
         sproc_payload/1,

         info/0,
         info/1]).

-compile({no_auto_import, [get/2]}).

-type store_id() :: ra:cluster_name().
%% ID of a Khepri store.

-type ok(Type) :: {ok, Type}.

-type error() :: error(any()).

-type error(Type) :: {error, Type}.
%% Return value of a failed command or query.

-export_type([store_id/0,
              ok/1,
              error/0]).

%% -------------------------------------------------------------------
%% Service management.
%% -------------------------------------------------------------------

-spec start() -> Ret when
      Ret :: ok(StoreId) | error(),
      StoreId :: store_id().
%% @doc Starts a store on the default Ra system.
%%
%% The store uses the default Ra cluster name and cluster friendly name.
%%
%% @see khepri_cluster:start/0.

start() ->
    khepri_cluster:start().

-spec start(RaSystem) -> Ret when
      RaSystem :: atom(),
      Ret :: ok(StoreId) | error(),
      StoreId :: store_id().
%% @doc Starts a store on the specified Ra system.
%%
%% The store uses the default Ra cluster name and cluster friendly name.
%%
%% @param RaSystem the name of the Ra system.
%%
%% @see khepri_cluster:start/1.

start(RaSystem) ->
    khepri_cluster:start(RaSystem).

-spec start(RaSystem, ClusterName, FriendlyName) -> Ret when
      RaSystem :: atom(),
      ClusterName :: ra:cluster_name(),
      FriendlyName :: string(),
      Ret :: ok(StoreId) | error(),
      StoreId :: store_id().
%% @doc Starts a store on the specified Ra system.
%%
%% @param RaSystem the name of the Ra system.
%% @param ClusterName the name of the Ra cluster.
%% @param FriendlyName the friendly name of the Ra cluster.
%%
%% @see khepri_cluster:start/3.

start(RaSystem, ClusterName, FriendlyName) ->
    khepri_cluster:start(RaSystem, ClusterName, FriendlyName).

-spec reset(RaSystem, ClusterName) -> Ret when
      RaSystem :: atom(),
      ClusterName :: ra:cluster_name(),
      Ret :: ok | error() | {badrpc, any()}.
%% @doc Resets the store on this Erlang node.
%%
%% It does that by force-deleting the Ra local server.
%%
%% @param RaSystem the name of the Ra system.
%% @param ClusterName the name of the Ra cluster.
%%
%% @see khepri_cluster:reset/2.

reset(RaSystem, ClusterName) ->
    khepri_cluster:reset(RaSystem, ClusterName).

-spec get_store_ids() -> [StoreId] when
      StoreId :: store_id().
%% @doc Returns the list of running stores.
%%
%% @see khepri_cluster:get_store_ids/0.

get_store_ids() ->
    khepri_cluster:get_store_ids().

%% -------------------------------------------------------------------
%% Data manipulation.
%% This is the simple API. The complete/advanced one is exposed by the
%% `khepri_machine' module.
%% -------------------------------------------------------------------

-spec create(Path, Data) -> ok | error() when
      Path :: khepri_path:pattern() | string(),
      Data :: khepri_machine:data().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% Calling this function is the same as calling
%% `create(StoreId, Path, Data)' with the default store ID.
%%
%% @see create/3.

create(Path, Data) ->
    create(?DEFAULT_RA_CLUSTER_NAME, Path, Data).

-spec create(StoreId, Path, Data) -> ok | error() when
    StoreId :: store_id(),
    Path :: khepri_path:pattern() | string(),
    Data :: khepri_machine:data().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% The `Path' is the modified to include a `#if_node_exists{exists = false}'
%% condition on its last component.
%%
%% Once the path is possibly converted to a list of node names and conditions
%% and updated, it calls {@link khepri_machine:put/3}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:put/3}.
%%
%% @see khepri_machine:put/3.

create(StoreId, Path, Data) ->
    Path1 = khepri_path:maybe_from_string(Path),
    Path2 = khepri_path:combine_with_conditions(
              Path1, [#if_node_exists{exists = false}]),
    do_put(StoreId, Path2, Data).

-spec insert(Path, Data) -> ok | error() when
      Path :: khepri_path:pattern() | string(),
      Data :: khepri_machine:data().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling
%% `insert(StoreId, Path, Data)' with the default store ID.
%%
%% @see insert/3.

insert(Path, Data) ->
    insert(?DEFAULT_RA_CLUSTER_NAME, Path, Data).

-spec insert(StoreId, Path, Data) -> ok | error() when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      Data :: khepri_machine:data().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% Once the path is normalized to a list of tree node names and conditions and
%% updated, it calls {@link khepri_machine:put/3}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:put/3}.
%%
%% @see khepri_machine:put/3.

insert(StoreId, Path, Data) ->
    Path1 = khepri_path:maybe_from_string(Path),
    do_put(StoreId, Path1, Data).

-spec update(Path, Data) -> ok | error() when
      Path :: khepri_path:pattern() | string(),
      Data :: khepri_machine:data().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% Calling this function is the same as calling
%% `update(StoreId, Path, Data)' with the default store ID.
%%
%% @see update/3.

update(Path, Data) ->
    update(?DEFAULT_RA_CLUSTER_NAME, Path, Data).

-spec update(StoreId, Path, Data) -> ok | error() when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      Data :: khepri_machine:data().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% The `Path' is the modified to include a `#if_node_exists{exists = true}'
%% condition on its last component.
%%
%% Once the path is possibly converted to a list of node names and conditions
%% and updated, it calls {@link khepri_machine:put/3}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:put/3}.
%%
%% @see khepri_machine:put/3.

update(StoreId, Path, Data) ->
    Path1 = khepri_path:maybe_from_string(Path),
    Path2 = khepri_path:combine_with_conditions(
              Path1, [#if_node_exists{exists = true}]),
    do_put(StoreId, Path2, Data).

-spec compare_and_swap(Path, DataPattern, Data) -> ok | error() when
      Path :: khepri_path:pattern() | string(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_machine:data().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% Calling this function is the same as calling
%% `compare_and_swap(StoreId, Path, DataPattern, Data)' with the default store
%% ID.
%%
%% @see create/3.

compare_and_swap(Path, DataPattern, Data) ->
    compare_and_swap(?DEFAULT_RA_CLUSTER_NAME, Path, DataPattern, Data).

-spec compare_and_swap(StoreId, Path, DataPattern, Data) -> ok | error() when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_machine:data().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% The `Path' is the modified to include a
%% `#if_data_matches{pattern = DataPattern}' condition on its last component.
%%
%% Once the path is possibly converted to a list of node names and conditions
%% and updated, it calls {@link khepri_machine:put/3}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:put/3}.
%%
%% @see khepri_machine:put/3.

compare_and_swap(StoreId, Path, DataPattern, Data) ->
    Path1 = khepri_path:maybe_from_string(Path),
    Path2 = khepri_path:combine_with_conditions(
              Path1, [#if_data_matches{pattern = DataPattern}]),
    do_put(StoreId, Path2, Data).

-spec do_put(
        store_id(), khepri_path:pattern() | string(), khepri_machine:data()) ->
    ok | error().
%% @doc Calls {@link khepri_machine:put/3} and simplifies the return value.
%%
%% The "ok" tuple is converted to a single "ok" atom, getting rid of the map
%% of entries.
%%
%% The "error" tuple is left unmodified.
%%
%% @private

do_put(StoreId, Path, Fun) when is_function(Fun) ->
    case khepri_machine:put(StoreId, Path, #kpayload_sproc{sproc = Fun}) of
        {ok, _} -> ok;
        Error   -> Error
    end;
do_put(StoreId, Path, Data) ->
    case khepri_machine:put(StoreId, Path, #kpayload_data{data = Data}) of
        {ok, _} -> ok;
        Error   -> Error
    end.

-spec clear_payload(Path) -> ok | error() when
      Path :: khepri_path:pattern() | string().
%% @doc Clears the payload of an existing specific tree node in the tree structure.
%%
%% Calling this function is the same as calling
%% `clear_payload(StoreId, Path)' with the default store ID.
%%
%% @see create/3.

clear_payload(Path) ->
    clear_payload(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec clear_payload(StoreId, Path) -> ok | error() when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string().
%% @doc Clears the payload of an existing specific tree node in the tree structure.
%%
%% In other words, the payload is set to `none'.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% Once the path is possibly converted to a list of node names and conditions
%% and updated, it calls {@link khepri_machine:put/3}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:put/3}.
%%
%% @see khepri_machine:put/3.

clear_payload(StoreId, Path) ->
    Path1 = khepri_path:maybe_from_string(Path),
    case khepri_machine:put(StoreId, Path1, none) of
        {ok, _} -> ok;
        Error   -> Error
    end.

-spec delete(PathPattern) -> ok | error() when
      PathPattern :: khepri_path:pattern() | string().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling
%% `delete(StoreId, PathPattern)' with
%% the default store ID.
%%
%% @see delete/2.

delete(Path) ->
    delete(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec delete(StoreId, PathPattern) -> ok | error() when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern() | string().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% Once the path is possibly converted to a list of node names and conditions,
%% it calls {@link khepri_machine:delete/2}.
%%
%% @returns a single "ok" atom or an "error" tuple, unlike
%% {@link khepri_machine:delete/2}.
%%
%% @see delete/2.

delete(StoreId, Path) ->
    Path1 = khepri_path:maybe_from_string(Path),
    case khepri_machine:delete(StoreId, Path1) of
        {ok, _} -> ok;
        Error   -> Error
    end.

-spec get(PathPattern) -> Result when
      PathPattern :: khepri_path:pattern() | string(),
      Result :: khepri_machine:result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling
%% `get(StoreId, PathPattern)' with the default store ID.
%%
%% @see get/3.

get(Path) ->
    get(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec get
(StoreId, PathPattern) -> Result when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern() | string(),
      Result :: khepri_machine:result();
(PathPattern, Options) -> Result when
      PathPattern :: khepri_path:pattern() | string(),
      Options :: khepri_machine:query_options(),
      Result :: khepri_machine:result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`get(StoreId, Path)'. Calling it is the same as calling
%% `get(StoreId, PathPattern, #{})'.</li>
%% <li>`get(Path, Options'. Calling it is the same as calling
%% `get(StoreId, PathPattern, #{})' with the default store ID.</li>
%% </ul>
%%
%% @see get/3.

get(StoreId, Path) when is_atom(StoreId) ->
    get(StoreId, Path, #{});
get(Path, Options) when is_map(Options) ->
    get(?DEFAULT_RA_CLUSTER_NAME, Path, Options).

-spec get(StoreId, PathPattern, Options) -> Result when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern() | string(),
      Options :: khepri_machine:query_options(),
      Result :: khepri_machine:result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% Once the path is possibly converted to a list of node names and conditions,
%% it calls {@link khepri_machine:get/3}.
%%
%% @see khepri_machine:get/3.

get(StoreId, Path, Options) ->
    Path1 = khepri_path:maybe_from_string(Path),
    khepri_machine:get(StoreId, Path1, Options).

-spec exists(Path) -> Exists when
      Path :: khepri_path:pattern() | string(),
      Exists :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path exists,
%% otherwise `false'.
%%
%% Calling this function is the same as calling
%% `exists(StoreId, Path)' with the default store ID.
%%
%% @see exists/2.

exists(Path) ->
    exists(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec exists(StoreId, Path) -> Exists when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      Exists :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path exists,
%% otherwise `false'.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% The `Path' must point to a specific tree node and can't match multiple nodes.
%%
%% This function calls {@link get/3} and interpret its result.
%%
%% @see get/3.

exists(StoreId, Path) ->
    case get(StoreId, Path, #{expect_specific_node => true}) of
        {ok, _} -> true;
        _       -> false
    end.

-spec has_data(Path) -> HasData when
      Path :: khepri_path:pattern() | string(),
      HasData :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path has a data
%% payload, otherwise `false'.
%%
%% Calling this function is the same as calling
%% `has_data(StoreId, Path)' with the default store ID.
%%
%% @see has_data/2.

has_data(Path) ->
    has_data(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec has_data(StoreId, Path) -> HasData when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      HasData :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path has a data
%% payload, otherwise `false'.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% The `Path' must point to a specific tree node and can't match multiple nodes.
%%
%% This function calls {@link get/3} and interpret its result.
%%
%% @see get/3.

has_data(StoreId, Path) ->
    case get(StoreId, Path, #{expect_specific_node => true}) of
        {ok, Result} ->
            [NodeProps] = maps:values(Result),
            maps:is_key(data, NodeProps);
        _ ->
            false
    end.

-spec list(khepri_path:pattern() | string()) ->
    khepri_machine:result().

list(Path) ->
    list(?DEFAULT_RA_CLUSTER_NAME, Path).

-spec list(store_id(), khepri_path:pattern() | string()) ->
    khepri_machine:result().

list(StoreId, Path) ->
    Path1 = khepri_path:maybe_from_string(Path),
    Path2 = [?ROOT_NODE | Path1] ++ [?STAR],
    khepri_machine:get(StoreId, Path2).

-spec find(Path, Condition) ->
    Result when
      Path :: khepri_path:pattern() | string(),
      Condition :: khepri_path:pattern_component(),
      Result :: khepri_machine:result().
%% @doc Finds tree nodes below `Path' which match the given `Condition'.
%%
%% This function operates on the default store.
%%
%% @see find/3.

find(Path, Condition) ->
    find(?DEFAULT_RA_CLUSTER_NAME, Path, Condition).

-spec find(StoreId, Path, Condition) ->
    Result when
      StoreId :: store_id(),
      Path :: khepri_path:pattern() | string(),
      Condition :: khepri_path:pattern_component(),
      Result :: khepri_machine:result().
%% @doc Finds tree nodes under `Path' which match the given `Condition'.
%%
%% The `Path' can be provided as a list of node names and conditions or as a
%% string. See {@link khepri_path:from_string/1}.
%%
%% Nodes are searched deeply under the given `Path', not only among direct
%% child nodes.
%%
%% Example:
%% ```
%% %% Find nodes with data under `/foo/bar'.
%% Result = khepri:find(
%%            ra_cluster_name,
%%            [foo, bar],
%%            #if_has_data{has_data = true}),
%%
%% %% Here is the content of `Result'.
%% {ok, #{[foo, bar, baz] => #{data => baz_value,
%%                             payload_version => 2,
%%                             child_list_version => 1,
%%                             child_list_length => 0},
%%        [foo, bar, deep, under, qux] => #{data => qux_value,
%%                                          payload_version => 1,
%%                                          child_list_version => 1,
%%                                          child_list_length => 0}}} = Result.
%% '''
%%
%% @param StoreId the name of the Ra cluster.
%% @param Path the path indicating where to start the search from.
%% @param Condition the condition nodes must match to be part of the result.
%%
%% @returns an "ok" tuple with a map with zero, one or more entries, or an
%% "error" tuple.

find(StoreId, Path, Condition) ->
    Condition1 = #if_all{conditions = [?STAR_STAR, Condition]},
    Path1 = khepri_path:maybe_from_string(Path),
    Path2 = [?ROOT_NODE | Path1] ++ [Condition1],
    khepri_machine:get(StoreId, Path2).

-spec transaction(Fun) -> Ret when
      Fun :: khepri_tx:tx_fun(),
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().

transaction(Fun) ->
    transaction(?DEFAULT_RA_CLUSTER_NAME, Fun).

-spec transaction
(StoreId, Fun) -> Ret when
      StoreId :: store_id(),
      Fun :: khepri_tx:tx_fun(),
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort();
(Fun, ReadWrite) -> Ret when
      Fun :: khepri_tx:tx_fun(),
      ReadWrite :: ro | rw | auto,
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().

transaction(StoreId, Fun) when is_function(Fun) ->
    transaction(StoreId, Fun, auto);
transaction(Fun, ReadWrite) when is_function(Fun) ->
    transaction(?DEFAULT_RA_CLUSTER_NAME, Fun, ReadWrite).

-spec transaction(StoreId, Fun, ReadWrite) -> Ret when
      StoreId :: store_id(),
      Fun :: khepri_tx:tx_fun(),
      ReadWrite :: ro | rw | auto,
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().

transaction(StoreId, Fun, ReadWrite) ->
    khepri_machine:transaction(StoreId, Fun, ReadWrite).

run_sproc(Path, Args) ->
    run_sproc(?DEFAULT_RA_CLUSTER_NAME, Path, Args).

run_sproc(StoreId, Path, Args) ->
    khepri_machine:run_sproc(StoreId, Path, Args).

-spec clear_store() -> ok | error().

clear_store() ->
    clear_store(?DEFAULT_RA_CLUSTER_NAME).

-spec clear_store(store_id()) -> ok | error().

clear_store(StoreId) ->
    delete(StoreId, [?STAR]).

-spec no_payload() -> none.
%% @doc Returns `none'.
%%
%% This is a helper for cases where using records is inconvenient, like in an
%% Erlang shell.

no_payload() ->
    none.

-spec data_payload(Term) -> Payload when
      Term :: khepri_machine:data(),
      Payload :: #kpayload_data{}.
%% @doc Returns `#kpayload_data{data = Term}'.
%%
%% This is a helper for cases where using macros is inconvenient, like in an
%% Erlang shell.

data_payload(Term) ->
    #kpayload_data{data = Term}.

sproc_payload(Fun) when is_function(Fun) ->
    #kpayload_sproc{sproc = Fun};
sproc_payload(#standalone_fun{} = Fun) ->
    #kpayload_sproc{sproc = Fun}.

%% -------------------------------------------------------------------
%% Public helpers.
%% -------------------------------------------------------------------

info() ->
    StoreIds = get_store_ids(),
    case StoreIds of
        [] ->
            io:format("No stores running~n");
        _ ->
            io:format("Running stores:~n"),
            lists:foreach(
              fun(StoreId) ->
                      io:format("  ~ts~n", [StoreId])
              end, StoreIds)
    end.

-spec info(store_id()) -> ok.

info(StoreId) ->
    io:format("~n\033[1;32m== CLUSTER MEMBERS ==\033[0m~n~n", []),
    Nodes = lists:sort(
              [Node || {_, Node} <- khepri_cluster:members(StoreId)]),
    lists:foreach(fun(Node) -> io:format("~ts~n", [Node]) end, Nodes),

    case khepri_machine:get_keep_while_conds_state(StoreId) of
        {ok, KeepWhileConds} when KeepWhileConds =/= #{} ->
            io:format("~n\033[1;32m== LIFETIME DEPS ==\033[0m~n", []),
            WatcherList = lists:sort(maps:keys(KeepWhileConds)),
            lists:foreach(
              fun(Watcher) ->
                      io:format("~n\033[1m~p depends on:\033[0m~n", [Watcher]),
                      WatchedsMap = maps:get(Watcher, KeepWhileConds),
                      Watcheds = lists:sort(maps:keys(WatchedsMap)),
                      lists:foreach(
                        fun(Watched) ->
                                Condition = maps:get(Watched, WatchedsMap),
                                io:format(
                                  "    ~p:~n"
                                  "        ~p~n",
                                  [Watched, Condition])
                        end, Watcheds)
              end, WatcherList);
        _ ->
            ok
    end,

    case khepri_machine:get(StoreId, [#if_path_matches{regex = any}]) of
        {ok, Result} ->
            io:format("~n\033[1;32m== TREE ==\033[0m~n~n●~n", []),
            Tree = khepri_utils:flat_struct_to_tree(Result),
            khepri_utils:display_tree(Tree);
        _ ->
            ok
    end.
