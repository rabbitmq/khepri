%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Khepri service and cluster management API.
%%
%% This module provides the public API for the service and cluster management.
%% For convenience, some functions of this API are repeated in the {@link
%% khepri} module for easier access.
%%
%% == The Khepri store and the Ra cluster ==
%%
%% A Khepri store is a Ra server inside a Ra cluster. The Khepri store and the
%% Ra cluster share the same name in fact. The only constraint is that the name
%% must be an atom, even though Ra accepts other Erlang types as cluster names.
%%
%% By default, Khepri uses `khepri' as the store ID (and thus Ra cluster name).
%% This default can be overridden using an argument to the `start()' functions
%% or the `default_store_id' application environment variable.
%%
%% Examples:
%% <ul>
%% <li>Use the default Khepri store ID:
%% <pre>{ok, khepri} = khepri:start().</pre></li>
%% <li>Override the default store ID using an argument:
%% <pre>{ok, my_store} = khepri:start("/var/lib/khepri", my_store).</pre></li>
%% <li>Override the default store ID using an application environment variable:
%% <pre>ok = application:set_env(
%%        khepri, default_store_id, my_store, [{persistent, true}]),
%%
%% {ok, my_store} = khepri:start().</pre></li>
%% </ul>
%%
%% == The data directory and the Ra system ==
%%
%% A Ra server relies on a Ra system to provide various functions and to
%% configure the directory where the data should be stored on disk.
%%
%% By default, Khepri will configure its own Ra system to write data under
%% `khepri#Nodename' in the current working directory, where `Nodename' is
%% the name of the Erlang node.
%%
%% ```
%% {ok, StoreId} = khepri:start().
%%
%% %% If the Erlang node was started without distribution (the default), the
%% %% statement above will start a Ra system called like the store (`khepri')
%% %% and will use the `khepri#nonode@nohost' directory.
%% '''
%%
%% The default data directory or Ra system name can be overridden using an
%% argument to the `start()' or the `default_ra_system' application environment
%% variable. Both a directory (string or binary) or the name of an already
%% running Ra system are accepted.
%%
%% Examples:
%% <ul>
%% <li>Override the default with the name of a running Ra system using an
%% argument:
%% <pre>{ok, StoreId} = khepri:start(my_ra_system).</pre></li>
%% <li>Override the default data directory using an application environment
%% variable:
%% <pre>ok = application:set_env(
%%        khepri, default_ra_system, "/var/lib/khepri", [{persistent, true}]),
%%
%% {ok, StoreId} = khepri:start().</pre></li>
%% </ul>
%%
%% Please refer to <a href="https://github.com/rabbitmq/ra">Ra
%% documentation</a> to learn more about Ra systems and Ra clusters.
%%
%% == Managing Ra cluster members ==
%%
%% A Khepri/Ra cluster can be expanded by telling a node to join a remote
%% cluster. Note that the Khepri store/Ra server to add to the cluster must run
%% before it can join.
%%
%% ```
%% %% Start the local Khepri store.
%% {ok, StoreId} = khepri:start().
%%
%% %% Join a remote cluster.
%% ok = khepri_cluster:join(RemoteNode).
%% '''
%%
%% To remove the local Khepri store node from the cluster, it must be reset.
%%
%% ```
%% %% Start the local Khepri store.
%% ok = khepri_cluster:reset().
%% '''

-module(khepri_cluster).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").

-export([start/0, start/1, start/2, start/3,
         join/1, join/2,
         reset/0, reset/1, reset/2,
         stop/0, stop/1,
         members/1, members/2,
         locally_known_members/1, locally_known_members/2,
         nodes/1,
         locally_known_nodes/1,
         get_default_ra_system_or_data_dir/0,
         get_default_store_id/0,
         get_store_ids/0,
         is_store_running/1]).

%% Internal.
-export([node_to_member/2,
         this_member/1,
         wait_for_cluster_readiness/2,
         get_cached_leader/1,
         cache_leader/2,
         cache_leader_if_changed/3,
         clear_cached_leader/1]).

-ifdef(TEST).
-export([wait_for_ra_server_exit/1,
         generate_default_data_dir/0]).
-endif.

-dialyzer({no_underspecs, [start/1,
                           stop/0, stop/1,
                           stop_locked/1,
                           join/2,
                           wait_for_remote_cluster_readiness/3]}).

-define(IS_RA_SYSTEM(RaSystem), is_atom(RaSystem)).
-define(IS_DATA_DIR(DataDir), (is_list(DataDir) orelse is_binary(DataDir))).

-type incomplete_ra_server_config() :: map().
%% A Ra server config map.
%%
%% This configuration map can lack the required parameters, Khepri will fill
%% them if necessary. Important parameters for Khepri (e.g. `machine') will be
%% overridden anyway.
%%
%% @see ra_server:ra_server_config().

-type ra_server_config_with_cluster_name() :: #{cluster_name :=
                                                khepri:store_id()}.
%% Intermediate Ra server configuration with `cluster_name' set.

-type ra_server_config_with_id_and_cn() :: #{id := ra:server_id(),
                                             cluster_name :=
                                             khepri:store_id()}.
%% Intermediate Ra server configuration with `id' and `cluster_name' set.

-export_type([incomplete_ra_server_config/0]).

%% -------------------------------------------------------------------
%% Database management.
%% -------------------------------------------------------------------

-spec start() -> Ret when
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% Calling this function is the same as calling `start(DefaultRaSystem)' where
%% `DefaultRaSystem' is returned by {@link
%% get_default_ra_system_or_data_dir/0}.
%%
%% @see start/1.

start() ->
    case application:ensure_all_started(khepri) of
        {ok, _} ->
            RaSystemOrDataDir = get_default_ra_system_or_data_dir(),
            start(RaSystemOrDataDir);
        Error ->
            Error
    end.

-spec start(RaSystem | DataDir) -> Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% Calling this function is the same as calling `start(RaSystemOrDataDir,
%% DefaultStoreId)' where `DefaultStoreId' is returned by {@link
%% get_default_store_id/0}.
%%
%% @see start/2.

start(RaSystemOrDataDir) ->
    case application:ensure_all_started(khepri) of
        {ok, _} ->
            StoreId = get_default_store_id(),
            start(RaSystemOrDataDir, StoreId);
        Error ->
            Error
    end.

-spec start(RaSystem | DataDir, StoreId | RaServerConfig) -> Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      StoreId :: khepri:store_id(),
      RaServerConfig :: incomplete_ra_server_config(),
      Ret :: khepri:ok(StoreId) | khepri:error().
%% @doc Starts a store.
%%
%% Calling this function is the same as calling `start(RaSystemOrDataDir,
%% StoreIdOrRaServerConfig, DefaultTimeout)' where `DefaultTimeout' is
%% returned by {@link khepri_app:get_default_timeout/0}.
%%
%% @param RaSystem the name of the Ra system.
%% @param DataDir a directory to write data.
%% @param StoreId the name of the Khepri store.
%% @param RaServerConfig the skeleton of each Ra server's configuration.
%%
%% @see start/3.

start(RaSystemOrDataDir, StoreIdOrRaServerConfig) ->
    Timeout = khepri_app:get_default_timeout(),
    start(RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout).

-spec start(RaSystem | DataDir, StoreId | RaServerConfig, Timeout) ->
    Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      StoreId :: khepri:store_id(),
      RaServerConfig :: incomplete_ra_server_config(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error().
%% @doc Starts a store.
%%
%% It accepts either a Ra system name (atom) or a data directory (string or
%% binary) as its first argument. If a Ra system name is given, that Ra system
%% must be running when this function is called. If a data directory is given,
%% a new Ra system will be started, using this directory. The directory will
%% be created automatically if it doesn't exist. The Ra system will use the
%% same name as the Khepri store.
%%
%% It accepts a Khepri store ID or a Ra server configuration as its second
%% argument. If a store ID is given, a Ra server configuration will be created
%% based on it. If a Ra server configuration is given, the name of the Khepri
%% store will be derived from it.
%%
%% If this is a new store, the Ra server is started and an election is
%% triggered so that it becomes its own leader and is ready to process
%% commands and queries.
%%
%% If the store was started in the past and stopped, it will be restarted. In
%% this case, `RaServerConfig' will be ignored. Ra will take care of the
%% eletion automatically.
%%
%% @param RaSystem the name of the Ra system.
%% @param DataDir a directory to write data.
%% @param StoreId the name of the Khepri store.
%% @param RaServerConfig the skeleton of each Ra server's configuration.
%% @param TImeout a timeout.
%%
%% @returns the ID of the started store in an "ok" tuple, or an error tuple if
%% the store couldn't be started.

start(RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout) ->
    case application:ensure_all_started(khepri) of
        {ok, _} ->
            ensure_ra_server_config_and_start(
              RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout);
        Error ->
            Error
    end.

-spec ensure_ra_server_config_and_start(
        RaSystem | DataDir, StoreId | RaServerConfig, Timeout) ->
    Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      StoreId :: khepri:store_id(),
      RaServerConfig :: incomplete_ra_server_config(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error().
%% @private

ensure_ra_server_config_and_start(
  RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout)
  when (?IS_DATA_DIR(RaSystemOrDataDir) orelse
        ?IS_RA_SYSTEM(RaSystemOrDataDir)) andalso
       (?IS_STORE_ID(StoreIdOrRaServerConfig) orelse
        is_map(StoreIdOrRaServerConfig)) ->
    %% If the store ID derived from `StoreIdOrRaServerConfig' is not an atom,
    %% it will cause a cause clause exception below.
    RaServerConfig =
    case StoreIdOrRaServerConfig of
        _ when ?IS_STORE_ID(StoreIdOrRaServerConfig) ->
            #{cluster_name => StoreIdOrRaServerConfig};
        #{cluster_name := CN} when ?IS_STORE_ID(CN) ->
            StoreIdOrRaServerConfig;
        #{} when not is_map_key(cluster_name, StoreIdOrRaServerConfig) ->
            StoreIdOrRaServerConfig#{
              cluster_name => get_default_store_id()
             }
    end,
    verify_ra_system_and_start(RaSystemOrDataDir, RaServerConfig, Timeout).

-spec verify_ra_system_and_start(
        RaSystem | DataDir, RaServerConfig, Timeout) ->
    Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      RaServerConfig :: ra_server_config_with_cluster_name(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @private

verify_ra_system_and_start(RaSystem, RaServerConfig, Timeout)
  when ?IS_RA_SYSTEM(RaSystem) ->
    ensure_server_started(RaSystem, RaServerConfig, Timeout);
verify_ra_system_and_start(DataDir, RaServerConfig, Timeout)
  when is_list(DataDir) ->
    RaSystem = ?DEFAULT_RA_SYSTEM_NAME,
    DefaultConfig = ra_system:default_config(),
    RaSystemConfig = DefaultConfig#{name => RaSystem,
                                    data_dir => DataDir,
                                    wal_data_dir => DataDir,
                                    names => ra_system:derive_names(RaSystem)},
    ?LOG_DEBUG(
       "Starting Ra system \"~s\" using data dir \"~ts\"",
       [RaSystem, DataDir]),
    case ra_system:start(RaSystemConfig) of
        {ok, _} ->
            ensure_server_started(RaSystem, RaServerConfig, Timeout);
        {error, {already_started, _}} ->
            ensure_server_started(RaSystem, RaServerConfig, Timeout);
        Error ->
            Error
    end;
verify_ra_system_and_start(DataDir, RaServerConfig, Timeout)
  when is_binary(DataDir) ->
    DataDir1 = unicode:characters_to_list(DataDir),
    verify_ra_system_and_start(DataDir1, RaServerConfig, Timeout).

-spec ensure_server_started(RaSystem, RaServerConfig, Timeout) -> Ret when
      RaSystem :: atom(),
      RaServerConfig :: ra_server_config_with_cluster_name(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @private

ensure_server_started(
  RaSystem, #{cluster_name := StoreId} = RaServerConfig, Timeout) ->
    Lock = server_start_lock(StoreId),
    global:set_lock(Lock),
    try
        Ret = ensure_server_started_locked(RaSystem, RaServerConfig, Timeout),
        global:del_lock(Lock),
        Ret
    catch
        Class:Reason:Stacktrace ->
            global:del_lock(Lock),
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec ensure_server_started_locked(RaSystem, RaServerConfig, Timeout) ->
    Ret when
      RaSystem :: atom(),
      RaServerConfig :: ra_server_config_with_cluster_name(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @private

ensure_server_started_locked(
  RaSystem, #{cluster_name := StoreId} = RaServerConfig, Timeout) ->
    ThisMember = this_member(StoreId),
    RaServerConfig1 = RaServerConfig#{id => ThisMember},
    ?LOG_DEBUG(
       "Trying to restart local Ra server for store \"~s\" "
       "in Ra system \"~s\"",
       [StoreId, RaSystem]),
    case ra:restart_server(RaSystem, ThisMember) of
        {error, name_not_registered} ->
            ?LOG_DEBUG(
               "Ra server for store \"~s\" not registered in Ra system "
               "\"~s\", try to start a new one",
               [StoreId, RaSystem]),
            case do_start_server(RaSystem, RaServerConfig1) of
                ok ->
                    ok = trigger_election(RaServerConfig1, Timeout),
                    {ok, StoreId};
                Error ->
                    Error
            end;
        ok ->
            ok = remember_store(RaSystem, RaServerConfig1),
            {ok, StoreId};
        {error, {already_started, _}} ->
            {ok, StoreId};
        Error ->
            Error
    end.

-spec do_start_server(RaSystem, RaServerConfig) -> Ret when
      RaSystem :: atom(),
      RaServerConfig :: ra_server_config_with_id_and_cn(),
      Ret :: ok | khepri:error().
%% @private

do_start_server(RaSystem, RaServerConfig) ->
    RaServerConfig1 = complete_ra_server_config(RaServerConfig),
    #{cluster_name := StoreId} = RaServerConfig1,
    ?LOG_DEBUG(
       "Starting a Ra server with the following configuration:~n~p",
       [RaServerConfig1]),
    case ra:start_server(RaSystem, RaServerConfig1) of
        ok ->
            ok = remember_store(RaSystem, RaServerConfig1),
            ?LOG_DEBUG(
               "Started Ra server for store \"~s\"",
               [StoreId]),
            ok;
        {error, _} = Error ->
            ?LOG_ERROR(
               "Failed to start Ra server for store \"~s\" using the "
               "following Ra server configuration:~n~p",
               [StoreId, RaServerConfig1]),
            Error
    end.

-spec trigger_election(Member | RaServerConfig, Timeout) -> ok when
      Member :: ra:server_id(),
      RaServerConfig :: ra_server_config_with_id_and_cn(),
      Timeout :: timeout().
%% @private

trigger_election(#{id := Member}, Timeout) ->
    trigger_election(Member, Timeout);
trigger_election({StoreId, _Node} = Member, Timeout) ->
    ?LOG_DEBUG("Trigger election in store \"~s\"", [StoreId]),
    ok = ra:trigger_election(Member, Timeout),
    ok.

-spec stop() -> Ret when
      Ret :: ok | khepri:error().
%% @doc Stops a store.
%%
%% Calling this function is the same as calling `stop(DefaultStoreId)'
%% where `DefaultStoreId' is returned by {@link
%% get_default_store_id/0}.
%%
%% @see stop/1.

stop() ->
    StoreId = get_default_store_id(),
    stop(StoreId).

-spec stop(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: ok | khepri:error().
%% @doc Stops a store.
%%
%% @param StoreId the ID of the store to stop.
%%
%% @returns `ok' if it succeeds, an error tuple otherwise.

stop(StoreId) when ?IS_STORE_ID(StoreId) ->
    Lock = server_start_lock(StoreId),
    global:set_lock(Lock),
    try
        Ret = stop_locked(StoreId),
        global:del_lock(Lock),
        Ret
    catch
        Class:Reason:Stacktrace ->
            global:del_lock(Lock),
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec stop_locked(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: ok | khepri:error().

stop_locked(StoreId) ->
    ThisMember = this_member(StoreId),
    case get_store_prop(StoreId, ra_system) of
        {ok, RaSystem} ->
            ?LOG_DEBUG(
               "Stopping member ~0p in store \"~s\"",
               [ThisMember, StoreId]),
            case ra:stop_server(RaSystem, ThisMember) of
                ok ->
                    forget_store(StoreId),
                    wait_for_ra_server_exit(ThisMember);
                %% TODO: Handle idempotency: if the Ra server is not running,
                %% don't fail.
                {error, _} = Error ->
                    %% We don't call `forget_store()' in case the caller wants
                    %% to try again.
                    Error
            end;
        {error, _} ->
            %% The store is unknown, it must have been stopped already.
            ok
    end.

-spec wait_for_ra_server_exit(Member) -> ok when
      Member :: ra:server_id().
%% @private

wait_for_ra_server_exit({StoreId, _} = Member) ->
    %% FIXME: This monitoring can go away when/if the following pull request
    %% in Ra is merged:
    %% https://github.com/rabbitmq/ra/pull/270
    ?LOG_DEBUG(
       "Wait for Ra server ~0p to exit in store \"~s\"",
       [Member, StoreId]),
    MRef = erlang:monitor(process, Member),
    receive
        {'DOWN', MRef, _, _, noproc} ->
            ?LOG_DEBUG(
               "Ra server ~0p in store \"~s\" already exited",
               [Member, StoreId]),
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            ?LOG_DEBUG(
               "Ra server ~0p in store \"~s\" exited: ~p",
               [Member, StoreId, Reason]),
            ok
    end.

-spec join(RemoteMember | RemoteNode) -> Ret when
      RemoteMember :: ra:server_id(),
      RemoteNode :: node(),
      Ret :: ok | khepri:error().
%% @doc Adds the local running Khepri store to a remote cluster.
%%
%% This function accepts the following forms:
%% <ul>
%% <li>`join(RemoteNode)'. Calling it is the same as calling
%% `join(DefaultStoreId, RemoteNode)' where `DefaultStoreId' is
%% returned by {@link get_default_store_id/0}.</li>
%% <li>`join(RemoteMember)'. Calling it is the same as calling
%% `join(StoreId, RemoteNode)' where `StoreId' and `RemoteNode' are
%% derived from `RemoteMember'.</li>
%% </ul>
%%
%% @see join/2.

join(RemoteNode) when is_atom(RemoteNode) ->
    StoreId = get_default_store_id(),
    join(StoreId, RemoteNode);
join({StoreId, RemoteNode} = _RemoteMember) ->
    join(StoreId, RemoteNode).

-spec join(
        RemoteMember | RemoteNode | StoreId, Timeout | RemoteNode) ->
    Ret when
      RemoteMember :: ra:server_id(),
      RemoteNode :: node(),
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Adds the local running Khepri store to a remote cluster.
%%
%% This function accepts the following forms:
%% <ul>
%% <li>`join(RemoteNode, Timeout)'. Calling it is the same as calling
%% `join(DefaultStoreId, RemoteNode, Timeout)' where `DefaultStoreId'
%% is returned by {@link get_default_store_id/0}.</li>
%% <li>`join(StoreId, RemoteNode)'. Calling it is the same as calling
%% `join(StoreId, RemoteNode, DefaultTimeout)' where `DefaultTimeout' is
%% returned by {@link khepri_app:get_default_timeout/0}.</li>
%% <li>`join(RemoteMember, Timeout)'. Calling it is the same as calling
%% `join(StoreId, RemoteNode, Timeout)' where `StoreId' and
%% `RemoteNode' are derived from `RemoteMember'.</li>
%% </ul>
%%
%% @see join/3.

join(RemoteNode, Timeout)
  when is_atom(RemoteNode) andalso ?IS_TIMEOUT(Timeout) ->
    StoreId = get_default_store_id(),
    join(StoreId, RemoteNode, Timeout);
join(StoreId, RemoteNode)
  when ?IS_STORE_ID(StoreId) andalso is_atom(RemoteNode) ->
    Timeout = khepri_app:get_default_timeout(),
    join(StoreId, RemoteNode, Timeout);
join({StoreId, RemoteNode} = _RemoteMember, Timeout) ->
    join(StoreId, RemoteNode, Timeout).

-spec join(StoreId, RemoteMember | RemoteNode, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      RemoteMember :: ra:server_id(),
      RemoteNode :: node(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Adds the local running Khepri store to a remote cluster.
%%
%% The local Khepri store must have been started with {@link start/3} before
%% it can be added to a cluster. It is also expected that the remote store ID
%% is the same as the local one.
%%
%% `RemoteNode' is the entry point to the remote cluster. It must run for this
%% function to work. A cluster membership also requires a quorum, therefore
%% this join depends on it to succeed.
%%
%% If `RemoteMember' is specified, the remote node is derived from it. At the
%% same time, the function asserts that the specified `StoreId' matches
%% the one derived from `RemoteMember'.
%%
%% As part of this function, the local Khepri store will reset. It means it
%% will leave the cluster it is already part of (if any) and all its data
%% removed.
%%
%% @param StoreId the ID of the local Khepri store.
%% @param RemoteNode the name of remote Erlang node running Khepri to join.
%% @param Timeout the timeout.
%%
%% @returns `ok' if it succeeds, an error tuple otherwise.

join(StoreId, RemoteNode, Timeout) when is_atom(RemoteNode) ->
    %% We first ping the remote node. It serves two purposes:
    %% 1. Make sure we can reach it
    %% 2. Make sure they are connected before acquiring a lock, so that the
    %%    global lock is really global.
    case net_adm:ping(RemoteNode) of
        pong ->
            Lock = server_start_lock(StoreId),
            global:set_lock(Lock),
            try
                Ret = check_status_and_join_locked(
                        StoreId, RemoteNode, Timeout),
                global:del_lock(Lock),
                Ret
            catch
                Class:Reason:Stacktrace ->
                    global:del_lock(Lock),
                    erlang:raise(Class, Reason, Stacktrace)
            end;
        pang ->
            Reason = ?khepri_error(
                        failed_to_join_remote_khepri_node,
                        #{store_id => StoreId,
                          node => RemoteNode}),
            {error, Reason}
    end;
join(StoreId, {StoreId, RemoteNode} = _RemoteMember, Timeout) ->
    join(StoreId, RemoteNode, Timeout).

-spec check_status_and_join_locked(StoreId, RemoteNode, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      RemoteNode :: node(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @private

check_status_and_join_locked(StoreId, RemoteNode, Timeout) ->
    ThisMember = this_member(StoreId),
    RaServerRunning = erlang:is_pid(erlang:whereis(StoreId)),
    Prop1 = get_store_prop(StoreId, ra_system),
    Prop2 = get_store_prop(StoreId, ra_server_config),
    case {RaServerRunning, Prop1, Prop2} of
        {true, {ok, RaSystem}, {ok, RaServerConfig}} ->
            reset_and_join_locked(
              StoreId, ThisMember, RaSystem, RaServerConfig,
              RemoteNode, Timeout);
        {false, {error, _} = Error, _} ->
            Error;
        {false, _, {error, _} = Error} ->
            Error;
        {false, _, _} ->
            ?LOG_ERROR(
               "Local Ra server ~0p not running for store \"~s\", "
               "but properties are still available: ~0p and ~0p",
               [ThisMember, StoreId, Prop1, Prop2]),
            erlang:error(
              ?khepri_exception(
                 ra_server_not_running_but_props_available,
                 #{store_id => StoreId,
                   this_member => ThisMember,
                   ra_system => Prop1,
                   ra_server_config => Prop2}))
    end.

-spec reset_and_join_locked(
  StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      ThisMember :: ra:server_id(),
      RaSystem :: atom(),
      RaServerConfig :: ra_server:config(),
      RemoteNode :: node(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @private

reset_and_join_locked(
  StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode, Timeout) ->
    %% The local node is reset in case it is already a standalone elected
    %% leader (which would be the case after a successful call to
    %% `khepri_cluster:start()') or part of a cluster, and have any data.
    %%
    %% Just after the reset, we restart it skipping the `trigger_election()'
    %% step: this is required so that it does not become a leader before
    %% joining the remote node. Otherwise, we hit an assertion in Ra.
    %%
    %% TODO: Should we verify the cluster membership first? To avoid resetting
    %% a node which is already part of the cluster? On the other hand, such a
    %% check would not be atomic and the membership could change between the
    %% check and the reset...
    %%
    %% TODO: Do we want to provide an option to verify the state of the local
    %% node before resetting it? Like "if it has data in the Khepri database,
    %% abort". It may be difficult to make this kind of check atomic though.
    T0 = khepri_utils:start_timeout_window(Timeout),
    case do_reset(RaSystem, StoreId, ThisMember, Timeout) of
        ok ->
            NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
            case do_start_server(RaSystem, RaServerConfig) of
                ok ->
                    do_join_locked(
                      StoreId, ThisMember, RemoteNode, NewTimeout);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec do_join_locked(
  StoreId, ThisMember, RemoteNode, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      ThisMember :: ra:server_id(),
      RemoteNode :: node(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @private

do_join_locked(StoreId, ThisMember, RemoteNode, Timeout) ->
    RemoteMember = node_to_member(StoreId, RemoteNode),
    ?LOG_DEBUG(
       "Adding this node (~0p) to the remote node's cluster (~0p)",
       [ThisMember, RemoteMember]),
    T1 = khepri_utils:start_timeout_window(Timeout),
    Ret1 = rpc:call(
             RemoteNode,
             ra, add_member, [StoreId, ThisMember, Timeout],
             Timeout),
    Timeout1 = khepri_utils:end_timeout_window(Timeout, T1),
    case Ret1 of
        {ok, _, _StoreId} ->
            ?LOG_DEBUG(
               "Cluster for store \"~s\" successfully expanded",
               [StoreId]),
            ok;
        {error, cluster_change_not_permitted} ->
            T2 = khepri_utils:start_timeout_window(Timeout1),
            ?LOG_DEBUG(
               "Remote cluster (reached through node node ~0p) is not ready "
               "for a membership change yet; waiting", [RemoteNode]),
            Ret2 = wait_for_remote_cluster_readiness(
                     StoreId, RemoteNode, Timeout1),
            Timeout2 = khepri_utils:end_timeout_window(Timeout1, T2),
            case Ret2 of
                ok ->
                    do_join_locked(
                      StoreId, ThisMember, RemoteNode, Timeout2);
                Error ->
                    Error
            end;
        Error ->
            ?LOG_ERROR(
               "Failed to expand cluster for store \"~s\": ~p; aborting",
               [StoreId, Error]),
            %% After failing to join, the local Ra server is running
            %% standalone (after a reset) and needs an election to be in a
            %% working state again. We don't care about the result at this
            %% point.
            _ = trigger_election(ThisMember, Timeout1),
            case Error of
                {badrpc, _} -> {error, Error};
                _           -> Error
            end
    end.

-spec wait_for_cluster_readiness(StoreId, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error(?khepri_error(
                                  timeout_waiting_for_cluster_readiness,
                                  #{store_id := StoreId})).
%% @private

wait_for_cluster_readiness(StoreId, Timeout) ->
    %% If querying the cluster members succeeds, we must have a quorum, right?
    case members(StoreId, Timeout) of
        [_ | _] ->
            ok;
        [] ->
            Reason = ?khepri_error(
                        timeout_waiting_for_cluster_readiness,
                        #{store_id => StoreId}),
            {error, Reason}
    end.

-spec wait_for_remote_cluster_readiness(StoreId, RemoteNode, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      RemoteNode :: node(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @private

wait_for_remote_cluster_readiness(StoreId, RemoteNode, Timeout) ->
    Ret = rpc:call(
            RemoteNode,
            khepri_cluster, wait_for_cluster_readiness, [StoreId, Timeout],
            Timeout),
    case Ret of
        {badrpc, _} -> {error, Ret};
        _           -> Ret
    end.

-spec reset() -> Ret when
      Ret :: ok | khepri:error().
%% @doc Resets the store on this Erlang node.

reset() ->
    StoreId = get_default_store_id(),
    reset(StoreId).

-spec reset(StoreId | Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Resets the store on this Erlang node.

reset(Timeout)
  when ?IS_TIMEOUT(Timeout) ->
    StoreId = get_default_store_id(),
    reset(StoreId, Timeout);
reset(StoreId)
  when ?IS_STORE_ID(StoreId) ->
    Timeout = khepri_app:get_default_timeout(),
    reset(StoreId, Timeout).

-spec reset(StoreId, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Resets the store on this Erlang node.
%%
%% It does that by force-deleting the Ra local server.
%%
%% This function is also used to gracefully remove the local Khepri store node
%% from a cluster.
%%
%% @param StoreId the name of the Khepri store.

reset(StoreId, Timeout) ->
    Lock = server_start_lock(StoreId),
    global:set_lock(Lock),
    try
        Ret = reset_locked(StoreId, Timeout),
        global:del_lock(Lock),
        Ret
    catch
        Class:Reason:Stacktrace ->
            global:del_lock(Lock),
            erlang:raise(Class, Reason, Stacktrace)
    end.

reset_locked(StoreId, Timeout) ->
    ThisMember = this_member(StoreId),
    case get_store_prop(StoreId, ra_system) of
        {ok, RaSystem}     -> do_reset(RaSystem, StoreId, ThisMember, Timeout);
        {error, _} = Error -> Error
    end.

do_reset(RaSystem, StoreId, ThisMember, Timeout) ->
    ?LOG_DEBUG(
       "Detaching this node (~0p) in store \"~s\" from cluster (if any) "
       "before reset",
       [ThisMember, StoreId]),
    T1 = khepri_utils:start_timeout_window(Timeout),
    Ret1 = ra:remove_member(ThisMember, ThisMember, Timeout),
    Timeout1 = khepri_utils:end_timeout_window(Timeout, T1),
    case Ret1 of
        {ok, _, _} ->
            force_stop(RaSystem, StoreId, ThisMember);
        {error, not_member} ->
            force_stop(RaSystem, StoreId, ThisMember);
        {error, cluster_change_not_permitted} ->
            T2 = khepri_utils:start_timeout_window(Timeout1),
            ?LOG_DEBUG(
               "Cluster is not ready for a membership change yet; waiting",
               []),
            Ret2 = wait_for_cluster_readiness(StoreId, Timeout1),
            Timeout2 = khepri_utils:end_timeout_window(Timeout1, T2),
            case Ret2 of
                ok    -> do_reset(RaSystem, StoreId, ThisMember, Timeout2);
                Error -> Error
            end;
        {timeout, _} = TimedOut ->
            {error, TimedOut};
        {error, _} = Error ->
            Error
    end.

force_stop(RaSystem, StoreId, ThisMember) ->
    ?LOG_DEBUG(
       "Resetting member ~0p in store \"~s\"",
       [ThisMember, StoreId]),
    case ra:force_delete_server(RaSystem, ThisMember) of
        ok ->
            forget_store(StoreId),
            wait_for_ra_server_exit(ThisMember);
        {error, noproc} = Error ->
            forget_store(StoreId),
            Error;
        {error, _} = Error ->
            Error
    end.

-spec get_default_ra_system_or_data_dir() -> RaSystem | DataDir when
      RaSystem :: atom(),
      DataDir :: file:filename_all().
%% @doc Returns the default Ra system name or data directory.
%%
%% This is based on Khepri's `default_ra_system` application environment
%% variable. The variable can be set to:
%% <ul>
%% <li>A directory (a string or binary) where data should be stored. A new Ra
%% system called `khepri` will be initialized with this directory.</li>
%% <li>A Ra system name (an atom). In this case, the user is expected to
%% configure and start the Ra system before starting Khepri.</li>
%% </ul>
%%
%% If this application environment variable is unset, the default is to
%% configure a Ra system called `khepri' which will write data in
%% `"khepri-$NODE"' in the current working directory where `$NODE' is the
%% Erlang node name.
%%
%% Example of an Erlang configuration file for Khepri:
%% ```
%% {khepri, [{default_ra_system, "/var/db/khepri"}]}.
%% '''
%%
%% @returns the value of the `default_ra_system' application environment
%% variable.

get_default_ra_system_or_data_dir() ->
    RaSystemOrDataDir = application:get_env(
                          khepri, default_ra_system,
                          generate_default_data_dir()),
    if
        ?IS_DATA_DIR(RaSystemOrDataDir) ->
            ok;
        ?IS_RA_SYSTEM(RaSystemOrDataDir) ->
            ok;
        true ->
            ?LOG_ERROR(
               "Invalid Ra system or data directory set in "
               "`default_ra_system` application environment: ~p",
               [RaSystemOrDataDir]),
            ?khepri_misuse(
               invalid_default_ra_system_value,
               #{default_ra_system => RaSystemOrDataDir})
    end,
    RaSystemOrDataDir.

generate_default_data_dir() ->
    lists:flatten(io_lib:format("khepri#~s", [node()])).

-spec get_default_store_id() -> StoreId when
      StoreId :: khepri:store_id().
%% @doc Returns the default Khepri store ID.
%%
%% This is based on Khepri's `default_store_id' application environment
%% variable. The variable can be set to an atom. The default is `khepri'.
%%
%% @returns the value of the `default_store_id' application environment
%% variable.

get_default_store_id() ->
    StoreId = application:get_env(
                    khepri, default_store_id,
                    ?DEFAULT_STORE_ID),
    if
        ?IS_STORE_ID(StoreId) ->
            ok;
        true ->
            ?LOG_ERROR(
               "Invalid store ID set in `default_store_id` "
               "application environment: ~p",
               [StoreId]),
            ?khepri_misuse(
               invalid_default_store_id_value,
               #{default_store_id => StoreId})
    end,
    StoreId.

members(StoreId) ->
    Timeout = khepri_app:get_default_timeout(),
    members(StoreId, Timeout).

members(StoreId, Timeout) ->
    ThisMember = this_member(StoreId),
    do_query_members(StoreId, ThisMember, leader, Timeout).

locally_known_members(StoreId) ->
    Timeout = khepri_app:get_default_timeout(),
    locally_known_members(StoreId, Timeout).

locally_known_members(StoreId, Timeout) ->
    ThisMember = this_member(StoreId),
    do_query_members(StoreId, ThisMember, local, Timeout).

do_query_members(StoreId, RaServer, QueryType, Timeout) ->
    ?LOG_DEBUG("Query members in store \"~s\"", [StoreId]),
    T0 = khepri_utils:start_timeout_window(Timeout),
    Arg = case QueryType of
              leader -> RaServer;
              local  -> {local, RaServer}
          end,
    case ra:members(Arg, Timeout) of
        {ok, Members, _} ->
            ?LOG_DEBUG(
               "Found the following members in store \"~s\": ~p",
               [StoreId, Members]),
            Members;
        {error, noproc} = Error ->
            case khepri_utils:is_ra_server_alive(RaServer) of
                true ->
                    NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
                    NewTimeout = khepri_utils:sleep(
                                   ?NOPROC_RETRY_INTERVAL, NewTimeout0),
                    do_query_members(
                      StoreId, RaServer, QueryType, NewTimeout);
                false ->
                    ?LOG_WARNING(
                       "Failed to query members in store \"~s\": ~p",
                       [StoreId, Error]),
                    []
            end;
        Error ->
            ?LOG_WARNING(
               "Failed to query members in store \"~s\": ~p",
               [StoreId, Error]),
            []
    end.

nodes(StoreId) ->
    [Node || {_, Node} <- members(StoreId)].

locally_known_nodes(StoreId) ->
    [Node || {_, Node} <- locally_known_members(StoreId)].

-spec node_to_member(StoreId, Node) -> Member when
      StoreId :: khepri:store_id(),
      Node :: node(),
      Member :: ra:server_id().
%% @private

node_to_member(StoreId, Node) ->
    {StoreId, Node}.

-spec this_member(StoreId) -> Member when
      StoreId :: khepri:store_id(),
      Member :: ra:server_id().
%% @private

this_member(StoreId) ->
    ThisNode = node(),
    node_to_member(StoreId, ThisNode).

server_start_lock(StoreId) ->
    {{khepri, StoreId}, self()}.

complete_ra_server_config(#{cluster_name := StoreId,
                            id := Member} = RaServerConfig) ->
    %% We warn the caller if he sets `initial_members' that the setting will
    %% be ignored. The reason is we mandate the use of
    %% `khepri_cluster:start()' to start the local node (and can't use on the
    %% "auto-cluster start" of Ra) because we also populate a few
    %% persistent_term for Ra system & server config bookkeeping.
    RaServerConfig1 = case RaServerConfig of
                          #{initial_members := _} ->
                              ?LOG_WARNING(
                                 "Initial Ra cluster members in the "
                                 "following Ra server config "
                                 "(`initial_members`) will be ignored: ~p",
                                 [RaServerConfig]),
                              maps:remove(initial_members, RaServerConfig);
                          _ ->
                              RaServerConfig
                      end,

    %% We set a default friendly name for this Ra server if the caller didn't
    %% set any.
    RaServerConfig2 = case RaServerConfig1 of
                          #{friendly_name := _} ->
                              RaServerConfig1;
                          _ ->
                              FriendlyName = lists:flatten(
                                               io_lib:format(
                                                 "Khepri store \"~s\"",
                                                 [StoreId])),
                              RaServerConfig1#{friendly_name => FriendlyName}
                      end,

    UId = ra:new_uid(ra_lib:to_binary(StoreId)),
    MachineConfig0 = case RaServerConfig of
                         #{machine_config := MachineConfig00} ->
                             MachineConfig00;
                         _ ->
                             #{}
                     end,
    MachineConfig = MachineConfig0#{store_id => StoreId,
                                    member => Member},
    Machine = {module, khepri_machine, MachineConfig},
    RaServerConfig2#{uid => UId,
                     log_init_args => #{uid => UId},
                     machine => Machine}.

-define(PT_STORE_IDS, {khepri, store_ids}).

-spec remember_store(RaSystem, RaServerConfig) -> ok when
      RaSystem :: atom(),
      RaServerConfig :: ra_server:ra_server_config().
%% @private

remember_store(RaSystem, #{cluster_name := StoreId} = RaServerConfig) ->
    ?assert(maps:is_key(id, RaServerConfig)),
    StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
    Props = #{ra_system => RaSystem,
              ra_server_config => RaServerConfig},
    StoreIds1 = StoreIds#{StoreId => Props},
    persistent_term:put(?PT_STORE_IDS, StoreIds1),
    ok.

-spec get_store_prop(StoreId, PropName) -> Ret when
      StoreId :: khepri:store_id(),
      PropName :: ra_system | ra_server_config,
      PropValue :: any(),
      Ret :: khepri:ok(PropValue) |
             khepri:error(?khepri_error(
                             not_a_khepri_store,
                             #{store_id := StoreId})).
%% @private

get_store_prop(StoreId, PropName) ->
    case persistent_term:get(?PT_STORE_IDS, #{}) of
        #{StoreId := #{PropName := PropValue}} ->
            {ok, PropValue};
        _ ->
            Reason = ?khepri_error(
                        not_a_khepri_store,
                        #{store_id => StoreId}),
            {error, Reason}
    end.

-spec forget_store(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @private

forget_store(StoreId) ->
    ok = khepri_machine:clear_cache(StoreId),
    ok = clear_cached_leader(StoreId),
    StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
    StoreIds1 = maps:remove(StoreId, StoreIds),
    case maps:size(StoreIds1) of
        0 -> _ = persistent_term:erase(?PT_STORE_IDS);
        _ -> ok = persistent_term:put(?PT_STORE_IDS, StoreIds1)
    end,
    ok.

-spec get_store_ids() -> [StoreId] when
      StoreId :: khepri:store_id().
%% @doc Returns the list of running stores.

get_store_ids() ->
    maps:keys(persistent_term:get(?PT_STORE_IDS, #{})).

-spec is_store_running(StoreId) -> IsRunning when
      StoreId :: khepri:store_id(),
      IsRunning :: boolean().
%% @doc Indicates if `StoreId' is running or not.

is_store_running(StoreId) ->
    ThisNode = node(),
    RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
    Runs = case ra:ping(RaServer, khepri_app:get_default_timeout()) of
               {pong, _}  -> true;
               {error, _} -> false;
               timeout    -> false
           end,
    StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
    Known = maps:is_key(StoreId, StoreIds),
    ?assertEqual(Known, Runs),
    Runs.

%% Cache the Ra leader ID to avoid command/query redirections from a follower
%% to the leader. The leader ID is returned after each command or query. If we
%% don't know it yet, wait for a leader election using khepri_event_handler.

-define(RA_LEADER_CACHE_KEY(StoreId), {khepri, ra_leader_cache, StoreId}).

-spec get_cached_leader(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: LeaderId | undefined,
      LeaderId :: ra:server_id().

get_cached_leader(StoreId) ->
    Key = ?RA_LEADER_CACHE_KEY(StoreId),
    persistent_term:get(Key, undefined).

-spec cache_leader(StoreId, LeaderId) -> ok when
      StoreId :: khepri:store_id(),
      LeaderId :: ra:server_id().

cache_leader(StoreId, LeaderId) ->
    ok = persistent_term:put(?RA_LEADER_CACHE_KEY(StoreId), LeaderId).

-spec cache_leader_if_changed(StoreId, LeaderId, NewLeaderId) -> ok when
      StoreId :: khepri:store_id(),
      LeaderId :: ra:server_id(),
      NewLeaderId :: ra:server_id().

cache_leader_if_changed(_StoreId, LeaderId, LeaderId) ->
    ok;
cache_leader_if_changed(StoreId, undefined, NewLeaderId) ->
    case persistent_term:get(?RA_LEADER_CACHE_KEY(StoreId), undefined) of
        LeaderId when LeaderId =/= undefined ->
            cache_leader_if_changed(StoreId, LeaderId, NewLeaderId);
        undefined ->
            cache_leader(StoreId, NewLeaderId)
    end;
cache_leader_if_changed(StoreId, _OldLeaderId, NewLeaderId) ->
    cache_leader(StoreId, NewLeaderId).

-spec clear_cached_leader(StoreId) -> ok when
      StoreId :: khepri:store_id().

clear_cached_leader(StoreId) ->
    _ = persistent_term:erase(?RA_LEADER_CACHE_KEY(StoreId)),
    ok.
