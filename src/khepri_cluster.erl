%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
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
         members/0, members/1, members/2,
         nodes/0, nodes/1, nodes/2,
         wait_for_leader/0, wait_for_leader/1, wait_for_leader/2,
         get_default_ra_system_or_data_dir/0,
         get_default_store_id/0,
         get_store_ids/0,
         is_store_running/1]).

%% Internal.
-export([node_to_member/2,
         this_member/1]).

-ifdef(TEST).
-export([wait_for_ra_server_exit/1,
         generate_default_data_dir/0]).
-endif.

-dialyzer({no_underspecs, [start/1,
                           stop/0, stop/1,
                           stop_locked/1,
                           join/2]}).

-define(IS_RA_SYSTEM(RaSystem), is_atom(RaSystem)).
-define(IS_RA_SERVER(RaServer), (is_tuple(RaServer) andalso
                                 size(RaServer) =:= 2 andalso
                                 is_atom(element(1, RaServer)) andalso
                                 is_atom(element(2, RaServer)))).
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
       (?IS_KHEPRI_STORE_ID(StoreIdOrRaServerConfig) orelse
        is_map(StoreIdOrRaServerConfig)) ->
    %% If the store ID derived from `StoreIdOrRaServerConfig' is not an atom,
    %% it will cause a case clause exception below.
    RaServerConfig =
    case StoreIdOrRaServerConfig of
        _ when ?IS_KHEPRI_STORE_ID(StoreIdOrRaServerConfig) ->
            #{cluster_name => StoreIdOrRaServerConfig};
        #{cluster_name := CN} when ?IS_KHEPRI_STORE_ID(CN) ->
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
                    try
                        trigger_election(RaServerConfig1, Timeout),
                        {ok, StoreId}
                    catch
                        Class:Reason:Stacktrace ->
                            ?LOG_ERROR(
                               "Failed to trigger election on the freshly "
                               "started Ra server for store \"~s\"::~n~p",
                               [StoreId, Reason]),
                            erlang:raise(Class, Reason, Stacktrace)
                    end;
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

stop(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
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
        {error, _} = Error ->
            %% The store is unknown, it must have been stopped already.
            ?LOG_DEBUG(
               "Unknown Ra system for store \"~s\" on member ~0p: ~0p",
               [StoreId, ThisMember, Error]),
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
        {'DOWN', MRef, _, Pid, noproc} ->
            ?LOG_DEBUG(
               "Ra server ~0p (~0p) in store \"~s\" already exited",
               [Member, Pid, StoreId]),
            ok;
        {'DOWN', MRef, _, Pid, Reason} ->
            ?LOG_DEBUG(
               "Ra server ~0p (~0p) in store \"~s\" exited: ~p",
               [Member, Pid, StoreId, Reason]),
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
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(RemoteNode) ->
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
%% removed. This is also the case if the node is already part of the given
%% cluster (i.e. the joining node will be reset and clustered again).
%%
%% If a clustered node loses its data directory for any reason, this function
%% can be called again to make it join the cluster again and restore its data.
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
            reset_remotely_and_join_locked(
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

-spec reset_remotely_and_join_locked(
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

reset_remotely_and_join_locked(
  StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode, Timeout)
  when RemoteNode =/= node() ->
    %% We attempt to remove the local Ra server from the remote cluster we
    %% want to join.
    %%
    %% This is usually a no-op because it is not part of it yet. However, if
    %% the local Ra server lost its data on disc for whatever reason, the
    %% cluster membership view will be inconsistent (the local Ra server won't
    %% know about its former cluster anymore).
    %%
    %% Therefore, it is safer to ask the remote cluster to remove the local Ra
    %% server, just in case. If we don't do that and the remote cluster starts
    %% to send messages to the local Ra server, the local Ra server might
    %% crash with a `leader_saw_append_entries_rpc_in_same_term' exception.
    %%
    %% TODO: Should we verify the cluster membership first? To avoid resetting
    %% a node which is already part of the cluster? On the other hand, such a
    %% check would not be atomic and the membership could change between the
    %% check and the reset...
    %%
    %% TODO: Do we want to provide an option to verify the state of the local
    %% node before resetting it? Like "if it has data in the Khepri database,
    %% abort". It may be difficult to make this kind of check atomic though.
    RemoteMember = node_to_member(StoreId, RemoteNode),
    ?LOG_DEBUG(
       "Removing this node (~0p) from the remote node's cluster (~0p) to "
       "make sure the membership view is consistent",
       [ThisMember, RemoteMember]),
    T1 = khepri_utils:start_timeout_window(Timeout),
    Ret1 = ra:remove_member(RemoteMember, ThisMember, Timeout),
    Timeout1 = khepri_utils:end_timeout_window(Timeout, T1),
    case Ret1 of
        {ok, _, _} ->
            reset_locally_and_join_locked(
              StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode,
              Timeout1);
        {error, not_member} ->
            reset_locally_and_join_locked(
              StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode,
              Timeout1);
        {error, cluster_change_not_permitted} ->
            T2 = khepri_utils:start_timeout_window(Timeout1),
            ?LOG_DEBUG(
               "Remote cluster (reached through node ~0p) is not ready "
               "for a membership change yet; waiting...", [RemoteNode]),
            Ret2 = wait_for_cluster_change_permitted(StoreId, Timeout1),
            Timeout2 = khepri_utils:end_timeout_window(Timeout1, T2),
            case Ret2 of
                ok ->
                    reset_remotely_and_join_locked(
                      StoreId, ThisMember, RaSystem, RaServerConfig,
                      RemoteNode, Timeout2);
                Error ->
                    Error
            end;
        {timeout, _} ->
            {error, timeout};
        {error, _} = Error ->
            Error
    end;
reset_remotely_and_join_locked(
  _StoreId, _ThisMember, _RaSystem, _RaServerConfig, RemoteNode, _Timeout)
  when RemoteNode =:= node() ->
    ok.

-spec reset_locally_and_join_locked(
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

reset_locally_and_join_locked(
  StoreId, ThisMember, RaSystem, RaServerConfig, RemoteNode, Timeout) ->
    %% The local node is reset in case it is already a standalone elected
    %% leader (which would be the case after a successful call to
    %% `khepri_cluster:start()') or part of a cluster, and have any data.
    %%
    %% Just after the reset, we restart it skipping the `trigger_election()'
    %% step: this is required so that it does not become a leader before
    %% joining the remote node. Otherwise, we hit an assertion in Ra.
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
    RemoteMember = node_to_member(StoreId, RemoteNode),
    T1 = khepri_utils:start_timeout_window(Timeout),
    Ret1 = ra:add_member(RemoteMember, ThisMember, Timeout),
    Timeout1 = khepri_utils:end_timeout_window(Timeout, T1),
    case Ret1 of
        {ok, _, _StoreId} ->
            ?LOG_DEBUG(
               "Cluster for store \"~s\" successfully expanded",
               [StoreId]),
            ok;
        {error, already_member} ->
            ?LOG_DEBUG(
               "This node (~0p) is already a member of the remote node's "
               "cluster (~0p)",
               [ThisMember, RemoteMember]),
            ok;
        {error, cluster_change_not_permitted} ->
            T2 = khepri_utils:start_timeout_window(Timeout1),
            ?LOG_DEBUG(
               "Remote cluster (reached through node ~0p) is not ready "
               "for a membership change yet; waiting...", [RemoteNode]),
            Ret2 = wait_for_cluster_change_permitted(RemoteMember, Timeout1),
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
            trigger_election(ThisMember, Timeout1),
            case Error of
                {timeout, _} -> {error, timeout};
                {error, _}   -> Error
            end
    end.

wait_for_cluster_change_permitted(RaMemberOrStoreId, Timeout) ->
    Ret = do_wait_for_leader(RaMemberOrStoreId, false, Timeout),

    %% We wait for an additional fixed amount of time because the
    %% cluster could have a leader and still not be ready to accept
    %% a cluster change. This avoids too many retries that will
    %% just eat resources.
    timer:sleep(?TRANSIENT_ERROR_RETRY_INTERVAL),

    Ret.

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
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
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
       "Detaching this node (~0p) in store \"~s\" from its cluster (if any) "
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
            try
                Ret2 = wait_for_cluster_change_permitted(StoreId, Timeout1),
                Timeout2 = khepri_utils:end_timeout_window(Timeout1, T2),
                case Ret2 of
                    ok ->
                        do_reset(RaSystem, StoreId, ThisMember, Timeout2);
                    {error, noproc} ->
                        ?LOG_DEBUG(
                           "The local Ra server exited while we were waiting "
                           "for it to be ready for a membership change. It "
                           "means it was removed from the cluster by another "
                           "member; we can proceed with the reset."),
                        forget_store(StoreId),
                        ok;
                    Error ->
                        Error
                end
            catch
                exit:{normal, _} ->
                    ?LOG_DEBUG(
                       "The local Ra server exited while we were waiting "
                       "for it to be ready for a membership change. It "
                       "means it was removed from the cluster by another "
                       "member; we can proceed with the reset."),
                    forget_store(StoreId),
                    ok
            end;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} when Reason =:= noproc orelse Reason =:= normal ->
            ?LOG_DEBUG(
               "The local Ra server exited while we tried to detach it from "
               "its cluster. It means it was removed from the cluster by "
               "another member; we can proceed with the reset."),
            forget_store(StoreId),
            ok;
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
        ?IS_KHEPRI_STORE_ID(StoreId) ->
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

-spec members() -> Ret when
      Ret :: khepri:ok(Members) | khepri:error(),
      Members :: [ra:server_id(), ...].
%% @doc Returns the list of Ra members that are part of the cluster.
%%
%% Calling this function is the same as calling `members(StoreId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see members/1.
%% @see members/2.

members() ->
    StoreId = get_default_store_id(),
    members(StoreId).

-spec members(StoreId | Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(Members) | khepri:error(),
      Members :: [ra:server_id(), ...].
%% @doc Returns the list of Ra members that are part of the cluster.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`members(StoreId)'. Calling it is the same as calling `members(StoreId,
%% #{})'.</li>
%% <li>`members(Options)'. Calling it is the same as calling `members(StoreId,
%% Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see members/2.

members(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    members(StoreId, #{});
members(Options) when is_map(Options) ->
    StoreId = get_default_store_id(),
    members(StoreId, Options).

-spec members(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(Members) | khepri:error(),
      Members :: [ra:server_id(), ...].
%% @doc Returns the list of Ra members that are part of the cluster.
%%
%% If the `favor' option is set to `consistency', the Ra leader is queried for
%% the list of members, therefore the membership view is consistent with the
%% rest of the cluster.
%%
%% If the `favor' option is set to `low_latency', the local Ra server is
%% queried. The query will be faster at the cost of returning a possibly
%% out-of-date result.
%%
%% The `condition' query option is unsupported.
%%
%% @param StoreId the ID of the store to stop.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, Members}' tuple or an `{error, Reason}' tuple. `Members'
%% is a non-empty list of Ra server IDs.

members(StoreId, Options) when is_map(Options) ->
    ThisMember = this_member(StoreId),
    Timeout = maps:get(timeout, Options, khepri_app:get_default_timeout()),
    QueryType = case maps:get(favor, Options, consistency) of
                    consistency -> leader;
                    low_latency -> local
                end,
    case QueryType of
        local ->
            case ra_leaderboard:lookup_members(StoreId) of
                Members when is_list(Members) ->
                    {ok, lists:sort(Members)};
                undefined ->
                    do_query_members(StoreId, ThisMember, QueryType, Timeout)
            end;
        leader ->
            do_query_members(StoreId, ThisMember, QueryType, Timeout)
    end.

-spec do_query_members(StoreId, RaServer, QueryType, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      RaServer :: ra:server_id(),
      QueryType :: leader | local,
      Timeout :: timeout(),
      Ret :: khepri:ok(Members) | khepri:error(),
      Members :: [ra:server_id(), ...].
%% @private

do_query_members(StoreId, RaServer, QueryType, Timeout) ->
    T0 = khepri_utils:start_timeout_window(Timeout),
    Arg = case QueryType of
              leader -> RaServer;
              local  -> {local, RaServer}
          end,
    case ra:members(Arg, Timeout) of
        {ok, Members, _} ->
            {ok, lists:sort(Members)};
        {error, noproc} = Error
          when ?HAS_TIME_LEFT(Timeout) ->
            case khepri_utils:is_ra_server_alive(RaServer) of
                true ->
                    NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
                    NewTimeout = khepri_utils:sleep(
                                   ?TRANSIENT_ERROR_RETRY_INTERVAL,
                                   NewTimeout0),
                    do_query_members(
                      StoreId, RaServer, QueryType, NewTimeout);
                false ->
                    ?LOG_DEBUG(
                       "Cannot query members in store \"~s\": "
                       "the store is stopped or non-existent",
                       [StoreId]),
                    Error
            end;
        {error, Reason}
          when ?HAS_TIME_LEFT(Timeout) andalso
               (Reason == noconnection orelse
                Reason == nodedown orelse
                Reason == shutdown orelse
                Reason == normal) ->
            NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
            NewTimeout = khepri_utils:sleep(
                           ?TRANSIENT_ERROR_RETRY_INTERVAL, NewTimeout0),
            do_query_members(
              StoreId, RaServer, QueryType, NewTimeout);
        {timeout, _} ->
            ?LOG_WARNING(
               "Timeout while querying members in store \"~s\"",
               [StoreId]),
            {error, timeout};
        Error ->
            ?LOG_WARNING(
               "Failed to query members in store \"~s\": ~p",
               [StoreId, Error]),
            Error
    end.

-spec nodes() -> Ret when
      Ret :: khepri:ok(Nodes) | khepri:error(),
      Nodes :: [node(), ...].
%% @doc Returns the list of Erlang nodes that are part of the cluster.
%%
%% Calling this function is the same as calling `nodes(StoreId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see nodes/1.
%% @see nodes/2.

nodes() ->
    case members() of
        {ok, Members} -> {ok, [Node || {_, Node} <- Members]};
        Error         -> Error
    end.

-spec nodes(StoreId | Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(Nodes) | khepri:error(),
      Nodes :: [node(), ...].
%% @doc Returns the list of Erlang nodes that are part of the cluster.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`nodes(StoreId)'. Calling it is the same as calling `nodes(StoreId,
%% #{})'.</li>
%% <li>`nodes(Options)'. Calling it is the same as calling `nodes(StoreId,
%% Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see nodes/2.

nodes(StoreIdOrOptions) ->
    case members(StoreIdOrOptions) of
        {ok, Members} -> {ok, [Node || {_, Node} <- Members]};
        Error         -> Error
    end.

-spec nodes(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(Nodes) | khepri:error(),
      Nodes :: [node(), ...].
%% @doc Returns the list of Erlang nodes that are part of the cluster.
%%
%% If the `favor' option is set to `consistency', the Ra leader is queried for
%% the list of members, therefore the membership view is consistent with the
%% rest of the cluster.
%%
%% If the `favor' option is set to `low_latency', the local Ra server is
%% queried. The query will be faster at the cost of returning a possibly
%% out-of-date result.
%%
%% The `condition' query option is unsupported.
%%
%% @see members/2.

nodes(StoreId, Options) ->
    case members(StoreId, Options) of
        {ok, Members} -> {ok, [Node || {_, Node} <- Members]};
        Error         -> Error
    end.

-spec wait_for_leader() -> Ret when
      Ret :: ok | khepri:error().
%% @doc Waits for a leader to be elected.
%%
%% Calling this function is the same as calling `wait_for_leader(StoreId)'
%% with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see wait_for_leader/1.
%% @see wait_for_leader/2.

wait_for_leader() ->
    StoreId = get_default_store_id(),
    wait_for_leader(StoreId).

-spec wait_for_leader(StoreIdOrRaServer) -> Ret when
      StoreIdOrRaServer :: StoreId | RaServer,
      StoreId :: khepri:store_id(),
      RaServer :: ra:server_id(),
      Ret :: ok | khepri:error().
%% @doc Waits for a leader to be elected.
%%
%% Calling this function is the same as calling `wait_for_leader(StoreId,
%% DefaultTimeout)' where `DefaultTimeout' is returned by {@link
%% khepri_app:get_default_timeout/0}.
%%
%% @see wait_for_leader/2.

wait_for_leader(StoreIdOrRaServer) ->
    Timeout = khepri_app:get_default_timeout(),
    wait_for_leader(StoreIdOrRaServer, Timeout).

-spec wait_for_leader(StoreIdOrRaServer, Timeout) -> Ret when
      StoreIdOrRaServer :: StoreId | RaServer,
      StoreId :: khepri:store_id(),
      RaServer :: ra:server_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Waits for a leader to be elected.
%%
%% This is useful if you want to be sure the clustered store is ready before
%% issueing writes and queries. Note that there are obviously no guaranties
%% that the Raft quorum will be lost just after this call.
%%
%% @param StoreId the ID of the store that should elect a leader before this
%%        call can return successfully.
%% @param Timeout the timeout.
%%
%% @returns `ok' if a leader was elected or an `{error, Reason}' tuple.

wait_for_leader(StoreIdOrRaServer, Timeout) ->
    do_wait_for_leader(StoreIdOrRaServer, true, Timeout).

do_wait_for_leader(StoreId, WaitForProcToStart, Timeout)
  when is_atom(StoreId) ->
    ThisMember = this_member(StoreId),
    do_wait_for_leader(ThisMember, WaitForProcToStart, Timeout);
do_wait_for_leader(RaServer, WaitForProcToStart, Timeout) ->
    T0 = khepri_utils:start_timeout_window(Timeout),
    case ra:members(RaServer, Timeout) of
        {ok, _Members, _LeaderId} ->
            ok;
        {error, Reason}
          when ?HAS_TIME_LEFT(Timeout) andalso
               ((Reason == noproc andalso WaitForProcToStart) orelse
                Reason == noconnection orelse
                Reason == nodedown orelse
                Reason == shutdown orelse
                Reason == normal) ->
            NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
            NewTimeout = khepri_utils:sleep(
                           ?TRANSIENT_ERROR_RETRY_INTERVAL, NewTimeout0),
            do_wait_for_leader(RaServer, WaitForProcToStart, NewTimeout);
        {timeout, _} ->
            {error, timeout};
        {error, _} = Error ->
            Error
    end.

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

    LogInitArgs0 = #{uid => UId},
    LogInitArgs = case MachineConfig0 of
                      #{snapshot_interval := SnapshotInterval} ->
                          %% Ra takes a snapshot when the number of applied
                          %% commands is _greater than_ the interval (not
                          %% equal), so we need to subtract one from Khepri's
                          %% configured snapshot interval so that Ra snapshots
                          %% exactly at the interval.
                          MinSnapshotInterval = SnapshotInterval - 1,
                          LogInitArgs0#{min_snapshot_interval =>
                                        MinSnapshotInterval};
                      _ ->
                          LogInitArgs0
                  end,

    RaServerConfig2#{uid => UId,
                     log_init_args => LogInitArgs,
                     machine => Machine}.

-define(PT_STORE_IDS, {khepri, store_ids}).

-spec remember_store(RaSystem, RaServerConfig) -> ok when
      RaSystem :: atom(),
      RaServerConfig :: ra_server:ra_server_config().
%% @private

remember_store(RaSystem, #{cluster_name := StoreId} = RaServerConfig) ->
    ?assert(maps:is_key(id, RaServerConfig)),
    Lock = store_ids_lock(),
    global:set_lock(Lock, [node()]),
    try
        StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
        Props = #{ra_system => RaSystem,
                  ra_server_config => RaServerConfig},
        StoreIds1 = StoreIds#{StoreId => Props},
        persistent_term:put(?PT_STORE_IDS, StoreIds1)
    after
            global:del_lock(Lock, [node()])
    end,
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
    Lock = store_ids_lock(),
    global:set_lock(Lock, [node()]),
    try
        StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
        StoreIds1 = maps:remove(StoreId, StoreIds),
        case maps:size(StoreIds1) of
            0 -> _ = persistent_term:erase(?PT_STORE_IDS);
            _ -> ok = persistent_term:put(?PT_STORE_IDS, StoreIds1)
        end
    after
            global:del_lock(Lock, [node()])
    end,
    ok.

store_ids_lock() ->
    {?PT_STORE_IDS, self()}.

-spec get_store_ids() -> [StoreId] when
      StoreId :: khepri:store_id().
%% @doc Returns the list of running stores.

get_store_ids() ->
    StoreIds0 = maps:keys(persistent_term:get(?PT_STORE_IDS, #{})),
    StoreIds1 = lists:filter(fun is_store_running/1, StoreIds0),
    StoreIds1.

-spec is_store_running(StoreId) -> IsRunning when
      StoreId :: khepri:store_id(),
      IsRunning :: boolean().
%% @doc Indicates if `StoreId' is running or not.

is_store_running(StoreId) ->
    %% FIXME: Ra has no API to know if a Ra server is running or not. We could
    %% use a public API such as `ra:key_metrics/2', but unfortunately, it is
    %% not as efficient as querying the process directly. Therefore, we bypass
    %% Ra and rely on the fact that the server ID is internally a registered
    %% process name and resolve it to determine if it is running.
    Runs = erlang:whereis(StoreId) =/= undefined,

    %% We know the real state of the Ra server. In the case the Ra server
    %% stopped behind the back of Khepri, we update the cached list of running
    %% stores as a side effect here.
    StoreIds = persistent_term:get(?PT_STORE_IDS, #{}),
    case maps:is_key(StoreId, StoreIds) of
        true when Runs ->
            ok;
        false when not Runs ->
            ok;
        false when Runs ->
            %% This function was called between the start of the Ra server and
            %% the record of its configuration. This is a race, but that's ok.
            ok;
        true when not Runs ->
            ?LOG_DEBUG(
               "Ra server for store ~s stopped behind the back of Khepri",
               [StoreId]),
            forget_store(StoreId)
    end,

    Runs.
