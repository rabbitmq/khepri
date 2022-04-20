%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(test_ra_server_helpers).

-include_lib("stdlib/include/assert.hrl").

-export([setup/1,
         cleanup/1]).

setup(Testcase) ->
    _ = logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(khepri),
    ok = application:set_env(
           khepri,
           consistent_query_interval_in_compromise,
           2),

    RaSystem = Testcase,
    StoreDir = store_dir_name(RaSystem),
    _ = remove_store_dir(StoreDir),
    Default = ra_system:default_config(),
    RaSystemConfig = Default#{name => RaSystem,
                              data_dir => StoreDir,
                              wal_data_dir => StoreDir,
                              wal_max_size_bytes => 16 * 1024,
                              names => ra_system:derive_names(RaSystem)},
    case ra_system:start(RaSystemConfig) of
        {ok, RaSystemPid} ->
            {ok, StoreId} = khepri:start(
                              RaSystem,
                              Testcase,
                              atom_to_list(Testcase)),
            ok = khepri_machine:clear_cache(StoreId),
            #{ra_system => RaSystem,
              ra_system_pid => RaSystemPid,
              store_dir => StoreDir,
              store_id => StoreId};
        {error, _} = Error ->
            throw(Error)
    end.

cleanup(#{ra_system := RaSystem,
          store_dir := StoreDir,
          store_id := StoreId}) ->
    ServerIds = khepri_cluster:members(StoreId),
    _ = application:stop(khepri),
    %% FIXME: This monitoring can go away when/if the following pull request
    %% in Ra is merged:
    %% https://github.com/rabbitmq/ra/pull/270
    MRefs = [erlang:monitor(process, ServerId) || ServerId <- ServerIds],
    ?assertMatch({ok, _}, ra:delete_cluster(ServerIds)),
    lists:foreach(
      fun(MRef) -> receive {'DOWN', MRef, _, _, _} -> ok end end,
      MRefs),
    ?assertEqual(ok, supervisor:terminate_child(ra_systems_sup, RaSystem)),
    ?assertEqual(ok, supervisor:delete_child(ra_systems_sup, RaSystem)),
    _ = remove_store_dir(StoreDir),
    ok.

store_dir_name(RaSystem) ->
    lists:flatten(
      io_lib:format("_test." ?MODULE_STRING ".~s", [RaSystem])).

remove_store_dir(StoreDir) ->
    OnWindows = case os:type() of
                    {win32, _} -> true;
                    _          -> false
                end,
    case file:del_dir_r(StoreDir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, eexist} when OnWindows ->
            %% FIXME: Some files are not deleted on Windows... Are they still
            %% open in Ra?
            io:format(
              standard_error,
              "Files remaining in ~ts: ~p~n",
              [StoreDir, file:list_dir_all(StoreDir)]),
            ok;
        Error ->
            throw(Error)
    end.
