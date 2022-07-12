%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(helpers).

-include_lib("stdlib/include/assert.hrl").

-export([init_list_of_modules_to_skip/0,
         start_ra_system/1,
         stop_ra_system/1,
         store_dir_name/1,
         remove_store_dir/1]).

init_list_of_modules_to_skip() ->
    _ = application:load(khepri),
    khepri_utils:init_list_of_modules_to_skip().

start_ra_system(RaSystem) ->
    {ok, _} = application:ensure_all_started(ra),
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
            #{ra_system => RaSystem,
              ra_system_pid => RaSystemPid,
              store_dir => StoreDir};
        {error, _} = Error ->
            throw(Error)
    end.

stop_ra_system(#{ra_system := RaSystem,
                 store_dir := StoreDir}) ->
    ?assertEqual(ok, supervisor:terminate_child(ra_systems_sup, RaSystem)),
    ?assertEqual(ok, supervisor:delete_child(ra_systems_sup, RaSystem)),
    _ = remove_store_dir(StoreDir),
    ok.

store_dir_name(RaSystem) ->
    Node = node(),
    lists:flatten(
      io_lib:format("_test.~s.~s", [RaSystem, Node])).

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
