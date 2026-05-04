%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri machine config V0 handling functions.
%%
%% @hidden

-module(khepri_config).

-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_config.hrl").

-export([new/1]).

%% Internal functions to access the opaque #config{} state.
-export([get_store_id/1,
         get_snapshot_interval/1]).

%% Record representing the state machine configuration.
-record(config,
        {store_id :: khepri:store_id(),
         member :: ra:server_id(),
         snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

-opaque machine_config() :: #config{}.
%% Configuration record, holding read-only or rarely changing fields.

-export_type([machine_config/0]).

-spec new(InitArgs) -> Config when
      InitArgs :: khepri_machine:machine_init_args(),
      Config :: khepri_config:machine_config().
%% @doc Creates a new opaque configuration record.

new(#{store_id := StoreId,
      member := Member} = InitArgs) ->
    Config = case InitArgs of
                 #{snapshot_interval := SnapshotInterval} ->
                     #config{store_id = StoreId,
                             member = Member,
                             snapshot_interval = SnapshotInterval};
                 _ ->
                     #config{store_id = StoreId,
                             member = Member}
             end,
    Config.

-spec get_store_id(Config) -> StoreId when
      Config :: khepri_config:machine_config(),
      StoreId :: khepri:store_id().
%% @doc Returns the store ID from the given machine configuration.

get_store_id(#config{store_id = StoreId}) ->
    StoreId.

-spec get_snapshot_interval(Config) -> SnapshotInterval when
      Config :: khepri_config:machine_config(),
      SnapshotInterval :: non_neg_integer().
%% @doc Returns the snapshot interval from the given machine configuration.

get_snapshot_interval(#config{snapshot_interval = SnapshotInterval}) ->
    SnapshotInterval.
