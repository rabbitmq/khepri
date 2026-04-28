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

%% Internal functions to access the opaque #config{} state.
-export([is_config/1,
         get_store_id/1,
         get_snapshot_interval/1,
         get_unreleased_command_footprint_threshold/1,
         convert_config/4]).

%% Record representing the state machine configuration.
-record(config,
        {store_id :: khepri:store_id(),
         member :: ra:server_id(),
         snapshot_interval =
         ?SNAPSHOT_INTERVAL :: non_neg_integer(),
         unreleased_command_footprint_threshold =
         ?UNRELEASED_COMMAND_FOOTPRINT_THRESHOLD :: non_neg_integer()}).

-opaque machine_config_v1() :: #config{}.
%% Configuration record, version 1.

-type machine_config() :: machine_config_v1() | khepri_config_v0:machine_config().
%% Configuration record, holding read-only or rarely changing fields.

-export_type([machine_config/0,
              machine_config_v1/0]).

-spec is_config(Config) -> IsConfig when
      Config :: khepri_config:machine_config(),
      IsConfig :: boolean().
%% @doc Tells if the given argument is a valid configuration.
%%
%% @private

is_config(Config) ->
    is_record(Config, config) orelse khepri_config_v0:is_config(Config).

-spec get_store_id(Config) -> StoreId when
      Config :: khepri_config:machine_config(),
      StoreId :: khepri:store_id().
%% @doc Returns the store ID from the given machine configuration.

get_store_id(#config{store_id = StoreId}) ->
    StoreId;
get_store_id(Config) ->
    khepri_config_v0:get_store_id(Config).

-spec get_snapshot_interval(Config) -> SnapshotInterval when
      Config :: khepri_config:machine_config(),
      SnapshotInterval :: non_neg_integer().
%% @doc Returns the snapshot interval from the given machine configuration.

get_snapshot_interval(#config{snapshot_interval = SnapshotInterval}) ->
    SnapshotInterval;
get_snapshot_interval(Config) ->
    khepri_config_v0:get_snapshot_interval(Config).

-spec get_unreleased_command_footprint_threshold(Config) -> Threshold when
      Config :: khepri_config:machine_config(),
      Threshold :: non_neg_integer().
%% @doc Returns the unreleased command footprint threshold from the given state
%% configuration.

get_unreleased_command_footprint_threshold(
  #config{unreleased_command_footprint_threshold = Threshold}) ->
    Threshold.

-spec convert_config(Config, OldVersion, NewVersion, InitArgs) ->
    NewConfig when
      Config :: khepri_config:machine_config(),
      OldVersion :: non_neg_integer(),
      NewVersion :: non_neg_integer(),
      InitArgs :: khepri_machine:machine_init_args(),
      NewConfig :: khepri_config:machine_config().
%% @doc Converts an old config record to a newer one.
%%
%% @private.

convert_config(Config, OldVersion, NewVersion, InitArgs) ->
    lists:foldl(
      fun(N, Config1) ->
              OldVersion1 = N,
              NewVersion1 = erlang:min(N + 1, NewVersion),
              convert_config1(Config1, OldVersion1, NewVersion1, InitArgs)
      end, Config, lists:seq(OldVersion, NewVersion)).

convert_config1(Config, Version, Version, _InitArgs) ->
    Config;
convert_config1(Config, 0, 1, InitArgs) ->
    %% To go from version 0 to version 1, we add the `dedups' fields at the
    %% end of the record. The default value is an empty map.
    ?assert(khepri_config_v0:is_config(Config)),
    Threshold = maps:get(
                  unreleased_command_footprint_threshold, InitArgs,
                  ?UNRELEASED_COMMAND_FOOTPRINT_THRESHOLD),
    Fields0 = khepri_config_v0:config_to_list(Config),
    Fields1 = Fields0 ++ [Threshold],
    Config1 = list_to_tuple(Fields1),
    ?assert(is_config(Config1)),
    Config1.
