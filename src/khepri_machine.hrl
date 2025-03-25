%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-include("src/khepri_node.hrl").

%% TODO: Query this value from Ra itself.
-define(SNAPSHOT_INTERVAL, 4096).

%% Record representing the state machine configuration.
-record(config,
        {store_id :: khepri:store_id(),
         member :: ra:server_id(),
         snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

-record(khepri_machine_aux,
        {store_id :: khepri:store_id(),
         delayed_aux_queries = [] :: [khepri_machine:delayed_aux_query()]}).

%% State machine commands and aux. effects.

-record(put, {path :: khepri_path:native_pattern(),
              payload = ?NO_PAYLOAD :: khepri_payload:payload(),
              options = #{} :: khepri:tree_options() | khepri:put_options()}).

-record(delete, {path :: khepri_path:native_pattern(),
                 options  = #{} :: khepri:tree_options()}).

-record(tx, {'fun' :: horus:horus_fun() | khepri_path:pattern(),
             args = [] :: list()}).

-record(register_trigger, {id :: khepri:trigger_id(),
                           event_filter :: khepri_evf:event_filter(),
                           sproc :: khepri_path:native_path()}).

-record(ack_triggered, {triggered :: [khepri_machine:triggered()]}).

-record(triggered, {id :: khepri:trigger_id(),
                    %% TODO: Do we need a ref to distinguish multiple
                    %% instances of the same trigger?
                    event_filter :: khepri_evf:event_filter(),
                    sproc :: horus:horus_fun(),
                    props = #{} :: map()}).

-record(register_projection, {pattern :: khepri_path:native_pattern(),
                              projection :: khepri_projection:projection()}).

-record(unregister_projections, {names :: all | [khepri_projection:name()]}).

-record(trigger_projection, {path :: khepri_path:native_path(),
                             old_props :: khepri:node_props(),
                             new_props :: khepri:node_props(),
                             projection :: khepri_projection:projection()}).

-record(restore_projection, {pattern :: khepri_path:native_pattern(),
                             projection :: khepri_projection:projection()}).

-record(dedup, {ref :: reference(),
                expiry :: integer(),
                command :: khepri_machine:command()}).

-record(dedup_ack, {ref :: reference()}).

-record(drop_dedups, {refs :: [reference()]}).
%% A command introduced in machine version 2 which is meant to drop expired
%% dedups.
%%
%% This is emitted internally by the `handle_aux/5' callback clause which
%% handles the `tick' Ra aux effect.

%% Old commands, kept for backward-compatibility.

-record(unregister_projection, {name :: khepri_projection:name()}).
