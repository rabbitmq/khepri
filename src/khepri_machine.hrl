%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-include("src/khepri_node.hrl").

-record(khepri_machine_aux,
        {store_id :: khepri:store_id(),
         delayed_aux_queries = [] :: [khepri_machine:delayed_aux_query()]}).

%% The current/latest version of the state machine is defined in this macro
%% instead of `khepri_machine:version/0' only. This way, it can be used in
%% function specs too.
-define(LATEST_MACVER, 3).

%% Map API behaviours with the state machine version they were introduced in.
-define(API_BEHAV_MACVER_MAP,
        #{dedup_protection            => 1,

          delete_reason_in_node_props => 2,
          expire_dedups_from_tick     => 2,
          indirect_deletes_in_ret     => 2,
          uniform_write_ret           => 2,

          multi_table_projections     => 3}).

%% Get the state machine version the given API behaviour was introduced in.
%% This is similar to `khepri_machine:api_behaviour_to_machine_version/1' but
%% can be used in guards.
-define(API_BEHAV_MACVER(Behaviour),
        map_get(Behaviour, ?API_BEHAV_MACVER_MAP)).

%% -------------------------------------------------------------------
%% State machine commands.
%% -------------------------------------------------------------------

-record(put, {path :: khepri_path:native_pattern(),
              payload = ?NO_PAYLOAD :: khepri_payload:payload(),
              options = #{} :: khepri:tree_options() | khepri:put_options()}).

-record(delete, {path :: khepri_path:native_pattern(),
                 options = #{} :: khepri:tree_options()}).

-record(tx, {'fun' :: horus:horus_fun() | khepri_path:pattern(),
             args = [] :: list()}).

-record(register_trigger, {id :: khepri:trigger_id(),
                           event_filter :: khepri_evf:event_filter(),
                           sproc :: khepri_path:native_path()}).

-record(ack_triggered, {triggered :: [khepri_machine:triggered()]}).

-record(register_projection, {pattern :: khepri_path:native_pattern(),
                              projection :: khepri_projection:projection()}).

-record(unregister_projections, {names :: all | [khepri_projection:name()]}).

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

%% -------------------------------------------------------------------
%% Old commands, kept for backward-compatibility.
%% -------------------------------------------------------------------

-record(unregister_projection, {name :: khepri_projection:name()}).

%% -------------------------------------------------------------------
%% State machine aux. effects.
%% -------------------------------------------------------------------

-record(trigger_projection, {path :: khepri_path:native_path(),
                             old_props :: khepri:node_props(),
                             new_props :: khepri:node_props(),
                             projection :: khepri_projection:projection()}).

-record(restore_projection, {pattern :: khepri_path:native_pattern(),
                             projection :: khepri_projection:projection()}).

%% -------------------------------------------------------------------
%% Other records.
%% -------------------------------------------------------------------

-record(triggered, {id :: khepri:trigger_id(),
                    %% TODO: Do we need a ref to distinguish multiple
                    %% instances of the same trigger?
                    event_filter :: khepri_evf:event_filter(),
                    sproc :: horus:horus_fun(),
                    props = #{} :: map()}).
