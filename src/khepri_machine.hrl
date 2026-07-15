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
-define(LATEST_MACVER, 4).

%% Map API behaviours with the state machine version they were introduced in.
-define(API_BEHAV_MACVER_MAP,
        #{dedup_protection            => 1,

          delete_reason_in_node_props => 2,
          expire_dedups_from_tick     => 2,
          indirect_deletes_in_ret     => 2,
          uniform_write_ret           => 2,

          multi_table_projections     => 3,

          uniform_commands            => 4,
          request_snapshot            => 4,
          extended_trigger            => 4}).

%% Get the state machine version the given API behaviour was introduced in.
%% This is similar to `khepri_machine:api_behaviour_to_machine_version/1' but
%% can be used in guards.
-define(API_BEHAV_MACVER(Behaviour),
        map_get(Behaviour, ?API_BEHAV_MACVER_MAP)).

%% -------------------------------------------------------------------
%% State machine commands.
%% -------------------------------------------------------------------

-record(common_v1, {command_size = 0 :: non_neg_integer(),
                    dedup_ref = undefined :: reference() | undefined,
                    dedup_expiry = undefined :: integer() | undefined}).
%% Set of attributes shared by all commands.

-record(put_v1,
        {path :: khepri_path:native_pattern(),
         payload = ?NO_PAYLOAD :: khepri_payload:payload(),
         put_options = #{} :: khepri:put_options(),
         tree_options = #{} :: khepri:tree_options()}).
%% Put-specific attributes.
%%
%% <ul>
%% <li>`path' is the path pattern of the tree node(s) to create or update.</li>
%% <li>`payload' is the payload to store in the tree nodes matching the path
%% pattern.</li>
%% <li>`put_options' is a map of put-specific options.</li>
%% <li>`tree_options' is a map of tree-handling options.</li>
%% </ul>

-record(put_v,
        {args :: #put_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the put command.
%%
%% It is used to create or update one or more tree nodes.
%%
%% <ul>
%% <li>`args' contains the put-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(delete_v1,
        {path :: khepri_path:native_pattern(),
         tree_options = #{} :: khepri:tree_options()}).
%% Delete-specific attributes.
%%
%% <ul>
%% <li>`path' is the path pattern of the tree node(s) to delete.</li>
%% <li>`tree_options' is a map of tree-handling options.</li>
%% </ul>

-record(delete_v,
        {args :: #delete_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the delete command.
%%
%% It is used to delete one or more tree nodes.
%%
%% <ul>
%% <li>`args' contains the delete-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(tx_v1,
        {'fun' :: horus:horus_fun() | khepri_path:pattern(),
         args = [] :: list()}).
%% Transaction-specific attributes.
%%
%% <ul>
%% <li>`fun' is the anonymous function of the transaction.</li>
%% <li>`args' is a list of terms to pass to the transaction function.</li>
%% </ul>

-record(tx_v,
        {args :: #tx_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the transaction command.
%%
%% It is used to execute a transaction on the store.
%%
%% <ul>
%% <li>`args' contains the transaction-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(register_trigger_v1,
        {id :: khepri:trigger_id(),
         event_filter :: khepri_evf:event_filter(),
         action :: khepri_event_handler:trigger_action()}).
%% Trigger registration-specific attributes.
%%
%% <ul>
%% <li>`id' is of identifier of the trigger.</li>
%% <li>`event_filter' is the event filter for that trigger.</li>
%% <li>`action' is the action to be executed.</li>
%% </ul>

-record(register_trigger_v,
        {args :: #register_trigger_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the trigger registration command.
%%
%% It creates a trigger based on the given event filter and stored procedure.
%%
%% <ul>
%% <li>`args' contains the trigger registration-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(ack_triggered_v1,
        {triggered :: [khepri_machine:triggered()]}).
%% Trigger ack-specific attributes.
%%
%% <ul>
%% <li>`triggered' is a list of `#triggered{}' records.</li>
%% </ul>

-record(ack_triggered_v2,
        {triggered :: [khepri_machine:triggered_v2()]}).
%% Trigger ack-specific attributes.
%%
%% <ul>
%% <li>`triggered' is a list of `#triggered{}' or `#triggered_v2{}'
%% records.</li>
%% </ul>

-record(ack_triggered_v,
        {args :: #ack_triggered_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the trigger ack command.
%%
%% It is used by the event handler to ack trigger execution.
%%
%% <ul>
%% <li>`args' contains the trigger ack-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(register_projection_v1,
        {pattern :: khepri_path:native_pattern(),
         projection :: khepri_projection:projection()}).
%% Projection registration-specific attributes.
%%
%% <ul>
%% <li>`pattern' is the path pattern describing tree nodes to project.</li>
%% <li>`projection' is the projection function descriptor.</li>
%% </ul>

-record(register_projection_v,
        {args :: #register_projection_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the projection registration command.
%%
%% It is used to register a projection.
%%
%% <ul>
%% <li>`args' contains the projection registration-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(unregister_projections_v1,
        {names :: all | [khepri_projection:name()]}).
%% Projection unregistration-specific attributes.
%%
%% <ul>
%% <li>`names' is of projection names to unregister.</li>
%% </ul>

-record(unregister_projections_v,
        {args :: #unregister_projections_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the projection unregistration command.
%%
%% It is used to unregister projections.
%%
%% <ul>
%% <li>`args' contains the projection unregistration-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(dedup,
        {ref :: reference(),
         expiry :: integer(),
         command :: khepri_machine:command() | khepri_machine:old_command()}).
%% Command to help deduplicate the wrapped command in case it is submitted to
%% the Ra leader multiple times.

-record(dedup_ack_v1,
        {ref :: reference()}).
%% Dedup ack-specific attributes.
%%
%% <ul>
%% <li>`ref' is a reference to the dedup command being acked.</li>
%% </ul>

-record(dedup_ack_v,
        {args :: #dedup_ack_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the dedup ack command.
%%
%% It is used to acknowledge a deduplicated command got a replied.
%%
%% <ul>
%% <li>`args' contains the dedup ack-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(drop_dedups_v1,
        {refs :: [reference()]}).
%% New versioned variant of the dedups drop command.
%%
%% It is used drop expired dedups. It is introduced in machine version 2.
%%
%% This is emitted internally by the `handle_aux/5' callback clause which
%% handles the `tick' Ra aux effect.
%%
%% <ul>
%% <li>`refs' is a list of references of dedups to drop.</li>
%% </ul>

-record(drop_dedups_v,
        {args :: #drop_dedups_v1{},
         common = none :: #common_v1{} | none}).
%% Versioned variant of the dedup drop command.
%%
%% It is used to drop a list dedup references.
%%
%% <ul>
%% <li>`args' contains the dedup drop-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

-record(request_snapshot_v1, {reason :: string()}).
%% Arguments to the snapshot request command, version 1.
%%
%% <ul>
%% <li>`reasaon' is string dscribing the reason why a snapshot is
%% requested.</li>
%% </ul>

-record(request_snapshot, {args :: #request_snapshot_v1{},
                           common = none :: #common_v1{} | none}).
%% Command to unconditionally request a snapshot.
%%
%% <ul>
%% <li>`args' contains the snapshot request-specific attributes.</li>
%% <li>`common' contains the attributes shared by all commands.</li>
%% </ul>

%% -------------------------------------------------------------------
%% Old commands, kept for backward-compatibility.
%% -------------------------------------------------------------------

-record(put, {path :: khepri_path:native_pattern(),
              payload = ?NO_PAYLOAD :: khepri_payload:payload(),
              options = #{} :: khepri:tree_options() | khepri:put_options()}).
%% Non-versioned old variant of the put command.
%%
%% Replaced by `#pub_v{}'.

-record(delete, {path :: khepri_path:native_pattern(),
                 options = #{} :: khepri:tree_options()}).
%% Old non-versioned variant of the delete command.
%%
%% Replaced by `#delete_v{}'.

-record(tx, {'fun' :: horus:horus_fun() | khepri_path:pattern(),
             args = [] :: list()}).
%% Old non-versioned variant of the transaction command.
%%
%% Replaced by `#tx_v{}'.

-record(register_trigger, {id :: khepri:trigger_id(),
                           event_filter :: khepri_evf:event_filter(),
                           sproc :: khepri_path:native_path()}).
%% Old non-versioned variant of the trigger registration command.
%%
%% Replaced by `#register_trigger_v{}'.

-record(ack_triggered, {triggered :: [khepri_machine:triggered()]}).
%% Old non-versioned variant of the trigger ack command.
%%
%% Replaced by `#ack_triggered_v{}'.

-record(register_projection, {pattern :: khepri_path:native_pattern(),
                              projection :: khepri_projection:projection()}).
%% Old non-versioned variant of the projection registration command.
%%
%% Replaced by `register_projection_v{}'.

-record(unregister_projection, {name :: khepri_projection:name()}).
%% Command to unregister one projection.
%%
%% Replaced by `#unregister_projections{}'.

-record(unregister_projections, {names :: all | [khepri_projection:name()]}).
%% Old non-versioned projection unregistration command.
%%
%% Replaced by `#unregister_projections_v{}'.

-record(dedup_ack, {ref :: reference()}).
%% Old non-versioned variant of the dedup ack command.
%%
%% Replaced by `#dedup_ack_v{}'.

-record(drop_dedups, {refs :: [reference()]}).
%% Old non-versioned variant of the dedups drop command.
%%
%% Replaced by `#drop_dedups_v{}'.

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

-record(triggered_v2, {id :: khepri:trigger_id(),
                       %% TODO: Do we need a ref to distinguish multiple
                       %% instances of the same trigger?
                       event_filter :: khepri_evf:event_filter(),
                       action :: khepri_event_handler:triggered_action(),
                       where :: khepri_event_handler:trigger_exec_loc(),
                       event = khepri_evf:event()}).
