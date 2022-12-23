%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("src/khepri_payload.hrl").

%% Structure representing each node in the tree, including the root node.

-define(INIT_DATA_VERSION, 1).
-define(INIT_CHILD_LIST_VERSION, 1).
-define(INIT_NODE_PROPS, #{payload_version => ?INIT_DATA_VERSION,
                           child_list_version => ?INIT_CHILD_LIST_VERSION}).

%% TODO: Query this value from Ra itself.
-define(SNAPSHOT_INTERVAL, 4096).

-record(node, {props = ?INIT_NODE_PROPS :: khepri_machine:props(),
               payload = ?NO_PAYLOAD :: khepri_payload:payload(),
               child_nodes = #{} :: #{khepri_path:component() := #node{}}}).

%% Record representing the state machine configuration.
-record(config,
        {store_id :: khepri:store_id(),
         member :: ra:server_id(),
         snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

%% State machine's internal state record.
-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         root = #node{} :: khepri_machine:tree_node(),
         keep_while_conds = #{} :: khepri_machine:keep_while_conds_map(),
         keep_while_conds_revidx = #{}
           :: khepri_machine:keep_while_conds_revidx(),
         triggers = #{} ::
           #{khepri:trigger_id() =>
             #{sproc := khepri_path:native_path(),
               event_filter := khepri_evf:event_filter()}},
         emitted_triggers = [] :: [khepri_machine:triggered()],
         projections = #{} :: khepri_machine:projections_map(),
         locks = #{} :: khepri_machine:lock_map(),
         lock_monitors = #{} :: khepri_machine:lock_monitor_map(),
         metrics = #{} :: #{applied_command_count => non_neg_integer()}}).

-record(khepri_machine_aux,
        {store_id :: khepri:store_id()}).

-record(lock_hold,
        {holder :: pid(),
         acquired_at :: calendar:datetime1970(),
         release_on_disconnect :: boolean()}).

-record(lock_entry,
        {group :: khepri_lock:group(),
         recursive :: boolean(),
         holds = [] :: [#lock_hold{}]}).

%% State machine commands.

-record(put, {path :: khepri_path:native_pattern(),
              payload = ?NO_PAYLOAD :: khepri_payload:payload(),
              options = #{} :: khepri:tree_options() | khepri:put_options()}).

-record(delete, {path :: khepri_path:native_pattern(),
                 options  = #{} :: khepri:tree_options()}).

-record(tx, {'fun' :: khepri_fun:standalone_fun() | khepri_path:pattern(),
             args = [] :: list()}).

-record(register_trigger, {id :: khepri:trigger_id(),
                           event_filter :: khepri_evf:event_filter(),
                           sproc :: khepri_path:native_path()}).

-record(ack_triggered, {triggered :: [khepri_machine:triggered()]}).

-record(triggered, {id :: khepri:trigger_id(),
                    %% TODO: Do we need a ref to distinguish multiple
                    %% instances of the same trigger?
                    event_filter :: khepri_evf:event_filter(),
                    sproc :: khepri_fun:standalone_fun(),
                    props = #{} :: map()}).

-record(register_projection, {pattern :: khepri_path:native_pattern(),
                              projection :: khepri_projection:projection()}).

-record(trigger_projection, {path :: khepri_path:native_path(),
                             old_props :: khepri:node_props(),
                             new_props :: khepri:node_props(),
                             projection :: khepri_projection:projection()}).

-record(attempt_lock, {lock :: khepri_lock:lock(),
                       acquirer :: pid()}).

-record(release_lock, {lock :: khepri_lock:lock(),
                       releaser :: pid()}).

-record(force_release_lock, {lock_id :: khepri_lock:lock_id()}).
