%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("src/khepri_tree.hrl").

%% TODO: Query this value from Ra itself.
-define(SNAPSHOT_INTERVAL, 4096).

%% Record representing the state machine configuration.
-record(config,
        {store_id :: khepri:store_id(),
         member :: ra:server_id(),
         snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

%% State machine's internal state record.
-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         tree = #tree{} :: khepri_machine:tree(),
         triggers = #{} ::
           #{khepri:trigger_id() =>
             #{sproc := khepri_path:native_path(),
               event_filter := khepri_evf:event_filter()}},
         emitted_triggers = [] :: [khepri_machine:triggered()],
         projections = #{} :: khepri_machine:projections_map(),
         metrics = #{} :: #{applied_command_count => non_neg_integer()}}).

-record(khepri_machine_aux,
        {store_id :: khepri:store_id()}).

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
