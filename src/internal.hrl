%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(INIT_DATA_VERSION, 1).
-define(INIT_CHILD_LIST_VERSION, 1).
-define(INIT_NODE_STAT, #{payload_version => ?INIT_DATA_VERSION,
                          child_list_version => ?INIT_CHILD_LIST_VERSION}).

-define(TX_STATE_KEY, khepri_tx_machine_state).
-define(TX_PROPS, khepri_tx_properties).

%% Structure representing each node in the tree, including the root node.
%% TODO: Rename stat to something more correct?
-record(node, {stat = ?INIT_NODE_STAT :: khepri_machine:stat(),
               payload = none :: khepri_machine:payload(),
               child_nodes = #{} :: #{khepri_path:component() := #node{}}}).

%% State machine commands.

-record(put, {path :: khepri_path:pattern(),
              payload = none :: khepri_machine:payload(),
              extra = #{} :: #{keep_while =>
                               khepri_machine:keep_while_conds_map()}}).

-record(delete, {path :: khepri_path:pattern()}).

-record(tx, {'fun' :: khepri_fun:standalone_fun()}).

%% Structure representing an anonymous function "extracted" as a compiled
%% module for storage.
-record(standalone_fun, {module :: module(),
                         beam :: binary(),
                         arity :: arity(),
                         env :: list()}).
