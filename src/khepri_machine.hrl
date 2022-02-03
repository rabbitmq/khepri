%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% TODO: Query this value from Ra itself.
-define(SNAPSHOT_INTERVAL, 4096).

-record(config,
        {snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         root = #node{stat = ?INIT_NODE_STAT} :: khepri_machine:tree_node(),
         keep_while_conds = #{} :: khepri_machine:keep_while_conds_map(),
         keep_while_conds_revidx = #{}
           :: khepri_machine:keep_while_conds_revidx(),
         metrics = #{}}).
