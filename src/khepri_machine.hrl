%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% TODO: Query this value from Ra itself.
-define(SNAPSHOT_INTERVAL, 4096).

-record(config,
        {snapshot_interval = ?SNAPSHOT_INTERVAL :: non_neg_integer()}).

-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         root = #node{stat = ?INIT_NODE_STAT} :: khepri_machine:tree_node(),
         keep_untils = #{} :: khepri_machine:keep_untils_map(),
         keep_untils_revidx = #{} :: khepri_machine:keep_untils_revidx(),
         metrics = #{}}).
