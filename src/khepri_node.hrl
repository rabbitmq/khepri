%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-include("src/khepri_payload.hrl").

%% Structure representing each node in the tree, including the root node.

-define(INIT_DATA_VERSION, 1).
-define(INIT_CHILD_LIST_VERSION, 1).
-define(INIT_NODE_PROPS, #{payload_version => ?INIT_DATA_VERSION,
                           child_list_version => ?INIT_CHILD_LIST_VERSION}).

-define(DEFAULT_PROPS_TO_RETURN, [payload,
                                  payload_version,
                                  delete_reason]).

-record(node, {props = ?INIT_NODE_PROPS :: khepri_machine:props(),
               payload = ?NO_PAYLOAD :: khepri_payload:payload(),
               child_nodes = #{} :: #{khepri_path:component() := #node{}}}).
