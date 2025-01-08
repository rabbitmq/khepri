%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-define(TX_STATE_KEY, khepri_tx_machine_state).
-define(TX_PROPS, khepri_tx_properties).

-define(TX_ABORT(Reason), {'$__ABORTED_KHEPRI_TX__', Reason}).
