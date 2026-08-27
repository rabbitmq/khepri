%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-record(batch, {commands = [] :: [khepri_machine:command()],
                options = #{} :: khepri_batch:options(),
                machine_version :: ra_machine:version()}).

-define(IS_KHEPRI_BATCH(Batch), is_record(Batch, batch)).
