%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(DEFAULT_RA_SYSTEM_NAME, khepri).
-define(DEFAULT_STORE_ID, khepri).

%% timer:sleep/1 time used as a retry interval when the local Ra server is
%% unaware of a leader exit.
-define(NOPROC_RETRY_INTERVAL, 200).

-define(IS_TIMEOUT(Timeout), (Timeout =:= infinity orelse
                              (is_integer(Timeout) andalso Timeout >= 0))).
