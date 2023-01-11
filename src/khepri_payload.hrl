%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(NO_PAYLOAD, '$__NO_PAYLOAD__').
-record(p_data, {data :: khepri:data()}).
-record(p_sproc, {sproc :: khepri_fun:standalone_fun(),
                  is_valid_as_tx_fun :: ro | rw | false}).

-define(IS_KHEPRI_PAYLOAD(Payload), (Payload =:= ?NO_PAYLOAD orelse
                                     is_record(Payload, p_data) orelse
                                     is_record(Payload, p_sproc))).
