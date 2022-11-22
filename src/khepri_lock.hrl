%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(khepri_lock, {store_id :: khepri:store_id(),
                      lock_id :: khepri_lock:lock_id(),
                      group :: khepri_lock:group(),
                      recursive :: boolean(),
                      release_on_disconnect :: boolean()}).
