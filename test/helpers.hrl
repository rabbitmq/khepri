%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(META, #{index => ?LINE,
                term => 1,
                system_time => ?LINE}).

-define(MACH_PARAMS(),
        #{store_id => ?FUNCTION_NAME,
          member => khepri_cluster:this_member(?FUNCTION_NAME)}).
-define(MACH_PARAMS(Commands),
        #{store_id => ?FUNCTION_NAME,
          member => khepri_cluster:this_member(?FUNCTION_NAME),
          commands => Commands}).
