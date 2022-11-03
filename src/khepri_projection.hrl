%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-record(khepri_projection,
        {%% The name of the projection. This atom should be unique to an Erlang
         %% node as it will become the name of the ETS table.
         name :: atom(),
         %% A function compiled by `khepri_fun' which "projects" entries from
         %% a Khepri store into records stored in a "projected" ETS table.
         projection_fun :: khepri_fun:standalone_fun(),
         %% Options passed to `ets:new/2'
         ets_options :: [atom() | tuple()]}).
