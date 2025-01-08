%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-record(khepri_projection,
        {%% The name of the projection. This atom should be unique to an Erlang
         %% node as it will become the name of the ETS table.
         name :: atom(),
         %% A function compiled by Horus which "projects" entries from
         %% a Khepri store into records stored in a "projected" ETS table.
         %% This field may be the atom `copy' instead. This is a special case
         %% for a simple and common-case projection that inserts each tree
         %% node's payload as the record in the ETS table.
         projection_fun :: copy | horus:horus_fun(),
         %% Options passed to `ets:new/2'
         ets_options :: [atom() | tuple()]}).
