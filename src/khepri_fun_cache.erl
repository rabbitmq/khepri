%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc A cache interface for transaction functions

-module(khepri_fun_cache).

-export([get/2, put/2]).

-callback get(Key :: term(), Default :: term()) -> term().
-callback put(Key :: term(), Fun :: term()) -> ok.

get(Key, Default) -> persistent_term:get(Key, Default).
put(Key, Fun) -> persistent_term:put(Key, Fun).
