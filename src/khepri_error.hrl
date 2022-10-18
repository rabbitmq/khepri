%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(
   khepri_error(Name, Props),
   {khepri, Name, Props}).

-define(
   khepri_exception(Name, Props),
   {khepri_ex, Name, Props}).

-define(
   khepri_misuse(Exception),
   erlang:error(Exception)).

-define(
   khepri_misuse(Name, Props),
   ?khepri_misuse(?khepri_exception(Name, Props))).

-define(
   khepri_raise_misuse(Name, Props, Stacktrace),
   erlang:raise(error, ?khepri_exception(Name, Props), Stacktrace)).
