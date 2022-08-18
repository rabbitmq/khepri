%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(
   report_invalid_call(Format, Args),
   erlang:error(
     {khepri, invalid_call,
      lists:flatten(
        io_lib:format(
          "Invalid use of ~s:~s/~b:~n" ++ (Format),
          [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY] ++ (Args)))})).

-define(
   reject_path_targetting_many_nodes(PathPattern),
   ?report_invalid_call(
      "Called with a path pattern which could match many nodes:~n~p",
      [PathPattern])).

-define(
   report_invalid_path(Path),
   erlang:error(
     {khepri, invalid_path,
      lists:flatten(
        io_lib:format(
          "Invalid path or path pattern passed to ~s:~s/~b:~n~p",
          [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Path]))})).

-define(
   reject_update_in_ro_tx(),
   erlang:error(
     {khepri, denied_update_in_readonly_tx,
      "Updates to the tree are denied in a read-only transaction"})).
