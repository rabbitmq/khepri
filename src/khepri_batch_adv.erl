%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Khepri advanced batching API.

-module(khepri_batch_adv).

-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").

-export([put/3, put/4]).

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec put(Draft, PathPattern, Data) -> NewDraft when
      Draft :: khepri_batch:draft(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      NewDraft :: khepri_batch:draft().
%% @doc Appends a put command to the given draft batch.
%%
%% When the batch is committed, this sets the payload of the tree node pointed
%% to by the given path pattern.
%%
%% This is the same as {@link khepri_adv:put/3} but delayed until batch
%% execution.
%%
%% @see khepri:put/3.

put(Draft, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:put(PathPattern, Data),
    ?assertEqual(#{}, CommandOptions),
    khepri_batch:append(
      Draft,
      Command,
      fun(Ret) -> ?common_ret_to_single_result_ret(Ret) end).

-spec put(Draft, PathPattern, Data, Options) -> NewDraft when
      Draft :: khepri_batch:draft(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      NewDraft :: khepri_batch:draft().
%% @doc Appends a put command to the given draft batch.
%%
%% When the batch is committed, this sets the payload of the tree node pointed
%% to by the given path pattern.
%%
%% This is the same as {@link khepri_adv:put/4} but delayed until batch
%% execution.
%%
%% @returns the updated draft batch.

put(Draft, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:put(
                                  PathPattern, Data, Options),
    ?assertEqual(#{}, CommandOptions),
    khepri_batch:append(
      Draft,
      Command,
      fun(Ret) -> ?common_ret_to_single_result_ret(Ret) end).
