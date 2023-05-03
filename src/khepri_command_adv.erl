%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Khepri command construction (advanced use cases).

-module(khepri_command_adv).

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-export([put/2, put/3,
         put_many/2, put_many/3,
         create/2, create/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         clear_many_payloads/1, clear_many_payloads/2]).

-spec put(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

put(PathPattern, Data) ->
    put(PathPattern, Data, #{}).

-spec put(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

put(PathPattern, Data, Options) ->
    Options1 = Options#{expect_specific_node => true},
    put_many(PathPattern, Data, Options1).

-spec put_many(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

put_many(PathPattern, Data) ->
    put_many(PathPattern, Data, #{}).

-spec put_many(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

put_many(PathPattern, Data, Options) ->
    do_put(PathPattern, Data, Options).

-spec create(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

create(PathPattern, Data) ->
    create(PathPattern, Data, #{}).

-spec create(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

create(PathPattern, Data, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = false}]),
    Options1 = Options#{expect_specific_node => true},
    do_put(PathPattern2, Data, Options1).

-spec update(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

update(PathPattern, Data) ->
    update(PathPattern, Data, #{}).

-spec update(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

update(PathPattern, Data, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = true}]),
    Options1 = Options#{expect_specific_node => true},
    do_put(PathPattern2, Data, Options1).

-spec compare_and_swap(PathPattern, DataPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

compare_and_swap(PathPattern, DataPattern, Data) ->
    compare_and_swap(PathPattern, DataPattern, Data, #{}).

-spec compare_and_swap(PathPattern, DataPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

compare_and_swap(PathPattern, DataPattern, Data, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_data_matches{pattern = DataPattern}]),
    Options1 = Options#{expect_specific_node => true},
    do_put(PathPattern2, Data, Options1).

-spec clear_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

clear_many_payloads(PathPattern) ->
    clear_many_payloads(PathPattern, #{}).

-spec clear_many_payloads(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @private

clear_many_payloads(PathPattern, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = true}]),
    do_put(PathPattern2, khepri_payload:none(), Options).

%% -------------------------------------------------------------------
%% Internal command creation.
%% -------------------------------------------------------------------

-spec do_put(PathPattern, Payload, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: {Command, CommandOptions},
      Command :: #put{},
      CommandOptions :: khepri:command_options().
%% @doc Prepares a `put' command.
%%
%% @private

do_put(PathPattern, Payload, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Payload1 = khepri_payload:wrap(Payload),
    Payload2 = khepri_payload:prepare(Payload1),
    {CommandOptions,
     TreeAndPutOptions} = khepri_machine:split_command_options(Options),
    Command = #put{path = PathPattern1,
                   payload = Payload2,
                   options = TreeAndPutOptions},
    {Command, CommandOptions}.
