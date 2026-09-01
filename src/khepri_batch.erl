%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(khepri_batch).

-include("include/khepri.hrl").
-include("src/khepri_batch.hrl").

-export([new/0, new/1,
         add/3,
         submit/1, submit/2, submit/3,
         is_batch/1,
         get_commands/1,
         get_options/1,
         get_machine_version/1]).

-type batch() :: #batch{}.
%% A batch record.
%%
%% The batch record holds:
%% <ul>
%% <li>the reverse-ordered list of batched commands</li>
%% <li>the state machine version the batched commands are based on</li>
%% <li>some some options</li>
%% </ul>

-type options() :: #{machine_version_from_store => khepri:store_id()}.
%% Options specific to batches.
%%
%% <ul>
%% <li>`machine_version_from_store' indicates the store ID to query the
%% effective machine version from. This is used as the reference machine
%% version for commands added to the batch, because the actual store ID is only
%% known when the batch is actually submitted. This avoids a machine version
%% query per added command and a change of version in the middle of a batch.
%% Defaults to the default store ID.</li>
%% </ul>

-type batched_command_ret() :: any().
%% Return value of a batch command.
%%
%% The batch itself does not know what return values are for each batch
%% command, therefore the type is set to be anything.

-export_type([batch/0,
              options/0,
              batched_command_ret/0]).

-spec new() -> Batch when
      Batch :: khepri_batch:batch().
%% @doc Creates a new empty batch.
%%
%% Calling this function is the same as calling `new(#{})'.
%%
%% @see new/1.

new() ->
    new(#{}).

-spec new(Options) -> Batch when
      Options :: khepri_batch:options(),
      Batch :: khepri_batch:batch().
%% @doc Creates a new empty batch.
%%
%% A new batch is initialised with a specific state machine version. This is
%% later used when a command is appended to the batch: the command needs to
%% know that machine version to determine what is supported and what is not.
%% The state machine version is queried from the store given in the
%% `machine_version_from_store' option or from the default store (as in {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see new/0.

new(#{} = Options) ->
    MacVer = determine_effective_machine_version_to_use(Options),
    Batch = #batch{options = Options,
                   machine_version = MacVer},
    Batch.

-spec determine_effective_machine_version_to_use(Options) -> MacVer when
      Options :: khepri_batch:options(),
      MacVer :: non_neg_integer().
%% @doc Determine the state machine version to use when creating and filling
%% the batch.
%%
%% The state machine version is queried from the store specified in the options
%% or the default store if none was specified.
%%
%% If the query fails for whatever reasons, the machine version defaults to 0.

determine_effective_machine_version_to_use(
  #{machine_version_from_store := StoreId}) ->
    use_effective_machive_version_from_store(StoreId);
determine_effective_machine_version_to_use(
  _Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    use_effective_machive_version_from_store(StoreId).

use_effective_machive_version_from_store(StoreId)
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    %% If we fail to query the effective machine version, we default to 0.
    case khepri_machine:effective_version(StoreId) of
        {ok, EffectiveMacVer} -> EffectiveMacVer;
        {error, _}            -> 0
    end.

add(#batch{commands = Commands} = Batch, Command, _Options) ->
    Commands1 = [Command | Commands],
    Batch1 = Batch#batch{commands = Commands1},
    Batch1.

-spec submit(Batch) -> Ret when
      Batch :: khepri_batch:batch(),
      Ret :: khepri:ok([khepri_batch:batched_command_ret()]) | khepri:error().
%% @doc Submits a batch to the default store.
%%
%% Calling this function is the same as calling `submit(StoreId, Batch)' with
%% the default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see submit/2.
%% @see submit/3.

submit(Batch) ->
    StoreId = khepri_cluster:get_default_store_id(),
    submit(StoreId, Batch).

-spec submit
(StoreId, Batch) -> Ret when
      StoreId :: khepri:store_id(),
      Batch :: khepri_batch:batch(),
      Ret :: khepri:ok([khepri_batch:batched_command_ret()]) | khepri:error();
(Batch, Options) -> Ret when
      Batch :: khepri_batch:batch(),
      Options :: khepri:command_options(),
      Ret :: khepri:ok([khepri_batch:batched_command_ret()]) | khepri:error().
%% @doc Submits a batch to a store.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`submit(StoreId, Batch)'. Calling it is the same as calling
%% `submit(StoreId, Batch, #{})'.</li>
%% <li>`submit(Batch, Options)'. Calling it is the same as calling
%% `submit(StoreId, Batch, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see submit/3.

submit(StoreId, Batch) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    submit(StoreId, Batch, #{});
submit(Batch, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    submit(StoreId, Batch, Options).

-spec submit(StoreId, Batch, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Batch :: khepri_batch:batch(),
      Options :: khepri:command_options(),
      Ret :: khepri:ok([khepri_batch:batched_command_ret()]) | khepri:error().
%% @doc Submits a batch to a store.
%%
%% The batch is submitted as is to the given store. If the store was upgraded
%% to a newer machine version, the batched commands will still be based on the
%% state machine version used when the batch was created.
%%
%% If the batch is empty, `{ok, []}' is returned immediately, regardless of the
%% status of the store. Thus, it will return success even if the store does not
%% exist, is not running, or is in a state where it cannot make progress.
%%
%% @param StoreId the name of the Khepri store.
%% @param Batch the batch to submit
%% @param Options command options such as the command type.
%%
%% @returns `{ok, ListOfCommandReturnValues}' or an `{error, Reason}' tuple.

submit(StoreId, #batch{} = Batch, Options) ->
    khepri_machine:submit_batch(StoreId, Batch, Options).

-spec is_batch(Term) -> IsBatch when
      Term :: any(),
      IsBatch :: boolean().
%% @doc Indicates if the argument is a batch.

is_batch(Term) ->
    ?IS_KHEPRI_BATCH(Term).

-spec get_commands(Batch) -> Commands when
      Batch :: khepri_batch:batch(),
      Commands :: [khepri_machine:command()].
%% @doc Returns the list of commands from the batch.

get_commands(#batch{commands = Commands}) ->
    lists:reverse(Commands).

-spec get_options(Batch) -> Options when
      Batch :: khepri_batch:batch(),
      Options :: khepri_batch:options().
%% @doc Returns the batch options.

get_options(#batch{options = Options}) ->
    Options.

-spec get_machine_version(Batch) -> MacVer when
      Batch :: khepri_batch:batch(),
      MacVer :: non_neg_integer().
%% @doc Returns the machine version the batch is based on.

get_machine_version(#batch{machine_version = MacVer}) ->
    MacVer.
