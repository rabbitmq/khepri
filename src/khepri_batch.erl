%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Khepri batching API.

-module(khepri_batch).

-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_cluster.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").

-export([new/0, new/1, new/2,

         put/3, put/4,

         commit/1,

         get_pending/1,
         count_pending/1]).

%% For internal use only.
-export([append/2,
         append/3,
         to_command/1]).

-record(draft_batch, {store_id :: khepri:store_id(),
                      batch :: #batch{},
                      mapping_funs :: [khepri_batch:mapping_fun() | none],
                      command_options :: khepri:command_options()}).

-type draft() :: #draft_batch{}.
%% A draft batch.
%%
%% In addition to batched commands, it contains the store ID and command
%% options. This allows a batch to be committed automatically or explicitly at
%% any time.

-type mapping_fun_ret() :: khepri:minimal_ret() |
                           khepri:payload_ret() |
                           khepri:many_payloads_ret().
%% Return value of {@link mapping_fun()}.

-type mapping_fun() :: fun(
                         (khepri:many_payloads_ret()) ->
                           khepri_batch:mapping_fun_ret()).
%% A function to map an advanced result to any term.

-export_type([draft/0,
              mapping_fun/0,
              mapping_fun_ret/0]).

%% -------------------------------------------------------------------
%% new().
%% -------------------------------------------------------------------

-spec new() -> Draft when
      Draft :: khepri_batch:draft().
%% @doc Initializes a draft batch.
%%
%% Calling this function is the same as calling `new(StoreId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see new/1.
%% @see new/2.

new() ->
    StoreId = khepri_cluster:get_default_store_id(),
    new(StoreId).

-spec new(StoreId) -> Draft when
      StoreId :: khepri:store_id(),
      Draft :: khepri_batch:draft().
%% @doc Initializes a draft batch.
%%
%% Calling this function is the same as calling `new(StoreId, #{})'.
%%
%% @see new/2.

new(StoreId) when ?IS_STORE_ID(StoreId) ->
    new(StoreId, #{}).

-spec new(StoreId, Options) -> Draft when
      StoreId :: khepri:store_id(),
      Options :: khepri:batch_options(),
      Draft :: khepri_batch:draft().
%% @doc Initializes a draft batch.
%%
%% @param StoreId the name of the Khepri store.
%% @param Options command and batch options.
%%
%% @returns the draft batch.

new(StoreId, Options) ->
    {CommandOptions, BatchOptions} = split_batch_options(Options),
    Batch = #batch{options = BatchOptions},
    MappingFuns = [],
    Draft = #draft_batch{store_id = StoreId,
                         batch = Batch,
                         mapping_funs = MappingFuns,
                         command_options = CommandOptions},
    Draft.

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
%% This is the same as {@link khepri:put/3} but delayed until batch execution.
%%
%% @see khepri:put/3.

put(Draft, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command:put(PathPattern, Data),
    ?assertEqual(#{}, CommandOptions),
    append(Draft, Command, fun(Ret) -> ?result_ret_to_minimal_ret(Ret) end).

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
%% This is the same as {@link khepri:put/4} but delayed until batch execution.
%%
%% @returns the updated draft batch.

put(Draft, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command:put(
                                  PathPattern, Data, Options),
    ?assertEqual(#{}, CommandOptions),
    append(Draft, Command, fun(Ret) -> ?result_ret_to_minimal_ret(Ret) end).

%% -------------------------------------------------------------------
%% append().
%% -------------------------------------------------------------------

-spec append(Draft, Command) -> NewDraft when
      Draft :: khepri_batch:draft(),
      Command :: khepri_machine:command(),
      NewDraft :: khepri_batch:draft().
%% Appends a command to the batch.
%%
%% @param Draft the draft batch to update.
%%
%% @returns the updated draft batch.

append(Draft, Command) ->
    append(Draft, Command, none).

-spec append(Draft, Command, MappingFun) -> NewDraft when
      Draft :: khepri_batch:draft(),
      Command :: khepri_machine:command(),
      MappingFun :: khepri_batch:mapping_fun() | none,
      NewDraft :: khepri_batch:draft().
%% Appends a command to the batch.
%%
%% @param Draft the draft batch to update.
%%
%% @returns the updated draft batch.

append(
  #draft_batch{batch = Batch,
               mapping_funs = MappingFuns} = Draft,
  Command,
  MappingFun) ->
    Batch1 = append1(Batch, Command),
    MappingFuns1 = [MappingFun | MappingFuns],
    Draft1 = Draft#draft_batch{batch = Batch1,
                               mapping_funs = MappingFuns1},
    Draft1.

append1(#batch{commands = Commands} = Batch, Command) ->
    %% Commands are stored in reversed order. They will be reversed at the
    %% commit time.
    Commands1 = [Command | Commands],
    Batch1 = Batch#batch{commands = Commands1},
    Batch1.

%% -------------------------------------------------------------------
%% commit().
%% -------------------------------------------------------------------

-spec commit(Draft) -> Ret when
      Draft :: khepri_batch:draft(),
      Ret :: khepri:ok([khepri_batch:mapping_fun_ret()]) | khepri:error().
%% Finalized the given draft batch and commits it.
%%
%% @param Draft the draft batch to commit.
%%
%% @returns an `{ok, Rets}' tuple with a list of return values where each
%% entry corresponds to a command inside the batch, or an `{error, Reason}'
%% tuple.

commit(#draft_batch{store_id = StoreId,
                    batch = Batch,
                    command_options = CommandOptions} = Draft) ->
    Command = to_command(Batch),
    case to_command(Batch) of
        #batch{} = Command ->
            Ret = khepri_machine:process_command(
                    StoreId, Command, CommandOptions),
            map_result(Draft, Ret);
        Command when is_tuple(Command) ->
            Ret = khepri_machine:process_command(
                    StoreId, Command, CommandOptions),
            Ret1 = case Ret of
                       {ok, _} -> {ok, [Ret]};
                       Error   -> Error
                   end,
            map_result(Draft, Ret1);
        none ->
            {ok, []}
    end.

map_result(#draft_batch{mapping_funs = MappingFuns}, {ok, Rets}) ->
    MappingFuns1 = lists:reverse(MappingFuns),
    map_result1(Rets, MappingFuns1, []);
map_result(_Draft, Error) ->
    Error.

map_result1([Ret | Rets], [none | MappingFuns], MappedRets) ->
    MappedRets1 = [Ret | MappedRets],
    map_result1(Rets, MappingFuns, MappedRets1);
map_result1([Ret | Rets], [MappingFun | MappingFuns], MappedRets) ->
    MappedRet = MappingFun(Ret),
    MappedRets1 = [MappedRet | MappedRets],
    map_result1(Rets, MappingFuns, MappedRets1);
map_result1([], [], MappedRets) ->
    MappedRets1 = lists:reverse(MappedRets),
    {ok, MappedRets1}.

%% -------------------------------------------------------------------
%% get_pending().
%% -------------------------------------------------------------------

-spec get_pending(Draft) -> Commands when
      Draft :: khepri_batch:draft(),
      Commands :: [khepri_machine:command()].
%% @doc Returns the list of pending commands.
%%
%% @param Draft the draft batch to query.
%%
%% @returns the list of pending commands.

get_pending(#draft_batch{batch = Batch}) ->
    get_pending1(Batch).

get_pending1(#batch{commands = Commands}) ->
    Commands1 = lists:reverse(Commands),
    Commands1.

%% -------------------------------------------------------------------
%% count_pending().
%% -------------------------------------------------------------------

-spec count_pending(Draft) -> Count when
      Draft :: khepri_batch:draft(),
      Count :: non_neg_integer().
%% @doc Returns the number of pending commands.
%%
%% @param Draft the draft batch to query.
%%
%% @returns the number of pending commands.

count_pending(#draft_batch{batch = Batch}) ->
    count_pending1(Batch).

count_pending1(#batch{commands = Commands}) ->
    Count = length(Commands),
    Count.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec split_batch_options(Options) ->
    {CommandOptions, BatchOptions} when
      Options :: CommandOptions | BatchOptions,
      CommandOptions :: khepri:command_options(),
      BatchOptions :: khepri:batch_options().
%% @private

split_batch_options(Options) ->
    Options1 = set_default_options(Options),
    maps:fold(
      fun
          (Option, Value, {C, B}) when
                Option =:= reply_from orelse
                Option =:= timeout orelse
                Option =:= async ->
              C1 = C#{Option => Value},
              {C1, B};
          (atomic = Option, Value, {C, B}) when is_boolean(Value) ->
              B1 = B#{Option => Value},
              {C, B1}
      end, {#{}, #{}}, Options1).

set_default_options(Options) ->
    %% By default, consider commands inside the batch as independent: if one
    %% fails, other commands are still applied.
    Options1 = case Options of
                   #{atomic := _} ->
                       Options;
                   _ ->
                       Options#{atomic => false}
               end,
    Options1.

-spec to_command(Draft) -> Command | none when
      Draft :: khepri_batch:draft() | #batch{},
      Command :: khepri_machine:command().
%% @private

to_command(#draft_batch{batch = Batch}) ->
    to_command(Batch);
to_command(#batch{commands = []}) ->
    none;
to_command(#batch{commands = [Command]}) ->
    Command;
to_command(#batch{commands = Commands} = Batch) ->
    Batch1 = Batch#batch{commands = lists:reverse(Commands)},
    Batch1.
