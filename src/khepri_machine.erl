%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Khepri private low-level API.
%%
%% This module exposes the private "low-level" API to the Khepri database and
%% state machine. Main functions correspond to Ra commands implemented by the
%% state machine. All functions in {@link khepri} are built on top of this
%% module.
%%
%% This module is private. The documentation is still visible because it may
%% help understand some implementation details. However, this module should
%% never be called directly outside of Khepri.

-module(khepri_machine).
-behaviour(ra_machine).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_evf.hrl").
-include("src/khepri_fun.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").
-include("src/khepri_tx.hrl").

-export([get/3,
         count/3,
         put/4,
         delete/3,
         transaction/5,
         run_sproc/4,
         register_trigger/5]).
-export([get_keep_while_conds_state/2]).

%% ra_machine callbacks.
-export([init/1,
         apply/3,
         state_enter/2]).

%% For internal use only.
-export([clear_cache/1,
         ack_triggers_execution/2,
         split_query_options/1,
         split_command_options/1,
         find_matching_nodes/3,
         count_matching_nodes/3,
         insert_or_update_node/5,
         delete_matching_nodes/3,
         handle_tx_exception/1]).

-ifdef(TEST).
-export([are_keep_while_conditions_met/2,
         get_root/1,
         get_keep_while_conds/1,
         get_keep_while_conds_revidx/1,
         get_last_consistent_call_atomics/1]).
-endif.

-compile({no_auto_import, [apply/3]}).

-type tree_node() :: #node{}.
%% A node in the tree structure.

-type props() :: #{payload_version := khepri:payload_version(),
                   child_list_version := khepri:child_list_version()}.
%% Properties attached to each node in the tree structure.

-type triggered() :: #triggered{}.

-type command() :: #put{} |
                   #delete{} |
                   #tx{} |
                   #register_trigger{} |
                   #ack_triggered{}.
%% Commands specific to this Ra machine.

-type machine_init_args() :: #{store_id := khepri:store_id(),
                               member := ra:server_id(),
                               snapshot_interval => non_neg_integer(),
                               commands => [command()],
                               atom() => any()}.
%% Structure passed to {@link init/1}.

-type machine_config() :: #config{}.
%% Configuration record, holding read-only or rarely changing fields.

-type keep_while_conds_map() :: #{khepri_path:native_path() =>
                                  khepri_condition:native_keep_while()}.
%% Per-node `keep_while' conditions.

-type keep_while_conds_revidx() :: #{khepri_path:native_path() =>
                                     #{khepri_path:native_path() => ok}}.
%% Internal reverse index of the keep_while conditions. If node A depends on a
%% condition on node B, then this reverse index will have a "node B => node A"
%% entry.

-type keep_while_aftermath() :: #{khepri_path:native_path() =>
                                  khepri:node_props() | delete}.
%% Internal index of the per-node changes which happened during a traversal.
%% This is used when the tree is walked back up to determine the list of tree
%% nodes to remove after some keep_while condition evaluates to false.

-type state() :: #?MODULE{}.
%% State of this Ra state machine.

-type query_fun() :: fun((state()) -> any()).
%% Function representing a query and used {@link process_query/3}.

-type walk_down_the_tree_extra() :: #{include_root_props =>
                                      boolean(),
                                      keep_while_conds =>
                                      keep_while_conds_map(),
                                      keep_while_conds_revidx =>
                                      keep_while_conds_revidx(),
                                      keep_while_aftermath =>
                                      keep_while_aftermath()}.

-type walk_down_the_tree_fun() ::
    fun((khepri_path:native_path(),
         tree_node() | {interrupted, any(), map()},
         Acc :: any()) ->
        ok(tree_node() | keep | delete, any()) |
        khepri:error()).
%% Function called to handle a node found (or an error) and used in {@link
%% walk_down_the_tree/6}.

-type ok(Type1, Type2) :: {ok, Type1, Type2}.
-type ok(Type1, Type2, Type3) :: {ok, Type1, Type2, Type3}.

-type common_ret() :: khepri:ok(khepri_adv:node_props_map()) |
                      khepri:error().

-type tx_ret() :: khepri:ok(khepri_tx:tx_fun_result()) |
                  khepri_tx:tx_abort() |
                  no_return().

-type async_ret() :: ok.

-export_type([common_ret/0,
              tx_ret/0,
              async_ret/0,

              state/0,
              machine_config/0,
              tree_node/0,
              props/0,
              triggered/0,
              keep_while_conds_map/0,
              keep_while_conds_revidx/0]).

-define(HAS_TIME_LEFT(Timeout), (Timeout =:= infinity orelse Timeout > 0)).

%% -------------------------------------------------------------------
%% Machine protocol.
%% -------------------------------------------------------------------

%% TODO: Verify arguments carefully to avoid the construction of an invalid
%% command.

-spec get(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri_machine:common_ret().
%% @doc Returns all tree nodes matching the given path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to get.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, NodePropsMap}' tuple with a map with zero, one or more
%% entries, or an `{error, Reason}' tuple.

get(StoreId, PathPattern, Options) when ?IS_STORE_ID(StoreId) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    {QueryOptions, TreeOptions} = split_query_options(Options),
    Query = fun(#?MODULE{root = Root}) ->
                    find_matching_nodes(Root, PathPattern1, TreeOptions)
            end,
    process_query(StoreId, Query, QueryOptions).

-spec count(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(Count) | khepri:error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to count.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, Count}' tuple with the number of matching tree nodes, or
%% an `{error, Reason}' tuple.

count(StoreId, PathPattern, Options) when ?IS_STORE_ID(StoreId) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    {QueryOptions, TreeOptions} = split_query_options(Options),
    Query = fun(#?MODULE{root = Root}) ->
                    count_matching_nodes(Root, PathPattern1, TreeOptions)
            end,
    process_query(StoreId, Query, QueryOptions).

-spec put(StoreId, PathPattern, Payload, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_payload:payload(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_machine:common_ret() | khepri_machine:async_ret().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the node to create or
%%        modify.
%% @param Payload the payload to put in the specified node.
%% @param Options command, tree and put options.
%%
%% @returns in the case of a synchronous put, an `{ok, NodePropsMap}' tuple
%% with a map with zero, one or more entries, or an `{error, Reason}' tuple;
%% in the case of an asynchronous put, always `ok' (the actual return value
%% may be sent by a message if a correlation ID was specified).
%%
%% @private

put(StoreId, PathPattern, Payload, Options)
  when ?IS_STORE_ID(StoreId) andalso ?IS_KHEPRI_PAYLOAD(Payload) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Payload1 = khepri_payload:prepare(Payload),
    {CommandOptions, TreeOptions, Extra} = split_command_options(Options),
    Command = #put{path = PathPattern1,
                   payload = Payload1,
                   extra = Extra,
                   options = TreeOptions},
    process_command(StoreId, Command, CommandOptions);
put(_StoreId, PathPattern, Payload, _Options) ->
    ?khepri_misuse(invalid_payload, #{path => PathPattern,
                                      payload => Payload}).

-spec delete(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_machine:common_ret() | khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to delete.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous delete, an `{ok, NodePropsMap}' tuple
%% with a map with zero, one or more entries, or an `{error, Reason}' tuple;
%% in the case of an asynchronous put, always `ok' (the actual return value
%% may be sent by a message if a correlation ID was specified).

delete(StoreId, PathPattern, Options) when ?IS_STORE_ID(StoreId) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    {CommandOptions, TreeOptions, _Extra} = split_command_options(Options),
    %% TODO: Ensure `Extra' is unset.
    Command = #delete{path = PathPattern1,
                      options = TreeOptions},
    process_command(StoreId, Command, CommandOptions).

-spec transaction(StoreId, FunOrPath, Args, ReadWrite, Options) -> Ret when
      StoreId :: khepri:store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      ReadWrite :: ro | rw | auto,
      Options :: khepri:command_options() | khepri:query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().
%% @doc Runs a transaction and returns the result.
%%
%% @param StoreId the name of the Ra cluster.
%% @param FunOrPath an arbitrary anonymous function or a path pattern pointing
%%        to a stored procedure.
%% @param Args a list of arguments to pass to `FunOrPath'.
%% @param ReadWrite the read/write or read-only nature of the transaction.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous transaction, `{ok, Result}' where
%% `Result' is the return value of `FunOrPath', or `{error, Reason}' if the
%% anonymous function was aborted; in the case of an asynchronous transaction,
%% always `ok' (the actual return value may be sent by a message if a
%% correlation ID was specified).

transaction(StoreId, Fun, Args, auto = ReadWrite, Options)
  when ?IS_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    case khepri_tx_adv:to_standalone_fun(Fun, ReadWrite) of
        #standalone_fun{} = StandaloneFun ->
            readwrite_transaction(StoreId, StandaloneFun, Args, Options);
        _ ->
            readonly_transaction(StoreId, Fun, Args, Options)
    end;
transaction(StoreId, PathPattern, Args, auto, Options)
  when ?IS_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    readwrite_transaction(StoreId, PathPattern1, Args, Options);
transaction(StoreId, Fun, Args, rw = ReadWrite, Options)
  when ?IS_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    StandaloneFun = khepri_tx_adv:to_standalone_fun(Fun, ReadWrite),
    readwrite_transaction(StoreId, StandaloneFun, Args, Options);
transaction(StoreId, PathPattern, Args, rw, Options)
  when ?IS_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    readwrite_transaction(StoreId, PathPattern1, Args, Options);
transaction(StoreId, Fun, Args, ro, Options)
  when ?IS_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    readonly_transaction(StoreId, Fun, Args, Options);
transaction(StoreId, PathPattern, Args, ro, Options)
  when ?IS_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    readonly_transaction(StoreId, PathPattern1, Args, Options);
transaction(StoreId, Fun, Args, ReadWrite, Options)
  when ?IS_STORE_ID(StoreId) andalso
       is_function(Fun) andalso
       is_list(Args) andalso
       is_atom(ReadWrite) andalso
       is_map(Options) ->
    {arity, Arity} = erlang:fun_info(Fun, arity),
    ?khepri_misuse(
       denied_tx_fun_with_invalid_args,
       #{'fun' => Fun, arity => Arity, args => Args}).

-spec readonly_transaction(StoreId, FunOrPath, Args, Options) -> Ret when
      StoreId :: khepri:store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: khepri:query_options(),
      Ret :: khepri_machine:tx_ret().

readonly_transaction(StoreId, Fun, Args, Options)
  when is_list(Args) andalso is_function(Fun, length(Args)) ->
    Query = fun(State) ->
                    %% It is a read-only transaction, therefore we assert that
                    %% the state is unchanged and that there are no side
                    %% effects.
                    {State, Ret, []} = khepri_tx_adv:run(
                                         State, Fun, Args, false),
                    Ret
            end,
    case process_query(StoreId, Query, Options) of
        {exception, _, _, _} = Exception ->
            handle_tx_exception(Exception);
        Ret ->
            {ok, Ret}
    end;
readonly_transaction(StoreId, PathPattern, Args, Options)
  when ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso is_list(Args) ->
    Query = fun(State) ->
                    %% It is a read-only transaction, therefore we assert that
                    %% the state is unchanged and that there are no side
                    %% effects.
                    {State, Ret, []} = locate_sproc_and_execute_tx(
                                         State, PathPattern, Args, false),
                    Ret
            end,
    case process_query(StoreId, Query, Options) of
        {exception, _, _, _} = Exception ->
            handle_tx_exception(Exception);
        Ret ->
            {ok, Ret}
    end.

-spec readwrite_transaction(StoreId, FunOrPath, Args, Options) -> Ret when
      StoreId :: khepri:store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_fun:standalone_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: khepri:command_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().

readwrite_transaction(
  StoreId, #standalone_fun{arity = Arity} = StandaloneFun, Args, Options)
  when is_list(Args) andalso length(Args) =:= Arity ->
    readwrite_transaction1(StoreId, StandaloneFun, Args, Options);
readwrite_transaction(
  StoreId, Fun, Args, Options)
  when is_list(Args) andalso is_function(Fun, length(Args)) ->
    readwrite_transaction1(StoreId, Fun, Args, Options);
readwrite_transaction(
  StoreId, PathPattern, Args, Options)
  when ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso is_list(Args) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    readwrite_transaction1(StoreId, PathPattern1, Args, Options).

readwrite_transaction1(StoreId, StandaloneFunOrPath, Args, Options) ->
    Command = #tx{'fun' = StandaloneFunOrPath, args = Args},
    case process_command(StoreId, Command, Options) of
        {exception, _, _, _} = Exception ->
            handle_tx_exception(Exception);
        ok = Ret ->
            CommandType = select_command_type(Options),
            case CommandType of
                sync          -> {ok, Ret};
                {async, _, _} -> Ret
            end;
        Ret ->
            {ok, Ret}
    end.

handle_tx_exception(
  {exception, _, ?TX_ABORT(Reason), _}) ->
    {error, Reason};
handle_tx_exception(
  {exception, error, ?khepri_exception(_, _) = Reason, _Stacktrace}) ->
    %% If the exception is a programming misuse of Khepri, we
    %% re-throw a new exception instead of using `erlang:raise()'.
    %%
    %% The reason is that the default stacktrace is limited to 8 frames by
    %% default (see `erlang:system_flag(backtrace_depth, Depth)' to reconfigure
    %% it). Most if not all of those 8 frames might be taken by Khepri's
    %% internal calls, making the stacktrace uninformative to the caller.
    %%
    %% By throwing a new exception, we increase the chance that there
    %% is a frame pointing to the transaction function.
    ?khepri_misuse(Reason);
handle_tx_exception(
  {exception, Class, Reason, Stacktrace}) ->
    erlang:raise(Class, Reason, Stacktrace).

-spec run_sproc(StoreId, PathPattern, Args, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Args :: [any()],
      Options :: khepri:query_options(),
      Ret :: any().
%% @doc Executes a stored procedure.
%%
%% The stored procedure is executed in the context of the caller of {@link
%% run_sproc/3}.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path to the stored procedure.
%% @param Args the list of args to pass to the stored procedure; its length
%%        must be equal to the stored procedure arity.
%% @param Options options to tune the tree traversal or the returned structure
%%        content.
%%
%% @returns the result of the stored procedure execution, or throws an
%% exception if the node does not exist, does not hold a stored procedure or
%% if there was an error.

run_sproc(StoreId, PathPattern, Args, Options)
  when ?IS_STORE_ID(StoreId) andalso is_list(Args) ->
    Options1 = Options#{expect_specific_node => true},
    case get(StoreId, PathPattern, Options1) of
        {ok, NodePropsMap} ->
            [NodeProps] = maps:values(NodePropsMap),
            case NodeProps of
                #{sproc := StandaloneFun} ->
                    khepri_sproc:run(StandaloneFun, Args);
                _ ->
                    [Path] = maps:keys(NodePropsMap),
                    throw(?khepri_exception(
                             denied_execution_of_non_sproc_node,
                             #{path => Path,
                               args => Args,
                               node_props => NodeProps}))
            end;
        {error, Reason} ->
            throw(?khepri_error(
                     failed_to_get_sproc,
                     #{path => PathPattern,
                       args => Args,
                       error => Reason}))
    end.

-spec register_trigger(
        StoreId, TriggerId, EventFilter, StoredProcPath, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      TriggerId :: khepri:trigger_id(),
      EventFilter :: khepri_evf:event_filter() | khepri_path:pattern(),
      StoredProcPath :: khepri_path:path(),
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Registers a trigger.
%%
%% @param StoreId the name of the Ra cluster.
%% @param TriggerId the name of the trigger.
%% @param EventFilter the event filter used to associate an event with a
%%        stored procedure.
%% @param StoredProcPath the path to the stored procedure to execute when the
%%        corresponding event occurs.
%%
%% @returns `ok' if the trigger was registered, an `{error, Reason}' tuple
%% otherwise.

register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath, Options)
  when ?IS_STORE_ID(StoreId) ->
    EventFilter1 = khepri_evf:wrap(EventFilter),
    StoredProcPath1 = khepri_path:from_string(StoredProcPath),
    khepri_path:ensure_is_valid(StoredProcPath1),
    Command = #register_trigger{id = TriggerId,
                                sproc = StoredProcPath1,
                                event_filter = EventFilter1},
    process_command(StoreId, Command, Options).

-spec ack_triggers_execution(StoreId, TriggeredStoredProcs) ->
    Ret when
      StoreId :: khepri:store_id(),
      TriggeredStoredProcs :: [triggered()],
      Ret :: ok | khepri:error().
%% @doc Acknowledges the execution of a trigger.
%%
%% This is part of a mechanism to ensure that a trigger is executed at least
%% once.
%%
%% @private

ack_triggers_execution(StoreId, TriggeredStoredProcs)
  when ?IS_STORE_ID(StoreId) ->
    Command = #ack_triggered{triggered = TriggeredStoredProcs},
    process_command(StoreId, Command, #{async => true}).

-spec get_keep_while_conds_state(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(keep_while_conds_map()) | khepri:error().
%% @doc Returns the `keep_while' conditions internal state.
%%
%% The returned state consists of all the `keep_while' condition set so far.
%% However, it doesn't include the reverse index.
%%
%% @param StoreId the name of the Ra cluster.
%%
%% @returns the `keep_while' conditions internal state.
%%
%% @private

get_keep_while_conds_state(StoreId, Options)
  when ?IS_STORE_ID(StoreId) ->
    Query = fun(#?MODULE{keep_while_conds = KeepWhileConds}) ->
                    {ok, KeepWhileConds}
            end,
    Options1 = Options#{favor => consistency},
    process_query(StoreId, Query, Options1).

-spec split_query_options(Options) -> {QueryOptions, TreeOptions} when
      Options :: QueryOptions | TreeOptions,
      QueryOptions :: khepri:query_options(),
      TreeOptions :: khepri:tree_options().
%% @private

split_query_options(Options) ->
    Options1 = set_default_options(Options),
    maps:fold(
      fun
          (Option, Value, {Q, T}) when
                Option =:= timeout orelse
                Option =:= favor ->
              Q1 = Q#{Option => Value},
              {Q1, T};
          (props_to_return, [], {Q, T}) ->
              {Q, T};
          (Option, Value, {Q, T}) when
                Option =:= expect_specific_node orelse
                Option =:= props_to_return orelse
                Option =:= include_root_props ->
              T1 = T#{Option => Value},
              {Q, T1}
      end, {#{}, #{}}, Options1).

-spec split_command_options(Options) ->
    {CommandOptions, TreeOptions, Extra} when
      Options :: CommandOptions | TreeOptions | Extra,
      CommandOptions :: khepri:command_options(),
      TreeOptions :: khepri:tree_options(),
      Extra :: khepri:put_options().
%% @private

split_command_options(Options) ->
    Options1 = set_default_options(Options),
    maps:fold(
      fun
          (keep_while, KeepWhile, {C, T, E}) ->
              KeepWhile1 = khepri_condition:ensure_native_keep_while(
                             KeepWhile),
              E1 = E#{keep_while => KeepWhile1},
              {C, T, E1};
          (Option, Value, {C, T, E}) when
                Option =:= timeout orelse
                Option =:= async ->
              C1 = C#{Option => Value},
              {C1, T, E};
          (props_to_return, [], Acc) ->
              Acc;
          (Option, Value, {C, T, E}) when
                Option =:= expect_specific_node orelse
                Option =:= props_to_return orelse
                Option =:= include_root_props ->
              T1 = T#{Option => Value},
              {C, T1, E}
      end, {#{}, #{}, #{}}, Options1).

-define(DEFAULT_PROPS_TO_RETURN, [payload,
                                  payload_version]).

set_default_options(Options) ->
    %% By default, return payload-related properties. The caller can set
    %% `props_to_return' to `none' to get a minimal return value.
    Options1 = case Options of
                   #{props_to_return := _} ->
                       Options;
                   _ ->
                       Options#{props_to_return => ?DEFAULT_PROPS_TO_RETURN}
               end,
    Options1.

-spec process_command(StoreId, Command, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Command :: command(),
      Options :: khepri:command_options(),
      Ret :: any().
%% @doc Processes a command which is appended to the Ra log and processed by
%% this state machine code.
%%
%% `Command' may modify the state of the machine.
%%
%% The command associated code is executed in the context of the state machine
%% process on each Ra members.
%%
%% @param StoreId the name of the Ra cluster.
%%
%% @returns the result of the command or an "error" tuple.
%%
%% @private

process_command(StoreId, Command, Options) ->
    CommandType = select_command_type(Options),
    case CommandType of
        sync ->
            process_sync_command(StoreId, Command, Options);
        {async, Correlation, Priority} ->
            process_async_command(
              StoreId, Command, Correlation, Priority)
    end.

process_sync_command(StoreId, Command, Options) ->
    Timeout = get_timeout(Options),
    T0 = khepri_utils:start_timeout_window(Timeout),
    LeaderId = khepri_cluster:get_cached_leader(StoreId),
    RaServer = use_leader_or_local_ra_server(StoreId, LeaderId),
    case ra:process_command(RaServer, Command, Timeout) of
        {ok, Ret, NewLeaderId} ->
            khepri_cluster:cache_leader_if_changed(
              StoreId, LeaderId, NewLeaderId),
            just_did_consistent_call(StoreId),
            ?raise_exception_if_any(Ret);
        {timeout, _} = TimedOut ->
            {error, TimedOut};
        {error, Reason}
          when LeaderId =/= undefined andalso ?HAS_TIME_LEFT(Timeout) andalso
               (Reason == noproc orelse Reason == nodedown) ->
            %% The cached leader is no more. We simply clear the cache
            %% entry and retry.
            khepri_cluster:clear_cached_leader(StoreId),
            NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
            Options1 = Options#{timeout => NewTimeout},
            process_sync_command(StoreId, Command, Options1);
        {error, Reason} = Error
          when LeaderId =:= undefined andalso ?HAS_TIME_LEFT(Timeout) andalso
               (Reason == noproc orelse Reason == nodedown) ->
            case khepri_utils:is_ra_server_alive(RaServer) of
                true ->
                    %% The follower doesn't know about the new leader yet.
                    %% Retry again after waiting a bit.
                    NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
                    NewTimeout = khepri_utils:sleep(
                                   ?NOPROC_RETRY_INTERVAL, NewTimeout0),
                    Options1 = Options#{timeout => NewTimeout},
                    process_sync_command(StoreId, Command, Options1);
                false ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

process_async_command(StoreId, Command, Correlation, Priority) ->
    LocalServerId = {StoreId, node()},
    ra:pipeline_command(LocalServerId, Command, Correlation, Priority).

-spec select_command_type(Options) -> CommandType when
      Options :: khepri:command_options(),
      CommandType :: sync | {async, Correlation, Priority},
      Correlation :: ra_server:command_correlation(),
      Priority :: ra_server:command_priority().
%% @doc Selects the command type depending on what the caller wants.
%%
%% @private

-define(DEFAULT_RA_COMMAND_CORRELATION, no_correlation).
-define(DEFAULT_RA_COMMAND_PRIORITY, low).
-define(IS_RA_COMMAND_CORRELATION(Correlation),
        (is_integer(Correlation) orelse is_reference(Correlation))).
-define(IS_RA_COMMAND_PRIORITY(Priority),
        (Priority =:= normal orelse Priority =:= low)).

select_command_type(Options) when not is_map_key(async, Options) ->
    sync;
select_command_type(#{async := false}) ->
    sync;
select_command_type(#{async := true}) ->
    {async, ?DEFAULT_RA_COMMAND_CORRELATION, ?DEFAULT_RA_COMMAND_PRIORITY};
select_command_type(#{async := Correlation})
  when ?IS_RA_COMMAND_CORRELATION(Correlation) ->
    {async, Correlation, ?DEFAULT_RA_COMMAND_PRIORITY};
select_command_type(#{async := Priority})
  when ?IS_RA_COMMAND_PRIORITY(Priority) ->
    {async, ?DEFAULT_RA_COMMAND_CORRELATION, Priority};
select_command_type(#{async := {Correlation, Priority}})
  when ?IS_RA_COMMAND_CORRELATION(Correlation) andalso
       ?IS_RA_COMMAND_PRIORITY(Priority) ->
    {async, Correlation, Priority}.

-spec process_query(StoreId, QueryFun, Options) -> Ret when
      StoreId :: khepri:store_id(),
      QueryFun :: query_fun(),
      Options :: khepri:query_options(),
      Ret :: any().
%% @doc Processes a query which is by the Ra leader.
%%
%% The `QueryFun' function takes the machine state as an argument and can
%% return anything. However, the machine state is never modified. The query
%% does not go through the Ra log and is not replicated.
%%
%% The `QueryFun' function is executed from a process on the leader Ra member.
%%
%% @param StoreId the name of the Ra cluster.
%%
%% @returns the result of the query or an "error" tuple.
%%
%% @private

process_query(StoreId, QueryFun, Options) ->
    QueryType = select_query_type(StoreId, Options),
    Timeout = get_timeout(Options),
    case QueryType of
        local -> process_local_query(StoreId, QueryFun, Timeout);
        _     -> process_non_local_query(StoreId, QueryFun, QueryType, Timeout)
    end.

-spec process_local_query(StoreId, QueryFun, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      QueryFun :: query_fun(),
      Timeout :: timeout(),
      Ret :: any().

process_local_query(StoreId, QueryFun, Timeout) ->
    LocalServerId = {StoreId, node()},
    Ret = ra:local_query(LocalServerId, QueryFun, Timeout),
    process_query_response(
      StoreId, LocalServerId, false, QueryFun, local, Timeout, Ret).

-spec process_non_local_query(StoreId, QueryFun, QueryType, Timeout) ->
    Ret when
      StoreId :: khepri:store_id(),
      QueryFun :: query_fun(),
      QueryType :: leader | consistent,
      Timeout :: timeout(),
      Ret :: any().

process_non_local_query(StoreId, QueryFun, QueryType, Timeout)
  when QueryType =:= leader orelse
       QueryType =:= consistent ->
    T0 = khepri_utils:start_timeout_window(Timeout),
    LeaderId = khepri_cluster:get_cached_leader(StoreId),
    RaServer = use_leader_or_local_ra_server(StoreId, LeaderId),
    Ret = case QueryType of
              leader     -> ra:leader_query(RaServer, QueryFun, Timeout);
              consistent -> ra:consistent_query(RaServer, QueryFun, Timeout)
          end,
    NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
    %% TODO: If the consistent query times out in the context of
    %% `QueryType=compromise`, should we retry with a local query to
    %% never block the query and let the caller continue?
    process_query_response(
      StoreId, RaServer, LeaderId =/= undefined, QueryFun, QueryType,
      NewTimeout, Ret).

-spec process_query_response(
        StoreId, RaServer, IsLeader, QueryFun, QueryType, Timeout,
        Response) ->
    Ret when
      StoreId :: khepri:store_id(),
      RaServer :: ra:server_id(),
      IsLeader :: boolean(),
      QueryFun :: query_fun(),
      QueryType :: local | leader | consistent,
      Timeout :: timeout(),
      Response :: {ok, {RaIndex, any()}, NewLeaderId} |
                  {ok, any(), NewLeaderId} |
                  {error, any()} |
                  {timeout, ra:server_id()},
      RaIndex :: ra:index(),
      NewLeaderId :: ra:server_id(),
      Ret :: any().

process_query_response(
  StoreId, RaServer, IsLeader, _QueryFun, consistent, _Timeout,
  {ok, Ret, NewLeaderId}) ->
    case IsLeader of
        true ->
            khepri_cluster:cache_leader_if_changed(
              StoreId, RaServer, NewLeaderId);
        false ->
            khepri_cluster:cache_leader(StoreId, NewLeaderId)
    end,
    just_did_consistent_call(StoreId),
    ?raise_exception_if_any(Ret);
process_query_response(
  StoreId, RaServer, IsLeader, _QueryFun, _QueryType, _Timeout,
  {ok, {_RaIndex, Ret}, NewLeaderId}) ->
    case IsLeader of
        true ->
            khepri_cluster:cache_leader_if_changed(
              StoreId, RaServer, NewLeaderId);
        false ->
            khepri_cluster:cache_leader(StoreId, NewLeaderId)
    end,
    ?raise_exception_if_any(Ret);
process_query_response(
  _StoreId, _RaServer, _IsLeader, _QueryFun, _QueryType, _Timeout,
  {timeout, _} = TimedOut) ->
    {error, TimedOut};
process_query_response(
  StoreId, _RaServer, true = _IsLeader, QueryFun, QueryType, Timeout,
  {error, Reason})
  when QueryType =/= local andalso ?HAS_TIME_LEFT(Timeout) andalso
       (Reason == noproc orelse Reason == nodedown) ->
    %% The cached leader is no more. We simply clear the cache
    %% entry and retry. It may time out eventually.
    khepri_cluster:clear_cached_leader(StoreId),
    process_non_local_query(StoreId, QueryFun, QueryType, Timeout);
process_query_response(
  StoreId, RaServer, false = _IsLeader, QueryFun, QueryType, Timeout,
  {error, Reason} = Error)
  when QueryType =/= local andalso ?HAS_TIME_LEFT(Timeout) andalso
       (Reason == noproc orelse Reason == nodedown)->
    case khepri_utils:is_ra_server_alive(RaServer) of
        true ->
            %% The follower doesn't know about the new leader yet. Retry again
            %% after waiting a bit.
            NewTimeout = khepri_utils:sleep(?NOPROC_RETRY_INTERVAL, Timeout),
            process_non_local_query(StoreId, QueryFun, QueryType, NewTimeout);
        false ->
            Error
    end;
process_query_response(
  _StoreId, _RaServer, _IsLeader, _QueryFun, _QueryType, _Timeout,
  {error, _} = Error) ->
    Error.

-spec select_query_type(StoreId, Options) -> QueryType when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      QueryType :: local | leader | consistent.
%% @doc Selects the query type depending on what the caller favors.
%%
%% @private

select_query_type(StoreId, #{favor := Favor}) ->
    do_select_query_type(StoreId, Favor);
select_query_type(StoreId, _Options) ->
    do_select_query_type(StoreId, compromise).

-define(
   LAST_CONSISTENT_CALL_TS_REF(StoreId),
   {khepri, last_consistent_call_ts_ref, StoreId}).

do_select_query_type(StoreId, compromise) ->
    Key = ?LAST_CONSISTENT_CALL_TS_REF(StoreId),
    Idx = 1,
    case persistent_term:get(Key, undefined) of
        AtomicsRef when AtomicsRef =/= undefined ->
            %% We verify when was the last time we did a command or a
            %% consistent query (i.e. we made sure there was an active leader
            %% in a cluster with a quorum of active members).
            %%
            %% If the last one was more than 10 seconds ago, we force a
            %% consistent query to verify the cluster health at the same time.
            %% Otherwise, we select a leader query which is a good balance
            %% between freshness and latency.
            Last = atomics:get(AtomicsRef, Idx),
            Now = erlang:system_time(second),
            ConsistentAgainAfter = application:get_env(
                                     khepri,
                                     consistent_query_interval_in_compromise,
                                     10),
            if
                Now - Last < ConsistentAgainAfter -> leader;
                true                              -> consistent
            end;
        undefined ->
            consistent
    end;
do_select_query_type(_StoreId, consistency) ->
    consistent;
do_select_query_type(_StoreId, low_latency) ->
    local.

just_did_consistent_call(StoreId) ->
    %% We record the timestamp of the successful command or consistent query
    %% which just returned. This timestamp is used in the `compromise' query
    %% strategy to perform a consistent query from time to time, and leader
    %% queries the rest of the time.
    %%
    %% We store the system time as seconds in an `atomics' structure. The
    %% reference of that structure is stored in a persistent term. We don't
    %% store the timestamp directly in a persistent term because it is not
    %% suited for frequent writes. This way, we store the `atomics' reference
    %% once and update the `atomics' afterwards.
    Idx = 1,
    AtomicsRef = case get_last_consistent_call_atomics(StoreId) of
                     Ref when Ref =/= undefined ->
                         Ref;
                     undefined ->
                         Key = ?LAST_CONSISTENT_CALL_TS_REF(StoreId),
                         Ref = atomics:new(1, []),
                         persistent_term:put(Key, Ref),
                         Ref
                 end,
    Now = erlang:system_time(second),
    ok = atomics:put(AtomicsRef, Idx, Now),
    ok.

get_last_consistent_call_atomics(StoreId) ->
    Key = ?LAST_CONSISTENT_CALL_TS_REF(StoreId),
    persistent_term:get(Key, undefined).

-spec get_timeout(Options) -> Timeout when
      Options :: khepri:command_options() | khepri:query_options(),
      Timeout :: timeout().
%% @private

get_timeout(#{timeout := Timeout}) -> Timeout;
get_timeout(_)                     -> khepri_app:get_default_timeout().

use_leader_or_local_ra_server(_StoreId, LeaderId)
  when LeaderId =/= undefined ->
    LeaderId;
use_leader_or_local_ra_server(StoreId, undefined) ->
    ThisNode = node(),
    khepri_cluster:node_to_member(StoreId, ThisNode).

-spec clear_cache(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Clears the cached data for the given `StoreId'.
%%
%% @private

clear_cache(StoreId) ->
    _ = persistent_term:erase(?LAST_CONSISTENT_CALL_TS_REF(StoreId)),
    ok.

%% -------------------------------------------------------------------
%% ra_machine callbacks.
%% -------------------------------------------------------------------

-spec init(Params) -> State when
      Params :: machine_init_args(),
      State :: state().
%% @private

init(#{store_id := StoreId,
       member := Member} = Params) ->
    Config = case Params of
                 #{snapshot_interval := SnapshotInterval} ->
                     #config{store_id = StoreId,
                             member = Member,
                             snapshot_interval = SnapshotInterval};
                 _ ->
                     #config{store_id = StoreId,
                             member = Member}
             end,
    State = #?MODULE{config = Config},

    %% Create initial "schema" if provided.
    Commands = maps:get(commands, Params, []),
    State3 = lists:foldl(
               fun (Command, State1) ->
                       Meta = #{index => 0,
                                term => 0,
                                system_time => 0},
                       {S, _, _} = apply(Meta, Command, State1),
                       S
               end, State, Commands),
    reset_applied_command_count(State3).

-spec apply(Meta, Command, State) -> {State, Ret, SideEffects} when
      Meta :: ra_machine:command_meta_data(),
      Command :: command(),
      State :: state(),
      Ret :: any(),
      SideEffects :: ra_machine:effects().
%% @private

%% TODO: Handle unknown/invalid commands.
apply(
  Meta,
  #put{path = PathPattern, payload = Payload, extra = Extra,
       options = Options},
  State) ->
    Ret = insert_or_update_node(State, PathPattern, Payload, Extra, Options),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #delete{path = PathPattern, options = Options},
  State) ->
    Ret = delete_matching_nodes(State, PathPattern, Options),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #tx{'fun' = StandaloneFun, args = Args},
  State) when ?IS_STANDALONE_FUN(StandaloneFun) ->
    Ret = khepri_tx_adv:run(State, StandaloneFun, Args, true),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #tx{'fun' = PathPattern, args = Args},
  State) when ?IS_KHEPRI_PATH_PATTERN(PathPattern) ->
    Ret = locate_sproc_and_execute_tx(State, PathPattern, Args, true),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #register_trigger{id = TriggerId,
                    sproc = StoredProcPath,
                    event_filter = EventFilter},
  #?MODULE{triggers = Triggers} = State) ->
    StoredProcPath1 = khepri_path:realpath(StoredProcPath),
    EventFilter1 = case EventFilter of
                       #evf_tree{path = Path} ->
                           Path1 = khepri_path:realpath(Path),
                           EventFilter#evf_tree{path = Path1}
                   end,
    Triggers1 = Triggers#{TriggerId => #{sproc => StoredProcPath1,
                                         event_filter => EventFilter1}},
    State1 = State#?MODULE{triggers = Triggers1},
    Ret = {State1, ok},
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #ack_triggered{triggered = ProcessedTriggers},
  #?MODULE{emitted_triggers = EmittedTriggers} = State) ->
    EmittedTriggers1 = EmittedTriggers -- ProcessedTriggers,
    State1 = State#?MODULE{emitted_triggers = EmittedTriggers1},
    Ret = {State1, ok},
    bump_applied_command_count(Ret, Meta).

-spec bump_applied_command_count(ApplyRet, Meta) ->
    {State, Ret, SideEffects} when
      ApplyRet :: {State, Ret} | {State, Ret, SideEffects},
      State :: state(),
      Ret :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @private

bump_applied_command_count({State, Result}, Meta) ->
    bump_applied_command_count({State, Result, []}, Meta);
bump_applied_command_count(
  {#?MODULE{config = #config{snapshot_interval = SnapshotInterval},
            metrics = Metrics} = State,
   Result,
   SideEffects},
  #{index := RaftIndex}) ->
    AppliedCmdCount0 = maps:get(applied_command_count, Metrics, 0),
    AppliedCmdCount = AppliedCmdCount0 + 1,
    case AppliedCmdCount < SnapshotInterval of
        true ->
            Metrics1 = Metrics#{applied_command_count => AppliedCmdCount},
            State1 = State#?MODULE{metrics = Metrics1},
            {State1, Result, SideEffects};
        false ->
            ?LOG_DEBUG(
               "Move release cursor after ~b commands applied "
               "(>= ~b commands)",
               [AppliedCmdCount, SnapshotInterval],
               #{domain => [khepri, ra_machine]}),
            State1 = reset_applied_command_count(State),
            ReleaseCursor = {release_cursor, RaftIndex, State1},
            SideEffects1 = [ReleaseCursor | SideEffects],
            {State1, Result, SideEffects1}
    end.

reset_applied_command_count(#?MODULE{metrics = Metrics} = State) ->
    Metrics1 = maps:remove(applied_command_count, Metrics),
    State#?MODULE{metrics = Metrics1}.

%% @private

state_enter(StateName, State) ->
    SideEffects1 = emitted_triggers_to_side_effects(StateName, State),
    SideEffects1.

%% @private

emitted_triggers_to_side_effects(
  leader,
  #?MODULE{emitted_triggers = []}) ->
    [];
emitted_triggers_to_side_effects(
  leader,
  #?MODULE{config = #config{store_id = StoreId},
           emitted_triggers = EmittedTriggers}) ->
    SideEffect = {mod_call,
                  khepri_event_handler,
                  handle_triggered_sprocs,
                  [StoreId, EmittedTriggers]},
    [SideEffect];
emitted_triggers_to_side_effects(_StateName, _State) ->
    [].

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec create_node_record(Payload) -> Node when
      Payload :: khepri_payload:payload(),
      Node :: tree_node().
%% @private

create_node_record(Payload) ->
    #node{props = ?INIT_NODE_PROPS,
          payload = Payload}.

-spec set_node_payload(Node, Payload) -> Node when
      Node :: tree_node(),
      Payload :: khepri_payload:payload().
%% @private

set_node_payload(#node{payload = Payload} = Node, Payload) ->
    Node;
set_node_payload(#node{props = #{payload_version := DVersion} = Stat} = Node,
                 Payload) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{props = Stat1, payload = Payload}.

-spec remove_node_payload(Node) -> Node when
      Node :: tree_node().
%% @private

remove_node_payload(
  #node{payload = ?NO_PAYLOAD} = Node) ->
    Node;
remove_node_payload(
  #node{props = #{payload_version := DVersion} = Stat} = Node) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{props = Stat1, payload = khepri_payload:none()}.

-spec add_node_child(Node, ChildName, Child) -> Node when
      Node :: tree_node(),
      Child :: tree_node(),
      ChildName :: khepri_path:component().

add_node_child(#node{props = #{child_list_version := CVersion} = Stat,
                     child_nodes = Children} = Node,
               ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{props = Stat1, child_nodes = Children1}.

-spec update_node_child(Node, ChildName, Child) -> Node when
      Node :: tree_node(),
      Child :: tree_node(),
      ChildName :: khepri_path:component().

update_node_child(#node{child_nodes = Children} = Node, ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Node#node{child_nodes = Children1}.

-spec remove_node_child(Node, ChildName) -> Node when
      Node :: tree_node(),
      ChildName :: khepri_path:component().

remove_node_child(#node{props = #{child_list_version := CVersion} = Stat,
                        child_nodes = Children} = Node,
                 ChildName) ->
    ?assert(maps:is_key(ChildName, Children)),
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Children1 = maps:remove(ChildName, Children),
    Node#node{props = Stat1, child_nodes = Children1}.

-spec remove_node_child_nodes(Node) -> Node when
      Node :: tree_node().

remove_node_child_nodes(
  #node{child_nodes = Children} = Node) when Children =:= #{} ->
    Node;
remove_node_child_nodes(
  #node{props = #{child_list_version := CVersion} = Stat} = Node) ->
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{props = Stat1, child_nodes = #{}}.

-spec gather_node_props(Node, Options) -> NodeProps when
      Node :: tree_node(),
      Options :: khepri:tree_options(),
      NodeProps :: khepri:node_props().

gather_node_props(#node{props = #{payload_version := PVersion,
                                  child_list_version := CVersion},
                        payload = Payload,
                        child_nodes = Children},
                  #{props_to_return := WantedProps}) ->
    lists:foldl(
      fun
          (payload_version, Acc) ->
              Acc#{payload_version => PVersion};
          (child_list_version, Acc) ->
              Acc#{child_list_version => CVersion};
          (child_list_length, Acc) ->
              Acc#{child_list_length => maps:size(Children)};
          (child_names, Acc) ->
              Acc#{child_names => maps:keys(Children)};
          (payload, Acc) ->
              case Payload of
                  #p_data{data = Data}  -> Acc#{data => Data};
                  #p_sproc{sproc = Fun} -> Acc#{sproc => Fun};
                  _                     -> Acc
              end;
          (has_payload, Acc) ->
              case Payload of
                  #p_data{data = _}   -> Acc#{has_data => true};
                  #p_sproc{sproc = _} -> Acc#{is_sproc => true};
                  _                   -> Acc
              end;
          (raw_payload, Acc) ->
              Acc#{raw_payload => Payload}
      end, #{}, WantedProps);
gather_node_props(#node{}, _Options) ->
    #{}.

gather_node_props_from_old_and_new_nodes(OldNode, NewNode, Options) ->
    OldNodeProps = case OldNode of
                       undefined -> #{};
                       _         -> gather_node_props(OldNode, Options)
                   end,
    NewNodeProps0 = gather_node_props(NewNode, Options),
    NewNodeProps1 = maps:remove(data, NewNodeProps0),
    NewNodeProps2 = maps:remove(sproc, NewNodeProps1),
    maps:merge(OldNodeProps, NewNodeProps2).

gather_node_props_for_error(Node) ->
    gather_node_props(Node, #{props_to_return => ?DEFAULT_PROPS_TO_RETURN}).

-spec to_absolute_keep_while(BasePath, KeepWhile) -> KeepWhile when
      BasePath :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while().
%% @private

to_absolute_keep_while(BasePath, KeepWhile) ->
    maps:fold(
      fun(Path, Cond, Acc) ->
              AbsPath = khepri_path:abspath(Path, BasePath),
              Acc#{AbsPath => Cond}
      end, #{}, KeepWhile).

-spec are_keep_while_conditions_met(Node, KeepWhile) -> Ret when
      Node :: tree_node(),
      KeepWhile :: khepri_condition:native_keep_while(),
      Ret :: true | {false, any()}.
%% @private

are_keep_while_conditions_met(_, KeepWhile)
  when KeepWhile =:= #{} ->
    true;
are_keep_while_conditions_met(Root, KeepWhile) ->
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    maps:fold(
      fun
          (Path, Condition, true) ->
              case find_matching_nodes(Root, Path, TreeOptions) of
                  {ok, Result} when Result =/= #{} ->
                      are_keep_while_conditions_met1(Result, Condition);
                  {ok, _} ->
                      {false, {pattern_matches_no_nodes, Path}};
                  {error, Reason} ->
                      {false, Reason}
              end;
          (_, _, False) ->
              False
      end, true, KeepWhile).

are_keep_while_conditions_met1(Result, Condition) ->
    maps:fold(
      fun
          (Path, NodeProps, true) ->
              khepri_condition:is_met(Condition, Path, NodeProps);
          (_, _, False) ->
              False
      end, true, Result).

is_keep_while_condition_met_on_self(
  Path, Node, #{keep_while_conds := KeepWhileConds}) ->
    case KeepWhileConds of
        #{Path := #{Path := Condition}} ->
            khepri_condition:is_met(Condition, Path, Node);
        _ ->
            true
    end;
is_keep_while_condition_met_on_self(_, _, _) ->
    true.

-spec update_keep_while_conds_revidx(
        KeepWhileConds, KeepWhileCondsRevIdx, Watcher, KeepWhile) ->
    KeepWhileConds when
      KeepWhileConds :: keep_while_conds_map(),
      KeepWhileCondsRevIdx :: keep_while_conds_revidx(),
      Watcher :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while().

update_keep_while_conds_revidx(
  KeepWhileConds, KeepWhileCondsRevIdx, Watcher, KeepWhile) ->
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_while.
    OldWatcheds = maps:get(Watcher, KeepWhileConds, #{}),
    KeepWhileCondsRevIdx1 = maps:fold(
                          fun(Watched, _, KWRevIdx) ->
                                  Watchers = maps:get(Watched, KWRevIdx),
                                  Watchers1 = maps:remove(Watcher, Watchers),
                                  case maps:size(Watchers1) of
                                      0 -> maps:remove(Watched, KWRevIdx);
                                      _ -> KWRevIdx#{Watched => Watchers1}
                                  end
                          end, KeepWhileCondsRevIdx, OldWatcheds),
    %% Then, record the watched paths.
    maps:fold(
      fun(Watched, _, KWRevIdx) ->
              Watchers = maps:get(Watched, KWRevIdx, #{}),
              Watchers1 = Watchers#{Watcher => ok},
              KWRevIdx#{Watched => Watchers1}
      end, KeepWhileCondsRevIdx1, KeepWhile).

locate_sproc_and_execute_tx(
  #?MODULE{root = Root} = State, PathPattern, Args, AllowUpdates) ->
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [raw_payload]},
    {StandaloneFun, Args1} =
    case find_matching_nodes(Root, PathPattern, TreeOptions) of
        {ok, Result} ->
            case maps:values(Result) of
                [#{raw_payload := #p_sproc{
                                     sproc = StoredProc,
                                     is_valid_as_tx_fun = ReadWrite}}]
                  when AllowUpdates andalso
                       (ReadWrite =:= ro orelse ReadWrite =:= rw) ->
                    {StoredProc, Args};
                [#{raw_payload := #p_sproc{
                                     sproc = StoredProc,
                                     is_valid_as_tx_fun = ro}}]
                  when not AllowUpdates ->
                    {StoredProc, Args};
                [#{raw_payload := #p_sproc{}}] ->
                    Reason = ?khepri_error(
                                sproc_invalid_as_tx_fun,
                                #{path => PathPattern}),
                    {fun failed_to_locate_sproc/1, [Reason]};
                _ ->
                    Reason = ?khepri_error(
                                no_sproc_at_given_path,
                                #{path => PathPattern}),
                    {fun failed_to_locate_sproc/1, [Reason]}
            end;
        {error, Reason} ->
            {fun failed_to_locate_sproc/1, [Reason]}
    end,
    khepri_tx_adv:run(State, StandaloneFun, Args1, AllowUpdates).

-spec failed_to_locate_sproc(Reason) -> no_return() when
      Reason :: any().
%% @private

failed_to_locate_sproc(Reason) ->
    khepri_tx:abort(Reason).

-spec find_matching_nodes(Root, PathPattern, Options) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:tree_options(),
      Result :: khepri_machine:common_ret().
%% @private

find_matching_nodes(Root, PathPattern, Options) ->
    IncludeRootProps = maps:get(
                         include_root_props, Options,
                         khepri_path:pattern_includes_root_node(PathPattern)),
    Extra = #{include_root_props => IncludeRootProps},
    do_find_matching_nodes(Root, PathPattern, Options, Extra, #{}).

-spec count_matching_nodes(Root, PathPattern, Options) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:tree_options(),
      Result :: khepri:ok(integer()) | khepri:error().
%% @private

count_matching_nodes(Root, PathPattern, Options) ->
    IncludeRootProps = maps:get(
                         include_root_props, Options,
                         khepri_path:pattern_includes_root_node(PathPattern)),
    Extra = #{include_root_props => IncludeRootProps},
    do_find_matching_nodes(Root, PathPattern, Options, Extra, 0).

-spec do_find_matching_nodes
(Root, PathPattern, Options, Extra, Map) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:tree_options(),
      Extra :: #{include_root_props => boolean()},
      Map :: map(),
      Result :: khepri_machine:common_ret();
(Root, PathPattern, Options, Extra, Integer) ->
    Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:tree_options(),
      Extra :: #{include_root_props => boolean()},
      Integer :: integer(),
      Result :: khepri:ok(integer()) | khepri:error().
%% @private

do_find_matching_nodes(Root, PathPattern, Options, Extra, MapOrInteger) ->
    Fun = fun(Path, Node, Result) ->
                  find_matching_nodes_cb(Path, Node, Options, Result)
          end,
    WorkOnWhat = case Options of
                     #{expect_specific_node := true} -> specific_node;
                     _                               -> many_nodes
                 end,
    Ret = walk_down_the_tree(
            Root, PathPattern, WorkOnWhat, Extra, Fun, MapOrInteger),
    case Ret of
        {ok, NewRoot, _, Result} ->
            ?assertEqual(Root, NewRoot),
            {ok, Result};
        Error ->
            Error
    end.

find_matching_nodes_cb(Path, #node{} = Node, Options, Map)
  when is_map(Map) ->
    NodeProps = gather_node_props(Node, Options),
    {ok, keep, Map#{Path => NodeProps}};
find_matching_nodes_cb(_Path, #node{} = _Node, _Options, Count)
  when is_integer(Count) ->
    {ok, keep, Count + 1};
find_matching_nodes_cb(
  _,
  {interrupted, node_not_found = Reason, Info},
  #{expect_specific_node := true},
  Map)
  when is_map(Map) ->
    %% If we are collecting node properties (the result is a map) and the path
    %% targets a specific node which is not found, we return an error.
    %%
    %% If we are counting nodes, that's fine and the next function clause will
    %% run. The walk won't be interrupted.
    Reason1 = ?khepri_error(Reason, Info),
    {error, Reason1};
find_matching_nodes_cb(_, {interrupted, _, _}, _, Result) ->
    {ok, keep, Result}.

-spec insert_or_update_node(State, PathPattern, Payload, Extra, Options) ->
    Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: khepri_payload:payload(),
      Extra :: khepri:put_options(),
      Options :: khepri:tree_options(),
      Ret :: {State, Result} | {State, Result, ra_machine:effects()},
      Result :: khepri_machine:common_ret().
%% @private

insert_or_update_node(
  #?MODULE{root = Root,
           keep_while_conds = KeepWhileConds,
           keep_while_conds_revidx = KeepWhileCondsRevIdx} = State,
  PathPattern, Payload,
  #{keep_while := KeepWhile},
  Options) ->
    Fun = fun(Path, Node, {_, _, Result}) ->
                  Ret = insert_or_update_node_cb(
                          Path, Node, Payload, Options, Result),
                  case Ret of
                      {ok, Node1, Result1} when Result1 =/= #{} ->
                          AbsKeepWhile = to_absolute_keep_while(
                                           Path, KeepWhile),
                          KeepWhileOnOthers = maps:remove(Path, AbsKeepWhile),
                          KWMet = are_keep_while_conditions_met(
                                    Root, KeepWhileOnOthers),
                          case KWMet of
                              true ->
                                  {ok, Node1, {updated, Path, Result1}};
                              {false, Reason} ->
                                  %% The keep_while condition is not met. We
                                  %% can't insert the node and return an
                                  %% error instead.
                                  NodeName = case Path of
                                                 [] -> ?KHEPRI_ROOT_NODE;
                                                 _  -> lists:last(Path)
                                             end,
                                  Reason1 = ?khepri_error(
                                               keep_while_conditions_not_met,
                                               #{node_name => NodeName,
                                                 node_path => Path,
                                                 keep_while_reason => Reason}),
                                  {error, Reason1}
                          end;
                      {ok, Node1, Result1} ->
                          {ok, Node1, {updated, Path, Result1}};
                      Error ->
                          Error
                  end
          end,
    WorkOnWhat = case Options of
                     #{expect_specific_node := true} -> specific_node;
                     _                               -> many_nodes
                 end,
    Ret1 = walk_down_the_tree(
             Root, PathPattern, WorkOnWhat,
             #{keep_while_conds => KeepWhileConds,
               keep_while_conds_revidx => KeepWhileCondsRevIdx,
               keep_while_aftermath => #{}},
             Fun, {undefined, [], #{}}),
    case Ret1 of
        {ok, Root1, #{keep_while_conds := KeepWhileConds1,
                      keep_while_conds_revidx := KeepWhileCondsRevIdx1,
                      keep_while_aftermath := KeepWhileAftermath},
         {updated, ResolvedPath, Ret2}} ->
            AbsKeepWhile = to_absolute_keep_while(ResolvedPath, KeepWhile),
            KeepWhileCondsRevIdx2 = update_keep_while_conds_revidx(
                                  KeepWhileConds1, KeepWhileCondsRevIdx1,
                                  ResolvedPath, AbsKeepWhile),
            KeepWhileConds2 = KeepWhileConds1#{ResolvedPath => AbsKeepWhile},
            State1 = State#?MODULE{root = Root1,
                                   keep_while_conds = KeepWhileConds2,
                                   keep_while_conds_revidx =
                                   KeepWhileCondsRevIdx2},
            {State2, SideEffects} = create_tree_change_side_effects(
                                      State, State1, Ret2, KeepWhileAftermath),
            {State2, {ok, Ret2}, SideEffects};
        Error ->
            ?assertMatch({error, _}, Error),
            {State, Error}
    end;
insert_or_update_node(
  #?MODULE{root = Root,
           keep_while_conds = KeepWhileConds,
           keep_while_conds_revidx = KeepWhileCondsRevIdx} = State,
  PathPattern, Payload,
  _Extra, Options) ->
    Fun = fun(Path, Node, Result) ->
                  insert_or_update_node_cb(
                    Path, Node, Payload, Options, Result)
          end,
    WorkOnWhat = case Options of
                     #{expect_specific_node := true} -> specific_node;
                     _                               -> many_nodes
                 end,
    Ret1 = walk_down_the_tree(
             Root, PathPattern, WorkOnWhat,
             #{keep_while_conds => KeepWhileConds,
               keep_while_conds_revidx => KeepWhileCondsRevIdx,
               keep_while_aftermath => #{}},
             Fun, #{}),
    case Ret1 of
        {ok, Root1, #{keep_while_conds := KeepWhileConds1,
                      keep_while_conds_revidx := KeepWhileCondsRevIdx1,
                      keep_while_aftermath := KeepWhileAftermath},
         Ret2} ->
            State1 = State#?MODULE{root = Root1,
                                   keep_while_conds = KeepWhileConds1,
                                   keep_while_conds_revidx =
                                   KeepWhileCondsRevIdx1},
            {State2, SideEffects} = create_tree_change_side_effects(
                                      State, State1, Ret2, KeepWhileAftermath),
            {State2, {ok, Ret2}, SideEffects};
        Error ->
            ?assertMatch({error, _}, Error),
            {State, Error}
    end.

insert_or_update_node_cb(
  Path, #node{} = Node, Payload, Options, Result) ->
    case maps:is_key(Path, Result) of
        false ->
            %% After a node is modified, we collect properties from the updated
            %% `#node{}', except the payload which is from the old one.
            Node1 = set_node_payload(Node, Payload),
            NodeProps = gather_node_props_from_old_and_new_nodes(
                          Node, Node1, Options),
            {ok, Node1, Result#{Path => NodeProps}};
        true ->
            {ok, Node, Result}
    end;
insert_or_update_node_cb(
  Path, {interrupted, node_not_found = Reason, Info}, Payload, Options,
  Result) ->
    %% We store the payload when we reached the target node only, not in the
    %% parent nodes we have to create in between.
    IsTarget = maps:get(node_is_target, Info),
    case can_continue_update_after_node_not_found(Info) of
        true when IsTarget ->
            Node = create_node_record(Payload),
            NodeProps = gather_node_props_from_old_and_new_nodes(
                          undefined, Node, Options),
            {ok, Node, Result#{Path => NodeProps}};
        true ->
            Node = create_node_record(khepri_payload:none()),
            {ok, Node, Result};
        false ->
            Reason1 = ?khepri_error(Reason, Info),
            {error, Reason1}
    end;
insert_or_update_node_cb(_, {interrupted, Reason, Info}, _, _, _) ->
    Reason1 = ?khepri_error(Reason, Info),
    {error, Reason1}.

can_continue_update_after_node_not_found(#{condition := Condition}) ->
    can_continue_update_after_node_not_found1(Condition);
can_continue_update_after_node_not_found(#{node_name := NodeName}) ->
    can_continue_update_after_node_not_found1(NodeName).

can_continue_update_after_node_not_found1(ChildName)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildName) ->
    true;
can_continue_update_after_node_not_found1(#if_node_exists{exists = false}) ->
    true;
can_continue_update_after_node_not_found1(#if_all{conditions = Conds}) ->
    lists:all(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(#if_any{conditions = Conds}) ->
    lists:any(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(_) ->
    false.

-spec delete_matching_nodes(State, PathPattern, Options) -> Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:tree_options(),
      Ret :: {State, Result} | {State, Result, ra_machine:effects()},
      Result :: khepri_machine:common_ret().
%% @private

delete_matching_nodes(
  #?MODULE{root = Root,
           keep_while_conds = KeepWhileConds,
           keep_while_conds_revidx = KeepWhileCondsRevIdx} = State,
  PathPattern,
  Options) ->
    Ret1 = do_delete_matching_nodes(
             PathPattern, Root,
             #{keep_while_conds => KeepWhileConds,
               keep_while_conds_revidx => KeepWhileCondsRevIdx,
               keep_while_aftermath => #{}},
             Options),
    case Ret1 of
        {ok, Root1, #{keep_while_conds := KeepWhileConds1,
                      keep_while_conds_revidx := KeepWhileCondsRevIdx1,
                      keep_while_aftermath := KeepWhileAftermath},
         Ret2} ->
            State1 = State#?MODULE{
                              root = Root1,
                              keep_while_conds = KeepWhileConds1,
                              keep_while_conds_revidx = KeepWhileCondsRevIdx1},
            {State2, SideEffects} = create_tree_change_side_effects(
                                      State, State1, Ret2, KeepWhileAftermath),
            {State2, {ok, Ret2}, SideEffects};
        Error ->
            {State, Error}
    end.

do_delete_matching_nodes(PathPattern, Root, Extra, Options) ->
    Fun = fun(Path, Node, Result) ->
                  delete_matching_nodes_cb(Path, Node, Options, Result)
          end,
    WorkOnWhat = case Options of
                     #{expect_specific_node := true} -> specific_node;
                     _                               -> many_nodes
                 end,
    walk_down_the_tree(Root, PathPattern, WorkOnWhat, Extra, Fun, #{}).

delete_matching_nodes_cb([] = Path, #node{} = Node, Options, Result) ->
    Node1 = remove_node_payload(Node),
    Node2 = remove_node_child_nodes(Node1),
    NodeProps = gather_node_props(Node, Options),
    {ok, Node2, Result#{Path => NodeProps}};
delete_matching_nodes_cb(Path, #node{} = Node, Options, Result) ->
    NodeProps = gather_node_props(Node, Options),
    {ok, delete, Result#{Path => NodeProps}};
delete_matching_nodes_cb(_, {interrupted, _, _}, _Options, Result) ->
    {ok, keep, Result}.

create_tree_change_side_effects(
  #?MODULE{triggers = Triggers} = _InitialState,
  NewState, _Ret, _KeepWhileAftermath)
  when Triggers =:= #{} ->
    {NewState, []};
create_tree_change_side_effects(
  %% We want to consider the new state (with the updated tree), but we want
  %% to use triggers from the initial state, in case they were updated too.
  %% In other words, we want to evaluate triggers in the state they were at
  %% the time the change to the tree was requested.
  #?MODULE{triggers = Triggers,
           emitted_triggers = EmittedTriggers} = _InitialState,
  #?MODULE{config = #config{store_id = StoreId},
           root = Root} = NewState,
  Ret, KeepWhileAftermath) ->
    %% We make a map where for each affected tree node, we indicate the type
    %% of change.
    Changes0 = maps:merge(Ret, KeepWhileAftermath),
    Changes1 = maps:map(
                 fun
                     (_, NodeProps) when NodeProps =:= #{} -> create;
                     (_, #{} = _NodeProps)                 -> update;
                     (_, delete)                           -> delete
                 end, Changes0),
    TriggeredStoredProcs = list_triggered_sprocs(Root, Changes1, Triggers),

    %% We record the list of triggered stored procedures in the state
    %% machine's state. This is used to guaranty at-least-once execution of
    %% the trigger: the event handler process is supposed to ack when it
    %% executed the triggered stored procedure. If the Ra cluster changes
    %% leader in between, we know that we need to retry the execution.
    %%
    %% This could lead to multiple execution of the same trigger, therefore
    %% the stored procedure must be idempotent.
    NewState1 = NewState#?MODULE{
                            emitted_triggers =
                            EmittedTriggers ++ TriggeredStoredProcs},

    %% We still emit a `mod_call' effect to wake up the event handler process
    %% so it doesn't have to poll the internal list.
    SideEffect = {mod_call,
                  khepri_event_handler,
                  handle_triggered_sprocs,
                  [StoreId, TriggeredStoredProcs]},
    {NewState1, [SideEffect]}.

list_triggered_sprocs(Root, Changes, Triggers) ->
    TriggeredStoredProcs =
    maps:fold(
      fun(Path, Change, TSP) ->
              % For each change, we evaluate each trigger.
              maps:fold(
                fun(TriggerId, TriggerProps, TSP1) ->
                        evaluate_trigger(
                          Root, Path, Change, TriggerId, TriggerProps, TSP1)
                end, TSP, Triggers)
      end, [], Changes),
    sort_triggered_sprocs(TriggeredStoredProcs).

evaluate_trigger(
  Root, Path, Change, TriggerId,
  #{sproc := StoredProcPath,
    event_filter := #evf_tree{path = PathPattern,
                              props = EventFilterProps} = EventFilter},
  TriggeredStoredProcs) ->
    %% For each trigger based on a tree event:
    %%   1. we verify the path of the changed tree node matches the monitored
    %%      path pattern in the event filter.
    %%   2. we verify the type of change matches the change filter in the
    %%      event filter.
    PathMatches = does_path_match(Path, PathPattern, [], Root),
    DefaultWatchedChanges = [create, update, delete],
    WatchedChanges = case EventFilterProps of
                         #{on_actions := []} ->
                             DefaultWatchedChanges;
                         #{on_actions := OnActions}
                           when is_list(OnActions) ->
                             OnActions;
                         _ ->
                             DefaultWatchedChanges
                     end,
    ChangeMatches = lists:member(Change, WatchedChanges),
    case PathMatches andalso ChangeMatches of
        true ->
            %% We then locate the stored procedure. If the path doesn't point
            %% to an existing tree node, or if this tree node is not a stored
            %% procedure, the trigger is ignored.
            %%
            %% TODO: Should we return an error or at least log something? This
            %% could be considered noise if the trigger exists regardless of
            %% the presence of the stored procedure on purpose (for instance
            %% the caller code is being updated).
            case find_stored_proc(Root, StoredProcPath) of
                undefined ->
                    TriggeredStoredProcs;
                StoredProc ->
                    %% TODO: Use a record to format
                    %% stored procedure arguments?
                    EventProps = #{path => Path,
                                   on_action => Change},
                    Triggered = #triggered{
                                   id = TriggerId,
                                   event_filter = EventFilter,
                                   sproc = StoredProc,
                                   props = EventProps
                                  },
                    [Triggered | TriggeredStoredProcs]
            end;
        false ->
            TriggeredStoredProcs
    end;
evaluate_trigger(
  _Root, _Path, _Change, _TriggerId, _TriggerProps, TriggeredStoredProcs) ->
    TriggeredStoredProcs.

does_path_match(PathRest, PathRest, _ReversedPath, _Root) ->
    true;
does_path_match([], _PathPatternRest, _ReversedPath, _Root) ->
    false;
does_path_match(_PathRest, [], _ReversedPath, _Root) ->
    false;
does_path_match(
  [Component | Path], [Component | PathPattern], ReversedPath, Root)
  when ?IS_KHEPRI_PATH_COMPONENT(Component) ->
    does_path_match(Path, PathPattern, [Component | ReversedPath], Root);
does_path_match(
  [Component | _Path], [Condition | _PathPattern], _ReversedPath, _Root)
  when ?IS_KHEPRI_PATH_COMPONENT(Component) andalso
       ?IS_KHEPRI_PATH_COMPONENT(Condition) ->
    false;
does_path_match(
  [Component | Path], [Condition | PathPattern], ReversedPath, Root) ->
    %% Query the tree node, required to evaluate the condition.
    ReversedPath1 = [Component | ReversedPath],
    CurrentPath = lists:reverse(ReversedPath1),
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    {ok, #{CurrentPath := Node}} = khepri_machine:find_matching_nodes(
                                     Root,
                                     lists:reverse([Component | ReversedPath]),
                                     TreeOptions),
    case khepri_condition:is_met(Condition, Component, Node) of
        true ->
            ConditionMatchesGrandchildren =
            case khepri_condition:applies_to_grandchildren(Condition) of
                true ->
                    does_path_match(
                      Path, [Condition | PathPattern], ReversedPath1, Root);
                false ->
                    false
            end,
            ConditionMatchesGrandchildren orelse
              does_path_match(Path, PathPattern, ReversedPath1, Root);
        {false, _} ->
            false
    end.

find_stored_proc(Root, StoredProcPath) ->
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    Ret = khepri_machine:find_matching_nodes(
            Root, StoredProcPath, TreeOptions),
    %% Non-existing nodes and nodes which are not stored procedures are
    %% ignored.
    case Ret of
        {ok, #{StoredProcPath := #{sproc := StoredProc}}} -> StoredProc;
        _                                                 -> undefined
    end.

sort_triggered_sprocs(TriggeredStoredProcs) ->
    %% We first sort by priority, then by triggered ID if priorities are equal.
    %% The priority can be any integer (even negative integers). The default
    %% priority is 0.
    %%
    %% A higher priority (a greater integer) means that the triggered stored
    %% procedure will be executed before another one with lower priority
    %% (smaller integer).
    %%
    %% If the priorities are equal, a trigger with an ID earlier in
    %% alphabetical order will be executed before another one with an ID later
    %% in alphabetical order.
    lists:sort(
      fun(#triggered{id = IdA, event_filter = EventFilterA},
          #triggered{id = IdB, event_filter = EventFilterB}) ->
              PrioA = khepri_evf:get_priority(EventFilterA),
              PrioB = khepri_evf:get_priority(EventFilterB),
              if
                  PrioA =:= PrioB -> IdA =< IdB;
                  true            -> PrioA > PrioB
              end
      end,
      TriggeredStoredProcs).

%% -------

-spec walk_down_the_tree(
        Root, PathPattern, WorkOnWhat, Extra, Fun, FunAcc) -> Ret when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      WorkOnWhat :: specific_node | many_nodes,
      Extra :: walk_down_the_tree_extra(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      Node :: tree_node(),
      Ret :: ok(Node, Extra, FunAcc) | khepri:error().
%% @private

walk_down_the_tree(Root, PathPattern, WorkOnWhat, Extra, Fun, FunAcc) ->
    CompiledPathPattern = khepri_path:compile(PathPattern),
    walk_down_the_tree1(
      Root, CompiledPathPattern, WorkOnWhat,
      [], %% Used to remember the path of the node currently on.
      [], %% Used to update parents up in the tree in a tail-recursive
          %% function.
      Extra, Fun, FunAcc).

-spec walk_down_the_tree1(
        Root, CompiledPathPattern, WorkOnWhat,
        ReversedPath, ReversedParentTree, Extra, Fun, FunAcc) -> Ret when
      Root :: tree_node(),
      CompiledPathPattern :: khepri_path:native_pattern(),
      WorkOnWhat :: specific_node | many_nodes,
      ReversedPath :: khepri_path:native_pattern(),
      ReversedParentTree :: [Node | {Node, child_created}],
      Extra :: walk_down_the_tree_extra(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      Node :: tree_node(),
      Ret :: ok(Node, Extra, FunAcc) | khepri:error().
%% @private

walk_down_the_tree1(
  CurrentNode,
  [?KHEPRI_ROOT_NODE | PathPattern],
  WorkOnWhat, ReversedPath, ReversedParentTree, Extra, Fun, FunAcc) ->
    ?assertEqual([], ReversedPath),
    ?assertEqual([], ReversedParentTree),
    walk_down_the_tree1(
      CurrentNode, PathPattern, WorkOnWhat,
      ReversedPath,
      ReversedParentTree,
      Extra, Fun, FunAcc);
walk_down_the_tree1(
  CurrentNode,
  [?THIS_KHEPRI_NODE | PathPattern],
  WorkOnWhat, ReversedPath, ReversedParentTree, Extra, Fun, FunAcc) ->
    walk_down_the_tree1(
      CurrentNode, PathPattern, WorkOnWhat,
      ReversedPath, ReversedParentTree, Extra, Fun, FunAcc);
walk_down_the_tree1(
  _CurrentNode,
  [?PARENT_KHEPRI_NODE | PathPattern],
  WorkOnWhat,
  [_CurrentName | ReversedPath], [ParentNode0 | ReversedParentTree],
  Extra, Fun, FunAcc) ->
    ParentNode = case ParentNode0 of
                     {PN, child_created} -> PN;
                     _                   -> ParentNode0
                 end,
    walk_down_the_tree1(
      ParentNode, PathPattern, WorkOnWhat,
      ReversedPath, ReversedParentTree, Extra, Fun, FunAcc);
walk_down_the_tree1(
  CurrentNode,
  [?PARENT_KHEPRI_NODE | PathPattern],
  WorkOnWhat,
  [] = ReversedPath, [] = ReversedParentTree,
  Extra, Fun, FunAcc) ->
    %% The path tries to go above the root node, like "cd /..". In this case,
    %% we stay on the root node.
    walk_down_the_tree1(
      CurrentNode, PathPattern, WorkOnWhat,
      ReversedPath, ReversedParentTree, Extra, Fun, FunAcc);
walk_down_the_tree1(
  #node{child_nodes = Children} = CurrentNode,
  [ChildName | PathPattern],
  WorkOnWhat, ReversedPath, ReversedParentTree, Extra, Fun, FunAcc)
  when ?IS_KHEPRI_NODE_ID(ChildName) ->
    case Children of
        #{ChildName := Child} ->
            walk_down_the_tree1(
              Child, PathPattern, WorkOnWhat,
              [ChildName | ReversedPath],
              [CurrentNode | ReversedParentTree],
              Extra, Fun, FunAcc);
        _ ->
            interrupted_walk_down(
              node_not_found,
              #{node_name => ChildName,
                node_path => lists:reverse([ChildName | ReversedPath])},
              PathPattern, WorkOnWhat,
              [ChildName | ReversedPath],
              [CurrentNode | ReversedParentTree],
              Extra, Fun, FunAcc)
    end;
walk_down_the_tree1(
  #node{child_nodes = Children} = CurrentNode,
  [Condition | PathPattern], specific_node = WorkOnWhat,
  ReversedPath, ReversedParentTree, Extra, Fun, FunAcc)
  when ?IS_KHEPRI_CONDITION(Condition) ->
    %% We distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?KHEPRI_ROOT_NODE or
    %% ?THIS_KHEPRI_NODE in the condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?KHEPRI_ROOT_NODE orelse
               NodeName =:= ?THIS_KHEPRI_NODE ->
            CurrentName = special_component_to_node_name(
                            NodeName, ReversedPath),
            CondMet = khepri_condition:is_met(
                        Condition, CurrentName, CurrentNode),
            case CondMet of
                true ->
                    walk_down_the_tree1(
                      CurrentNode, PathPattern, WorkOnWhat,
                      ReversedPath, ReversedParentTree,
                      Extra, Fun, FunAcc);
                {false, Cond} ->
                    interrupted_walk_down(
                      mismatching_node,
                      #{node_name => CurrentName,
                        node_path => lists:reverse(ReversedPath),
                        node_props => gather_node_props_for_error(CurrentNode),
                        condition => Cond},
                      PathPattern, WorkOnWhat, ReversedPath,
                      ReversedParentTree, Extra, Fun, FunAcc)
            end;
        {true, ChildName} when ChildName =/= ?PARENT_KHEPRI_NODE ->
            case Children of
                #{ChildName := Child} ->
                    CondMet = khepri_condition:is_met(
                                Condition, ChildName, Child),
                    case CondMet of
                        true ->
                            walk_down_the_tree1(
                              Child, PathPattern, WorkOnWhat,
                              [ChildName | ReversedPath],
                              [CurrentNode | ReversedParentTree],
                              Extra, Fun, FunAcc);
                        {false, Cond} ->
                            interrupted_walk_down(
                              mismatching_node,
                              #{node_name => ChildName,
                                node_path => lists:reverse(
                                               [ChildName | ReversedPath]),
                                node_props => gather_node_props_for_error(
                                                Child),
                                condition => Cond},
                              PathPattern, WorkOnWhat,
                              [ChildName | ReversedPath],
                              [CurrentNode | ReversedParentTree],
                              Extra, Fun, FunAcc)
                    end;
                _ ->
                    interrupted_walk_down(
                      node_not_found,
                      #{node_name => ChildName,
                        node_path => lists:reverse([ChildName | ReversedPath]),
                        condition => Condition},
                      PathPattern, WorkOnWhat,
                      [ChildName | ReversedPath],
                      [CurrentNode | ReversedParentTree],
                      Extra, Fun, FunAcc)
            end;
        {true, ?PARENT_KHEPRI_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           condition_targets_parent_node,
                           #{path => BadPathPattern,
                             condition => Condition}),
            {error, Exception};
        false ->
            %% The caller expects that the path matches a single specific node
            %% (no matter if it exists or not), but the condition could match
            %% several nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           possibly_matching_many_nodes_denied,
                           #{path => BadPathPattern}),
            {error, Exception}
    end;
walk_down_the_tree1(
  #node{child_nodes = Children} = CurrentNode,
  [Condition | PathPattern] = WholePathPattern, many_nodes = WorkOnWhat,
  ReversedPath, ReversedParentTree, Extra, Fun, FunAcc)
  when ?IS_KHEPRI_CONDITION(Condition) ->
    %% Like with WorkOnWhat =:= specific_node function clause above, We
    %% distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?KHEPRI_ROOT_NODE or
    %% ?THIS_KHEPRI_NODE in the condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?KHEPRI_ROOT_NODE orelse
               NodeName =:= ?THIS_KHEPRI_NODE ->
            CurrentName = special_component_to_node_name(
                            NodeName, ReversedPath),
            CondMet = khepri_condition:is_met(
                        Condition, CurrentName, CurrentNode),
            case CondMet of
                true ->
                    walk_down_the_tree1(
                      CurrentNode, PathPattern, WorkOnWhat,
                      ReversedPath, ReversedParentTree,
                      Extra, Fun, FunAcc);
                {false, _} ->
                    StartingNode = starting_node_in_rev_parent_tree(
                                     ReversedParentTree, CurrentNode),
                    {ok, StartingNode, Extra, FunAcc}
            end;
        {true, ?PARENT_KHEPRI_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           condition_targets_parent_node,
                           #{path => BadPathPattern,
                             condition => Condition}),
            {error, Exception};
        _ ->
            %% There is a special case if the current node is the root node.
            %% The caller can request that the root node's properties are
            %% included. This is true by default if the path is `[]'. This
            %% allows to get its props and payload atomically in a single
            %% query.
            IsRoot = ReversedPath =:= [],
            IncludeRootProps = maps:get(include_root_props, Extra, false),
            Ret0 = case IsRoot andalso IncludeRootProps of
                       true ->
                           walk_down_the_tree1(
                             CurrentNode, [], WorkOnWhat,
                             [], [],
                             Extra, Fun, FunAcc);
                       _ ->
                           {ok, CurrentNode, Extra, FunAcc}
                   end,

            %% The result of the first part (the special case for the root
            %% node if relevant) is used as a starting point for handling all
            %% child nodes.
            Ret1 = maps:fold(
                     fun
                         (ChildName, Child, {ok, CurNode, Extra1, FunAcc1}) ->
                             handle_branch(
                               CurNode, ChildName, Child,
                               WholePathPattern, WorkOnWhat,
                               ReversedPath,
                               Extra1, Fun, FunAcc1);
                         (_, _, Error) ->
                             Error
                     end, Ret0, Children),
            case Ret1 of
                {ok, CurrentNode, Extra, FunAcc2} ->
                    %% The current node didn't change, no need to update the
                    %% tree and evaluate keep_while conditions.
                    StartingNode = starting_node_in_rev_parent_tree(
                                     ReversedParentTree, CurrentNode),
                    {ok, StartingNode, Extra, FunAcc2};
                {ok, CurrentNode1, Extra2, FunAcc2} ->
                    CurrentNode2 = case CurrentNode1 of
                                       CurrentNode ->
                                           CurrentNode;
                                       _ ->
                                           %% Because of the loop, payload &
                                           %% child list versions may have
                                           %% been increased multiple times.
                                           %% We want them to increase once
                                           %% for the whole (atomic)
                                           %% operation.
                                           squash_version_bumps(
                                             CurrentNode, CurrentNode1)
                                   end,
                    walk_back_up_the_tree(
                      CurrentNode2, ReversedPath, ReversedParentTree,
                      Extra2, FunAcc2);
                Error ->
                    Error
            end
    end;
walk_down_the_tree1(
  #node{} = CurrentNode, [], _,
  ReversedPath, ReversedParentTree, Extra, Fun, FunAcc) ->
    CurrentPath = lists:reverse(ReversedPath),
    case Fun(CurrentPath, CurrentNode, FunAcc) of
        {ok, keep, FunAcc1} ->
            StartingNode = starting_node_in_rev_parent_tree(
                             ReversedParentTree, CurrentNode),
            {ok, StartingNode, Extra, FunAcc1};
        {ok, delete, FunAcc1} ->
            walk_back_up_the_tree(
              delete, ReversedPath, ReversedParentTree, Extra, FunAcc1);
        {ok, #node{} = CurrentNode1, FunAcc1} ->
            walk_back_up_the_tree(
              CurrentNode1, ReversedPath, ReversedParentTree, Extra, FunAcc1);
        Error ->
            Error
    end.

-spec special_component_to_node_name(SpecialComponent, ReversedPath) ->
    NodeName when
      SpecialComponent :: ?KHEPRI_ROOT_NODE | ?THIS_KHEPRI_NODE,
      ReversedPath :: khepri_path:native_path(),
      NodeName :: khepri_path:component().

special_component_to_node_name(?KHEPRI_ROOT_NODE = NodeName, []) ->
    NodeName;
special_component_to_node_name(?THIS_KHEPRI_NODE, [NodeName | _]) ->
    NodeName;
special_component_to_node_name(?THIS_KHEPRI_NODE, []) ->
    ?KHEPRI_ROOT_NODE.

-spec starting_node_in_rev_parent_tree(ReversedParentTree) -> Node when
      Node :: tree_node(),
      ReversedParentTree :: [Node].
%% @private

starting_node_in_rev_parent_tree(ReversedParentTree) ->
    hd(lists:reverse(ReversedParentTree)).

-spec starting_node_in_rev_parent_tree(ReversedParentTree, Node) -> Node when
      Node :: tree_node(),
      ReversedParentTree :: [Node].
%% @private

starting_node_in_rev_parent_tree([], CurrentNode) ->
    CurrentNode;
starting_node_in_rev_parent_tree(ReversedParentTree, _) ->
    starting_node_in_rev_parent_tree(ReversedParentTree).

-spec handle_branch(
        Node, ChildName, Child, WholePathPattern, WorkOnWhat,
        ReversedPath, Extra, Fun, FunAcc) -> Ret when
      Node :: tree_node(),
      ChildName :: khepri_path:component(),
      Child :: tree_node(),
      WholePathPattern :: khepri_path:native_pattern(),
      WorkOnWhat :: specific_node | many_nodes,
      ReversedPath :: [Node | {Node, child_created}],
      Extra :: walk_down_the_tree_extra(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      Ret :: ok(Node, Extra, FunAcc) | khepri:error().
%% @private

handle_branch(
  CurrentNode, ChildName, Child,
  [Condition | PathPattern] = WholePathPattern,
  WorkOnWhat, ReversedPath, Extra, Fun, FunAcc) ->
    %% FIXME: A condition such as #if_path_matches{regex = any} at the end of
    %% a path matches non-leaf nodes as well: we should call Fun() for them!
    CondMet = khepri_condition:is_met(
                Condition, ChildName, Child),
    Ret = case CondMet of
              true ->
                  walk_down_the_tree1(
                    Child, PathPattern, WorkOnWhat,
                    [ChildName | ReversedPath],
                    [CurrentNode],
                    Extra, Fun, FunAcc);
              {false, _} ->
                  {ok, CurrentNode, Extra, FunAcc}
          end,
    case Ret of
        {ok, CurrentNode1, Extra1, FunAcc1} ->
            case khepri_condition:applies_to_grandchildren(Condition) of
                false ->
                    Ret;
                true ->
                    walk_down_the_tree1(
                      Child, WholePathPattern, WorkOnWhat,
                      [ChildName | ReversedPath],
                      [CurrentNode1],
                      Extra1, Fun, FunAcc1)
            end;
        Error ->
            Error
    end.

-spec interrupted_walk_down(
        Reason, Info, PathPattern, WorkOnWhat,
        ReversedPath, ReversedParentTree,
        Extra, Fun, FunAcc) -> Ret when
      Reason :: mismatching_node | node_not_found,
      Info :: map(),
      PathPattern :: khepri_path:native_pattern(),
      WorkOnWhat :: specific_node | many_nodes,
      ReversedPath :: khepri_path:native_path(),
      Node :: tree_node(),
      ReversedParentTree :: [Node | {Node, child_created}],
      Extra :: walk_down_the_tree_extra(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      Ret :: ok(Node, Extra, FunAcc) | khepri:error().
%% @private

interrupted_walk_down(
  Reason, Info, PathPattern, WorkOnWhat, ReversedPath, ReversedParentTree,
  Extra, Fun, FunAcc) ->
    NodePath = lists:reverse(ReversedPath),
    IsTarget = khepri_path:realpath(PathPattern) =:= [],
    Info1 = Info#{node_is_target => IsTarget},
    ErrorTuple = {interrupted, Reason, Info1},
    case Fun(NodePath, ErrorTuple, FunAcc) of
        {ok, ToDo, FunAcc1}
          when ToDo =:= keep orelse ToDo =:= delete ->
            ?assertNotEqual([], ReversedParentTree),
            StartingNode = starting_node_in_rev_parent_tree(
                             ReversedParentTree),
            {ok, StartingNode, Extra, FunAcc1};
        {ok, #node{} = NewNode, FunAcc1} ->
            ReversedParentTree1 =
            case Reason of
                node_not_found ->
                    %% We record the fact the child is a new node. This is used
                    %% to reset the child's stats if it got new payload or
                    %% child nodes at the same time.
                    [{hd(ReversedParentTree), child_created}
                     | tl(ReversedParentTree)];
                _ ->
                    ReversedParentTree
            end,
            case PathPattern of
                [] ->
                    %% We reached the target node. We could call
                    %% walk_down_the_tree1() again, but it would call Fun() a
                    %% second time.
                    walk_back_up_the_tree(
                      NewNode, ReversedPath, ReversedParentTree1,
                      Extra, FunAcc1);
                _ ->
                    walk_down_the_tree1(
                      NewNode, PathPattern, WorkOnWhat,
                      ReversedPath, ReversedParentTree1,
                      Extra, Fun, FunAcc1)
            end;
        Error ->
            Error
    end.

-spec reset_versions(Node) -> Node when
      Node :: tree_node().
%% @private

reset_versions(#node{props = Stat} = CurrentNode) ->
    Stat1 = Stat#{payload_version => ?INIT_DATA_VERSION,
                  child_list_version => ?INIT_CHILD_LIST_VERSION},
    CurrentNode#node{props = Stat1}.

-spec squash_version_bumps(OldNode, NewNode) -> Node when
      OldNode :: tree_node(),
      NewNode :: tree_node(),
      Node :: tree_node().
%% @private

squash_version_bumps(
  #node{props = #{payload_version := DVersion,
                  child_list_version := CVersion}},
  #node{props = #{payload_version := DVersion,
                  child_list_version := CVersion}} = CurrentNode) ->
    CurrentNode;
squash_version_bumps(
  #node{props = #{payload_version := OldDVersion,
                  child_list_version := OldCVersion}},
  #node{props = #{payload_version := NewDVersion,
                  child_list_version := NewCVersion} = Stat} = CurrentNode) ->
    DVersion = case NewDVersion > OldDVersion of
                   true  -> OldDVersion + 1;
                   false -> OldDVersion
               end,
    CVersion = case NewCVersion > OldCVersion of
                   true  -> OldCVersion + 1;
                   false -> OldCVersion
               end,
    Stat1 = Stat#{payload_version => DVersion,
                  child_list_version => CVersion},
    CurrentNode#node{props = Stat1}.

-spec walk_back_up_the_tree(
  Child, ReversedPath, ReversedParentTree, Extra, FunAcc) -> Ret when
      Node :: tree_node(),
      Child :: Node | delete,
      ReversedPath :: khepri_path:native_path(),
      ReversedParentTree :: [Node | {Node, child_created}],
      Extra :: walk_down_the_tree_extra(),
      FunAcc :: any(),
      Ret :: ok(Node, Extra, FunAcc).
%% @private

walk_back_up_the_tree(
  Child, ReversedPath, ReversedParentTree, Extra, FunAcc) ->
    walk_back_up_the_tree(
      Child, ReversedPath, ReversedParentTree, Extra, #{}, FunAcc).

-spec walk_back_up_the_tree(
  Child, ReversedPath, ReversedParentTree, Extra, KeepWhileAftermath,
  FunAcc) -> Ret when
      Node :: tree_node(),
      Child :: Node | delete,
      ReversedPath :: khepri_path:native_path(),
      ReversedParentTree :: [Node | {Node, child_created}],
      Extra :: walk_down_the_tree_extra(),
      KeepWhileAftermath :: #{khepri_path:native_path() => Node | delete},
      FunAcc :: any(),
      Ret :: ok(Node, Extra, FunAcc).
%% @private

walk_back_up_the_tree(
  delete,
  [ChildName | ReversedPath] = WholeReversedPath,
  [ParentNode | ReversedParentTree], Extra, KeepWhileAftermath, FunAcc) ->
    %% Evaluate keep_while of nodes which depended on ChildName (it is
    %% removed) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    KeepWhileAftermath1 = KeepWhileAftermath#{Path => delete},

    %% Evaluate keep_while of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = remove_node_child(ParentNode, ChildName),
    handle_keep_while_for_parent_update(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepWhileAftermath1, FunAcc);
walk_back_up_the_tree(
  Child,
  [ChildName | ReversedPath],
  [{ParentNode, child_created} | ReversedParentTree],
  Extra, KeepWhileAftermath, FunAcc) ->
    %% No keep_while to evaluate, the child is new and no nodes depend on it
    %% at this stage.
    %% FIXME: Perhaps there is a condition in a if_any{}?
    Child1 = reset_versions(Child),

    %% Evaluate keep_while of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = add_node_child(ParentNode, ChildName, Child1),
    handle_keep_while_for_parent_update(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepWhileAftermath, FunAcc);
walk_back_up_the_tree(
  Child,
  [ChildName | ReversedPath] = WholeReversedPath,
  [ParentNode | ReversedParentTree],
  Extra, KeepWhileAftermath, FunAcc) ->
    %% Evaluate keep_while of nodes which depend on ChildName (it is
    %% modified) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    NodeProps = gather_node_props(Child, TreeOptions),
    KeepWhileAftermath1 = KeepWhileAftermath#{Path => NodeProps},

    %% No need to evaluate keep_while of ParentNode, its child_count is
    %% unchanged.
    ParentNode1 = update_node_child(ParentNode, ChildName, Child),
    walk_back_up_the_tree(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepWhileAftermath1, FunAcc);
walk_back_up_the_tree(
  StartingNode,
  [], %% <-- We reached the root (i.e. not in a branch, see handle_branch())
  [], Extra, KeepWhileAftermath, FunAcc) ->
    Extra1 = merge_keep_while_aftermath(Extra, KeepWhileAftermath),
    handle_keep_while_aftermath(StartingNode, Extra1, FunAcc);
walk_back_up_the_tree(
  StartingNode,
  _ReversedPath,
  [], Extra, KeepWhileAftermath, FunAcc) ->
    Extra1 = merge_keep_while_aftermath(Extra, KeepWhileAftermath),
    {ok, StartingNode, Extra1, FunAcc}.

handle_keep_while_for_parent_update(
  ParentNode,
  ReversedPath,
  ReversedParentTree,
  Extra, KeepWhileAftermath, FunAcc) ->
    ParentPath = lists:reverse(ReversedPath),
    IsMet = is_keep_while_condition_met_on_self(
              ParentPath, ParentNode, Extra),
    case IsMet of
        true ->
            %% We continue with the update.
            walk_back_up_the_tree(
              ParentNode, ReversedPath, ReversedParentTree,
              Extra, KeepWhileAftermath, FunAcc);
        {false, _Reason} ->
            %% This parent node must be removed because it doesn't meet its
            %% own keep_while condition. keep_while conditions for nodes
            %% depending on this one will be evaluated with the recursion.
            walk_back_up_the_tree(
              delete, ReversedPath, ReversedParentTree,
              Extra, KeepWhileAftermath, FunAcc)
    end.

merge_keep_while_aftermath(Extra, KeepWhileAftermath) ->
    OldKWA = maps:get(keep_while_aftermath, Extra, #{}),
    NewKWA = maps:fold(
               fun
                   (Path, delete, KWA1) ->
                       KWA1#{Path => delete};
                   (Path, NodeProps, KWA1) ->
                       case KWA1 of
                           #{Path := delete} -> KWA1;
                           _                 -> KWA1#{Path => NodeProps}
                       end
               end, OldKWA, KeepWhileAftermath),
    Extra#{keep_while_aftermath => NewKWA}.

handle_keep_while_aftermath(
  Root,
  #{keep_while_aftermath := KeepWhileAftermath} = Extra,
  FunAcc)
  when KeepWhileAftermath =:= #{} ->
    {ok, Root, Extra, FunAcc};
handle_keep_while_aftermath(
  Root,
  #{keep_while_conds := KeepWhileConds,
    keep_while_conds_revidx := KeepWhileCondsRevIdx,
    keep_while_aftermath := KeepWhileAftermath} = Extra,
  FunAcc) ->
    ToRemove = eval_keep_while_conditions(
                 KeepWhileAftermath, KeepWhileConds, KeepWhileCondsRevIdx,
                 Root),

    {KeepWhileConds1,
     KeepWhileCondsRevIdx1} = maps:fold(
                            fun
                                (RemovedPath, delete, {KW, KWRevIdx}) ->
                                    KW1 = maps:remove(RemovedPath, KW),
                                    KWRevIdx1 = update_keep_while_conds_revidx(
                                                  KW, KWRevIdx,
                                                  RemovedPath, #{}),
                                    {KW1, KWRevIdx1};
                                (_, _, Acc) ->
                                    Acc
                            end, {KeepWhileConds, KeepWhileCondsRevIdx},
                            KeepWhileAftermath),
    Extra1 = Extra#{keep_while_conds => KeepWhileConds1,
                    keep_while_conds_revidx => KeepWhileCondsRevIdx1},

    ToRemove1 = filter_and_sort_paths_to_remove(ToRemove, KeepWhileAftermath),
    remove_expired_nodes(ToRemove1, Root, Extra1, FunAcc).

eval_keep_while_conditions(
  KeepWhileAftermath, KeepWhileConds, KeepWhileCondsRevIdx, Root) ->
    %% KeepWhileAftermath lists all nodes which were modified or removed. We
    %% want to transform that into a list of nodes to remove.
    %%
    %% Those marked as `delete' in KeepWhileAftermath are already gone. We
    %% need to find the nodes which depended on them, i.e. their keep_while
    %% condition is not met anymore. Note that removed nodes' child nodes are
    %% gone as well and must be handled (they are not specified in
    %% KeepWhileAftermath).
    %%
    %% Those modified in KeepWhileAftermath must be evaluated again to decide
    %% if they should be removed.
    maps:fold(
      fun
          (RemovedPath, delete, ToRemove) ->
              maps:fold(
                fun(Path, Watchers, ToRemove1) ->
                        case lists:prefix(RemovedPath, Path) of
                            true ->
                                eval_keep_while_conditions_after_removal(
                                  Watchers, KeepWhileConds, Root, ToRemove1);
                            false ->
                                ToRemove1
                        end
                end, ToRemove, KeepWhileCondsRevIdx);
          (UpdatedPath, NodeProps, ToRemove) ->
              case KeepWhileCondsRevIdx of
                  #{UpdatedPath := Watchers} ->
                      eval_keep_while_conditions_after_update(
                        UpdatedPath, NodeProps,
                        Watchers, KeepWhileConds, Root, ToRemove);
                  _ ->
                      ToRemove
              end
      end, #{}, KeepWhileAftermath).

eval_keep_while_conditions_after_update(
  UpdatedPath, NodeProps, Watchers, KeepWhileConds, Root, ToRemove) ->
    maps:fold(
      fun(Watcher, ok, ToRemove1) ->
              KeepWhile = maps:get(Watcher, KeepWhileConds),
              CondOnUpdated = maps:get(UpdatedPath, KeepWhile),
              IsMet = khepri_condition:is_met(
                        CondOnUpdated, UpdatedPath, NodeProps),
              case IsMet of
                  true ->
                      ToRemove1;
                  {false, _} ->
                      case are_keep_while_conditions_met(Root, KeepWhile) of
                          true       -> ToRemove1;
                          {false, _} -> ToRemove1#{Watcher => remove}
                      end
              end
      end, ToRemove, Watchers).

eval_keep_while_conditions_after_removal(
  Watchers, KeepWhileConds, Root, ToRemove) ->
    maps:fold(
      fun(Watcher, ok, ToRemove1) ->
              KeepWhile = maps:get(Watcher, KeepWhileConds),
              case are_keep_while_conditions_met(Root, KeepWhile) of
                  true       -> ToRemove1;
                  {false, _} -> ToRemove1#{Watcher => delete}
              end
      end, ToRemove, Watchers).

filter_and_sort_paths_to_remove(ToRemove, KeepWhileAftermath) ->
    Paths1 = lists:sort(
               fun
                   (A, B) when length(A) =:= length(B) ->
                       A < B;
                   (A, B) ->
                       length(A) < length(B)
               end,
               maps:keys(ToRemove)),
    Paths2 = lists:foldl(
               fun(Path, Map) ->
                       case KeepWhileAftermath of
                           #{Path := delete} ->
                               Map;
                           _ ->
                               case is_parent_being_removed(Path, Map) of
                                   false -> Map#{Path => delete};
                                   true  -> Map
                               end
                       end
               end, #{}, Paths1),
    maps:keys(Paths2).

is_parent_being_removed([], _) ->
    false;
is_parent_being_removed(Path, Map) ->
    is_parent_being_removed1(lists:reverse(Path), Map).

is_parent_being_removed1([_ | Parent], Map) ->
    case maps:is_key(lists:reverse(Parent), Map) of
        true  -> true;
        false -> is_parent_being_removed1(Parent, Map)
    end;
is_parent_being_removed1([], _) ->
    false.

remove_expired_nodes([], Root, Extra, FunAcc) ->
    {ok, Root, Extra, FunAcc};
remove_expired_nodes([PathToRemove | Rest], Root, Extra, FunAcc) ->
    case do_delete_matching_nodes(PathToRemove, Root, Extra, #{}) of
        {ok, Root1, Extra1, _} ->
            remove_expired_nodes(Rest, Root1, Extra1, FunAcc)
    end.

-ifdef(TEST).
get_root(#?MODULE{root = Root}) ->
    Root.

get_keep_while_conds(
  #?MODULE{keep_while_conds = KeepWhileConds}) ->
    KeepWhileConds.

get_keep_while_conds_revidx(
  #?MODULE{keep_while_conds_revidx = KeepWhileCondsRevIdx}) ->
    KeepWhileCondsRevIdx.
-endif.
