%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
-include("src/khepri_fun.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").

-export([put/5,
         get/3,
         count/3,
         delete/3,
         transaction/4,
         run_sproc/4,
         register_trigger/5]).
-export([get_keep_while_conds_state/2]).
-export([init/1,
         apply/3,
         state_enter/2]).
%% For internal use only.
-export([clear_cache/1,
         ack_triggers_execution/2,
         find_matching_nodes/3,
         insert_or_update_node/4,
         delete_matching_nodes/2]).

-ifdef(TEST).
-export([are_keep_while_conditions_met/2,
         get_root/1,
         get_keep_while_conds/1,
         get_keep_while_conds_revidx/1,
         get_cached_leader/1,
         get_last_consistent_call_atomics/1]).
-endif.

-compile({no_auto_import, [apply/3]}).

-type tree_node() :: #node{}.
%% A node in the tree structure.

-type stat() :: #{payload_version := khepri:payload_version(),
                  child_list_version := khepri:child_list_version()}.
%% Stats attached to each node in the tree structure.

-type triggered() :: #triggered{}.

-type command() :: #put{} |
                   #delete{} |
                   #tx{} |
                   #register_trigger{} |
                   #ack_triggered{}.
%% Commands specific to this Ra machine.

-type machine_init_args() :: #{store_id := khepri:store_id(),
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

-export_type([state/0,
              machine_config/0,
              tree_node/0,
              stat/0,
              triggered/0,
              keep_while_conds_map/0,
              keep_while_conds_revidx/0]).

%% -------------------------------------------------------------------
%% Machine protocol.
%% -------------------------------------------------------------------

%% TODO: Verify arguments carefully to avoid the construction of an invalid
%% command.

-spec put(StoreId, PathPattern, Payload, Extra, Options) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_payload:payload(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: khepri:command_options(),
      Result :: khepri:result() | NoRetIfAsync,
      NoRetIfAsync :: ok.
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the node to create or
%%        modify.
%% @param Payload the payload to put in the specified node.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous put, an `{ok, Result}' tuple with a
%% map with one entry, or an `{error, Reason}' tuple; in the case of an
%% asynchronous put, always `ok' (the actual return value may be sent by a
%% message if a correlation ID was specified).
%%
%% @private

put(StoreId, PathPattern, Payload, Extra, Options)
  when ?IS_KHEPRI_PAYLOAD(Payload) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Payload1 = khepri_payload:prepare(Payload),
    Extra1 = case Extra of
                 #{keep_while := KeepWhile} ->
                     KeepWhile1 = khepri_condition:ensure_native_keep_while(
                                    KeepWhile),
                     Extra#{keep_while => KeepWhile1};
                 _ ->
                     Extra
             end,
    Command = #put{path = PathPattern1,
                   payload = Payload1,
                   extra = Extra1},
    process_command(StoreId, Command, Options);
put(_StoreId, PathPattern, Payload, _Extra, _Options) ->
    throw({invalid_payload, PathPattern, Payload}).

-spec get(StoreId, PathPattern, Options) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to get.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, Result}' tuple with a map with zero, one or more entries,
%% or an `{error, Reason}' tuple.

get(StoreId, PathPattern, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Query = fun(#?MODULE{root = Root}) ->
                    find_matching_nodes(Root, PathPattern1, Options)
            end,
    process_query(StoreId, Query, Options).

-spec count(StoreId, PathPattern, Options) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:ok(integer()) | khepri:error().
%% @doc Counts all tree nodes matching the path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to count.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, Count}' tuple with the number of matching tree nodes, or
%% an `{error, Reason}' tuple.

count(StoreId, PathPattern, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Query = fun(#?MODULE{root = Root}) ->
                    count_matching_nodes(Root, PathPattern1, Options)
            end,
    process_query(StoreId, Query, Options).

-spec delete(StoreId, PathPattern, Options) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options(),
      Result :: khepri:result() | NoRetIfAsync,
      NoRetIfAsync :: ok.
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to delete.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous delete, an `{ok, Result}' tuple with
%% a map with zero, one or more entries, or an `{error, Reason}' tuple; in the
%% case of an asynchronous put, always `ok' (the actual return value may be
%% sent by a message if a correlation ID was specified).

delete(StoreId, PathPattern, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Command = #delete{path = PathPattern1},
    process_command(StoreId, Command, Options).

-spec transaction(StoreId, Fun, ReadWrite, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Fun :: khepri_tx:tx_fun(),
      ReadWrite :: ro | rw | auto,
      Options :: khepri:command_options() | khepri:query_options(),
      Ret :: Atomic | Aborted | NoRetIfAsync,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort(),
      NoRetIfAsync :: ok.
%% @doc Runs a transaction and returns the result.
%%
%% @param StoreId the name of the Ra cluster.
%% @param Fun an arbitrary anonymous function.
%% @param ReadWrite the read/write or read-only nature of the transaction.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous transaction, `{atomic, Result}' where
%% `Result' is the return value of `Fun', or `{aborted, Reason}' if the
%% anonymous function was aborted; in the case of an asynchronous transaction,
%% always `ok' (the actual return value may be sent by a message if a
%% correlation ID was specified).

transaction(StoreId, Fun, auto = ReadWrite, Options)
  when is_function(Fun, 0) ->
    case khepri_tx:to_standalone_fun(Fun, ReadWrite) of
        #standalone_fun{} = StandaloneFun ->
            readwrite_transaction(StoreId, StandaloneFun, Options);
        _ ->
            readonly_transaction(StoreId, Fun, Options)
    end;
transaction(StoreId, Fun, rw = ReadWrite, Options)
  when is_function(Fun, 0) ->
    StandaloneFun = khepri_tx:to_standalone_fun(Fun, ReadWrite),
    readwrite_transaction(StoreId, StandaloneFun, Options);
transaction(StoreId, Fun, ro, Options) when is_function(Fun, 0) ->
    readonly_transaction(StoreId, Fun, Options);
transaction(_StoreId, Fun, _ReadWrite, _Options) when is_function(Fun) ->
    {arity, Arity} = erlang:fun_info(Fun, arity),
    throw({invalid_tx_fun, {requires_args, Arity}});
transaction(_StoreId, Term, _ReadWrite, _Options) ->
    throw({invalid_tx_fun, Term}).

-spec readonly_transaction(StoreId, Fun, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Fun :: khepri_tx:tx_fun(),
      Options :: khepri:query_options(),
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().

readonly_transaction(StoreId, Fun, Options) when is_function(Fun, 0) ->
    Query = fun(State) ->
                    %% It is a read-only transaction, therefore we assert that
                    %% the state is unchanged and that there are no side
                    %% effects.
                    {State, Ret, []} = khepri_tx:run(State, Fun, false),
                    Ret
            end,
    case process_query(StoreId, Query, Options) of
        {exception, _, {aborted, _} = Aborted, _} ->
            Aborted;
        {exception, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        Ret ->
            {atomic, Ret}
    end.

-spec readwrite_transaction(StoreId, Fun, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Fun :: khepri_fun:standalone_fun(),
      Options :: khepri:command_options(),
      Ret :: Atomic | Aborted | NoRetIfAsync,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort(),
      NoRetIfAsync :: ok.

readwrite_transaction(StoreId, StandaloneFun, Options) ->
    Command = #tx{'fun' = StandaloneFun},
    case process_command(StoreId, Command, Options) of
        {exception, _, {aborted, _} = Aborted, _} ->
            Aborted;
        {exception, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        ok = Ret ->
            CommandType = select_command_type(Options),
            case CommandType of
                sync          -> {atomic, Ret};
                {async, _, _} -> Ret
            end;
        Ret ->
            {atomic, Ret}
    end.

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

run_sproc(StoreId, PathPattern, Args, Options) when is_list(Args) ->
    Options1 = Options#{expect_specific_node => true},
    case get(StoreId, PathPattern, Options1) of
        {ok, Result} ->
            [Value] = maps:values(Result),
            case Value of
                #{sproc := StandaloneFun} ->
                    khepri_sproc:run(StandaloneFun, Args);
                _ ->
                    [Path] = maps:keys(Result),
                    throw({invalid_sproc_fun,
                           {no_sproc, Path, Value}})
            end;
        Error ->
            throw({invalid_sproc_fun, Error})
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

register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath, Options) ->
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

ack_triggers_execution(StoreId, TriggeredStoredProcs) ->
    Command = #ack_triggered{triggered = TriggeredStoredProcs},
    process_command(StoreId, Command, #{}).

-spec get_keep_while_conds_state(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: {ok, keep_while_conds_map()} | khepri:error().
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

get_keep_while_conds_state(StoreId, Options) ->
    Query = fun(#?MODULE{keep_while_conds = KeepWhileConds}) ->
                    {ok, KeepWhileConds}
            end,
    Options1 = Options#{favor => consistency},
    process_query(StoreId, Query, Options1).

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
    case get_leader(StoreId) of
        LeaderId when LeaderId =/= undefined ->
            Timeout = get_timeout(Options),
            case ra:process_command(LeaderId, Command, Timeout) of
                {ok, Ret, NewLeaderId} ->
                    cache_leader_if_changed(
                      StoreId, LeaderId, NewLeaderId),
                    just_did_consistent_call(StoreId),
                    Ret;
                {timeout, _} = TimedOut ->
                    {error, TimedOut};
                {error, _} = Error ->
                    Error
            end;
        undefined ->
            {error, ra_leader_unknown}
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
    process_query_response(StoreId, undefined, local, Ret).

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
    case get_leader(StoreId) of
        LeaderId when LeaderId =/= undefined ->
            Ret = case QueryType of
                      leader ->
                          ra:leader_query(LeaderId, QueryFun, Timeout);
                      consistent ->
                          ra:consistent_query(LeaderId, QueryFun, Timeout)
                  end,
            %% TODO: If the consistent query times out in the context of
            %% `QueryType=compromise`, should we retry with a local query to
            %% never block the query and let the caller continue?
            process_query_response(StoreId, LeaderId, QueryType, Ret);
        undefined ->
            {error, ra_leader_unknown}
    end.

-spec process_query_response(StoreId, LeaderId, QueryType, Response) ->
    Ret when
      StoreId :: khepri:store_id(),
      LeaderId :: ra:server_id() | undefined,
      QueryType :: local | leader | consistent,
      Response :: {ok, {RaIndex, any()}, NewLeaderId} |
                  {ok, any(), NewLeaderId} |
                  {error, any()} |
                  {timeout, ra:server_id()},
      RaIndex :: ra:index(),
      NewLeaderId :: ra:server_id(),
      Ret :: any().

process_query_response(
  StoreId, LeaderId, consistent, {ok, Ret, NewLeaderId}) ->
    cache_leader_if_changed(StoreId, LeaderId, NewLeaderId),
    just_did_consistent_call(StoreId),
    Ret;
process_query_response(
  StoreId, LeaderId, _QueryType, {ok, {_RaIndex, Ret}, NewLeaderId}) ->
    cache_leader_if_changed(StoreId, LeaderId, NewLeaderId),
    Ret;
process_query_response(
  _StoreId, _LeaderId, _QueryType, {timeout, _} = TimedOut) ->
    {error, TimedOut};
process_query_response(
  _StoreId, _LeaderId, _QueryType, {error, _} = Error) ->
    Error.

%% Cache the Ra leader ID to avoid command/query redirections from a follower
%% to the leader. The leader ID is returned after each command or query. If we
%% don't know it yet, ask Ra what the leader ID is.

-define(RA_LEADER_CACHE_KEY(StoreId), {khepri, ra_leader_cache, StoreId}).

-spec get_leader(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: LeaderId | undefined,
      LeaderId :: ra:server_id().

get_leader(StoreId) ->
    case get_cached_leader(StoreId) of
        LeaderId when LeaderId =/= undefined ->
            LeaderId;
        undefined ->
            %% StoreId is the same as Ra's cluster name.
            case ra_leaderboard:lookup_leader(StoreId) of
                LeaderId when LeaderId =/= undefined ->
                    cache_leader(StoreId, LeaderId),
                    LeaderId;
                undefined ->
                    undefined
            end
    end.

-spec get_cached_leader(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: LeaderId | undefined,
      LeaderId :: ra:server_id().

get_cached_leader(StoreId) ->
    Key = ?RA_LEADER_CACHE_KEY(StoreId),
    persistent_term:get(Key, undefined).

-spec cache_leader(StoreId, LeaderId) -> ok when
      StoreId :: khepri:store_id(),
      LeaderId :: ra:server_id().

cache_leader(StoreId, LeaderId) ->
    ok = persistent_term:put(?RA_LEADER_CACHE_KEY(StoreId), LeaderId).

-spec cache_leader_if_changed(StoreId, LeaderId, NewLeaderId) -> ok when
      StoreId :: khepri:store_id(),
      LeaderId :: ra:server_id(),
      NewLeaderId :: ra:server_id().

cache_leader_if_changed(_StoreId, LeaderId, LeaderId) ->
    ok;
cache_leader_if_changed(StoreId, undefined, NewLeaderId) ->
    case persistent_term:get(?RA_LEADER_CACHE_KEY(StoreId), undefined) of
        LeaderId when LeaderId =/= undefined ->
            cache_leader_if_changed(StoreId, LeaderId, NewLeaderId);
        undefined ->
            cache_leader(StoreId, NewLeaderId)
    end;
cache_leader_if_changed(StoreId, _OldLeaderId, NewLeaderId) ->
    cache_leader(StoreId, NewLeaderId).

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
get_timeout(_)                     -> infinity.

-spec clear_cache(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Clears the cached data for the given `StoreId'.
%%
%% @private

clear_cache(StoreId) ->
    _ = persistent_term:erase(?RA_LEADER_CACHE_KEY(StoreId)),
    _ = persistent_term:erase(?LAST_CONSISTENT_CALL_TS_REF(StoreId)),
    ok.

%% -------------------------------------------------------------------
%% ra_machine callbacks.
%% -------------------------------------------------------------------

-spec init(Params) -> State when
      Params :: machine_init_args(),
      State :: state().
%% @private

init(#{store_id := StoreId} = Params) ->
    Config = case Params of
                 #{snapshot_interval := SnapshotInterval} ->
                     #config{store_id = StoreId,
                             snapshot_interval = SnapshotInterval};
                 _ ->
                     #config{store_id = StoreId}
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
  #put{path = PathPattern, payload = Payload, extra = Extra},
  State) ->
    Ret = insert_or_update_node(State, PathPattern, Payload, Extra),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #delete{path = PathPattern},
  State) ->
    Ret = delete_matching_nodes(State, PathPattern),
    bump_applied_command_count(Ret, Meta);
apply(
  Meta,
  #tx{'fun' = StandaloneFun},
  State) ->
    Fun = case is_function(StandaloneFun) of
              false -> fun() -> khepri_fun:exec(StandaloneFun, []) end;
              true  -> StandaloneFun
          end,
    Ret = khepri_tx:run(State, Fun, true),
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

state_enter(
  leader,
  #?MODULE{emitted_triggers = []}) ->
    [];
state_enter(
  leader,
  #?MODULE{config = #config{store_id = StoreId},
           emitted_triggers = EmittedTriggers}) ->
    SideEffect = {mod_call,
                  khepri_event_handler,
                  handle_triggered_sprocs,
                  [StoreId, EmittedTriggers]},
    [SideEffect];
state_enter(_StateName, _State) ->
    [].

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec create_node_record(Payload) -> Node when
      Payload :: khepri_payload:payload(),
      Node :: tree_node().
%% @private

create_node_record(Payload) ->
    #node{stat = ?INIT_NODE_STAT,
          payload = Payload}.

-spec set_node_payload(Node, Payload) -> Node when
      Node :: tree_node(),
      Payload :: khepri_payload:payload().
%% @private

set_node_payload(#node{payload = Payload} = Node, Payload) ->
    Node;
set_node_payload(#node{stat = #{payload_version := DVersion} = Stat} = Node,
                 Payload) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{stat = Stat1, payload = Payload}.

-spec remove_node_payload(Node) -> Node when
      Node :: tree_node().
%% @private

remove_node_payload(
  #node{payload = ?NO_PAYLOAD} = Node) ->
    Node;
remove_node_payload(
  #node{stat = #{payload_version := DVersion} = Stat} = Node) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{stat = Stat1, payload = khepri_payload:none()}.

-spec add_node_child(Node, ChildName, Child) -> Node when
      Node :: tree_node(),
      Child :: tree_node(),
      ChildName :: khepri_path:component().

add_node_child(#node{stat = #{child_list_version := CVersion} = Stat,
                     child_nodes = Children} = Node,
               ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{stat = Stat1, child_nodes = Children1}.

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

remove_node_child(#node{stat = #{child_list_version := CVersion} = Stat,
                        child_nodes = Children} = Node,
                 ChildName) ->
    ?assert(maps:is_key(ChildName, Children)),
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Children1 = maps:remove(ChildName, Children),
    Node#node{stat = Stat1, child_nodes = Children1}.

-spec remove_node_child_nodes(Node) -> Node when
      Node :: tree_node().

remove_node_child_nodes(
  #node{child_nodes = Children} = Node) when Children =:= #{} ->
    Node;
remove_node_child_nodes(
  #node{stat = #{child_list_version := CVersion} = Stat} = Node) ->
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{stat = Stat1, child_nodes = #{}}.

-spec gather_node_props(Node, Options) -> NodeProps when
      Node :: tree_node(),
      Options :: khepri:command_options() | khepri:query_options(),
      NodeProps :: khepri:node_props().

gather_node_props(#node{stat = #{payload_version := DVersion,
                                 child_list_version := CVersion},
                        payload = Payload,
                        child_nodes = Children},
                  Options) ->
    Result0 = #{payload_version => DVersion,
                child_list_version => CVersion,
                child_list_length => maps:size(Children)},
    Result1 = case Options of
                  #{include_child_names := true} ->
                      Result0#{child_names => maps:keys(Children)};
                  _ ->
                      Result0
              end,
    case Payload of
        #p_data{data = Data}  -> Result1#{data => Data};
        #p_sproc{sproc = Fun} -> Result1#{sproc => Fun};
        _                            -> Result1
    end.

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
    maps:fold(
      fun
          (Path, Condition, true) ->
              case find_matching_nodes(Root, Path, #{}) of
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

-spec find_matching_nodes(Root, PathPattern, Options) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:result().
%% @private

find_matching_nodes(Root, PathPattern, Options) ->
    IncludeRootProps = khepri_path:pattern_includes_root_node(PathPattern),
    Extra = #{include_root_props => IncludeRootProps},
    do_find_matching_nodes(Root, PathPattern, Options, Extra, #{}).

-spec count_matching_nodes(Root, PathPattern, Options) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:ok(integer()) | khepri:error().
%% @private

count_matching_nodes(Root, PathPattern, Options) ->
    Extra = #{include_root_props => false},
    do_find_matching_nodes(Root, PathPattern, Options, Extra, 0).

-spec do_find_matching_nodes
(Root, PathPattern, Options, Extra, Map) -> Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:query_options(),
      Extra :: #{include_root_props => boolean()},
      Map :: map(),
      Result :: khepri:result();
(Root, PathPattern, Options, Extra, Integer) ->
    Result when
      Root :: tree_node(),
      PathPattern :: khepri_path:native_pattern(),
      Options :: khepri:query_options(),
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
    {error, {Reason, Info}};
find_matching_nodes_cb(_, {interrupted, _, _}, _, Result) ->
    {ok, keep, Result}.

-spec insert_or_update_node(State, PathPattern, Payload, Extra) -> Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: khepri_payload:payload(),
      Extra :: #{keep_while => khepri_condition:native_keep_while()},
      Ret :: {State, Result} | {State, Result, ra_machine:effects()},
      Result :: khepri:result().
%% @private

insert_or_update_node(
  #?MODULE{root = Root,
           keep_while_conds = KeepWhileConds,
           keep_while_conds_revidx = KeepWhileCondsRevIdx} = State,
  PathPattern, Payload,
  #{keep_while := KeepWhile}) ->
    Fun = fun(Path, Node, {_, _, Result}) ->
                  Ret = insert_or_update_node_cb(
                          Path, Node, Payload, Result),
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
                                  %% error.
                                  NodeName = case Path of
                                                 [] -> ?ROOT_NODE;
                                                 _  -> lists:last(Path)
                                             end,
                                  Info = #{node_name => NodeName,
                                           node_path => Path,
                                           keep_while_reason => Reason},
                                  {error,
                                   {keep_while_conditions_not_met, Info}}
                          end;
                      {ok, Node1, Result1} ->
                          {ok, Node1, {updated, Path, Result1}};
                      Error ->
                          Error
                  end
          end,
    %% TODO: Should we support setting many nodes with the same value?
    Ret1 = walk_down_the_tree(
             Root, PathPattern, specific_node,
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
  _Extra) ->
    Fun = fun(Path, Node, Result) ->
                  insert_or_update_node_cb(
                    Path, Node, Payload, Result)
          end,
    Ret1 = walk_down_the_tree(
             Root, PathPattern, specific_node,
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
  Path, #node{} = Node, Payload, Result) ->
    case maps:is_key(Path, Result) of
        false ->
            Node1 = set_node_payload(Node, Payload),
            NodeProps = gather_node_props(Node, #{}),
            {ok, Node1, Result#{Path => NodeProps}};
        true ->
            {ok, Node, Result}
    end;
insert_or_update_node_cb(
  Path, {interrupted, node_not_found = Reason, Info}, Payload, Result) ->
    %% We store the payload when we reached the target node only, not in the
    %% parent nodes we have to create in between.
    IsTarget = maps:get(node_is_target, Info),
    case can_continue_update_after_node_not_found(Info) of
        true when IsTarget ->
            Node = create_node_record(Payload),
            NodeProps = #{},
            {ok, Node, Result#{Path => NodeProps}};
        true ->
            Node = create_node_record(khepri_payload:none()),
            {ok, Node, Result};
        false ->
            {error, {Reason, Info}}
    end;
insert_or_update_node_cb(_, {interrupted, Reason, Info}, _, _) ->
    {error, {Reason, Info}}.

can_continue_update_after_node_not_found(#{condition := Condition}) ->
    can_continue_update_after_node_not_found1(Condition);
can_continue_update_after_node_not_found(#{node_name := NodeName}) ->
    can_continue_update_after_node_not_found1(NodeName).

can_continue_update_after_node_not_found1(ChildName)
  when ?IS_PATH_COMPONENT(ChildName) ->
    true;
can_continue_update_after_node_not_found1(#if_node_exists{exists = false}) ->
    true;
can_continue_update_after_node_not_found1(#if_all{conditions = Conds}) ->
    lists:all(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(#if_any{conditions = Conds}) ->
    lists:any(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(_) ->
    false.

-spec delete_matching_nodes(State, PathPattern) -> Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      Ret :: {State, Result} | {State, Result, ra_machine:effects()},
      Result :: khepri:result().
%% @private

delete_matching_nodes(
  #?MODULE{root = Root,
           keep_while_conds = KeepWhileConds,
           keep_while_conds_revidx = KeepWhileCondsRevIdx} = State,
  PathPattern) ->
    Ret1 = do_delete_matching_nodes(
             PathPattern, Root,
             #{keep_while_conds => KeepWhileConds,
               keep_while_conds_revidx => KeepWhileCondsRevIdx,
               keep_while_aftermath => #{}}),
    case Ret1 of
        {ok, Root1, #{keep_while_conds := KeepWhileConds1,
                      keep_while_conds_revidx := KeepWhileCondsRevIdx1,
                      keep_while_aftermath := KeepWhileAftermath},
         Ret2} ->
            State1 = State#?MODULE{root = Root1,
                                   keep_while_conds = KeepWhileConds1,
                                   keep_while_conds_revidx = KeepWhileCondsRevIdx1},
            {State2, SideEffects} = create_tree_change_side_effects(
                                      State, State1, Ret2, KeepWhileAftermath),
            {State2, {ok, Ret2}, SideEffects};
        Error ->
            {State, Error}
    end.

do_delete_matching_nodes(PathPattern, Root, Extra) ->
    Fun = fun delete_matching_nodes_cb/3,
    walk_down_the_tree(Root, PathPattern, many_nodes, Extra, Fun, #{}).

delete_matching_nodes_cb([] = Path, #node{} = Node, Result) ->
    Node1 = remove_node_payload(Node),
    Node2 = remove_node_child_nodes(Node1),
    NodeProps = gather_node_props(Node, #{}),
    {ok, Node2, Result#{Path => NodeProps}};
delete_matching_nodes_cb(Path, #node{} = Node, Result) ->
    NodeProps = gather_node_props(Node, #{}),
    {ok, delete, Result#{Path => NodeProps}};
delete_matching_nodes_cb(_, {interrupted, _, _}, Result) ->
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
  when ?IS_PATH_COMPONENT(Component) ->
    does_path_match(Path, PathPattern, [Component | ReversedPath], Root);
does_path_match(
  [Component | _Path], [Condition | _PathPattern], _ReversedPath, _Root)
  when ?IS_PATH_COMPONENT(Component) andalso
       ?IS_PATH_COMPONENT(Condition) ->
    false;
does_path_match(
  [Component | Path], [Condition | PathPattern], ReversedPath, Root) ->
    %% Query the tree node, required to evaluate the condition.
    ReversedPath1 = [Component | ReversedPath],
    CurrentPath = lists:reverse(ReversedPath1),
    {ok, #{CurrentPath := Node}} = khepri_machine:find_matching_nodes(
                                     Root,
                                     lists:reverse([Component | ReversedPath]),
                                     #{expect_specific_node => true}),
    case khepri_condition:is_met(Condition, Component, Node) of
        true       -> does_path_match(Path, PathPattern, ReversedPath1, Root);
        {false, _} -> false
    end.

find_stored_proc(Root, StoredProcPath) ->
    Ret = khepri_machine:find_matching_nodes(
            Root, StoredProcPath, #{expect_specific_node => true}),
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
  [?ROOT_NODE | PathPattern],
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
  [?THIS_NODE | PathPattern],
  WorkOnWhat, ReversedPath, ReversedParentTree, Extra, Fun, FunAcc) ->
    walk_down_the_tree1(
      CurrentNode, PathPattern, WorkOnWhat,
      ReversedPath, ReversedParentTree, Extra, Fun, FunAcc);
walk_down_the_tree1(
  _CurrentNode,
  [?PARENT_NODE | PathPattern],
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
  [?PARENT_NODE | PathPattern],
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
  when ?IS_NODE_ID(ChildName) ->
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
  when ?IS_CONDITION(Condition) ->
    %% We distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?ROOT_NODE or ?THIS_NODE in the
    %% condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?ROOT_NODE orelse NodeName =:= ?THIS_NODE ->
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
                        node_props => gather_node_props(CurrentNode, #{}),
                        condition => Cond},
                      PathPattern, WorkOnWhat, ReversedPath,
                      ReversedParentTree, Extra, Fun, FunAcc)
            end;
        {true, ChildName} when ChildName =/= ?PARENT_NODE ->
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
                                node_props => gather_node_props(Child, #{}),
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
        {true, ?PARENT_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            {error, targets_dot_dot};
        false ->
            %% The caller expects that the path matches a single specific node
            %% (no matter if it exists or not), but the condition could match
            %% several nodes.
            {error, {possibly_matching_many_nodes_denied, Condition}}
    end;
walk_down_the_tree1(
  #node{child_nodes = Children} = CurrentNode,
  [Condition | PathPattern] = WholePathPattern, many_nodes = WorkOnWhat,
  ReversedPath, ReversedParentTree, Extra, Fun, FunAcc)
  when ?IS_CONDITION(Condition) ->
    %% Like with WorkOnWhat =:= specific_node function clause above, We
    %% distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?ROOT_NODE or ?THIS_NODE in the
    %% condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?ROOT_NODE orelse NodeName =:= ?THIS_NODE ->
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
        {true, ?PARENT_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            {error, targets_parent_node};
        _ ->
            %% There is a special case if the current node is the root node
            %% and the pattern is of the form of e.g. [#if_name_matches{regex
            %% = any}]. In this situation, we consider the condition should be
            %% compared to that root node as well. This allows to get its
            %% props and payload atomically in a single suery.
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
                {ok, CurrentNode, Extra2, FunAcc2} ->
                    %% The current node didn't change, no need to update the
                    %% tree and evaluate keep_while conditions.
                    ?assertEqual(Extra, Extra2),
                    StartingNode = starting_node_in_rev_parent_tree(
                                     ReversedParentTree, CurrentNode),
                    {ok, StartingNode, Extra, FunAcc2};
                {ok, CurrentNode1, Extra2, FunAcc2} ->
                    %% Because of the loop, payload & child list versions may
                    %% have been increased multiple times. We want them to
                    %% increase once for the whole (atomic) operation.
                    CurrentNode2 = squash_version_bumps(
                                     CurrentNode, CurrentNode1),
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
      SpecialComponent :: ?ROOT_NODE | ?THIS_NODE,
      ReversedPath :: khepri_path:native_path(),
      NodeName :: khepri_path:component().

special_component_to_node_name(?ROOT_NODE = NodeName, [])  -> NodeName;
special_component_to_node_name(?THIS_NODE, [NodeName | _]) -> NodeName;
special_component_to_node_name(?THIS_NODE, [])             -> ?ROOT_NODE.

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

reset_versions(#node{stat = Stat} = CurrentNode) ->
    Stat1 = Stat#{payload_version => ?INIT_DATA_VERSION,
                  child_list_version => ?INIT_CHILD_LIST_VERSION},
    CurrentNode#node{stat = Stat1}.

-spec squash_version_bumps(OldNode, NewNode) -> Node when
      OldNode :: tree_node(),
      NewNode :: tree_node(),
      Node :: tree_node().
%% @private

squash_version_bumps(
  #node{stat = #{payload_version := DVersion,
                 child_list_version := CVersion}},
  #node{stat = #{payload_version := DVersion,
                 child_list_version := CVersion}} = CurrentNode) ->
    CurrentNode;
squash_version_bumps(
  #node{stat = #{payload_version := OldDVersion,
                 child_list_version := OldCVersion}},
  #node{stat = #{payload_version := NewDVersion,
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
    CurrentNode#node{stat = Stat1}.

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
    NodeProps = gather_node_props(Child, #{}),
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
    case do_delete_matching_nodes(PathToRemove, Root, Extra) of
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
