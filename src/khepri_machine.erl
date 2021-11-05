%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Khepri low-level API.
%%
%% This module exposes the "low-level" API to the Khepri database and state
%% machine. All functions in {@link khepri} are built on top of this module.
%%
%% The API is divided into two parts:
%% <ol>
%% <li>Functions to manipulate a simple set of tree nodes directly.</li>
%% <li>Functions to perform transactional queries and updates.</li>
%% </ol>
%%
%% == The store ID ==
%%
%% All functions require a store ID ({@link store_id/0}). The store ID
%% corresponds to the name of the Ra cluster Khepri was started with.
%%
%% See {@link khepri} for more details about Ra systems and clusters.
%%
%% == Direct manipulation on tree nodes ==
%%
%% The API provides the following three functions:
%% <ul>
%% <li>{@link get/2} and {@link get/3}: returns all tree node matching the given
%% path pattern.</li>
%% <li>{@link put/3} and {@link put/4}: updates a single specific tree node.</li>
%% <li>{@link delete/2}: removes all tree node matching the given path
%% pattern.</li>
%% </ul>
%%
%% All functions take a native path pattern. They do not accept Unix-like
%% paths.
%%
%% All functions return one of these tuples:
%% <ul>
%% <li>`{ok, NodePropsMap}' where `NodePropsMap' is a {@link
%% node_props_map/0}:
%% <ul>
%% <li>The map returned by {@link get/2}, {@link get/3} and {@link delete/2}
%% contains one entry per node matching the path pattern.</li>
%% <li>The map returned by {@link put/3} and {@link put/4} contains a single
%% entry if the modified node existed before the update, or no entry if it
%% didn't.</li>
%% </ul></li>
%% <li>`{error, Reason}' if an error occured. In the case, no modifications to
%% the tree was performed.</li>
%% </ul>
%%
%% == Transactional queries and updates ==
%%
%% Transactions are handled by {@link transaction/2} and {@link
%% transaction/3}.
%%
%% Both functions take an anonymous function. See {@link khepri_tx} for more
%% details about those functions and in particular their restrictions.
%%
%% The return value is whatever the anonymous function returns if it succeeded
%% or the reason why it aborted, similar to what {@link mnesia:transaction/1}
%% returns.

-module(khepri_machine).
-behaviour(ra_machine).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").

-export([put/3, put/4,
         get/2, get/3,
         delete/2,
         transaction/2,
         transaction/3]).
-export([get_keep_untils_state/1]).
-export([init/1,
         apply/3,
         init_aux/1,
         handle_aux/6]).
%% For internal user only.
-export([find_matching_nodes/3,
         insert_or_update_node/4,
         delete_matching_nodes/2]).

-ifdef(TEST).
-export([are_keep_until_conditions_met/2,
         get_root/1,
         get_keep_untils/1,
         get_keep_untils_revidx/1]).
-endif.

-compile({no_auto_import, [apply/3]}).

-type data() :: any().
%% Data stored in a node's payload.

-type payload_version() :: pos_integer().
%% Number of changes made to the payload of a node.
%%
%% The payload version starts at 1 when a node is created. It is increased by 1
%% each time the payload is added, modified or removed.

-type child_list_version() :: pos_integer().
%% Number of changes made to the list of child nodes of a node (child nodes
%% added or removed).
%%
%% The child list version starts at 1 when a node is created. It is increased
%% by 1 each time a child is added or removed. Changes made to existing nodes
%% are not reflected in this version.

-type child_list_length() :: non_neg_integer().
%% Number of direct child nodes under a tree node.

-type node_props() ::
    #{data => data(),
      payload_version := payload_version(),
      child_list_version := child_list_version(),
      child_list_length := child_list_length(),
      child_nodes => #{khepri_path:node_id() => node_props()}}.
%% Structure used to return properties, payload and child nodes for a specific
%% node.
%%
%% <ul>
%% <li>Payload version, child list version, and child list count are always
%% included in the structure.</li>
%% <li>Data is only included if there is data in the node's payload. Absence of
%% data is represented as no `data' entry in this structure.</li>
%% <li>Child nodes are only included if requested.</li>
%% </ul>

-type node_props_map() :: #{khepri_path:path() => node_props()}.
%% Structure used to return a map of nodes and their associated properties,
%% payload and child nodes.
%%
%% This structure is used in the return value of all commands and queries.

-type result() :: khepri:ok(node_props_map()) |
                  khepri:error().
%% Return value of a command or query.

-type stat() :: #{payload_version := payload_version(),
                  child_list_version := child_list_version()}.
%% Stats attached to each node in the tree structure.

-type payload() :: ?NO_PAYLOAD | ?DATA_PAYLOAD(data()).
%% All types of payload stored in the nodes of the tree structure.
%%
%% Beside the absence of payload, the only type of payload supported is data.

-type tree_node() :: #node{}.
%% A node in the tree structure.

-type command() :: #put{} |
                   #delete{} |
                   #tx{}.
%% Commands specific to this Ra machine.

-type machine_init_args() :: #{commands => [command()],
                               atom() => any()}.
%% Structure passed to {@link init/1}.

-type machine_config() :: #config{}.
%% Configuration record, holding read-only or rarely changing fields.

-type keep_untils_map() :: #{khepri_path:path() =>
                             khepri_condition:keep_until()}.
%% Internal index of the per-node keep_until conditions.

-type keep_untils_revidx() :: #{khepri_path:path() =>
                                #{khepri_path:path() => ok}}.
%% Internal reverse index of the keep_until conditions. If node A depends on a
%% condition on node B, then this reverse index will have a "node B => node A"
%% entry.

-type operation_options() :: #{expect_specific_node => boolean(),
                               include_child_names => boolean()}.
%% Options used in {@link find_matching_nodes/3}.

-type state() :: #?MODULE{}.
%% State of this Ra state machine.

-type query_fun() :: fun((state()) -> any()).
%% Function representing a query and used {@link process_query/2}.

-type walk_down_the_tree_extra() :: #{include_root_props =>
                                      boolean(),
                                      keep_untils =>
                                      keep_untils_map(),
                                      keep_untils_revidx =>
                                      keep_untils_revidx()}.

-type walk_down_the_tree_fun() ::
    fun((khepri_path:path(),
         khepri:ok(tree_node()) | error(any(), map()),
         Acc :: any()) ->
        ok(tree_node() | keep | remove, any()) |
        khepri:error()).
%% Function called to handle a node found (or an error) and used in {@link
%% walk_down_the_tree/6}.

-type ok(Type1, Type2) :: {ok, Type1, Type2}.
-type ok(Type1, Type2, Type3) :: {ok, Type1, Type2, Type3}.
-type error(Type1, Type2) :: {error, Type1, Type2}.

-export_type([data/0,
              stat/0,
              payload/0,
              tree_node/0,
              payload_version/0,
              child_list_version/0,
              child_list_length/0,
              node_props/0,
              node_props_map/0,
              result/0,
              operation_options/0]).
-export_type([state/0,
              machine_config/0,
              keep_untils_map/0,
              keep_untils_revidx/0]).

%% -------------------------------------------------------------------
%% Machine protocol.
%% -------------------------------------------------------------------

%% TODO: Verify arguments carefully to avoid the construction of an invalid
%% command.

-spec put(StoreId, PathPattern, Payload) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: payload(),
      Result :: result().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling
%% `put(StoreId, PathPattern, Payload, #{})'.
%%
%% @see put/4.

put(StoreId, PathPattern, Payload) ->
    put(StoreId, PathPattern, Payload, #{}).

-spec put(StoreId, PathPattern, Payload, Extra) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: payload(),
      Extra :: #{keep_until => khepri_condition:keep_until()},
      Result :: result().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% The path or path pattern must target a specific tree node.
%%
%% When using a simple path, if the target node does not exists, it is created
%% using the given payload. If the target node exists, it is updated with the
%% given payload and its payload version is increased by one. Missing parent
%% nodes are created on the way.
%%
%% When using a path pattern, the behavior is the same. However if a condition
%% in the path pattern is not met, an error is returned and the tree structure
%% is not modified.
%%
%% If the target node is modified, the returned structure in the "ok" tuple
%% will have a single key corresponding to the path of the target node. That
%% key will point to a map containing the properties and payload (if any) of
%% the node before the modification.
%%
%% If the target node is created , the returned structure in the "ok" tuple
%% will have a single key corresponding to the path of the target node. That
%% key will point to empty map, indicating there was no existing node (i.e.
%% there was no properties or payload to return).
%%
%% The payload must be one of the following form:
%% <ul>
%% <li>`?NO_PAYLOAD', meaning there will be no payload attached to the
%% node</li>
%% <li>`?DATA_PAYLOAD(Term)' to store any type of term in the node</li>
%% </ul>
%%
%% Example:
%% ```
%% %% Insert a node at `/foo/bar', overwriting the previous value.
%% Result = khepri_machine:put(
%%            ra_cluster_name, [foo, bar], ?DATA_PAYLOAD(new_value)),
%%
%% %% Here is the content of `Result'.
%% {ok, #{[foo, bar] => #{data => old_value,
%%                        payload_version => 1,
%%                        child_list_version => 1,
%%                        child_list_length => 0}}} = Result.
%% '''
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the node to create or
%%        modify.
%% @param Payload the payload to put in the specified node.
%% @param Extra extra options such as `keep_until' conditions.
%%
%% @returns an "ok" tuple with a map with one entry, or an "error" tuple.

put(StoreId, PathPattern, Payload, Extra) when ?IS_PAYLOAD(Payload) ->
    khepri_path:ensure_is_valid(PathPattern),
    Command = #put{path = PathPattern,
                   payload = Payload,
                   extra = Extra},
    process_command(StoreId, Command);
put(_StoreId, PathPattern, Payload, _Extra) ->
    throw({invalid_payload, PathPattern, Payload}).

-spec get(StoreId, PathPattern) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Result :: result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling
%% `get(StoreId, PathPattern, #{})'.
%%
%% @see get/3.

get(StoreId, PathPattern) ->
    get(StoreId, PathPattern, #{}).

-spec get(StoreId, PathPattern, Options) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: operation_options(),
      Result :: result().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% The returned structure in the "ok" tuple will have a key corresponding to
%% the path per node which matched the pattern. Each key will point to a map
%% containing the properties and payload of that matching node.
%%
%% Example:
%% ```
%% %% Query the node at `/foo/bar'.
%% Result = khepri_machine:get(ra_cluster_name, [foo, bar]),
%%
%% %% Here is the content of `Result'.
%% {ok, #{[foo, bar] => #{data => new_value,
%%                        payload_version => 2,
%%                        child_list_version => 1,
%%                        child_list_length => 0}}} = Result.
%% '''
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to match against the nodes to
%%        retrieve.
%% @param Options options to tune the tree traversal or the returned structure
%%        content.
%%
%% @returns an "ok" tuple with a map with zero, one or more entries, or an
%% "error" tuple.

get(StoreId, PathPattern, Options) ->
    khepri_path:ensure_is_valid(PathPattern),
    Query = fun(#?MODULE{root = Root}) ->
                    find_matching_nodes(Root, PathPattern, Options)
            end,
    process_query(StoreId, Query).

-spec delete(StoreId, PathPattern) -> Result when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Result :: result().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% The returned structure in the "ok" tuple will have a key corresponding to
%% the path per node which was deleted. Each key will point to a map containing
%% the properties and payload of that deleted node.
%%
%% Example:
%% ```
%% %% Delete the node at `/foo/bar'.
%% Result = khepri_machine:delete(ra_cluster_name, [foo, bar]),
%%
%% %% Here is the content of `Result'.
%% {ok, #{[foo, bar] => #{data => new_value,
%%                        payload_version => 2,
%%                        child_list_version => 1,
%%                        child_list_length => 0}}} = Result.
%% '''
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to match against the nodes to
%%        delete.
%%
%% @returns an "ok" tuple with a map with zero, one or more entries, or an
%% "error" tuple.

delete(StoreId, PathPattern) ->
    khepri_path:ensure_is_valid(PathPattern),
    Command = #delete{path = PathPattern},
    process_command(StoreId, Command).

-spec transaction(StoreId, Fun) -> Ret when
      StoreId :: khepri:store_id(),
      Fun :: khepri_tx:tx_fun(),
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().
%% @doc Runs a transaction and returns the result.
%%
%% Calling this function is the same as calling
%% `transaction(StoreId, Fun, auto)'.
%%
%% @see transaction/3.

transaction(StoreId, Fun) ->
    transaction(StoreId, Fun, auto).

-spec transaction(StoreId, Fun, ReadWrite) -> Ret when
      StoreId :: khepri:store_id(),
      Fun :: khepri_tx:tx_fun(),
      ReadWrite :: auto | boolean(),
      Ret :: Atomic | Aborted,
      Atomic :: {atomic, khepri_tx:tx_fun_result()},
      Aborted :: khepri_tx:tx_abort().
%% @doc Runs a transaction and returns the result.
%%
%% `Fun' is an arbitrary anonymous function which takes no arguments.
%%
%% The `ReadWrite' flag determines what the anonymous function is allowed to
%% do and in which context it runs:
%%
%% <ul>
%% <li>If `ReadWrite' is true, `Fun' can use the {@link khepri_tx} transaction
%% API as well as any calls to other modules as long as those functions or what
%% they do is permitted. See {@link khepri_tx} for more details. If `Fun' does
%% or calls something forbidden, the transaction will be aborted. `Fun' is
%% executed in the context of the state machine process on each Ra
%% members.</li>
%% <li>If `ReadWrite' is false, `Fun' can do whatever it wants, except modify
%% the content of the store. In other words, uses of {@link khepri_tx:put/2}
%% or {@link khepri_tx:delete/1} are forbidden and will abort the function.
%% `Fun' is executed from a process on the leader Ra member.</li>
%% <li>If `ReadWrite' is `auto', `Fun' is analyzed to determine if it calls
%% {@link khepri_tx:put/2} or {@link khepri_tx:delete/1}, or uses any denied
%% operations for a read/write transaction. If it does, this is the same as
%% setting `ReadWrite' to true. Otherwise, this is the equivalent of setting
%% `ReadWrite' to false.</li>
%% </ul>
%%
%% The result of `Fun' can be any term. That result is returned in an
%% `{atomic, Result}' tuple.
%%
%% @param StoreId the name of the Ra cluster.
%% @param Fun an arbitrary anonymous function.
%%
%% @returns `{atomic, Result}' with the return value of `Fun', or `{aborted,
%% Reason}' if the anonymous function was aborted.

transaction(StoreId, Fun, auto = ReadWrite) when is_function(Fun, 0) ->
    case khepri_tx:to_standalone_fun(Fun, auto = ReadWrite) of
        #standalone_fun{} = StandaloneFun ->
            readwrite_transaction(StoreId, StandaloneFun);
        _ ->
            readonly_transaction(StoreId, Fun)
    end;
transaction(StoreId, Fun, true = ReadWrite) when is_function(Fun, 0) ->
    StandaloneFun = khepri_tx:to_standalone_fun(Fun, ReadWrite),
    readwrite_transaction(StoreId, StandaloneFun);
transaction(StoreId, Fun, false) when is_function(Fun, 0) ->
    readonly_transaction(StoreId, Fun);
transaction(_StoreId, Fun, _ReadWrite) when is_function(Fun) ->
    {arity, Arity} = erlang:fun_info(Fun, arity),
    throw({invalid_tx_fun, {requires_args, Arity}});
transaction(_StoreId, Term, _ReadWrite) ->
    throw({invalid_tx_fun, Term}).

readonly_transaction(StoreId, Fun) when is_function(Fun, 0) ->
    Query = fun(State) ->
                    {_State, Ret} = khepri_tx:run(State, Fun, false),
                    Ret
            end,
    case process_query(StoreId, Query) of
        {exception, _, {aborted, _} = Aborted, _} ->
            Aborted;
        {exception, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        Ret ->
            {atomic, Ret}
    end.

readwrite_transaction(StoreId, StandaloneFun) ->
    Command = #tx{'fun' = StandaloneFun},
    case process_command(StoreId, Command) of
        {exception, _, {aborted, _} = Aborted, _} ->
            Aborted;
        {exception, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        Ret ->
            {atomic, Ret}
    end.

-spec get_keep_untils_state(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: {ok, keep_untils_map()} | khepri:error().
%% @doc Returns the `keep_until' conditions internal state.
%%
%% The returned state consists of all the `keep_until' condition set so far.
%% However, it doesn't include the reverse index.
%%
%% @param StoreId the name of the Ra cluster.
%%
%% @returns the `keep_until' conditions internal state.
%%
%% @private

get_keep_untils_state(StoreId) ->
    Query = fun(#?MODULE{keep_untils = KeepUntils}) ->
                    {ok, KeepUntils}
            end,
    process_query(StoreId, Query).

-spec process_command(StoreId, Command) -> Ret when
      StoreId :: khepri:store_id(),
      Command :: command(),
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

process_command(StoreId, Command) ->
    %% StoreId is the same as Ra's cluster name.
    case ra_leaderboard:lookup_leader(StoreId) of
        undefined ->
            {error, ra_leader_unknown};
        LeaderId ->
            case ra:process_command(LeaderId, Command) of
                {ok, Ret, _LeaderId}   -> Ret;
                {timeout, _} = Timeout -> {error, Timeout};
                {error, _} = Error     -> Error
            end
    end.

-spec process_query(StoreId, QueryFun) -> Ret when
      StoreId :: khepri:store_id(),
      QueryFun :: query_fun(),
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

process_query(StoreId, QueryFun) ->
    %% StoreId is the same as Ra's cluster name.
    case ra_leaderboard:lookup_leader(StoreId) of
        undefined ->
            {error, ra_leader_unknown};
        LeaderId ->
            %% TODO: Leader vs. consistent?
            case ra:leader_query(LeaderId, QueryFun) of
                {ok, {_RaIndex, Ret}, _} -> Ret;
                {timeout, _} = Timeout   -> {error, Timeout};
                {error, _} = Error       -> Error
            end
    end.

%% -------------------------------------------------------------------
%% ra_machine callbacks.
%% -------------------------------------------------------------------

-spec init(machine_init_args()) -> state().
%% @private

init(Params) ->
    Config = case Params of
                 #{snapshot_interval := SnapshotInterval} ->
                     #config{snapshot_interval = SnapshotInterval};
                 _ ->
                     #config{}
             end,
    State = #?MODULE{config = Config},

    %% Create initial "schema" if provided.
    Commands = maps:get(commands, Params, []),
    State3 = lists:foldl(
               fun (Command, State1) ->
                       Meta = #{index => 0,
                                term => 0,
                                system_time => 0},
                       case apply(Meta, Command, State1) of
                           {S, _}    -> S;
                           {S, _, _} -> S
                       end
               end, State, Commands),
    reset_applied_command_count(State3).

-spec apply(Meta, Command, State) ->
    {State, Ret} | {State, Ret, SideEffects} when
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
    bump_applied_command_count(Ret, Meta).

-spec bump_applied_command_count({State, Ret}, Meta) ->
    {State, Ret} | {State, Ret, SideEffects} when
      State :: state(),
      Ret :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @private

bump_applied_command_count(
  {#?MODULE{config = #config{snapshot_interval = SnapshotInterval},
            metrics = Metrics} = State,
   Result},
  #{index := RaftIndex}) ->
    AppliedCmdCount0 = maps:get(applied_command_count, Metrics, 0),
    AppliedCmdCount = AppliedCmdCount0 + 1,
    case AppliedCmdCount < SnapshotInterval of
        true ->
            Metrics1 = Metrics#{applied_command_count => AppliedCmdCount},
            State1 = State#?MODULE{metrics = Metrics1},
            {State1, Result};
        false ->
            ?LOG_DEBUG(
               "Move release cursor after ~b commands applied "
               "(>= ~b commands)",
               [AppliedCmdCount, SnapshotInterval],
               #{domain => [khepri, ra_machine]}),
            State1 = reset_applied_command_count(State),
            ReleaseCursor = {release_cursor, RaftIndex, State1},
            SideEffects = [ReleaseCursor],
            {State1, Result, SideEffects}
    end.

reset_applied_command_count(#?MODULE{metrics = Metrics} = State) ->
    Metrics1 = maps:remove(applied_command_count, Metrics),
    State#?MODULE{metrics = Metrics1}.

%% @private

init_aux(_) ->
    undefined.

%% @private

handle_aux(_RaftState, _Type, tick, AuxState, LogState, _MacState) ->
    {no_reply, AuxState, LogState};
handle_aux(_RaftState, _Type, Cmd, AuxState, LogState, _MacState) ->
    ?LOG_DEBUG(
       "Sending event to Khepri monitoring manager: ~p",
       [Cmd],
       #{domain => [khepri, ra_machine]}),
    {no_reply, AuxState, LogState}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec create_node_record(Payload) -> Node when
      Payload :: payload(),
      Node :: tree_node().
%% @private

create_node_record(Payload) ->
    #node{stat = ?INIT_NODE_STAT,
          payload = Payload}.

-spec set_node_payload(tree_node(), payload()) ->
    tree_node().
%% @private

set_node_payload(#node{payload = Payload} = Node, Payload) ->
    Node;
set_node_payload(#node{stat = #{payload_version := DVersion} = Stat} = Node,
                 Payload) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{stat = Stat1, payload = Payload}.

-spec remove_node_payload(tree_node()) -> tree_node().
%% @private

remove_node_payload(#node{payload = ?NO_PAYLOAD} = Node) ->
    Node;
remove_node_payload(#node{stat = #{payload_version := DVersion} = Stat} = Node) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{stat = Stat1, payload = ?NO_PAYLOAD}.

-spec add_node_child(tree_node(), khepri_path:component(), tree_node()) ->
    tree_node().

add_node_child(#node{stat = #{child_list_version := CVersion} = Stat,
                     child_nodes = Children} = Node,
               ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{stat = Stat1, child_nodes = Children1}.

-spec update_node_child(tree_node(), khepri_path:component(), tree_node()) ->
    tree_node().

update_node_child(#node{child_nodes = Children} = Node, ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Node#node{child_nodes = Children1}.

-spec remove_node_child(tree_node(), khepri_path:component()) ->
    tree_node().

remove_node_child(#node{stat = #{child_list_version := CVersion} = Stat,
                        child_nodes = Children} = Node,
                 ChildName) ->
    ?assert(maps:is_key(ChildName, Children)),
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Children1 = maps:remove(ChildName, Children),
    Node#node{stat = Stat1, child_nodes = Children1}.

-spec remove_node_child_nodes(tree_node()) -> tree_node().

remove_node_child_nodes(
  #node{child_nodes = Children} = Node) when Children =:= #{} ->
    Node;
remove_node_child_nodes(
  #node{stat = #{child_list_version := CVersion} = Stat} = Node) ->
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{stat = Stat1, child_nodes = #{}}.

-spec gather_node_props(tree_node(), operation_options()) ->
    node_props().

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
        ?DATA_PAYLOAD(Data) -> Result1#{data => Data};
        _                   -> Result1
    end.

-spec to_absolute_keep_until(BasePath, KeepUntil) -> KeepUntil when
      BasePath :: khepri_path:path(),
      KeepUntil :: khepri_condition:keep_until().
%% @private

to_absolute_keep_until(BasePath, KeepUntil) ->
    maps:fold(
      fun(Path, Cond, Acc) ->
              AbsPath = khepri_path:abspath(Path, BasePath),
              Acc#{AbsPath => Cond}
      end, #{}, KeepUntil).

-spec are_keep_until_conditions_met(
        tree_node(), khepri_condition:keep_until()) ->
    true | {false, any()}.
%% @private

are_keep_until_conditions_met(_, KeepUntil)
  when KeepUntil =:= #{} ->
    true;
are_keep_until_conditions_met(Root, KeepUntil) ->
    maps:fold(
      fun
          (Path, Condition, true) ->
              case find_matching_nodes(Root, Path, #{}) of
                  {ok, Result} when Result =/= #{} ->
                      are_keep_until_conditions_met1(Result, Condition);
                  {ok, _} ->
                      {false, {pattern_matches_no_nodes, Path}};
                  {error, Reason} ->
                      {false, Reason}
              end;
          (_, _, False) ->
              False
      end, true, KeepUntil).

are_keep_until_conditions_met1(Result, Condition) ->
    maps:fold(
      fun
          (Path, NodeProps, true) ->
              khepri_condition:is_met(Condition, Path, NodeProps);
          (_, _, False) ->
              False
      end, true, Result).

is_keep_until_condition_met_on_self(
  Path, Node, #{keep_untils := KeepUntils}) ->
    case KeepUntils of
        #{Path := #{Path := Condition}} ->
            khepri_condition:is_met(Condition, Path, Node);
        _ ->
            true
    end;
is_keep_until_condition_met_on_self(_, _, _) ->
    true.

-spec update_keep_untils_revidx(
        keep_untils_map(), keep_untils_revidx(),
        khepri_path:path(), khepri_condition:keep_until()) ->
    keep_untils_revidx().

update_keep_untils_revidx(
  KeepUntils, KeepUntilsRevIdx, Watcher, KeepUntil) ->
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_until.
    OldWatcheds = maps:get(Watcher, KeepUntils, #{}),
    KeepUntilsRevIdx1 = maps:fold(
                          fun(Watched, _, KURevIdx) ->
                                  Watchers = maps:get(Watched, KURevIdx),
                                  Watchers1 = maps:remove(Watcher, Watchers),
                                  case maps:size(Watchers1) of
                                      0 -> maps:remove(Watched, KURevIdx);
                                      _ -> KURevIdx#{Watched => Watchers1}
                                  end
                          end, KeepUntilsRevIdx, OldWatcheds),
    %% Then, record the watched paths.
    maps:fold(
      fun(Watched, _, KURevIdx) ->
              Watchers = maps:get(Watched, KURevIdx, #{}),
              Watchers1 = Watchers#{Watcher => ok},
              KURevIdx#{Watched => Watchers1}
      end, KeepUntilsRevIdx1, KeepUntil).

-spec find_matching_nodes(
        tree_node(),
        khepri_path:pattern(),
        operation_options()) ->
    result().
%% @private

find_matching_nodes(Root, PathPattern, Options) ->
    Fun = fun(Path, Node, Result) ->
                  find_matching_nodes_cb(Path, Node, Options, Result)
          end,
    WorkOnWhat = case Options of
                     #{expect_specific_node := true} -> specific_node;
                     _                               -> many_nodes
                 end,
    IncludeRootProps = khepri_path:pattern_includes_root_node(PathPattern),
    Extra = #{include_root_props => IncludeRootProps},
    case walk_down_the_tree(Root, PathPattern, WorkOnWhat, Extra, Fun, #{}) of
        {ok, NewRoot, _, Result} ->
            ?assertEqual(Root, NewRoot),
            {ok, Result};
        Error ->
            Error
    end.

find_matching_nodes_cb(Path, #node{} = Node, Options, Result) ->
    NodeProps = gather_node_props(Node, Options),
    {ok, keep, Result#{Path => NodeProps}};
find_matching_nodes_cb(
  _,
  {interrupted, node_not_found = Reason, Info},
  #{expect_specific_node := true},
  _) ->
    {error, {Reason, Info}};
find_matching_nodes_cb(_, {interrupted, _, _}, _, Result) ->
    {ok, keep, Result}.

-spec insert_or_update_node(
        state(), khepri_path:pattern(), payload(),
        #{keep_until => khepri_condition:keep_until()}) ->
    {state(), result()}.
%% @private

insert_or_update_node(
  #?MODULE{root = Root,
           keep_untils = KeepUntils,
           keep_untils_revidx = KeepUntilsRevIdx} = State,
  PathPattern, Payload,
  #{keep_until := KeepUntil}) ->
    Fun = fun(Path, Node, {_, _, Result}) ->
                  Ret = insert_or_update_node_cb(
                          Path, Node, Payload, Result),
                  case Ret of
                      {ok, Node1, Result1} when Result1 =/= #{} ->
                          AbsKeepUntil = to_absolute_keep_until(
                                           Path, KeepUntil),
                          KeepUntilOnOthers = maps:remove(Path, AbsKeepUntil),
                          KUMet = are_keep_until_conditions_met(
                                    Root, KeepUntilOnOthers),
                          case KUMet of
                              true ->
                                  {ok, Node1, {updated, Path, Result1}};
                              {false, Reason} ->
                                  %% The keep_until condition is not met. We
                                  %% can't insert the node and return an
                                  %% error.
                                  NodeName = case Path of
                                                 [] -> ?ROOT_NODE;
                                                 _  -> lists:last(Path)
                                             end,
                                  Info = #{node_name => NodeName,
                                           node_path => Path,
                                           keep_until_reason => Reason},
                                  {error,
                                   {keep_until_conditions_not_met, Info}}
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
             #{keep_untils => KeepUntils,
               keep_untils_revidx => KeepUntilsRevIdx},
             Fun, {undefined, [], #{}}),
    case Ret1 of
        {ok, Root1, #{keep_untils := KeepUntils1,
                      keep_untils_revidx := KeepUntilsRevIdx1},
         {updated, ResolvedPath, Ret2}} ->
            AbsKeepUntil = to_absolute_keep_until(ResolvedPath, KeepUntil),
            KeepUntilsRevIdx2 = update_keep_untils_revidx(
                                  KeepUntils1, KeepUntilsRevIdx1,
                                  ResolvedPath, AbsKeepUntil),
            KeepUntils2 = KeepUntils1#{ResolvedPath => AbsKeepUntil},
            State1 = State#?MODULE{root = Root1,
                                   keep_untils = KeepUntils2,
                                   keep_untils_revidx = KeepUntilsRevIdx2},
            {State1, {ok, Ret2}};
        {ok, Root1, #{keep_untils := KeepUntils1,
                      keep_untils_revidx := KeepUntilsRevIdx1},
         {removed, _, Ret2}} ->
            State1 = State#?MODULE{root = Root1,
                                   keep_untils = KeepUntils1,
                                   keep_untils_revidx = KeepUntilsRevIdx1},
            {State1, {ok, Ret2}};
        Error ->
            {State, Error}
    end;
insert_or_update_node(
  #?MODULE{root = Root,
           keep_untils = KeepUntils,
           keep_untils_revidx = KeepUntilsRevIdx} = State,
  PathPattern, Payload,
  _Extra) ->
    Fun = fun(Path, Node, Result) ->
                  insert_or_update_node_cb(
                    Path, Node, Payload, Result)
          end,
    Ret1 = walk_down_the_tree(
             Root, PathPattern, specific_node,
             #{keep_untils => KeepUntils,
               keep_untils_revidx => KeepUntilsRevIdx},
             Fun, #{}),
    case Ret1 of
        {ok, Root1, #{keep_untils := KeepUntils1,
                      keep_untils_revidx := KeepUntilsRevIdx1},
         Ret2} ->
            State1 = State#?MODULE{root = Root1,
                                   keep_untils = KeepUntils1,
                                   keep_untils_revidx = KeepUntilsRevIdx1},
            {State1, {ok, Ret2}};
        Error ->
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
            Node = create_node_record(none),
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

-spec delete_matching_nodes(state(), khepri_path:pattern()) ->
    {state(), result()}.
%% @private

delete_matching_nodes(
  #?MODULE{root = Root,
           keep_untils = KeepUntils,
           keep_untils_revidx = KeepUntilsRevIdx} = State,
  PathPattern) ->
    Ret1 = do_delete_matching_nodes(
             PathPattern, Root,
             #{keep_untils => KeepUntils,
               keep_untils_revidx => KeepUntilsRevIdx}),
    case Ret1 of
        {ok, Root1, #{keep_untils := KeepUntils1,
                      keep_untils_revidx := KeepUntilsRevIdx1},
         Ret2} ->
            State1 = State#?MODULE{root = Root1,
                                   keep_untils = KeepUntils1,
                                   keep_untils_revidx = KeepUntilsRevIdx1},
            {State1, {ok, Ret2}};
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
    {ok, remove, Result#{Path => NodeProps}};
delete_matching_nodes_cb(_, {interrupted, _, _}, Result) ->
    {ok, keep, Result}.

%% -------

-spec walk_down_the_tree(
        tree_node(), khepri_path:pattern(), specific_node | many_nodes,
        walk_down_the_tree_extra(),
        walk_down_the_tree_fun(), any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()) |
    khepri:error().
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
        tree_node(), khepri_path:pattern(), specific_node | many_nodes,
        khepri_path:pattern(), [tree_node() | {tree_node(), child_created}],
        walk_down_the_tree_extra(),
        walk_down_the_tree_fun(), any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()) |
    khepri:error().
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
            %% TODO: Should we provide more details about the error, like the
            %% list of matching nodes?
            {error, matches_many_nodes}
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
                    %% tree and evaluate keep_until conditions.
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
        {ok, remove, FunAcc1} ->
            walk_back_up_the_tree(
              remove, ReversedPath, ReversedParentTree, Extra, FunAcc1);
        {ok, #node{} = CurrentNode1, FunAcc1} ->
            walk_back_up_the_tree(
              CurrentNode1, ReversedPath, ReversedParentTree, Extra, FunAcc1);
        Error ->
            Error
    end.

-spec special_component_to_node_name(
        ?ROOT_NODE | ?THIS_NODE,
        khepri_path:pattern()) ->
    khepri_path:component().

special_component_to_node_name(?ROOT_NODE = NodeName, [])  -> NodeName;
special_component_to_node_name(?THIS_NODE, [NodeName | _]) -> NodeName;
special_component_to_node_name(?THIS_NODE, [])             -> ?ROOT_NODE.

-spec starting_node_in_rev_parent_tree([tree_node()]) ->
    tree_node().
%% @private

starting_node_in_rev_parent_tree(ReversedParentTree) ->
    hd(lists:reverse(ReversedParentTree)).

-spec starting_node_in_rev_parent_tree([tree_node()], tree_node()) ->
    tree_node().
%% @private

starting_node_in_rev_parent_tree([], CurrentNode) ->
    CurrentNode;
starting_node_in_rev_parent_tree(ReversedParentTree, _) ->
    starting_node_in_rev_parent_tree(ReversedParentTree).

-spec handle_branch(
        tree_node(), khepri_path:component(), tree_node(),
        khepri_path:pattern(), specific_node | many_nodes,
        [tree_node() | {tree_node(), child_created}],
        walk_down_the_tree_extra(),
        walk_down_the_tree_fun(), any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()) |
    khepri:error().
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
        mismatching_node | node_not_found,
        map(),
        khepri_path:pattern(), specific_node | many_nodes,
        khepri_path:path(), [tree_node() | {tree_node(), child_created}],
        walk_down_the_tree_extra(),
        walk_down_the_tree_fun(), any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()) |
    khepri:error().
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
          when ToDo =:= keep orelse ToDo =:= remove ->
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

-spec reset_versions(tree_node()) -> tree_node().
%% @private

reset_versions(#node{stat = Stat} = CurrentNode) ->
    Stat1 = Stat#{payload_version => ?INIT_DATA_VERSION,
                  child_list_version => ?INIT_CHILD_LIST_VERSION},
    CurrentNode#node{stat = Stat1}.

-spec squash_version_bumps(tree_node(), tree_node()) -> tree_node().
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
        tree_node() | remove, khepri_path:path(),
        [tree_node() | {tree_node(), child_created}],
        walk_down_the_tree_extra(),
        any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()).
%% @private

walk_back_up_the_tree(
  Child, ReversedPath, ReversedParentTree, Extra, FunAcc) ->
    walk_back_up_the_tree(
      Child, ReversedPath, ReversedParentTree, Extra, #{}, FunAcc).

-spec walk_back_up_the_tree(
        tree_node() | remove, khepri_path:path(),
        [tree_node() | {tree_node(), child_created}],
        walk_down_the_tree_extra(),
        #{khepri_path:path() => tree_node() | remove},
        any()) ->
    ok(tree_node(), walk_down_the_tree_extra(), any()).
%% @private

walk_back_up_the_tree(
  remove,
  [ChildName | ReversedPath] = WholeReversedPath,
  [ParentNode | ReversedParentTree], Extra, KeepUntilAftermath, FunAcc) ->
    %% Evaluate keep_until of nodes which depended on ChildName (it is
    %% removed) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    KeepUntilAftermath1 = KeepUntilAftermath#{Path => remove},

    %% Evaluate keep_until of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = remove_node_child(ParentNode, ChildName),
    handle_keep_until_for_parent_update(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepUntilAftermath1, FunAcc);
walk_back_up_the_tree(
  Child,
  [ChildName | ReversedPath],
  [{ParentNode, child_created} | ReversedParentTree],
  Extra, KeepUntilAftermath, FunAcc) ->
    %% No keep_until to evaluate, the child is new and no nodes depend on it
    %% at this stage.
    %% FIXME: Perhaps there is a condition in a if_any{}?
    Child1 = reset_versions(Child),

    %% Evaluate keep_until of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = add_node_child(ParentNode, ChildName, Child1),
    handle_keep_until_for_parent_update(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepUntilAftermath, FunAcc);
walk_back_up_the_tree(
  Child,
  [ChildName | ReversedPath] = WholeReversedPath,
  [ParentNode | ReversedParentTree],
  Extra, KeepUntilAftermath, FunAcc) ->
    %% Evaluate keep_until of nodes which depend on ChildName (it is
    %% modified) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    NodeProps = gather_node_props(Child, #{}),
    KeepUntilAftermath1 = KeepUntilAftermath#{Path => NodeProps},

    %% No need to evaluate keep_until of ParentNode, its child_count is
    %% unchanged.
    ParentNode1 = update_node_child(ParentNode, ChildName, Child),
    walk_back_up_the_tree(
      ParentNode1, ReversedPath, ReversedParentTree,
      Extra, KeepUntilAftermath1, FunAcc);
walk_back_up_the_tree(
  StartingNode,
  [], %% <-- We reached the root (i.e. not in a branch, see handle_branch())
  [], Extra, KeepUntilAftermath, FunAcc) ->
    Extra1 = merge_keep_until_aftermath(Extra, KeepUntilAftermath),
    handle_keep_until_aftermath(StartingNode, Extra1, FunAcc);
walk_back_up_the_tree(
  StartingNode,
  _ReversedPath,
  [], Extra, KeepUntilAftermath, FunAcc) ->
    Extra1 = merge_keep_until_aftermath(Extra, KeepUntilAftermath),
    {ok, StartingNode, Extra1, FunAcc}.

handle_keep_until_for_parent_update(
  ParentNode,
  ReversedPath,
  ReversedParentTree,
  Extra, KeepUntilAftermath, FunAcc) ->
    ParentPath = lists:reverse(ReversedPath),
    IsMet = is_keep_until_condition_met_on_self(
              ParentPath, ParentNode, Extra),
    case IsMet of
        true ->
            %% We continue with the update.
            walk_back_up_the_tree(
              ParentNode, ReversedPath, ReversedParentTree,
              Extra, KeepUntilAftermath, FunAcc);
        {false, _Reason} ->
            %% This parent node must be removed because it doesn't meet its
            %% own keep_until condition. keep_until conditions for nodes
            %% depending on this one will be evaluated with the recursion.
            walk_back_up_the_tree(
              remove, ReversedPath, ReversedParentTree,
              Extra, KeepUntilAftermath, FunAcc)
    end.

merge_keep_until_aftermath(Extra, KeepUntilAftermath) ->
    OldKUA = maps:get(keep_until_aftermath, Extra, #{}),
    NewKUA = maps:fold(
               fun
                   (Path, remove, KUA1) ->
                       KUA1#{Path => remove};
                   (Path, NodeProps, KUA1) ->
                       case KUA1 of
                           #{Path := remove} -> KUA1;
                           _                 -> KUA1#{Path => NodeProps}
                       end
               end, OldKUA, KeepUntilAftermath),
    Extra#{keep_until_aftermath => NewKUA}.

handle_keep_until_aftermath(
  Root,
  #{keep_until_aftermath := KeepUntilAftermath} = Extra,
  FunAcc)
  when KeepUntilAftermath =:= #{} ->
    {ok, Root, Extra, FunAcc};
handle_keep_until_aftermath(
  Root,
  #{keep_untils := KeepUntils,
    keep_untils_revidx := KeepUntilsRevIdx,
    keep_until_aftermath := KeepUntilAftermath} = Extra,
  FunAcc) ->
    ToRemove = eval_keep_until_conditions(
                 KeepUntilAftermath, KeepUntils, KeepUntilsRevIdx, Root),

    {KeepUntils1,
     KeepUntilsRevIdx1} = maps:fold(
                            fun
                                (RemovedPath, remove, {KU, KURevIdx}) ->
                                    KU1 = maps:remove(RemovedPath, KU),
                                    KURevIdx1 = update_keep_untils_revidx(
                                                  KU, KURevIdx,
                                                  RemovedPath, #{}),
                                    {KU1, KURevIdx1};
                                (_, _, Acc) ->
                                    Acc
                            end, {KeepUntils, KeepUntilsRevIdx},
                            KeepUntilAftermath),
    Extra1 = maps:remove(keep_until_aftermath, Extra),
    Extra2 = Extra1#{keep_untils => KeepUntils1,
                     keep_untils_revidx => KeepUntilsRevIdx1},

    ToRemove1 = filter_and_sort_paths_to_remove(ToRemove, KeepUntilAftermath),
    remove_expired_nodes(ToRemove1, Root, Extra2, FunAcc).

eval_keep_until_conditions(
  KeepUntilAftermath, KeepUntils, KeepUntilsRevIdx, Root) ->
    %% KeepUntilAftermath lists all nodes which were modified or removed. We
    %% want to transform that into a list of nodes to remove.
    %%
    %% Those marked as `remove' in KeepUntilAftermath are already gone. We
    %% need to find the nodes which depended on them, i.e. their keep_until
    %% condition is not met anymore. Note that removed nodes' child nodes are
    %% gone as well and must be handled (they are not specified in
    %% KeepUntilAftermath).
    %%
    %% Those modified in KeepUntilAftermath must be evaluated again to decide
    %% if they should be removed.
    maps:fold(
      fun
          (RemovedPath, remove, ToRemove) ->
              maps:fold(
                fun(Path, Watchers, ToRemove1) ->
                        case lists:prefix(RemovedPath, Path) of
                            true ->
                                eval_keep_until_conditions_after_removal(
                                  Watchers, KeepUntils, Root, ToRemove1);
                            false ->
                                ToRemove1
                        end
                end, ToRemove, KeepUntilsRevIdx);
          (UpdatedPath, NodeProps, ToRemove) ->
              case KeepUntilsRevIdx of
                  #{UpdatedPath := Watchers} ->
                      eval_keep_until_conditions_after_update(
                        UpdatedPath, NodeProps,
                        Watchers, KeepUntils, Root, ToRemove);
                  _ ->
                      ToRemove
              end
      end, #{}, KeepUntilAftermath).

eval_keep_until_conditions_after_update(
  UpdatedPath, NodeProps, Watchers, KeepUntils, Root, ToRemove) ->
    maps:fold(
      fun(Watcher, ok, ToRemove1) ->
              KeepUntil = maps:get(Watcher, KeepUntils),
              CondOnUpdated = maps:get(UpdatedPath, KeepUntil),
              IsMet = khepri_condition:is_met(
                        CondOnUpdated, UpdatedPath, NodeProps),
              case IsMet of
                  true ->
                      ToRemove1;
                  {false, _} ->
                      case are_keep_until_conditions_met(Root, KeepUntil) of
                          true       -> ToRemove1;
                          {false, _} -> ToRemove1#{Watcher => remove}
                      end
              end
      end, ToRemove, Watchers).

eval_keep_until_conditions_after_removal(
  Watchers, KeepUntils, Root, ToRemove) ->
    maps:fold(
      fun(Watcher, ok, ToRemove1) ->
              KeepUntil = maps:get(Watcher, KeepUntils),
              case are_keep_until_conditions_met(Root, KeepUntil) of
                  true       -> ToRemove1;
                  {false, _} -> ToRemove1#{Watcher => remove}
              end
      end, ToRemove, Watchers).

filter_and_sort_paths_to_remove(ToRemove, KeepUntilAftermath) ->
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
                       case KeepUntilAftermath of
                           #{Path := remove} ->
                               Map;
                           _ ->
                               case is_parent_being_removed(Path, Map) of
                                   false -> Map#{Path => remove};
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

get_keep_untils(#?MODULE{keep_untils = KeepUntils}) ->
    KeepUntils.

get_keep_untils_revidx(#?MODULE{keep_untils_revidx = KeepUntilsRevIdx}) ->
    KeepUntilsRevIdx.
-endif.
