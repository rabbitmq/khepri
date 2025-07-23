%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
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
%%
%% == State machine history ==
%%
%% <table>
%% <tr><th>Version</th><th>What changed</th></tr>
%% <tr>
%% <td style="text-align: right; vertical-align: top;">0</td>
%% <td>Initial version</td>
%% </tr>
%% <tr>
%% <td style="text-align: right; vertical-align: top;">1</td>
%% <td>
%% <ul>
%% <li>Added deduplication mechanism:
%% <ul>
%% <li>new command option `protect_against_dups'</li>
%% <li>new commands `#dedup{}' and `#dedup_ack{}'</li>
%% <li>new state field `dedups'</li>
%% </ul></li>
%% <li>Added command `#unregister_projections{}'. The previous
%% `#unregister_projection{}' is still supported for backward-compatibility but
%% it is no longer created.</li>
%% </ul>
%% </td>
%% </tr>
%% <tr>
%% <td style="text-align: right; vertical-align: top;">2</td>
%% <td>
%% <ul>
%% <li>Changed the data structure for the reverse index used to track
%% keep-while conditions to be a prefix tree (see {@link khepri_prefix_tree}).
%% </li>
%% <li>Moved the expiration of dedups to the `tick' aux effect (see {@link
%% handle_aux/5}). This also introduces a new command `#drop_dedups{}'.</li>
%% <li>Added the `delete_reason' to the list of properties that can be
%% returned. It is returned by default if the effective machine version is 2 or
%% more.</li>
%% </ul>
%% </td>
%% </tr>
%% </table>

-module(khepri_machine).
-behaviour(ra_machine).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_evf.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").
-include("src/khepri_tx.hrl").
-include("src/khepri_projection.hrl").

-export([fold/5,
         fence/2,
         put/4,
         delete/3,
         transaction/5,
         register_trigger/5,
         register_projection/4,
         unregister_projections/3]).
-export([get_keep_while_conds_state/2,
         get_projections_state/2]).

%% ra_machine callbacks.
-export([init/1,
         init_aux/1,
         handle_aux/5,
         apply/3,
         state_enter/2,
         snapshot_installed/4,
         overview/1,
         version/0,
         which_module/1]).

%% For internal use only.
-export([clear_cache/1,
         ack_triggers_execution/2,
         split_query_options/2,
         split_command_options/2,
         split_put_options/1,
         insert_or_update_node/6,
         delete_matching_nodes/4,
         handle_tx_exception/1,
         process_query/3,
         process_command/3,
         does_api_comply_with/2]).

%% Internal functions to access the opaque #khepri_machine{} state.
-export([is_state/1,
         ensure_is_state/1,
         get_tree/1,
         get_root/1,
         get_keep_while_conds/1,
         get_triggers/1,
         get_emitted_triggers/1,
         get_projections/1,
         has_projection/2,
         get_metrics/1,
         get_dedups/1,
         get_store_id/1]).

-ifdef(TEST).
-export([do_process_sync_command/3,
         make_virgin_state/1,
         convert_state/3,
         set_tree/2]).
-endif.

-compile({no_auto_import, [apply/3]}).

-type props() :: #{payload_version := khepri:payload_version(),
                   child_list_version := khepri:child_list_version()}.
%% Properties attached to each node in the tree structure.

-type triggered() :: #triggered{}.

-type command() :: #put{} |
                   #delete{} |
                   #tx{} |
                   #register_trigger{} |
                   #ack_triggered{} |
                   #register_projection{} |
                   #unregister_projections{} |
                   #dedup{} |
                   #dedup_ack{}.
%% Commands specific to this Ra machine.

-type old_command() :: #unregister_projection{}.
%% Old commands that are still accepted by the Ra machine but never created.
%%
%% Even though Khepri no longer creates these commands, they may still be
%% present in existing Ra log files and thus be applied after an ugprade of
%% Khepri.
%%
%% We keep them supported for backward-compatibility.

-type machine_init_args() :: #{store_id := khepri:store_id(),
                               member := ra:server_id(),
                               snapshot_interval => non_neg_integer(),
                               commands => [command()],
                               atom() => any()}.
%% Structure passed to {@link init/1}.

-type machine_config() :: #config{}.
%% Configuration record, holding read-only or rarely changing fields.

%% State machine's internal state record.
-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         tree = khepri_tree:new() :: khepri_tree:tree(),
         triggers = #{} :: khepri_machine:triggers_map(),
         emitted_triggers = [] :: [khepri_machine:triggered()],
         projections = khepri_pattern_tree:empty() ::
                       khepri_machine:projection_tree(),
         metrics = #{} :: khepri_machine:metrics(),

         %% Added in machine version 1.
         dedups = #{} :: khepri_machine:dedups_map()}).

-opaque state_v1() :: #khepri_machine{}.
%% State of this Ra state machine, version 1.
%%
%% Note that this type is used also for machine version 2. Machine version 2
%% changes the type of an opaque member of the {@link khepri_tree} record and
%% doesn't need any changes to the `khepri_machine' type. See the moduledoc of
%% this module for more information about version 2.

-type state() :: state_v1() | khepri_machine_v0:state().
%% State of this Ra state machine.

-type triggers_map() :: #{khepri:trigger_id() =>
                          #{sproc := khepri_path:native_path(),
                            event_filter := khepri_evf:event_filter()}}.
%% Internal triggers map in the machine state.

-type metrics() :: #{applied_command_count => non_neg_integer()}.
%% Internal state machine metrics.

-type dedups_map() :: #{reference() => {any(), integer()}}.
%% Map to handle command deduplication.

-type aux_state() :: #khepri_machine_aux{}.
%% Auxiliary state of this Ra state machine.

-type aux_query() :: {query, query_fun(), khepri:query_options()}.
%% Query executed through the Ra aux mechanism.

-type delayed_aux_query() :: {aux_query(),
                              gen_statem:from(),
                              ra:query_condition() | none}.
%% Aux query that is delayed until a condition is met.

-type query_fun() :: fun((state()) -> any()) |
                     fun((ra_machine:command_meta_data(), state()) -> any()).
%% Function representing a query and used {@link process_query/3}.

-type write_ret() :: khepri:ok(khepri:node_props_map()) |
                     khepri:error().

-type tx_ret() :: khepri:ok(khepri_tx:tx_fun_result()) |
                  khepri_tx:tx_abort() |
                  no_return().

-type async_ret() :: ok.

-type projection_tree() :: khepri_pattern_tree:tree(
                             [khepri_projection:projection()]).
%% A pattern tree that holds all registered projections in the machine's state.

-type projection_map() :: #{khepri_projection:name() => khepri_path:pattern()}.
%% A mapping between the names of projections and patterns to which each
%% projection is registered.

-type api_behaviour() :: dedup_protection |
                         delete_reason_in_node_props |
                         indirect_deletes_in_ret |
                         uniform_write_ret |
                         atom().
%% Name of a state machine API behaviour.

-export_type([write_ret/0,
              tx_ret/0,
              async_ret/0,

              machine_init_args/0,
              state/0,
              state_v1/0,
              machine_config/0,
              triggers_map/0,
              metrics/0,
              dedups_map/0,
              props/0,
              triggered/0,
              projection_tree/0,
              projection_map/0,
              api_behaviour/0,
              command/0,
              old_command/0,
              delayed_aux_query/0]).

-define(PROJECTION_PROPS_TO_RETURN, [payload_version,
                                     child_list_version,
                                     child_list_length,
                                     child_names,
                                     payload]).

-define(DEFAULT_RA_COMMAND_CORRELATION, no_correlation).
-define(DEFAULT_RA_COMMAND_PRIORITY, normal).
-define(IS_RA_COMMAND_CORRELATION(Correlation),
        (is_integer(Correlation) orelse is_reference(Correlation))).
-define(IS_RA_COMMAND_PRIORITY(Priority),
        (Priority =:= normal orelse Priority =:= low)).

%% -------------------------------------------------------------------
%% Machine protocol.
%% -------------------------------------------------------------------

%% TODO: Verify arguments carefully to avoid the construction of an invalid
%% command.

-spec fold(StoreId, PathPattern, Fun, Acc, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc.
%% @doc Returns all tree nodes matching the given path pattern.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the path (or path pattern) to the nodes to get.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, NodePropsMap}' tuple with a map with zero, one or more
%% entries, or an `{error, Reason}' tuple.

fold(StoreId, PathPattern, Fun, Acc, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       is_function(Fun, 3) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    {QueryOptions, TreeOptions} = split_query_options(StoreId, Options),
    Query = fun(State) ->
                    Tree = get_tree(State),
                    try
                        khepri_tree:fold(
                          Tree, PathPattern1, Fun, Acc, TreeOptions)
                    catch
                        Class:Reason:Stacktrace ->
                            {exception, Class, Reason, Stacktrace}
                    end
            end,
    case process_query(StoreId, Query, QueryOptions) of
        {exception, _, _, _} = Exception -> handle_tx_exception(Exception);
        Ret                              -> Ret
    end.

-spec fence(StoreId, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Blocks until all updates received by the cluster leader are applied
%% locally.
%%
%% @param StoreId the name of the Ra cluster
%% @param Timeout the time limit after which the call returns with an error.
%%
%% @returns `ok' or an `{error, Reason}' tuple.

fence(StoreId, Timeout) ->
    QueryFun = fun erlang:is_tuple/1,
    Options = #{favor => consistency,
                timeout => Timeout},
    case process_query(StoreId, QueryFun, Options) of
        {exception, _, _, _} = Exception -> handle_tx_exception(Exception);
        true                             -> ok;
        Other when Other =/= false       -> Other
    end.

-spec put(StoreId, PathPattern, Payload, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_payload:payload(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_machine:write_ret() | khepri_machine:async_ret().
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
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso ?IS_KHEPRI_PAYLOAD(Payload) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Payload1 = khepri_payload:prepare(Payload),
    {CommandOptions, TreeAndPutOptions} = split_command_options(
                                            StoreId, Options),
    Command = #put{path = PathPattern1,
                   payload = Payload1,
                   options = TreeAndPutOptions},
    process_command(StoreId, Command, CommandOptions);
put(_StoreId, PathPattern, Payload, _Options) ->
    ?khepri_misuse(invalid_payload, #{path => PathPattern,
                                      payload => Payload}).

-spec delete(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_machine:write_ret() | khepri_machine:async_ret().
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

delete(StoreId, PathPattern, Options) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    {CommandOptions, TreeOptions} = split_command_options(StoreId, Options),
    %% TODO: Ensure `PutOptions' are not set this map.
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
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    case khepri_tx_adv:to_standalone_fun(Fun, ReadWrite) of
        StandaloneFun when ?IS_HORUS_STANDALONE_FUN(StandaloneFun) ->
            Options1 = remove_query_options(Options),
            {CommandOptions, _} = split_command_options(StoreId, Options1),
            readwrite_transaction(
              StoreId, StandaloneFun, Args, CommandOptions);
        _ ->
            Options1 = remove_command_options(Options),
            {QueryOptions, _} = split_query_options(StoreId, Options1),
            readonly_transaction(StoreId, Fun, Args, QueryOptions)
    end;
transaction(StoreId, PathPattern, Args, auto, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Options1 = remove_query_options(Options),
    {CommandOptions, _} = split_command_options(StoreId, Options1),
    readwrite_transaction(StoreId, PathPattern1, Args, CommandOptions);
transaction(StoreId, Fun, Args, rw = ReadWrite, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    StandaloneFun = khepri_tx_adv:to_standalone_fun(Fun, ReadWrite),
    Options1 = remove_query_options(Options),
    {CommandOptions, _} = split_command_options(StoreId, Options1),
    readwrite_transaction(StoreId, StandaloneFun, Args, CommandOptions);
transaction(StoreId, PathPattern, Args, rw, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Options1 = remove_query_options(Options),
    {CommandOptions, _} = split_command_options(StoreId, Options1),
    readwrite_transaction(StoreId, PathPattern1, Args, CommandOptions);
transaction(StoreId, Fun, Args, ro, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       is_list(Args) andalso
       is_function(Fun, length(Args)) andalso
       is_map(Options) ->
    Options1 = remove_command_options(Options),
    {QueryOptions, _} = split_query_options(StoreId, Options1),
    readonly_transaction(StoreId, Fun, Args, QueryOptions);
transaction(StoreId, PathPattern, Args, ro, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso
       is_list(Args) andalso
       is_map(Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Options1 = remove_command_options(Options),
    {QueryOptions, _} = split_query_options(StoreId, Options1),
    readonly_transaction(StoreId, PathPattern1, Args, QueryOptions);
transaction(StoreId, Fun, Args, ReadWrite, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
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
    Query = fun(Meta, State) ->
                    %% It is a read-only transaction, therefore we assert that
                    %% the state is unchanged and that there are no side
                    %% effects.
                    {State1, Ret, []} = khepri_tx_adv:run(
                                          State, Fun, Args, false,
                                          Meta),
                    assert_equal(State, State1),
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
    Query = fun(Meta, State) ->
                    %% It is a read-only transaction, therefore we assert that
                    %% the state is unchanged and that there are no side
                    %% effects.
                    {State1, Ret, []} = locate_sproc_and_execute_tx(
                                          State, PathPattern, Args, false,
                                          Meta),
                    assert_equal(State, State1),
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
      Fun :: horus:horus_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: khepri:command_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().

readwrite_transaction(
  StoreId, Fun, Args, Options)
  when is_list(Args) andalso
       (is_function(Fun, length(Args)) orelse
        ?IS_HORUS_STANDALONE_FUN(Fun, length(Args))) ->
    readwrite_transaction1(StoreId, Fun, Args, Options);
readwrite_transaction(
  StoreId, PathPattern, Args, Options)
  when ?IS_KHEPRI_PATH_PATTERN(PathPattern) andalso is_list(Args) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    readwrite_transaction1(StoreId, PathPattern1, Args, Options).

readwrite_transaction1(StoreId, StandaloneFunOrPath, Args, Options) ->
    Command = #tx{'fun' = StandaloneFunOrPath, args = Args},
    Options1 = maps:merge(#{protect_against_dups => true}, Options),
    case process_command(StoreId, Command, Options1) of
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
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    EventFilter1 = khepri_evf:wrap(EventFilter),
    StoredProcPath1 = khepri_path:from_string(StoredProcPath),
    khepri_path:ensure_is_valid(StoredProcPath1),
    Command = #register_trigger{id = TriggerId,
                                sproc = StoredProcPath1,
                                event_filter = EventFilter1},
    process_command(StoreId, Command, Options).

-spec register_projection(StoreId, PathPattern, Projection, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Projection :: khepri_projection:projection(),
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Registers a projection.
%%
%% @param StoreId the name of the Ra cluster.
%% @param PathPattern the pattern of tree nodes which should be projected.
%% @param Projection the projection record created with {@link
%%        khepri_projection:new/3}.
%% @param Options command options such as the command type.
%%
%% @returns `ok' if the projection was registered, an `{error, Reason}' tuple
%% otherwise.

register_projection(
  StoreId, PathPattern0,
  #khepri_projection{name = Name,
                     projection_fun = ProjectionFun,
                     ets_options = EtsOptions} = Projection,
  Options)
  when is_atom(Name) andalso
       is_list(EtsOptions) andalso
       (?IS_HORUS_STANDALONE_FUN(ProjectionFun) orelse
        ProjectionFun =:= copy) ->
    PathPattern = khepri_path:from_string(PathPattern0),
    khepri_path:ensure_is_valid(PathPattern),
    Command = #register_projection{pattern = PathPattern,
                                   projection = Projection},
    process_command(StoreId, Command, Options).

-spec unregister_projections(StoreId, Names, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Names :: all | [khepri_projection:name()],
      Options :: khepri:command_options(),
      Ret :: khepri:ok(khepri_machine:projection_map()) | khepri:error().
%% @doc Removes the given projections from the store.
%%
%% `Names' may either be a list of projection names to remove or the atom
%% `all'. When `all' is passed, every projection in the store is removed.
%%
%% @param StoreId the name of the Ra cluster.
%% @param Names the names of projections to unregister or the atom `all' to
%%        remove all projections.
%% @param Options command options such as the command type.
%%
%% @returns `{ok, ProjectionMap}' if the command succeeds, `{error, Reason}'
%% otherwise. The `ProjectionMap' is a map with projection names ({@link
%% khepri_projection:name()} keys associated to the pattern to which each
%% projection was registered.

unregister_projections(StoreId, Names, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (Names =:= all orelse is_list(Names)) andalso
       is_map(Options) ->
    Command = #unregister_projections{names = Names},
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
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    Command = #ack_triggered{triggered = TriggeredStoredProcs},
    process_command(StoreId, Command, #{async => true}).

-spec get_keep_while_conds_state(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(khepri_tree:keep_while_conds_map()) | khepri:error().
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
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    Query = fun(State) ->
                    KeepWhileConds = get_keep_while_conds(State),
                    {ok, KeepWhileConds}
            end,
    process_query(StoreId, Query, Options).

-spec get_projections_state(StoreId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(ProjectionState) | khepri:error(),
      ProjectionState :: khepri_pattern_tree:tree(Projection),
      Projection :: khepri_projection:projection().
%% @doc Returns the `projections' internal state.
%%
%% The returned state is a pattern tree containing the projections registered
%% in the store. (See {@link khepri_pattern_tree:tree()} and {@link
%% khepri_projection:projection()}.)
%%
%% @see khepri_pattern_tree.
%% @see khepri_projection.
%%
%% @private

get_projections_state(StoreId, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    Query = fun(State) ->
                    Projections = get_projections(State),
                    {ok, Projections}
            end,
    process_query(StoreId, Query, Options).

-spec split_query_options(StoreId, Options) ->
    {QueryOptions, TreeOptions} when
      StoreId :: khepri:store_id(),
      Options :: QueryOptions | TreeOptions,
      QueryOptions :: khepri:query_options(),
      TreeOptions :: khepri:tree_options().
%% @private

split_query_options(StoreId, Options) ->
    Options1 = set_default_options(StoreId, Options),
    maps:fold(
      fun
          (Option, Value, {Q, T}) when
                Option =:= condition orelse
                Option =:= timeout orelse
                Option =:= favor ->
              Q1 = Q#{Option => Value},
              {Q1, T};
          (props_to_return, [], {Q, T}) ->
              {Q, T};
          (Option, Value, {Q, T}) when
                Option =:= expect_specific_node orelse
                Option =:= props_to_return orelse
                Option =:= include_root_props orelse
                Option =:= return_indirect_deletes ->
              T1 = T#{Option => Value},
              {Q, T1}
      end, {#{}, #{}}, Options1).

-spec split_command_options(StoreId, Options) ->
    {CommandOptions, TreeAndPutOptions} when
      StoreId :: khepri:store_id(),
      Options :: CommandOptions | TreeAndPutOptions,
      CommandOptions :: khepri:command_options(),
      TreeAndPutOptions :: khepri:tree_options() | khepri:put_options().
%% @private

split_command_options(StoreId, Options) ->
    Options1 = set_default_options(StoreId, Options),
    maps:fold(
      fun
          (Option, Value, {C, TP}) when
                Option =:= reply_from orelse
                Option =:= timeout orelse
                Option =:= async ->
              C1 = C#{Option => Value},
              {C1, TP};
          (props_to_return, [], Acc) ->
              Acc;
          (Option, Value, {C, TP}) when
                Option =:= expect_specific_node orelse
                Option =:= props_to_return orelse
                Option =:= include_root_props orelse
                Option =:= return_indirect_deletes ->
              TP1 = TP#{Option => Value},
              {C, TP1};
          (keep_while, KeepWhile, {C, TP}) ->
              %% `keep_while' is kept in `TreeAndPutOptions' here. The state
              %% machine will extract it in `apply()'.
              KeepWhile1 = khepri_condition:ensure_native_keep_while(
                             KeepWhile),
              TP1 = TP#{keep_while => KeepWhile1},
              {C, TP1}
      end, {#{}, #{}}, Options1).

-spec split_put_options(TreeAndPutOptions) -> {TreeOptions, PutOptions} when
      TreeAndPutOptions :: TreeOptions | PutOptions,
      TreeOptions :: khepri:tree_options(),
      PutOptions :: khepri:put_options().
%% @private

split_put_options(TreeAndPutOptions) ->
    maps:fold(
      fun
          (keep_while, KeepWhile, {T, P}) ->
              P1 = P#{keep_while => KeepWhile},
              {T, P1};
          (Option, Value, {T, P}) ->
              T1 = T#{Option => Value},
              {T1, P}
      end, {#{}, #{}}, TreeAndPutOptions).

remove_query_options(Options) ->
    maps:filter(
      fun
          (condition, _Value) -> false;
          (favor, _Value) -> false;
          (_Option, _Value) -> true
      end, Options).

remove_command_options(Options) ->
    maps:filter(
      fun
          (async, _Value) -> false;
          (protect_against_dups, _Value) -> false;
          (reply_from, _Value) -> false;
          (_Option, _Value) -> true
      end, Options).

set_default_options(StoreId, Options) ->
    %% By default, return payload-related properties. The caller can set
    %% `props_to_return' to an empty map to get a minimal return value.
    Options1 = case Options of
                   #{props_to_return := _} ->
                       Options;
                   _ ->
                       Options#{props_to_return => ?DEFAULT_PROPS_TO_RETURN}
               end,
    %% We need to remove `delete_reason' from the list if the whole cluster is
    %% still using a machine version that doesn't know about it. Otherwise old
    %% versions of Khepri will crash when gathering the properties.
    PropsToReturn0 = maps:get(props_to_return, Options1),
    KeepDeleteReason = does_api_comply_with(
                         delete_reason_in_node_props, StoreId),
    PropsToReturn1 = case KeepDeleteReason of
                         true ->
                             PropsToReturn0;
                         false ->
                             %% `delete_reason' was added in machine version
                             %% 2. Also, previous versions didn't ignore
                             %% unknown props_to_return and crashed.
                             PropsToReturn0 -- [delete_reason]
                     end,
    Options2 = Options1#{props_to_return => PropsToReturn1},
    Options2.

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

process_sync_command(
  StoreId, Command, #{protect_against_dups := true} = Options) ->
    %% The deduplication mechanism was added to machine version 1.
    case does_api_comply_with(dedup_protection, StoreId) of
        true ->
            %% When `protect_against_dups' is true, we wrap the command inside
            %% a #dedup{} one to give it a unique reference. This is used for
            %% non-idempotent commands which could be replayed when there is a
            %% change of leadership in the Ra cluster. The state machine uses
            %% this reference to remember what it replied to a given
            %% reference.
            %%
            %% `do_process_sync_command/3' is responsible for the retry loop
            %% like with any other commands. Once it returned, we cast a
            %% #dedup_ack{} command with the same reference to let the machine
            %% know it can forget about that reference and the associated
            %% reply.
            CommandRef = make_ref(),

            %% We can't keep a dedup entry forever in the machine state if the
            %% initial caller never acknowledges the reply. Therefore we set
            %% an expiration time after which the entry will be dropped.
            %%
            %% We base this expiry on the command's timeout. If it's
            %% `infinity' we default to 15 minutes from now.
            Now = erlang:system_time(millisecond),
            Expiry = case get_timeout(Options) of
                         infinity -> Now + 15 * 60 * 1000;
                         Timeout  -> Now + Timeout
                     end,
            DedupCommand = #dedup{ref = CommandRef,
                                  expiry = Expiry,
                                  command = Command},
            DedupAck = #dedup_ack{ref = CommandRef},
            Ret = do_process_sync_command(
                    StoreId, DedupCommand, Options),

            %% We acknowledge that we received the reply and all duplicates
            %% can be ignored.
            ThisNode = node(),
            RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
            _ = ra:pipeline_command(RaServer, DedupAck),
            Ret;
        false ->
            do_process_sync_command(StoreId, Command, Options)
    end;
process_sync_command(
  StoreId, Command, Options) ->
    do_process_sync_command(StoreId, Command, Options).

do_process_sync_command(StoreId, Command, Options) ->
    ThisNode = node(),
    RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
    Timeout = get_timeout(Options),
    ReplyFrom = maps:get(reply_from, Options, {member, RaServer}),
    CommandOptions = #{timeout => Timeout, reply_from => ReplyFrom},
    T0 = khepri_utils:start_timeout_window(Timeout),
    Dest = case ra_leaderboard:lookup_leader(StoreId) of
               LeaderId when LeaderId =/= undefined ->
                   sending_command_remotely(StoreId),
                   LeaderId;
               undefined ->
                   sending_sync_command_locally(StoreId),
                   RaServer
           end,
    case ra:process_command(Dest, Command, CommandOptions) of
        {ok, Ret, _LeaderId} ->
            ?raise_exception_if_any(Ret);
        {timeout, _LeaderId} ->
            {error, timeout};
        {error, Reason} = Error
          when ?HAS_TIME_LEFT(Timeout) andalso
               (Reason == noproc orelse Reason == nodedown orelse
                Reason == shutdown) ->
            %% We retry the command if either:
            %% - the command was sent directly to a remote server, or
            %% - the command was sent to the local server and it is still
            %%   alive.
            ShouldRetry = (Dest =/= RaServer orelse
                           khepri_utils:is_ra_server_alive(RaServer)),
            case ShouldRetry of
                true ->
                    %% The follower doesn't know about the new leader yet.
                    %% Retry again after waiting a bit.
                    NewTimeout0 = khepri_utils:end_timeout_window(Timeout, T0),
                    NewTimeout = khepri_utils:sleep(
                                   ?TRANSIENT_ERROR_RETRY_INTERVAL,
                                   NewTimeout0),
                    Options1 = Options#{timeout => NewTimeout},
                    do_process_sync_command(StoreId, Command, Options1);
                false ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

process_async_command(
  StoreId, Command, ?DEFAULT_RA_COMMAND_CORRELATION = Correlation, Priority) ->
    ThisNode = node(),
    RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
    sending_async_command_locally(StoreId),
    ra:pipeline_command(RaServer, Command, Correlation, Priority);
process_async_command(
  StoreId, Command, Correlation, Priority) ->
    case ra_leaderboard:lookup_leader(StoreId) of
        LeaderId when LeaderId =/= undefined ->
            sending_command_remotely(StoreId),
            ra:pipeline_command(LeaderId, Command, Correlation, Priority);
        undefined ->
            ThisNode = node(),
            RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
            sending_async_command_locally(StoreId),
            ra:pipeline_command(RaServer, Command, Correlation, Priority)
    end.

-spec select_command_type(Options) -> CommandType when
      Options :: khepri:command_options(),
      CommandType :: sync | {async, Correlation, Priority},
      Correlation :: ra_server:command_correlation() |
                     ?DEFAULT_RA_COMMAND_CORRELATION,
      Priority :: ra_server:command_priority().
%% @doc Selects the command type depending on what the caller wants.
%%
%% @private

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
%% @doc Processes a query on the local Ra server.
%%
%% The `QueryFun' function takes the machine state as an argument and can
%% return anything. However, the machine state is never modified. The query
%% does not go through the Ra log and is not replicated.
%%
%% The `QueryFun' function is executed from a process on the local Ra member.
%%
%% @param StoreId the name of the Ra cluster.
%%
%% @returns the result of the query or an "error" tuple.
%%
%% @private

process_query(StoreId, QueryFun, #{condition := _} = Options) ->
    %% `condition' takes precedence over `favor'.
    Options1 = maps:remove(favor, Options),
    process_query1(StoreId, QueryFun, Options1);
process_query(StoreId, QueryFun, Options) ->
    Favor = maps:get(favor, Options, low_latency),
    case Favor of
        low_latency ->
            process_query1(StoreId, QueryFun, Options);
        consistency ->
            case add_applied_condition(StoreId, Options) of
                {ok, Options1} ->
                    process_query1(StoreId, QueryFun, Options1);
                {error, _} = Error ->
                    Error
            end
    end.

process_query1(StoreId, QueryFun, Options) ->
    sending_query_locally(StoreId),
    LocalServerId = {StoreId, node()},
    Timeout = get_timeout(Options),
    try
        Ret = ra:aux_command(
                LocalServerId, {query, QueryFun, Options}, Timeout),
        ?raise_exception_if_any(Ret)
    catch
        exit:{noproc, _} ->
            {error, noproc};
        exit:{timeout, _} ->
            {error, timeout}
    end.

-spec add_applied_condition(StoreId, Options) -> NewOptions when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options(),
      NewOptions :: khepri:ok(khepri:query_options()) | khepri:error().
%% @private

add_applied_condition(StoreId, Options) ->
    Timeout = get_timeout(Options),
    add_applied_condition1(StoreId, Options, Timeout).

add_applied_condition1(StoreId, Options, Timeout) ->
    %% The `applied' condition permits that a query is only evaluated after
    %% the given index is applied on the local node. This is useful to enforce
    %% the order of operations between updates and queries. We have to follow
    %% several steps to prepare that condition.
    %%
    %% If the last message from the calling process to the local Ra server was
    %% an async command or if it never sent a command yet, we first send an
    %% arbitrary query to the local Ra server. This is to make sure that
    %% previously submitted pipelined commands were processed by that server.
    %%
    %% For instance, if there was a pipelined command without any correlation
    %% ID, it ensures it was forwarded to the leader. Likewise for a
    %% synchronous command without the `reply_from => local' option.
    %%
    %% We can't have this guaranty for pipelined commands with a correlation
    %% because the caller is responsible for receiving the rejection from the
    %% follower and handle the redirect to the leader.
    case can_skip_fence_preliminary_query(StoreId) of
        true ->
            add_applied_condition2(StoreId, Options, Timeout);
        false ->
            T0 = khepri_utils:start_timeout_window(Timeout),
            QueryFun = fun erlang:is_integer/1,
            sending_query_locally(StoreId),
            LocalServerId = {StoreId, node()},
            Ret = ra:local_query(LocalServerId, QueryFun, Options),
            case Ret of
                {ok, {_RaIdxTerm, false}, _NewLeaderId} ->
                    NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
                    add_applied_condition2(StoreId, Options, NewTimeout);
                {timeout, _LeaderId} ->
                    {error, timeout};
                {error, _} = Error ->
                    Error
            end
    end.

add_applied_condition2(StoreId, Options, Timeout) ->
    %% After the previous local query or sync command if there was one, there
    %% is a great chance that the leader was cached, though not 100%
    %% guarantied.
    case ra_leaderboard:lookup_leader(StoreId) of
        LeaderId when LeaderId =/= undefined ->
            add_applied_condition3(StoreId, Options, LeaderId, Timeout);
        undefined ->
            %% If the leader is unknown, executing a preliminary query should
            %% tell us who the leader is.
            ask_fence_preliminary_query(StoreId),
            add_applied_condition1(StoreId, Options, Timeout)
    end.

add_applied_condition3(StoreId, Options, LeaderId, Timeout) ->
    %% We query the leader to know the last index it committed in which term.
    %%
    %% We pay attention to its state because a map is still returned even if
    %% the Ra server is stopped.
    T0 = khepri_utils:start_timeout_window(Timeout),
    try ra:key_metrics(LeaderId, Timeout) of
        #{last_index := LastIndex, term := Term, state := State}
          when State =/= noproc andalso State =/= unknown ->
            NewTimeout1 = khepri_utils:end_timeout_window(Timeout, T0),

            %% Now that we know the last committed index of the leader, we can
            %% use it as a condition on a query to block its execution until
            %% the local Ra server reaches that index.
            Condition = {applied, {LastIndex, Term}},
            Options1 = Options#{condition => Condition,
                                timeout => NewTimeout1},
            {ok, Options1};
        _ ->
            timer:sleep(200),
            NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
            add_applied_condition1(StoreId, Options, NewTimeout)
    catch
        error:{erpc, timeout} ->
            {error, timeout};
        error:{erpc, noconnection} ->
            timer:sleep(200),
            NewTimeout2 = khepri_utils:end_timeout_window(Timeout, T0),
            add_applied_condition1(StoreId, Options, NewTimeout2)
    end.

-spec get_timeout(Options) -> Timeout when
      Options :: khepri:command_options() | khepri:query_options(),
      Timeout :: timeout().
%% @private

get_timeout(#{timeout := Timeout}) -> Timeout;
get_timeout(_)                     -> khepri_app:get_default_timeout().

-spec clear_cache(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Clears the cached data for the given `StoreId'.
%%
%% @private

clear_cache(_StoreId) ->
    ok.

-spec sending_sync_command_locally(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Records that a synchronous command is about to be sent locally.
%%
%% After that, we know we don't need a fence preliminary query.

sending_sync_command_locally(StoreId) ->
    Key = {khepri, can_skip_fence_preliminary_query, StoreId},
    _ = erlang:put(Key, true),
    ok.

-spec sending_query_locally(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Records that a query is about to be executed locally.
%%
%% After that, we know we don't need a fence preliminary query.

sending_query_locally(StoreId) ->
    %% Same behavior as a local sync command.
    sending_sync_command_locally(StoreId).

-spec sending_async_command_locally(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Records that an asynchronous command is about to be sent locally.

sending_async_command_locally(StoreId) ->
    Key = {khepri, can_skip_fence_preliminary_query, StoreId},
    _ = erlang:erase(Key),
    ok.

-spec sending_command_remotely(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Records that a command is about to be sent to a remote store.

sending_command_remotely(StoreId) ->
    %% Same behavior as a local async command.
    sending_async_command_locally(StoreId).

-spec ask_fence_preliminary_query(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Explicitly requests that a call to {@link
%% can_skip_fence_preliminary_query/1} returns `false'.

ask_fence_preliminary_query(StoreId) ->
    %% Same behavior as a local async command.
    sending_async_command_locally(StoreId).

-spec can_skip_fence_preliminary_query(StoreId) -> LastMsgWasSync when
      StoreId :: khepri:store_id(),
      LastMsgWasSync :: boolean().
%% @doc Indicates if the calling process sent a synchronous command or a query
%% before this call.
%%
%% The need for a preliminary query is tracked using a process dictionary
%% entry. The process dictionary is used because that property is specific to
%% a calling process. Alternatives were explored: a persistent_term was used
%% initially but it is not meant for frequent writes, and an ETS table is
%% unpracticle because if Khepri is not running on the calling process node,
%% it won't be able to create that table.
%%
%% The need is updated with calls to the `sending_*/1' functions above.
%%
%% @returns `true' if the calling process sent a synchrorous command or a query
%% to the given store before this call, `false' if the calling process never
%% sent anything to the given store, if the last message was an asynchrorous
%% command, or if the last message was sent to a remote store.

can_skip_fence_preliminary_query(StoreId) ->
    Key = {khepri, can_skip_fence_preliminary_query, StoreId},
    erlang:get(Key) =:= true.

%% -------------------------------------------------------------------
%% ra_machine callbacks.
%% -------------------------------------------------------------------

-spec init(Params) -> State when
      Params :: machine_init_args(),
      State :: khepri_machine_v0:state().
%% @private

init(Params) ->
    %% Initialize the state. This function always returns the oldest supported
    %% state format.
    State = khepri_machine_v0:init(Params),

    %% Create initial "schema" if provided.
    Commands = maps:get(commands, Params, []),
    State3 = lists:foldl(
               fun(Command, State1) ->
                       Meta = #{index => 0,
                                term => 0,
                                system_time => 0},
                       {S, _, _} = apply(Meta, Command, State1),
                       S
               end, State, Commands),
    reset_applied_command_count(State3).

-spec init_aux(StoreId :: khepri:store_id()) -> aux_state().
%% @private

init_aux(StoreId) ->
    #khepri_machine_aux{store_id = StoreId}.

-spec handle_aux(RaState, Type, Command, AuxState, IntState) ->
    Ret when
      RaState :: ra_server:ra_state(),
      Type :: {call, ra:from()} | cast,
      Command :: term(),
      AuxState :: aux_state(),
      IntState :: ra_aux:internal_state(),
      Ret :: {no_reply, AuxState, IntState} |
             {no_reply, AuxState, IntState, Effects},
      Effects :: ra_machine:effects().
%% @private

handle_aux(
  _RaState, {call, From},
  {query, _QueryFun, Options} = AuxQuery,
  #khepri_machine_aux{delayed_aux_queries = DelayedAuxQueries} = AuxState,
  IntState) ->
    Condition = maps:get(condition, Options, none),
    DelayedAuxQuery = {AuxQuery, From, Condition},
    DelayedAuxQueries1 = DelayedAuxQueries ++ [DelayedAuxQuery],
    AuxState1 = AuxState#khepri_machine_aux{
                  delayed_aux_queries = DelayedAuxQueries1},
    AuxState2 = handle_delayed_aux_queries(AuxState1, IntState),
    {no_reply, AuxState2, IntState};
handle_aux(
  _RaState, cast,
  trigger_delayed_aux_queries_eval,
  AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),
    {no_reply, AuxState1, IntState};
handle_aux(
  _RaState, cast,
  #trigger_projection{path = Path,
                      old_props = OldProps,
                      new_props = NewProps,
                      projection = Projection},
  AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),

    khepri_projection:trigger(Projection, Path, OldProps, NewProps),
    {no_reply, AuxState1, IntState};
handle_aux(
  _RaState, cast, restore_projections, AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),

    State = ra_aux:machine_state(IntState),
    Tree = get_tree(State),
    ProjectionTree = get_projections(State),
    khepri_pattern_tree:foreach(
      ProjectionTree,
      fun(PathPattern, Projections) ->
              [restore_projection(Projection, Tree, PathPattern) ||
               Projection <- Projections]
      end),
    {no_reply, AuxState1, IntState};
handle_aux(
  _RaState, cast,
  #restore_projection{projection = Projection, pattern = PathPattern},
  AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),

    State = ra_aux:machine_state(IntState),
    Tree = get_tree(State),
    ok = restore_projection(Projection, Tree, PathPattern),
    {no_reply, AuxState1, IntState};
handle_aux(leader, cast, tick, AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),

    %% Expiring dedups in the tick handler is only available on versions 2
    %% and greater. In versions 0 and 1, expiration of dedups is done in
    %% `drop_expired_dedups/2'. This proved to be quite expensive when handling
    %% a very large batch of transactions at once, so this expiration step was
    %% moved to the `tick' handler in version 2.
    case ra_aux:effective_machine_version(IntState) of
        EffectiveMacVer when EffectiveMacVer >= 2 ->
            State = ra_aux:machine_state(IntState),
            Timestamp = erlang:system_time(millisecond),
            Dedups = get_dedups(State),
            RefsToDrop = maps:fold(
                           fun(CommandRef, {_Reply, Expiry}, Acc) ->
                                   case Expiry =< Timestamp of
                                       true ->
                                           [CommandRef | Acc];
                                       false ->
                                           Acc
                                   end
                           end, [], Dedups),
            Effects = case RefsToDrop of
                          [] ->
                              [];
                          _ ->
                              DropDedups = #drop_dedups{refs = RefsToDrop},
                              [{append, DropDedups}]
                      end,
            {no_reply, AuxState1, IntState, Effects};
        _ ->
            {no_reply, AuxState1, IntState}
    end;
handle_aux(_RaState, _Type, _Command, AuxState, IntState) ->
    AuxState1 = handle_delayed_aux_queries(AuxState, IntState),
    {no_reply, AuxState1, IntState}.

handle_delayed_aux_queries(
  #khepri_machine_aux{delayed_aux_queries = []} = AuxState,
  _IntState) ->
    AuxState;
handle_delayed_aux_queries(
  #khepri_machine_aux{delayed_aux_queries = DelayedAuxQueries} = AuxState,
  IntState) ->
    Index = ra_aux:last_applied(IntState),
    Term = ra_aux:current_term(IntState),
    MacVer = ra_aux:effective_machine_version(IntState),
    Meta = #{system_time => erlang:system_time(millisecond),
             index => Index,
             term => Term,
             machine_version => MacVer},
    State = ra_aux:machine_state(IntState),
    DelayedAuxQueries2 = eval_delayed_aux_queries_condition(
                           Meta, DelayedAuxQueries, State, []),
    AuxState1 = AuxState#khepri_machine_aux{
                  delayed_aux_queries = DelayedAuxQueries2},
    AuxState1.

eval_delayed_aux_queries_condition(
  Meta,
  [{AuxQuery, From, none} | Rest],
  State, Acc) ->
    Meta1 = Meta#{from => From},
    perform_delayed_aux_query(Meta1, AuxQuery, State),
    eval_delayed_aux_queries_condition(Meta, Rest, State, Acc);
eval_delayed_aux_queries_condition(
  #{index := Index, term := Term} = Meta,
  [{AuxQuery, From, {applied, {TargetIndex, TargetTerm}}} | Rest],
  State, Acc)
  when (Term =:= TargetTerm andalso Index >= TargetIndex) orelse
       Term > TargetTerm ->
    Meta1 = Meta#{from => From},
    perform_delayed_aux_query(Meta1, AuxQuery, State),
    eval_delayed_aux_queries_condition(Meta, Rest, State, Acc);
eval_delayed_aux_queries_condition(
  Meta,
  [DelayedAuxQuery | Rest],
  State, Acc) ->
    Acc1 = [DelayedAuxQuery | Acc],
    eval_delayed_aux_queries_condition(Meta, Rest, State, Acc1);
eval_delayed_aux_queries_condition(
  _Meta, [], _State, Acc) ->
    lists:reverse(Acc).

perform_delayed_aux_query(
  #{from := From} = Meta,
  {query, QueryFun, _Options}, State) ->
    {arity, Arity} = erlang:fun_info(QueryFun, arity),
    Ret = case Arity of
              1 -> QueryFun(State);
              2 -> QueryFun(Meta, State)
          end,
    gen_statem:reply(From, Ret).

restore_projection(Projection, Tree, PathPattern) ->
    _ = khepri_projection:init(Projection),
    TreeOptions = #{props_to_return => ?PROJECTION_PROPS_TO_RETURN,
                    include_root_props => true},
    case khepri_tree:find_matching_nodes(Tree, PathPattern, TreeOptions) of
        {ok, MatchingNodes} ->
            maps:foreach(fun(Path, Props) ->
                                 khepri_projection:trigger(
                                   Projection, Path, #{}, Props)
                         end, MatchingNodes);
        Error ->
            ?LOG_DEBUG(
               "Failed to recover projection ~s due to an error: ~p",
               [khepri_projection:name(Projection), Error],
               #{domain => [khepri, ra_machine]}),
            ok
    end.

-spec apply(Meta, Command, State) -> {State, Ret, SideEffects} when
      Meta :: ra_machine:command_meta_data(),
      Command :: command() | old_command(),
      State :: state(),
      Ret :: any(),
      SideEffects :: ra_machine:effects().
%% @private

apply(
  Meta,
  #put{path = PathPattern, payload = Payload, options = TreeAndPutOptions},
  State) ->
    {TreeOptions, PutOptions} = split_put_options(TreeAndPutOptions),
    Ret = insert_or_update_node(
            State, PathPattern, Payload, PutOptions, TreeOptions, []),
    post_apply(Ret, Meta);
apply(
  Meta,
  #delete{path = PathPattern, options = TreeOptions},
  State) ->
    Ret = delete_matching_nodes(State, PathPattern, TreeOptions, []),
    post_apply(Ret, Meta);
apply(
  Meta,
  #tx{'fun' = StandaloneFun, args = Args},
  State) when ?IS_HORUS_FUN(StandaloneFun) ->
    Ret = khepri_tx_adv:run(State, StandaloneFun, Args, true, Meta),
    post_apply(Ret, Meta);
apply(
  Meta,
  #tx{'fun' = PathPattern, args = Args},
  State) when ?IS_KHEPRI_PATH_PATTERN(PathPattern) ->
    Ret = locate_sproc_and_execute_tx(State, PathPattern, Args, true, Meta),
    post_apply(Ret, Meta);
apply(
  Meta,
  #register_trigger{id = TriggerId,
                    sproc = StoredProcPath,
                    event_filter = EventFilter},
  State) ->
    Triggers = get_triggers(State),
    StoredProcPath1 = khepri_path:realpath(StoredProcPath),
    EventFilter1 = case EventFilter of
                       #evf_tree{path = Path} ->
                           Path1 = khepri_path:realpath(Path),
                           EventFilter#evf_tree{path = Path1}
                   end,
    Triggers1 = Triggers#{TriggerId => #{sproc => StoredProcPath1,
                                         event_filter => EventFilter1}},
    State1 = set_triggers(State, Triggers1),
    Ret = {State1, ok},
    post_apply(Ret, Meta);
apply(
  Meta,
  #ack_triggered{triggered = ProcessedTriggers},
  State) ->
    EmittedTriggers = get_emitted_triggers(State),
    EmittedTriggers1 = EmittedTriggers -- ProcessedTriggers,
    State1 = set_emitted_triggers(State, EmittedTriggers1),
    Ret = {State1, ok},
    post_apply(Ret, Meta);
apply(
  Meta,
  #register_projection{pattern = PathPattern, projection = Projection},
  State) ->
    ProjectionName = khepri_projection:name(Projection),
    ProjectionTree = get_projections(State),
    case has_projection(ProjectionTree, ProjectionName) of
        true ->
            Info = #{name => ProjectionName},
            Reason = ?khepri_error(projection_already_exists, Info),
            Reply = {error, Reason},
            Ret = {State, Reply},
            post_apply(Ret, Meta);
        false ->
            ProjectionTree1 = khepri_pattern_tree:update(
                                ProjectionTree,
                                PathPattern,
                                fun (?NO_PAYLOAD) ->
                                        [Projection];
                                    (Projections) ->
                                        [Projection | Projections]
                                end),
            %% The new projection has been registered so the cached compiled
            %% projection tree needs to be erased.
            clear_compiled_projection_tree(),
            State1 = set_projections(State, ProjectionTree1),
            AuxEffect = #restore_projection{projection = Projection,
                                            pattern = PathPattern},
            Effects = [{aux, AuxEffect}],
            Ret = {State1, ok, Effects},
            post_apply(Ret, Meta)
    end;
apply(
  Meta,
  #unregister_projections{names = Names},
  State) ->
    RemoveProjection = case Names of
                           all ->
                               fun(_Name) -> true end;
                           _ when is_list(Names) ->
                               Names1 = sets:from_list(Names, [{version, 2}]),
                               fun(Name) -> sets:is_element(Name, Names1) end
                       end,
    ProjectionTree = get_projections(State),
    {ProjectionTree1, RemovedProjectionsMap} =
    khepri_pattern_tree:map_fold(
      fun(Pattern, Projections, Acc) ->
              {RemovedProjections, Projections1} =
              lists:partition(
                fun(Projection) ->
                        Name = khepri_projection:name(Projection),
                        RemoveProjection(Name)
                end, Projections),
              Acc2 = lists:foldl(
                       fun(Projection, Acc1) ->
                               Name = khepri_projection:name(Projection),
                               _ = khepri_projection:delete(Projection),
                               maps:put(Name, Pattern, Acc1)
                       end, Acc, RemovedProjections),
              Projections2 = case Projections1 of
                                 [] ->
                                     ?NO_PAYLOAD;
                                 _ ->
                                     Projections1
                             end,
              {Projections2, Acc2}
      end, #{}, ProjectionTree),
    State1 = set_projections(State, ProjectionTree1),
    clear_compiled_projection_tree(),
    Reply = {ok, RemovedProjectionsMap},
    Ret = {State1, Reply},
    post_apply(Ret, Meta);
apply(
  Meta,
  #unregister_projection{name = Name},
  State) ->
    %% This command was replaced by `#unregister_projections{}'. Therefore,
    %% convert it and recurse.
    %%
    %% For backward-compatibility; see {@link old_command()}.
    NewCommand = #unregister_projections{names = [Name]},
    apply(Meta, NewCommand, State);
apply(
  #{machine_version := MacVer} = Meta,
  #dedup{ref = CommandRef, expiry = Expiry, command = Command},
  State)
  when is_reference(CommandRef) andalso
       is_integer(Expiry) andalso
       MacVer >= 1 ->
    Dedups = get_dedups(State),
    case Dedups of
        #{CommandRef := {Reply, _Expiry}} ->
            Ret = {State, Reply},
            post_apply(Ret, Meta);
        _ ->
            {State1, Reply, SideEffects} = apply(Meta, Command, State),
            Dedups1 = Dedups#{CommandRef => {Reply, Expiry}},
            State2 = set_dedups(State1, Dedups1),
            {State2, Reply, SideEffects}
    end;
apply(
  #{machine_version := MacVer} = Meta,
  #dedup_ack{ref = CommandRef},
  State)
  when is_reference(CommandRef) andalso
       MacVer >= 1 ->
    Dedups = get_dedups(State),
    State1 = case Dedups of
                 #{CommandRef := _} ->
                     Dedups1 = maps:remove(CommandRef, Dedups),
                     set_dedups(State, Dedups1);
                 _ ->
                     State
             end,
    Ret = {State1, ok},
    post_apply(Ret, Meta);
apply(
  #{machine_version := MacVer} = Meta,
  #drop_dedups{refs = RefsToDrop},
  State) when MacVer >= 2 ->
    %% `#drop_dedups{}' is emitted by the `handle_aux/5' clause for the `tick'
    %% effect to periodically drop dedups that have expired. This expiration
    %% was originally done in `post_apply/2' via `drop_expired_dedups/2' until
    %% machine version 2. Note that `drop_expired_dedups/2' is used until a
    %% cluster reaches an effective machine version of 2 or higher.
    Dedups = get_dedups(State),
    Dedups1 = maps:without(RefsToDrop, Dedups),
    State1 = set_dedups(State, Dedups1),
    Ret = {State1, ok},
    post_apply(Ret, Meta);
apply(Meta, {machine_version, OldMacVer, NewMacVer}, OldState) ->
    NewState = convert_state(OldState, OldMacVer, NewMacVer),
    Ret = {NewState, ok},
    post_apply(Ret, Meta);
apply(#{machine_version := MacVer} = Meta, UnknownCommand, State) ->
    Error = ?khepri_exception(
               unknown_khepri_state_machine_command,
               #{command => UnknownCommand,
                 machine_version => MacVer}),
    Reply = {error, Error},
    SideEffects = [{mod_call, logger, error,
                    ["Unknown Khepri state machine command with machine "
                     "version ~b:~n~p",
                     [MacVer, UnknownCommand],
                     #{domain => [khepri, ra_machine],
                       mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
                       file => ?FILE,
                       line => ?LINE}]}],
    Ret = {State, Reply, SideEffects},
    post_apply(Ret, Meta).

-spec post_apply(ApplyRet, Meta) -> {State, Result, SideEffects} when
      ApplyRet :: {State, Result} | {State, Result, SideEffects},
      State :: state(),
      Result :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @private

post_apply({State, Result}, Meta) ->
    post_apply({State, Result, []}, Meta);
post_apply({_State, _Result, _SideEffects} = Ret, Meta) ->
    Ret1 = bump_applied_command_count(Ret, Meta),
    Ret2 = drop_expired_dedups(Ret1, Meta),
    Ret3 = trigger_delayed_aux_queries_eval(Ret2, Meta),
    Ret3.

-spec bump_applied_command_count(ApplyRet, Meta) ->
    {State, Result, SideEffects} when
      ApplyRet :: {State, Result, SideEffects},
      State :: state(),
      Result :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @private

bump_applied_command_count(
  {State, Result, SideEffects},
  #{index := RaftIndex}) ->
    #config{snapshot_interval = SnapshotInterval} = get_config(State),
    Metrics = get_metrics(State),
    AppliedCmdCount0 = maps:get(applied_command_count, Metrics, 0),
    AppliedCmdCount = AppliedCmdCount0 + 1,
    case AppliedCmdCount < SnapshotInterval of
        true ->
            Metrics1 = Metrics#{applied_command_count => AppliedCmdCount},
            State1 = set_metrics(State, Metrics1),
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

reset_applied_command_count(State) ->
    Metrics = get_metrics(State),
    Metrics1 = maps:remove(applied_command_count, Metrics),
    set_metrics(State, Metrics1).

-spec drop_expired_dedups(ApplyRet, Meta) ->
    {State, Result, SideEffects} when
      ApplyRet :: {State, Result, SideEffects},
      State :: state(),
      Result :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @doc Removes any dedups from the `dedups' field in state that have expired
%% according to the timestamp in the handled command.
%%
%% This function is a no-op in any other version than version 1. This proved to
%% be expensive to execute as part of `apply/3' so dedup expiration moved to
%% the `handle_aux/5' for `tick' which is executed periodically. See that
%% function clause above for more information.
%%
%% @private

drop_expired_dedups(
  {State, Result, SideEffects},
  #{system_time := Timestamp,
    machine_version := MacVer}) when MacVer =< 1 ->
    Dedups = get_dedups(State),
    %% Historical note: `maps:filter/2' can be surprisingly expensive when
    %% used in a tight loop like `apply/3' depending on how many elements are
    %% retained. As of Erlang/OTP 27, the BIF which implements `maps:filter/2'
    %% collects any key-value pairs for which the predicate returns `true' into
    %% a list, sorts/dedups the list and then creates a new map. This is slow
    %% if the filter function always returns `true'. In cases like this where
    %% the common usage is to retain most elements, `maps:fold/3' plus a `case'
    %% expression and `maps:remove/2' is likely to be less expensive.
    Dedups1 = maps:filter(
                fun(_CommandRef, {_Reply, Expiry}) ->
                        Expiry >= Timestamp
                end, Dedups),
    State1 = set_dedups(State, Dedups1),
    {State1, Result, SideEffects};
drop_expired_dedups({State, Result, SideEffects}, _Meta) ->
    %% No-op on versions 2 and higher.
    {State, Result, SideEffects}.

-spec trigger_delayed_aux_queries_eval(ApplyRet, Meta) ->
    {State, Result, SideEffects} when
      ApplyRet :: {State, Result, SideEffects},
      State :: state(),
      Result :: any(),
      Meta :: ra_machine:command_meta_data(),
      SideEffects :: ra_machine:effects().
%% @doc Add an `aux' side effect to retrigger the eval of delayed aux queries.
%%
%% @private

trigger_delayed_aux_queries_eval({State, Result, SideEffects}, _Meta) ->
    SideEffects1 = [{aux, trigger_delayed_aux_queries_eval} | SideEffects],
    {State, Result, SideEffects1}.

%% @private

state_enter(leader, State) ->
    SideEffects1 = emitted_triggers_to_side_effects(State),
    SideEffects1;
state_enter(recovered, _State) ->
    SideEffect = {aux, restore_projections},
    [SideEffect];
state_enter(_StateName, _State) ->
    [].

%% @private

snapshot_installed(
  #{machine_version := NewMacVer}, NewState,
  #{machine_version := OldMacVer}, OldState) ->
    %% A snapshot might be installed on a follower member who has fallen
    %% sufficiently far behind in replication of the log from the leader. When
    %% a member installs a snapshot it needs to update its projections: new
    %% projections may have been registered since the snapshot or old ones
    %% unregistered. Projections which did not change need to be triggered
    %% with the new changes to state, similar to the `restore_projections' aux
    %% effect. Also see `update_projections/2'.
    %%
    %% Note that the snapshot installation might bump the effective machine
    %% version so we need to convert the old state to the new machine version.
    OldState1 = convert_state(OldState, OldMacVer, NewMacVer),
    ok = update_projections(OldState1, NewState),
    ok = clear_compiled_projection_tree(),
    [].

%% @private

emitted_triggers_to_side_effects(State) ->
    #config{store_id = StoreId} = get_config(State),
    EmittedTriggers = get_emitted_triggers(State),
    case EmittedTriggers of
        [_ | _] ->
            SideEffect = {mod_call,
                          khepri_event_handler,
                          handle_triggered_sprocs,
                          [StoreId, EmittedTriggers]},
            [SideEffect];
        [] ->
            []
    end.

-spec overview(State) -> Overview when
      State :: khepri_machine:state(),
      Overview :: #{store_id := StoreId,
                    tree := NodeTree,
                    triggers := Triggers,
                    keep_while_conds := KeepWhileConds},
      StoreId :: khepri:store_id(),
      NodeTree :: khepri_utils:display_tree(),
      Triggers :: khepri_machine:triggers_map(),
      KeepWhileConds :: khepri_tree:keep_while_conds_map().
%% @private

overview(State) ->
    #config{store_id = StoreId} = get_config(State),
    Tree = get_tree(State),
    KeepWhileConds = get_keep_while_conds(State),
    Triggers = get_triggers(State),
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length],
                    include_root_props => true},
    {ok, NodePropsMap} = khepri_tree:find_matching_nodes(
                           Tree, [?KHEPRI_WILDCARD_STAR_STAR], TreeOptions),
    MapFun = fun
                 (#{sproc := Sproc} = Props) ->
                     Props#{sproc => horus:to_fun(Sproc)};
                 (Props) ->
                     Props
             end,
    NodeTree = khepri_utils:flat_struct_to_tree(NodePropsMap, MapFun),
    #{store_id => StoreId,
      tree => NodeTree,
      triggers => Triggers,
      keep_while_conds => KeepWhileConds}.

-spec version() -> MacVer when
      MacVer :: 2.
%% @doc Returns the state machine version.

version() ->
    2.

-spec which_module(MacVer) -> Module when
      MacVer :: 0..2,
      Module :: ?MODULE.
%% @doc Returns the state machine module corresponding to the given version.

which_module(2) -> ?MODULE;
which_module(1) -> ?MODULE;
which_module(0) -> ?MODULE.

-spec effective_version(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: khepri:ok(EffectiveMacVer) | khepri:error(),
      EffectiveMacVer :: ra_machine:version().
%% @doc Returns the effective state machine version of the local Ra server.

effective_version(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    ThisNode = node(),
    RaServer = khepri_cluster:node_to_member(StoreId, ThisNode),
    case ra_counters:counters(RaServer, [effective_machine_version]) of
        #{effective_machine_version := EffectiveMacVer} ->
            {ok, EffectiveMacVer};
        _ ->
            case ra:member_overview(RaServer) of
                {ok, #{effective_machine_version := EffectiveMacVer}, _} ->
                    {ok, EffectiveMacVer};
                {error, _} = Error ->
                    Reason = ?khepri_error(
                                effective_machine_version_not_defined,
                                #{store_id => StoreId,
                                  ra_server => RaServer,
                                  error => Error}),
                    {error, Reason}
            end
    end.

-spec does_api_comply_with(Behaviour, MacVer | StoreId) -> DoesUse when
      Behaviour :: khepri_machine:api_behaviour(),
      MacVer :: ra_machine:version(),
      StoreId :: khepri:store_id(),
      DoesUse :: boolean().
%% @doc Indicates if a new behaviour of the transaction API is activated.
%%
%% The transaction code is compiled on one Erlang node with a specific version
%% of Khepri. However, it is executed on all members of the Khepri cluster.
%% Some Erlang nodes might use another version of Khepri, newer or older, and
%% the transaction API may differ.
%%
%% For instance in Khepri 0.17.x, the return values of the {@link
%% khepri_tx_adv} functions changed. The transaction code will have to handle
%% both versions of the API to work correctly. Thus it can use this function
%% to adapt.
%%
%% @returns true if the given behaviour is activated, false if it is not or if
%% the behaviour is unknown.

does_api_comply_with(dedup_protection, MacVer)
  when is_integer(MacVer) ->
    MacVer >= 1;
does_api_comply_with(delete_reason_in_node_props, MacVer)
  when is_integer(MacVer) ->
    MacVer >= 2;
does_api_comply_with(indirect_deletes_in_ret, MacVer)
  when is_integer(MacVer) ->
    MacVer >= 2;
does_api_comply_with(uniform_write_ret, MacVer)
  when is_integer(MacVer) ->
    MacVer >= 2;
does_api_comply_with(_Behaviour, MacVer)
  when is_integer(MacVer) ->
    false;
does_api_comply_with(Behaviour, StoreId)
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    case effective_version(StoreId) of
        {ok, MacVer} -> does_api_comply_with(Behaviour, MacVer);
        _            -> false
    end.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

locate_sproc_and_execute_tx(State, PathPattern, Args, AllowUpdates, Meta) ->
    Tree = get_tree(State),
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [raw_payload]},
    {StandaloneFun, Args1} =
    case khepri_tree:find_matching_nodes(Tree, PathPattern, TreeOptions) of
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
    khepri_tx_adv:run(State, StandaloneFun, Args1, AllowUpdates, Meta).

-spec failed_to_locate_sproc(Reason) -> no_return() when
      Reason :: any().
%% @private

failed_to_locate_sproc(Reason) ->
    khepri_tx:abort(Reason).

-spec insert_or_update_node(
    State, PathPattern, Payload, PutOptions, TreeOptions, SideEffects) ->
    Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: khepri_payload:payload(),
      PutOptions :: khepri:put_options(),
      TreeOptions :: khepri:tree_options(),
      SideEffects :: ra_machine:effects(),
      Ret :: {State, Result, ra_machine:effects()},
      Result :: khepri_machine:write_ret().
%% @private

insert_or_update_node(
  State, PathPattern, Payload, PutOptions, TreeOptions, SideEffects) ->
    Tree = get_tree(State),
    Ret1 = khepri_tree:insert_or_update_node(
             Tree, PathPattern, Payload, PutOptions, TreeOptions),
    case Ret1 of
        {ok, Tree1, AppliedChanges, Ret2} ->
            State1 = set_tree(State, Tree1),
            {State2, SideEffects1} = add_tree_change_side_effects(
                                       State, State1, AppliedChanges,
                                       SideEffects),
            {State2, {ok, Ret2}, SideEffects1};
        Error ->
            {State, Error, SideEffects}
    end.

-spec delete_matching_nodes(State, PathPattern, TreeOptions, SideEffects) ->
    Ret when
      State :: state(),
      PathPattern :: khepri_path:native_pattern(),
      TreeOptions :: khepri:tree_options(),
      SideEffects :: ra_machine:effects(),
      Ret :: {State, Result, ra_machine:effects()},
      Result :: khepri_machine:write_ret().
%% @private

delete_matching_nodes(State, PathPattern, TreeOptions, SideEffects) ->
    Tree = get_tree(State),
    Ret = khepri_tree:delete_matching_nodes(
            Tree, PathPattern, #{}, TreeOptions),
    case Ret of
        {ok, Tree1, AppliedChanges, Ret2} ->
            State1 = set_tree(State, Tree1),
            {State2, SideEffects1} = add_tree_change_side_effects(
                                       State, State1, AppliedChanges,
                                       SideEffects),
            {State2, {ok, Ret2}, SideEffects1};
        Error ->
            {State, Error, SideEffects}
    end.

add_tree_change_side_effects(
  InitialState, NewState, AppliedChanges, SideEffects) ->
    %% We make a map where for each affected tree node, we indicate the type
    %% of change.
    Changes = maps:map(
                fun
                    (_, {Type, _NodeProps})
                      when Type =:= create orelse Type =:= update ->
                        Type;
                    (_, delete) ->
                        delete
                end, AppliedChanges),
    NewSideEffects = create_projection_side_effects(
                       InitialState, NewState, Changes),
    {NewState1, NewSideEffects1} = add_trigger_side_effects(
                                     InitialState, NewState, Changes,
                                     NewSideEffects),
    SideEffects1 = lists:reverse(NewSideEffects1, SideEffects),
    {NewState1, SideEffects1}.

create_projection_side_effects(InitialState, NewState, Changes) ->
    InitialTree = get_tree(InitialState),
    NewTree = get_tree(NewState),
    ProjectionTree0 = get_projections(NewState),
    ProjectionTree = get_compiled_projection_tree(ProjectionTree0),
    maps:fold(
      fun(Path, Change, Effects) ->
              create_projection_side_effects1(
                InitialTree, NewTree, ProjectionTree, Path, Change, Effects)
      end, [], Changes).

create_projection_side_effects1(
  InitialTree, NewTree, ProjectionTree, Path, delete = Change, Effects) ->
    %% Deletion changes recursively delete the subtree below the deleted tree
    %% node. Find any children in the tree that were also deleted by this
    %% change and trigger any necessary projections for those children.
    ChildrenFindOptions = #{props_to_return => ?PROJECTION_PROPS_TO_RETURN,
                            expect_specific_node => false},
    ChildrenPattern = Path ++ [?KHEPRI_WILDCARD_STAR_STAR],
    EffectsForChildrenFun =
    fun(ChildPath, _NodeProps, EffectAcc) ->
            create_projection_side_effects2(
              InitialTree, NewTree, ProjectionTree,
              ChildPath, Change, EffectAcc)
    end,
    {ok, Effects1} = khepri_tree:fold(
                       InitialTree, ChildrenPattern,
                       EffectsForChildrenFun, Effects,
                       ChildrenFindOptions),
    %% Also trigger a change for the deleted path itself.
    create_projection_side_effects2(
      InitialTree, NewTree, ProjectionTree, Path, Change, Effects1);
create_projection_side_effects1(
  InitialTree, NewTree, ProjectionTree, Path, Change, Effects) ->
    create_projection_side_effects2(
      InitialTree, NewTree, ProjectionTree, Path, Change, Effects).

create_projection_side_effects2(
  InitialTree, NewTree, ProjectionTree, Path, Change, Effects) ->
    PatternMatchingTree = case Change of
                              create ->
                                  NewTree;
                              update ->
                                  NewTree;
                              delete ->
                                  InitialTree
                          end,
    khepri_pattern_tree:fold_matching(
      ProjectionTree,
      PatternMatchingTree,
      Path,
      fun(_PathPattern, Projections, Effects1) ->
              lists:foldl(
                fun(Projection, Effects2) ->
                        evaluate_projection(
                          InitialTree, NewTree, Path, Projection, Effects2)
                end, Effects1, Projections)
      end,
      Effects).

-spec evaluate_projection(InitialTree, NewTree, Path, Projection, Effects) ->
    Ret when
      InitialTree :: khepri_tree:tree(),
      NewTree :: khepri_tree:tree(),
      Path :: khepri_path:native_path(),
      Projection :: khepri_projection:projection(),
      Effects :: ra_machine:effects(),
      Ret :: ra_machine:effects().
%% @private

evaluate_projection(
  InitialTree, NewTree, Path, Projection, Effects) ->
    FindOptions = #{props_to_return => ?PROJECTION_PROPS_TO_RETURN,
                    expect_specific_node => true},
    InitialRet = khepri_tree:find_matching_nodes(
                   InitialTree, Path, FindOptions),
    InitialProps = case InitialRet of
                       {ok, #{Path := InitialProps0}} ->
                           InitialProps0;
                       _ ->
                           #{}
                   end,
    NewRet = khepri_tree:find_matching_nodes(
               NewTree, Path, FindOptions),
    NewProps = case NewRet of
                     {ok, #{Path := NewProps0}} ->
                         NewProps0;
                     _ ->
                         #{}
                 end,
    Trigger = #trigger_projection{path = Path,
                                  old_props = InitialProps,
                                  new_props = NewProps,
                                  projection = Projection},
    Effect = {aux, Trigger},
    [Effect | Effects].

add_trigger_side_effects(InitialState, NewState, Changes, SideEffects) ->
    %% We want to consider the new state (with the updated tree), but we want
    %% to use triggers from the initial state, in case they were updated too.
    %% In other words, we want to evaluate triggers in the state they were at
    %% the time the change to the tree was requested.
    Triggers = get_triggers(InitialState),
    case Triggers =:= #{} of
        true ->
            {NewState, SideEffects};
        false ->
            EmittedTriggers = get_emitted_triggers(InitialState),
            #config{store_id = StoreId} = get_config(NewState),
            Tree = get_tree(NewState),
            TriggeredStoredProcs = list_triggered_sprocs(
                                     Tree, Changes, Triggers),

            %% We record the list of triggered stored procedures in the state
            %% machine's state. This is used to guaranty at-least-once
            %% execution of the trigger: the event handler process is supposed
            %% to ack when it executed the triggered stored procedure. If the
            %% Ra cluster changes leader in between, we know that we need to
            %% retry the execution.
            %%
            %% This could lead to multiple execution of the same trigger,
            %% therefore the stored procedure must be idempotent.
            NewState1 = set_emitted_triggers(
                          NewState, EmittedTriggers ++ TriggeredStoredProcs),

            %% We still emit a `mod_call' effect to wake up the event handler
            %% process so it doesn't have to poll the internal list.
            SideEffect = {mod_call,
                          khepri_event_handler,
                          handle_triggered_sprocs,
                          [StoreId, TriggeredStoredProcs]},
            {NewState1, [SideEffect | SideEffects]}
    end.

list_triggered_sprocs(Tree, Changes, Triggers) ->
    TriggeredStoredProcs =
    maps:fold(
      fun(Path, Change, TSP) ->
              % For each change, we evaluate each trigger.
              maps:fold(
                fun(TriggerId, TriggerProps, TSP1) ->
                        evaluate_trigger(
                          Tree, Path, Change, TriggerId, TriggerProps, TSP1)
                end, TSP, Triggers)
      end, [], Changes),
    sort_triggered_sprocs(TriggeredStoredProcs).

evaluate_trigger(
  Tree, Path, Change, TriggerId,
  #{sproc := StoredProcPath,
    event_filter := #evf_tree{path = PathPattern,
                              props = EventFilterProps} = EventFilter},
  TriggeredStoredProcs) ->
    %% For each trigger based on a tree event:
    %%   1. we verify the path of the changed tree node matches the monitored
    %%      path pattern in the event filter.
    %%   2. we verify the type of change matches the change filter in the
    %%      event filter.
    PathMatches = khepri_tree:does_path_match(Path, PathPattern, Tree),
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
            case find_stored_proc(Tree, StoredProcPath) of
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
                                   props = EventProps},
                    [Triggered | TriggeredStoredProcs]
            end;
        false ->
            TriggeredStoredProcs
    end;
evaluate_trigger(
  _Root, _Path, _Change, _TriggerId, _TriggerProps, TriggeredStoredProcs) ->
    TriggeredStoredProcs.

find_stored_proc(Tree, StoredProcPath) ->
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    Ret = khepri_tree:find_matching_nodes(
            Tree, StoredProcPath, TreeOptions),
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

-spec get_compiled_projection_tree(ProjectionTree) -> CompiledProjectionTree
    when
      ProjectionTree :: khepri_machine:projection_tree(),
      CompiledProjectionTree :: khepri_machine:projection_tree().
%% @doc Gets the compiled version of the projection pattern tree.
%%
%% The pattern tree for projections must be compiled before it can be queried
%% into for changes to the store via {@link khepri_pattern_tree:fold/5}.
%%
%% This compiled pattern tree is cached in the process dictionary for the Ra
%% server process. If the cached value does not exist, `SourceProjectionTree'
%% is compiled with {@link khepri_pattern_tree:compile/1} and stored for
%% future lookups.
%%
%% @private

get_compiled_projection_tree(SourceProjectionTree) ->
    case get(compiled_projection_tree) of
        undefined ->
            CompiledProjectionTree = khepri_pattern_tree:compile(
                                       SourceProjectionTree),
            put(compiled_projection_tree, CompiledProjectionTree),
            CompiledProjectionTree;
        CompiledProjectionTree ->
            CompiledProjectionTree
    end.

-spec clear_compiled_projection_tree() -> ok.
%% @doc Erases the cached projection tree.
%%
%% This function should be called whenever the projection tree is changed:
%% whenever a projection is registered or unregistered.
%%
%% @see get_compiled_projection_tree/1.
%%
%% @private

clear_compiled_projection_tree() ->
    erase(compiled_projection_tree),
    ok.

%% -------------------------------------------------------------------
%% State record management functions.
%% -------------------------------------------------------------------

-spec is_state(State) -> IsState when
      State :: khepri_machine:state(),
      IsState :: boolean().
%% @doc Tells if the given argument is a valid state.
%%
%% @private

is_state(State) ->
    is_record(State, khepri_machine) orelse khepri_machine_v0:is_state(State).

-spec ensure_is_state(State) -> ok when
      State :: khepri_machine:state().
%% @doc Throws an exception if the given argument is not a valid state.
%%
%% @private

ensure_is_state(State) ->
    ?assert(is_state(State)),
    ok.

-spec get_config(State) -> Config when
      State :: khepri_machine:state(),
      Config :: khepri_machine:machine_config().
%% @doc Returns the config from the given state.
%%
%% @private

get_config(#khepri_machine{config = Config}) ->
    Config;
get_config(State) ->
    khepri_machine_v0:get_config(State).

-spec get_tree(State) -> Tree when
      State :: khepri_machine:state(),
      Tree :: khepri_tree:tree().
%% @doc Returns the tree from the given state.
%%
%% @private

get_tree(#khepri_machine{tree = Tree}) ->
    Tree;
get_tree(State) ->
    khepri_machine_v0:get_tree(State).

-spec set_tree(State, Tree) -> NewState when
      State :: khepri_machine:state(),
      Tree :: khepri_tree:tree(),
      NewState :: khepri_machine:state().
%% @doc Sets the tree in the given state.
%%
%% @private

set_tree(#khepri_machine{} = State, Tree) ->
    State#khepri_machine{tree = Tree};
set_tree(State, Tree) ->
    khepri_machine_v0:set_tree(State, Tree).

-spec get_root(State) -> Root when
      State :: khepri_machine:state(),
      Root :: khepri_tree:tree_node().
%% @doc Returns the root of the tree from the given state.
%%
%% @private

get_root(State) ->
    Tree = get_tree(State),
    Root = khepri_tree:get_root(Tree),
    Root.

-spec get_keep_while_conds(State) -> KeepWhileConds when
      State :: khepri_machine:state(),
      KeepWhileConds :: khepri_tree:keep_while_conds_map().
%% @doc Returns the `keep_while' conditions in the tree from the given state.
%%
%% @private

get_keep_while_conds(State) ->
    Tree = get_tree(State),
    KeepWhileConds = khepri_tree:get_keep_while_conds(Tree),
    KeepWhileConds.

-spec get_triggers(State) -> Triggers when
      State :: khepri_machine:state(),
      Triggers :: khepri_machine:triggers_map().
%% @doc Returns the triggers from the given state.
%%
%% @private

get_triggers(#khepri_machine{triggers = Triggers}) ->
    Triggers;
get_triggers(State) ->
    khepri_machine_v0:get_triggers(State).

-spec set_triggers(State, Triggers) -> NewState when
      State :: khepri_machine:state(),
      Triggers :: khepri_machine:triggers_map(),
      NewState :: khepri_machine:state().
%% @doc Sets the triggers in the given state.
%%
%% @private

set_triggers(#khepri_machine{} = State, Triggers) ->
    State#khepri_machine{triggers = Triggers};
set_triggers(State, Triggers) ->
    khepri_machine_v0:set_triggers(State, Triggers).

-spec get_emitted_triggers(State) -> EmittedTriggers when
      State :: khepri_machine:state(),
      EmittedTriggers :: [khepri_machine:triggered()].
%% @doc Returns the emitted_triggers from the given state.
%%
%% @private

get_emitted_triggers(#khepri_machine{emitted_triggers = EmittedTriggers}) ->
    EmittedTriggers;
get_emitted_triggers(State) ->
    khepri_machine_v0:get_emitted_triggers(State).

-spec set_emitted_triggers(State, EmittedTriggers) -> NewState when
      State :: khepri_machine:state(),
      EmittedTriggers :: [khepri_machine:triggered()],
      NewState :: khepri_machine:state().
%% @doc Sets the emitted_triggers in the given state.
%%
%% @private

set_emitted_triggers(#khepri_machine{} = State, EmittedTriggers) ->
    State#khepri_machine{emitted_triggers = EmittedTriggers};
set_emitted_triggers(State, EmittedTriggers) ->
    khepri_machine_v0:set_emitted_triggers(State, EmittedTriggers).

-spec get_projections(State) -> Projections when
      State :: khepri_machine:state(),
      Projections :: khepri_machine:projection_tree().
%% @doc Returns the projections from the given state.
%%
%% @private

get_projections(#khepri_machine{projections = Projections}) ->
    Projections;
get_projections(State) ->
    khepri_machine_v0:get_projections(State).

-spec has_projection(ProjectionTree, ProjectionName) -> boolean() when
      ProjectionTree :: khepri_machine:projection_tree(),
      ProjectionName :: khepri_projection:name().
%% @doc Determines if the given projection tree contains a projection.
%%
%% Two projections are considered equal if they have the same name.
%%
%% @private

has_projection(ProjectionTree, Name) when is_atom(Name) ->
    khepri_pattern_tree:any(
      ProjectionTree,
      fun(Projections) ->
              lists:any(
                fun(#khepri_projection{name = N}) ->
                        N =:= Name
                end, Projections)
      end).

-spec set_projections(State, Projections) -> NewState when
      State :: khepri_machine:state(),
      Projections :: khepri_machine:projection_tree(),
      NewState :: khepri_machine:state().
%% @doc Sets the projections in the given state.
%%
%% @private

set_projections(#khepri_machine{} = State, Projections) ->
    State#khepri_machine{projections = Projections};
set_projections(State, Projections) ->
    khepri_machine_v0:set_projections(State, Projections).

-spec get_metrics(State) -> Metrics when
      State :: khepri_machine:state(),
      Metrics :: khepri_machine:metrics().
%% @doc Returns the metrics from the given state.
%%
%% @private

get_metrics(#khepri_machine{metrics = Metrics}) ->
    Metrics;
get_metrics(State) ->
    khepri_machine_v0:get_metrics(State).

-spec set_metrics(State, Metrics) -> NewState when
      State :: khepri_machine:state(),
      Metrics :: khepri_machine:metrics(),
      NewState :: khepri_machine:state().
%% @doc Sets the metrics in the given state.
%%
%% @private

set_metrics(#khepri_machine{} = State, Metrics) ->
    State#khepri_machine{metrics = Metrics};
set_metrics(State, Metrics) ->
    khepri_machine_v0:set_metrics(State, Metrics).

-spec get_dedups(State) -> Dedups when
      State :: khepri_machine:state(),
      Dedups :: khepri_machine:dedups_map().
%% @doc Returns the dedups from the given state.
%%
%% @private

get_dedups(#khepri_machine{dedups = Dedups}) ->
    Dedups;
get_dedups(_State) ->
    #{}.

-spec set_dedups(State, Dedups) -> NewState when
      State :: khepri_machine:state(),
      Dedups :: khepri_machine:dedups_map(),
      NewState :: khepri_machine:state().
%% @doc Sets the dedups in the given state.
%%
%% @private

set_dedups(#khepri_machine{} = State, Dedups) ->
    State#khepri_machine{dedups = Dedups};
set_dedups(State, _Dedups) ->
    State.

-spec get_store_id(State) -> StoreId when
      State :: khepri_machine:state(),
      StoreId :: khepri:store_id().
%% @doc Returns the store ID from the given state.
%%
%% @private

get_store_id(State) ->
    #config{store_id = StoreId} = get_config(State),
    StoreId.

-spec assert_equal(State1, State2) -> ok when
      State1 :: khepri_machine:state(),
      State2 :: khepri_machine:state().

assert_equal(#khepri_machine{} = State1, #khepri_machine{} = State2) ->
    ?assertEqual(State1, State2),
    ok;
assert_equal(State1, State2) ->
    khepri_machine_v0:assert_equal(State1, State2).

-ifdef(TEST).
-spec make_virgin_state(Params) -> State when
      Params :: khepri_machine:machine_init_args(),
      State :: khepri_machine_v0:state().

make_virgin_state(Params) ->
    khepri_machine_v0:init(Params).
-endif.

-spec convert_state(OldState, OldMacVer, NewMacVer) -> NewState when
      OldState :: khepri_machine:state(),
      OldMacVer :: ra_machine:version(),
      NewMacVer :: ra_machine:version(),
      NewState :: khepri_machine:state().
%% @doc Converts a state to a newer version.
%%
%% @private

convert_state(State, OldMacVer, NewMacVer) ->
    lists:foldl(
      fun(N, State1) ->
              OldMacVer1 = N,
              NewMacVer1 = erlang:min(N + 1, NewMacVer),
              convert_state1(State1, OldMacVer1, NewMacVer1)
      end, State, lists:seq(OldMacVer, NewMacVer)).

convert_state1(State, MacVer, MacVer) ->
    State;
convert_state1(State, 0, 1) ->
    %% To go from version 0 to version 1, we add the `dedups' fields at the
    %% end of the record. The default value is an empty map.
    ?assert(khepri_machine_v0:is_state(State)),
    Fields0 = khepri_machine_v0:state_to_list(State),
    Fields1 = Fields0 ++ [#{}],
    State1 = list_to_tuple(Fields1),
    ?assert(is_state(State1)),
    State1;
convert_state1(State, 1, 2) ->
    Tree = get_tree(State),
    Tree1 = khepri_tree:convert_tree(Tree, 1, 2),
    set_tree(State, Tree1).

-spec update_projections(OldState, NewState) -> ok when
      OldState :: khepri_machine:state(),
      NewState :: khepri_machine:state().
%% @doc Updates the machine's projections to account for changes between two
%% states.
%%
%% This is used when installing a projection - the state will jump from the
%% given `OldState' before the snapshot was installed to the given `NewState'
%% after. When we swap states we need to update the projections: the records
%% in the projection tables themselves but also which projection tables exist.
%% The changes glossed over by the snapshot may include projection
%% registrations and unregistrations so we need to initialize new projections
%% and delete unregistered ones, and we need to ensure that the projection
%% tables are up to date for any projections which didn't change.
%%
%% @private

update_projections(OldState, NewState) ->
    OldTree = get_tree(OldState),
    OldProjections = set_of_projections(get_projections(OldState)),
    NewTree = get_tree(NewState),
    NewProjections = set_of_projections(get_projections(NewState)),

    CommonProjections = sets:intersection(OldProjections, NewProjections),
    DeletedProjections = sets:subtract(OldProjections, CommonProjections),
    CreatedProjections = sets:subtract(NewProjections, CommonProjections),

    %% Tear down any projections which were unregistered.
    sets:fold(
      fun({_Pattern, Projection}, _Acc) ->
              _ = khepri_projection:delete(Projection),
              ok
      end, ok, DeletedProjections),

    %% Initialize any new projections which were registered.
    sets:fold(
      fun({Pattern, Projection}, _Acc) ->
              ok = restore_projection(Projection, NewTree, Pattern)
      end, ok, CreatedProjections),

    %% Update in-place any projections which were not changed themselves (i.e.
    %% the projection name, function and pattern) between old and new states.
    %% To do this we will find the matching nodes in the old and new tree for
    %% the projection's pattern and trigger the projection based on each
    %% matching path's old and new properties.
    sets:fold(
      fun({Pattern, Projection}, _Acc) ->
              ok = update_projection(Pattern, Projection, OldTree, NewTree)
      end, ok, CommonProjections),

    ok.

-spec set_of_projections(ProjectionTree) -> Projections when
      ProjectionTree :: khepri_machine:projection_tree(),
      Element :: {khepri_path:native_pattern(),
                     khepri_projection:projection()},
      Projections :: sets:set(Element).
%% Folds the set of projections in a projection tree into a version 2 {@link
%% sets:set()}.
%%
%% @private

set_of_projections(ProjectionTree) ->
    khepri_pattern_tree:fold(
      ProjectionTree,
      fun(Pattern, Projections, Acc) ->
              lists:foldl(
                fun(Projection, Acc1) ->
                        Entry = {Pattern, Projection},
                        sets:add_element(Entry, Acc1)
                end, Acc, Projections)
      end, sets:new([{version, 2}])).

update_projection(Pattern, Projection, OldTree, NewTree) ->
    TreeOptions = #{props_to_return => ?PROJECTION_PROPS_TO_RETURN,
                    include_root_props => true},
    case khepri_tree:find_matching_nodes(OldTree, Pattern, TreeOptions) of
        {ok, OldMatchingNodes} ->
            Result = khepri_tree:find_matching_nodes(
                       NewTree, Pattern, TreeOptions),
            case Result of
                {ok, NewMatchingNodes} ->
                    Updates = diff_matching_nodes(
                                OldMatchingNodes, NewMatchingNodes),
                    maps:foreach(
                      fun(Path, {OldProps, NewProps}) ->
                              khepri_projection:trigger(
                                Projection, Path, OldProps, NewProps)
                      end, Updates);
                Error ->
                    ?LOG_DEBUG(
                       "Failed to refresh projection ~s due to an error "
                       "finding matching nodes in the new tree: ~p",
                       [khepri_projection:name(Projection), Error],
                       #{domain => [khepri, ra_machine]})
            end;
        Error ->
            ?LOG_DEBUG(
               "Failed to refresh projection ~s due to an error finding "
               "matching nodes in the old tree: ~p",
               [khepri_projection:name(Projection), Error],
               #{domain => [khepri, ra_machine]})
    end.

-spec diff_matching_nodes(OldNodeProps, NewNodeProps) -> Changes when
      OldNodeProps :: khepri:node_props_map(),
      NewNodeProps :: khepri:node_props_map(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Changes :: #{khepri_path:native_path() => {OldProps, NewProps}}.
%% @private

diff_matching_nodes(OldNodeProps, NewNodeProps) ->
    CommonProps = maps:intersect_with(
                    fun(_Path, OldProps, NewProps) -> {OldProps, NewProps} end,
                    OldNodeProps, NewNodeProps),
    CommonPaths = maps:keys(CommonProps),
    AllProps = maps:fold(
                 fun(Path, OldProps, Acc) ->
                        Acc#{Path => {OldProps, #{}}}
                 end, CommonProps, maps:without(CommonPaths, OldNodeProps)),
    maps:fold(
      fun(Path, NewProps, Acc) ->
              Acc#{Path => {#{}, NewProps}}
      end, AllProps, maps:without(CommonPaths, NewNodeProps)).
