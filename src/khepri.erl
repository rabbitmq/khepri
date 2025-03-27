%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri database API.
%%
%% This module exposes the database API to manipulate data.
%%
%% The API is mainly made of the functions used to perform simple direct atomic
%% operations and queries on the database: {@link get/1}, {@link put/2}, {@link
%% delete/1} and so on. In addition to that, {@link transaction/1} is the
%% starting point to run transaction functions. However the API to use inside
%% transaction functions is provided by {@link khepri_tx}.
%%
%% Functions in this module have simplified return values to cover most
%% frequent use cases. If you need more details about the queried or modified
%% tree nodes, like the ability to distinguish a non-existent tree node from a
%% tree node with no payload, you can use the {@link khepri_adv} module.
%%
%% This module also provides functions to start and stop a simple unclustered
%% Khepri store. For more advanced setup and clustering, see {@link
%% khepri_cluster}.
%%
%% == A Khepri store ==
%%
%% A Khepri store is one instance of Khepri running inside a Ra cluster (which
%% could be made of a single Erlang node). It is possible to run multiple
%% Khepri stores in parallel by creating multiple Ra clusters.
%%
%% A Khepri store is started and configured with {@link start/0}, {@link
%% start/1} or {@link start/3}. To setup a cluster, see {@link
%% khepri_cluster}.
%%
%% When a store is started, a store ID {@link store_id()} is returned. This
%% store ID is then used by the rest of this module's API. The returned store
%% ID currently corresponds exactly to the Ra cluster name. It must be an atom
%% though; other types are unsupported.
%%
%% == Interacting with the Khepri store ==
%%
%% The API provides two ways to interact with a Khepri store:
%% <ul>
%% <li>Direct atomic function for simple operations</li>
%% <li>Transactions for more complex operations</li>
%% </ul>
%%
%% Simple operations are calls like:
%% <ul>
%% <li>Queries: {@link get/1}, {@link exists/1}, {@link has_data/1}, etc.</li>
%% <li>Updates: {@link put/2}, {@link delete/1}, etc.</li>
%% </ul>
%%
%% Transactions are like Mnesia ones. The caller passes an anonymous function
%% to {@link transaction/1}, etc.:
%% ```
%% khepri:transaction(
%%   fun() ->
%%       khepri_tx:put(Path, Value)
%%   end).
%% '''
%%
%% Simple operations are more efficient than transactions, but transactions are
%% more flexible.

-module(khepri).

-include_lib("kernel/include/logger.hrl").

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_ret.hrl").

-export([
         %% Functions to start & stop a Khepri store; for more
         %% advanced functions, including clustering, see `khepri_cluster'.
         start/0, start/1, start/2, start/3,
         reset/0, reset/1, reset/2,
         stop/0, stop/1,
         get_store_ids/0,
         is_empty/0, is_empty/1, is_empty/2,

         %% Simple direct atomic operations & queries.
         get/1, get/2, get/3,
         get_or/2, get_or/3, get_or/4,
         get_many/1, get_many/2, get_many/3,
         get_many_or/2, get_many_or/3, get_many_or/4,

         exists/1, exists/2, exists/3,
         has_data/1, has_data/2, has_data/3,
         is_sproc/1, is_sproc/2, is_sproc/3,

         count/1, count/2, count/3,

         fold/3, fold/4, fold/5,
         foreach/2, foreach/3, foreach/4,
         map/2, map/3, map/4,
         filter/2, filter/3, filter/4,

         run_sproc/2, run_sproc/3, run_sproc/4,

         put/2, put/3, put/4,
         put_many/2, put_many/3, put_many/4,
         create/2, create/3, create/4,
         update/2, update/3, update/4,
         compare_and_swap/3, compare_and_swap/4, compare_and_swap/5,

         delete/1, delete/2, delete/3,
         delete_many/1, delete_many/2, delete_many/3,
         clear_payload/1, clear_payload/2, clear_payload/3,
         clear_many_payloads/1, clear_many_payloads/2,
         clear_many_payloads/3,

         register_trigger/3, register_trigger/4, register_trigger/5,

         register_projection/2, register_projection/3, register_projection/4,
         unregister_projections/1, unregister_projections/2,
         unregister_projections/3,
         has_projection/1, has_projection/2, has_projection/3,

         %% Transactions; `khepri_tx' provides the API to use inside
         %% transaction functions.
         transaction/1, transaction/2, transaction/3, transaction/4,
         transaction/5,

         fence/0, fence/1, fence/2,

         handle_async_ret/1, handle_async_ret/2,

         %% Bang functions: they return the value directly or throw an error.
         'get!'/1, 'get!'/2, 'get!'/3,
         'get_or!'/2, 'get_or!'/3, 'get_or!'/4,
         'get_many!'/1, 'get_many!'/2, 'get_many!'/3,
         'get_many_or!'/2, 'get_many_or!'/3, 'get_many_or!'/4,
         'exists!'/1, 'exists!'/2, 'exists!'/3,
         'has_data!'/1, 'has_data!'/2, 'has_data!'/3,
         'is_sproc!'/1, 'is_sproc!'/2, 'is_sproc!'/3,
         'count!'/1, 'count!'/2, 'count!'/3,
         'put!'/2, 'put!'/3, 'put!'/4,
         'put_many!'/2, 'put_many!'/3, 'put_many!'/4,
         'create!'/2, 'create!'/3, 'create!'/4,
         'update!'/2, 'update!'/3, 'update!'/4,
         'compare_and_swap!'/3, 'compare_and_swap!'/4, 'compare_and_swap!'/5,
         'delete!'/1, 'delete!'/2, 'delete!'/3,
         'delete_many!'/1, 'delete_many!'/2, 'delete_many!'/3,
         'clear_payload!'/1, 'clear_payload!'/2, 'clear_payload!'/3,
         'clear_many_payloads!'/1, 'clear_many_payloads!'/2,
         'clear_many_payloads!'/3,

         export/2, export/3, export/4,
         import/2, import/3,

         info/0,
         info/1, info/2]).

-compile({no_auto_import, [get/1, get/2, put/2, erase/1]}).

%% FIXME: Dialyzer complains about several functions with "optional" arguments
%% (but not all). I believe the specs are correct, but can't figure out how to
%% please Dialyzer. So for now, let's disable this specific check for the
%% problematic functions.
-dialyzer({no_underspecs, [start/1, start/2,
                           stop/0, stop/1,

                           put/2, put/3,
                           create/2, create/3,
                           update/2, update/3,
                           compare_and_swap/3, compare_and_swap/4,
                           exists/2,
                           has_data/2,
                           is_sproc/2,
                           run_sproc/3,
                           transaction/2, transaction/3,

                           unwrap_result/1]}).

-type store_id() :: atom().
%% ID of a Khepri store.
%%
%% This is the same as the Ra cluster name hosting the Khepri store.

-type data() :: any().
%% Data stored in a tree node's payload.

-type payload_version() :: pos_integer().
%% Number of changes made to the payload of a tree node.
%%
%% The payload version starts at 1 when a tree node is created. It is increased
%% by 1 each time the payload is added, modified or removed.

-type child_list_version() :: pos_integer().
%% Number of changes made to the list of child nodes of a tree node (child
%% nodes added or removed).
%%
%% The child list version starts at 1 when a tree node is created. It is
%% increased by 1 each time a child is added or removed. Changes made to
%% existing nodes are not reflected in this version.

-type child_list_length() :: non_neg_integer().
%% Number of direct child nodes under a tree node.

-type delete_reason() :: explicit | keep_while.
%% The reason why a tree node was removed from the store.
%%
%% <ul>
%% <li>`explicit' means that the tree node was removed from the store because a
%% deletion command targeted that tree node or its ancestor. This reason is
%% used when a tree node's path is provided to {@link khepri_adv:delete/3} for
%% example.</li>
%% <li>`keep_while': the tree node was removed because the keep-while condition
%% set when the node was created became unsatisfied. Note that adding or
%% updating a tree node can cause a keep-while condition to become unsatisfied,
%% so a put operation may result in a tree node being deleted with this delete
%% reason. See {@link khepri_condition:keep_while()} and {@link
%% khepri:put_options()}.</li>
%% </ul>

-type node_props() ::
    #{data => khepri:data(),
      has_data => boolean(),
      sproc => horus:horus_fun(),
      is_sproc => boolean(),
      payload_version => khepri:payload_version(),
      child_list_version => khepri:child_list_version(),
      child_list_length => khepri:child_list_length(),
      child_names => [khepri_path:node_id()],
      delete_reason => khepri:delete_reason()}.
%% Structure used to return properties, payload and child nodes for a specific
%% tree node.
%%
%% The payload in `data' or `sproc' is only returned if the tree node carries
%% something. If that key is missing from the returned properties map, it means
%% the tree node has no payload.
%%
%% By default, the payload (if any) and its version are returned by functions
%% exposed by {@link khepri_adv}. The list of returned properties can be
%% configured using the `props_to_return' option (see {@link tree_options()}).

-type node_props_map() :: #{khepri_path:native_path() => khepri:node_props()}.
%% Structure used to return a map of nodes and their associated properties,
%% payload and child nodes.

-type trigger_id() :: atom().
%% An ID to identify a registered trigger.

-type async_option() :: boolean() |
                        ra_server:command_correlation() |
                        ra_server:command_priority() |
                        {ra_server:command_correlation(),
                         ra_server:command_priority()}.
%% Option to indicate if the command should be synchronous or asynchronous.
%%
%% Values are:
%% <ul>
%% <li>`true' to perform an asynchronous command without a correlation
%% ID.</li>
%% <li>`false' to perform a synchronous command.</li>
%% <li>A correlation ID to perform an asynchronous low-priority command with
%% that correlation ID.</li>
%% <li>A priority to perform an asynchronous command with the specified
%% priority but without a correlation ID.</li>
%% <li>A combination of a correlation ID and a priority to perform an
%% asynchronous command with the specified parameters.</li>
%% </ul>

-type reply_from_option() :: leader | local | {member, ra:server_id()}.
%% Options to indicate which member of the cluster should reply to a command
%% request.
%%
%% Note that commands are always handled by the leader. This option only
%% controls which member of the cluster carries out the reply.
%%
%% <ul>
%% <li>`leader': the cluster leader will reply. This is the default value.</li>
%% <li>`{member, Member}': the given cluster member will reply.</li>
%% <li>`local': a member of the cluster on the same Erlang node as the caller
%% will perform the reply.</li>
%% </ul>
%%
%% When `reply_from' is `{member, Member}' and the given member is not part of
%% the cluster or when `reply_from' is `local' and there is no member local to
%% the caller, the leader member will perform the reply. This mechanism uses
%% the cluster membership information to decide which member should reply: if
%% the given `Member' or local member is a member of the cluster but is offline
%% or unreachable, no reply may be sent even though the leader may have
%% successfully handled the command.

-type command_options() :: #{timeout => timeout(),
                             async => async_option(),
                             reply_from => reply_from_option(),
                             protect_against_dups => boolean()}.
%% Options used in commands.
%%
%% Commands are {@link put/4}, {@link delete/3} and read-write {@link
%% transaction/4}.
%%
%% <ul>
%% <li>`timeout' is passed to Ra command processing function.</li>
%% <li>`async' indicates the synchronous or asynchronous nature of the
%% command; see {@link async_option()}.</li>
%% <li>`reply_from' indicates which cluster member should reply to the
%% command request; see {@link reply_from_option()}.</li>
%% <li>`protect_against_dups' indicates if the deduplication mechanism should
%% be used for the command. This mechanism helps to avoid the same command to
%% be processed multiple times if there is a Ra cluster member stopping or a
%% change of leadership occurring at the same time. It is disabled by default,
%% except for R/W transactions.</li>
%% </ul>

-type favor_option() :: consistency | low_latency.
%% Option to indicate where to put the cursor between freshness of the
%% returned data and low latency of queries.
%%
%% Values are:
%% <ul>
%% <li>`low_latency' means that a local query is used. It is the fastest and
%% have the lowest latency. However, the returned data is whatever the local Ra
%% server has. It could be out-of-date if the local Ra server did not get or
%% applied the latest updates yet. The chance of blocking and timing out is
%% very small.</li>
%% <li>`consistency' means that a local query is used. However the query uses
%% the fence mechanism to ensure that the previous updates from the calling
%% process are applied locally before the query is evaluated. It will return
%% the most up-to-date piece of data the cluster agreed on. Note that it could
%% block and eventually time out if there is no quorum in the Ra cluster.</li>
%% </ul>
%%
%% As described above, queries are always evaluated locally by the cluster
%% member that gets the call. The reason Ra's leader and consistent queries
%% are not exposed is that the remote execution of the query function may fail
%% in subtle on non-subtle ways. For instance, the remote node might run a
%% different version of Erlang or Khepri.

-type query_options() :: #{condition => ra:query_condition(),
                           timeout => timeout(),
                           favor => favor_option()}.
%% Options used in queries.
%%
%% <ul>
%% <li>`condition' indicates the condition on which the Ra server should wait
%% for before it executes the query.</li>
%% <li>`timeout' is passed to Ra query processing function.</li>
%% <li>`favor' indicates where to put the cursor between freshness of the
%% returned data and low latency of queries; see {@link favor_option()}.</li>
%% </ul>
%%
%% `favor' computes a `condition' internally. Therefore if both options are
%% set, `condition' takes precedence and `favor' is ignored.
%%
%% NOTE: When this list of query options is modified, {@link
%% khepri_machine:split_query_options/2} must be adapted.

-type tree_options() :: #{expect_specific_node => boolean(),
                          props_to_return => [known_prop_to_return() |
                                              unknown_prop_to_return()],
                          include_root_props => boolean(),
                          return_indirect_deletes => boolean()}.
%% Options used during tree traversal.
%%
%% <ul>
%% <li>`expect_specific_node' indicates if the path is expected to point to a
%% specific tree node or could match many nodes.</li>
%% <li>`props_to_return' indicates the list of properties to include in the
%% returned tree node properties map. The default is `[payload,
%% payload_version]'. Note that `payload' and `has_payload' are a bit special:
%% the actually returned properties will be `data'/`sproc' and
%% `has_data'/`is_sproc' respectively. `raw_payload' is for internal use
%% only.</li>
%% <li>`include_root_props' indicates if root properties and payload should be
%% returned as well.</li>
%% <li>`return_indirect_deletes' indicates if tree nodes deleted as a
%% byproduct of another put or delete should be part of the returned tree node
%% properties map. The default is to include them, except in transactions when
%% the effective machine version is too old.</li>
%% </ul>
%%
%% NOTE: When this list of tree options is modified, {@link
%% khepri_machine:split_query_options/2} and {@link
%% khepri_machine:split_command_options/2} must be adapted.

-type known_prop_to_return() :: payload_version |
                                child_list_version |
                                child_list_length |
                                child_names |
                                payload |
                                has_payload |
                                raw_payload |
                                delete_reason.
%% Name of a known property to return.
%%
%% To be used in {@link khepri:tree_options()} `props_to_return' list.

-type unknown_prop_to_return() :: atom().
%% Name of a property unknown to this version of Khepri.
%%
%% This can happen when a query is emitted by a newer version of Khepri that
%% added new properties to return.
%%
%% Can be seen in {@link khepri:tree_options()} `props_to_return' list.

-type put_options() :: #{keep_while => khepri_condition:keep_while()}.
%% Options specific to updates.
%%
%% <ul>
%% <li>`keep_while' allows to define keep-while conditions on the
%% created/updated tree node.</li>
%% </ul>
%%
%% NOTE: When this list of tree options is modified, {@link
%% khepri_machine:split_command_options/2} and {@link
%% khepri_machine:split_put_options/2} must be adapted.

-type fold_fun() :: fun((khepri_path:native_path(),
                         khepri:node_props(),
                         khepri:fold_acc()) -> khepri:fold_acc()).
%% Function passed to {@link khepri:fold/5}.

-type fold_acc() :: any().
%% Term passed to and returned by a {@link fold_fun/0}.

-type foreach_fun() :: fun((khepri_path:native_path(),
                            khepri:node_props()) -> any()).
%% Function passed to {@link khepri:foreach/4}.

-type map_fun() :: fun((khepri_path:native_path(),
                        khepri:node_props()) -> khepri:map_fun_ret()).
%% Function passed to {@link khepri:map/4}.

-type map_fun_ret() :: any().
%% Value returned by {@link khepri:map_fun/0}.

-type filter_fun() :: fun((khepri_path:native_path(),
                           khepri:node_props()) -> boolean()).
%% Function passed to {@link khepri:filter/4}.

-type ok(Type) :: {ok, Type}.
%% The result of a function after a successful call, wrapped in an "ok" tuple.

-type error(Type) :: {error, Type}.
%% Return value of a failed command or query.

-type error() :: error(any()).
%% The error tuple returned by a function after a failure.

-type minimal_ret() :: ok | khepri:error().
%% The return value of update functions in the {@link khepri} module.

-type payload_ret(Default) :: khepri:ok(khepri:data() |
                                        horus:horus_fun() |
                                        Default) |
                              khepri:error().
%% The return value of query functions in the {@link khepri} module that work
%% on a single tree node.
%%
%% `Default' is the value to return if a tree node has no payload attached to
%% it.

-type payload_ret() :: payload_ret(undefined).
%% The return value of query functions in the {@link khepri} module that work
%% on a single tree node.
%%
%% `undefined' is returned if a tree node has no payload attached to it.

-type many_payloads_ret(Default) :: khepri:ok(#{khepri_path:path() =>
                                                khepri:data() |
                                                horus:horus_fun() |
                                                Default}) |
                                    khepri:error().
%% The return value of query functions in the {@link khepri} module that work
%% on many nodes.
%%
%% `Default' is the value to return if a tree node has no payload attached to
%% it.

-type many_payloads_ret() :: many_payloads_ret(undefined).
%% The return value of query functions in the {@link khepri} module that work
%% on a many nodes.
%%
%% `undefined' is returned if a tree node has no payload attached to it.

-type async_ret() :: khepri_machine:write_ret() |
                     khepri_tx:tx_fun_result() |
                     khepri:error({not_leader, ra:server_id()}).
%% The value returned from of a command function which was executed
%% asynchronously.
%%
%% When a caller includes a correlation ID ({@link
%% ra_server:command_correlation()}) {@link async_option()} in their {@link
%% khepri:command_options()} on a command function, the caller will receive a
%% `ra_event' message. Handling the notification with {@link
%% khepri:handle_async_ret/2} will return a list of pairs of correlation IDs
%% ({@link ra_server:command_correlation()}) and the return values of the
%% commands which were applied, or `{error, {not_leader, LeaderId}}' if the
%% commands could not be applied since they were sent to a non-leader member.
%%
%% Note that when commands are successfully applied, the return values are
%% {@link khepri_machine:write_ret()} rather than {@link khepri:minimal_ret()},
%% even if the command was sent using a function from the {@link khepri} API
%% such as {@link khepri:put/4}.
%%
%% See {@link khepri:handle_async_ret/2}.

-export_type([store_id/0,
              ok/1,
              error/0, error/1,

              data/0,
              payload_version/0,
              child_list_version/0,
              child_list_length/0,
              delete_reason/0,
              node_props/0,
              node_props_map/0,
              trigger_id/0,

              async_option/0,
              reply_from_option/0,
              command_options/0,
              favor_option/0,
              query_options/0,
              tree_options/0,
              put_options/0,

              fold_fun/0,
              fold_acc/0,
              foreach_fun/0,
              map_fun/0,
              map_fun_ret/0,
              filter_fun/0,

              minimal_ret/0,
              payload_ret/0, payload_ret/1,
              many_payloads_ret/0, many_payloads_ret/1,
              unwrapped_minimal_ret/0,
              unwrapped_payload_ret/0,
              unwrapped_payload_ret/1,
              unwrapped_many_payloads_ret/0,
              unwrapped_many_payloads_ret/1,
              async_ret/0]).

%% -------------------------------------------------------------------
%% Service management.
%% -------------------------------------------------------------------

-spec start() -> Ret when
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% @see khepri_cluster:start/0.

start() ->
    khepri_cluster:start().

-spec start(RaSystem | DataDir) -> Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% @see khepri_cluster:start/1.

start(RaSystemOrDataDir) ->
    khepri_cluster:start(RaSystemOrDataDir).

-spec start(RaSystem | DataDir, StoreId | RaServerConfig) -> Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      StoreId :: store_id(),
      RaServerConfig :: khepri_cluster:incomplete_ra_server_config(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% @see khepri_cluster:start/2.

start(RaSystemOrDataDir, StoreIdOrRaServerConfig) ->
    khepri_cluster:start(RaSystemOrDataDir, StoreIdOrRaServerConfig).

-spec start(RaSystem | DataDir, StoreId | RaServerConfig, Timeout) ->
    Ret when
      RaSystem :: atom(),
      DataDir :: file:filename_all(),
      StoreId :: store_id(),
      RaServerConfig :: khepri_cluster:incomplete_ra_server_config(),
      Timeout :: timeout(),
      Ret :: khepri:ok(StoreId) | khepri:error(),
      StoreId :: khepri:store_id().
%% @doc Starts a store.
%%
%% @see khepri_cluster:start/3.

start(RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout) ->
    khepri_cluster:start(
      RaSystemOrDataDir, StoreIdOrRaServerConfig, Timeout).

-spec reset() -> Ret when
      Ret :: ok | error().
%% @doc Resets the store on this Erlang node.
%%
%% @see khepri_cluster:reset/0.

reset() ->
    khepri_cluster:reset().

-spec reset(StoreId | Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Resets the store on this Erlang node.
%%
%% @see khepri_cluster:reset/1.

reset(StoreIdOrTimeout) ->
    khepri_cluster:reset(StoreIdOrTimeout).

-spec reset(StoreId, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | error().
%% @doc Resets the store on this Erlang node.
%%
%% @see khepri_cluster:reset/2.

reset(StoreId, Timeout) ->
    khepri_cluster:reset(StoreId, Timeout).

-spec stop() -> Ret when
      Ret :: ok | khepri:error().
%% @doc Stops a store.
%%
%% @see khepri_cluster:stop/0.

stop() ->
    khepri_cluster:stop().

-spec stop(StoreId) -> Ret when
      StoreId :: khepri:store_id(),
      Ret :: ok | khepri:error().
%% @doc Stops a store.
%%
%% @see khepri_cluster:stop/1.

stop(StoreId) ->
    khepri_cluster:stop(StoreId).

-spec get_store_ids() -> [StoreId] when
      StoreId :: store_id().
%% @doc Returns the list of running stores.
%%
%% @see khepri_cluster:get_store_ids/0.

get_store_ids() ->
    khepri_cluster:get_store_ids().

%% -------------------------------------------------------------------
%% is_empty().
%% -------------------------------------------------------------------

-spec is_empty() -> IsEmpty | Error when
      IsEmpty :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the store is empty or not.
%%
%% Calling this function is the same as calling `is_empty(StoreId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see is_empty/1.
%% @see is_empty/2.

is_empty() ->
    StoreId = khepri_cluster:get_default_store_id(),
    is_empty(StoreId).

-spec is_empty
(StoreId) -> IsEmpty | Error when
      StoreId :: khepri:store_id(),
      IsEmpty :: boolean(),
      Error :: khepri:error();
(Options) -> IsEmpty | Error when
      Options :: khepri:query_options() | khepri:tree_options(),
      IsEmpty :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the store is empty or not.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`is_empty(StoreId)'. Calling it is the same as calling
%% `is_empty(StoreId, #{})'.</li>
%% <li>`is_empty(Options)'. Calling it is the same as calling
%% `is_empty(StoreId, Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see is_empty/2.

is_empty(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    is_empty(StoreId, #{});
is_empty(Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    is_empty(StoreId, Options).

-spec is_empty(StoreId, Options) -> IsEmpty | Error when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options() | khepri:tree_options(),
      IsEmpty :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the store is empty or not.
%%
%% @param StoreId the name of the Khepri store.
%% @param Options query options such as `favor'.
%%
%% @returns `true' if the store is empty, `false' if it is not, or an `{error,
%% Reason}' tuple.

is_empty(StoreId, Options) ->
    Path = [],
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [child_list_length]},
    case khepri_adv:get_many(StoreId, Path, Options1) of
        {ok, #{Path := #{child_list_length := Count}}} -> Count =:= 0;
        {error, _} = Error                             -> Error
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `get(StoreId, PathPattern)'
%% with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see get/2.
%% @see get/3.

get(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get(StoreId, PathPattern).

-spec get
(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:payload_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`get(StoreId, PathPattern)'. Calling it is the same as calling
%% `get(StoreId, PathPattern, #{})'.</li>
%% <li>`get(PathPattern, Options)'. Calling it is the same as calling
%% `get(StoreId, PathPattern, Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see get/3.

get(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    get(StoreId, PathPattern, #{});
get(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get(StoreId, PathPattern, Options).

-spec get(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% The returned `{ok, Payload}' tuple contains the payload of the targeted
%% tree node, or `{ok, undefined}' if the tree node had no payload.
%%
%% Example: query a tree node which holds the atom `value'
%% ```
%% %% Query the tree node at `/:foo/:bar'.
%% {ok, value} = khepri:get(StoreId, [foo, bar]).
%% '''
%%
%% Example: query an existing tree node with no payload
%% ```
%% %% Query the tree node at `/:no_payload'.
%% {ok, undefined} = khepri:get(StoreId, [no_payload]).
%% '''
%%
%% Example: query a non-existent tree node
%% ```
%% %% Query the tree node at `/:non_existent'.
%% {error, ?khepri_error(node_not_found, _)} = khepri:get(
%%                                               StoreId, [non_existent]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to get.
%% @param Options query options.
%%
%% @returns an `{ok, Payload | undefined}' tuple or an `{error, Reason}'
%% tuple.
%%
%% @see get_or/3.
%% @see get_many/3.
%% @see khepri_adv:get/3.

get(StoreId, PathPattern, Options) ->
    case khepri_adv:get(StoreId, PathPattern, Options) of
        {ok, NodePropsMap} ->
            NodeProps = khepri_utils:get_single_node_props(NodePropsMap),
            Payload = khepri_utils:node_props_to_payload(NodeProps, undefined),
            {ok, Payload};
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% get_or().
%% -------------------------------------------------------------------

-spec get_or(PathPattern, Default) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:payload_ret(Default).
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% Calling this function is the same as calling `get_or(StoreId, PathPattern,
%% Default)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see get_or/3.
%% @see get_or/4.

get_or(PathPattern, Default) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_or(StoreId, PathPattern, Default).

-spec get_or
(StoreId, PathPattern, Default) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:payload_ret(Default);
(PathPattern, Default, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:payload_ret(Default).
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`get_or(StoreId, PathPattern, Default)'. Calling it is the same as
%% calling `get_or(StoreId, PathPattern, Default, #{})'.</li>
%% <li>`get_or(PathPattern, Default, Options)'. Calling it is the same as
%% calling `get_or(StoreId, PathPattern, Default, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see get_or/4.

get_or(StoreId, PathPattern, Default) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    get_or(StoreId, PathPattern, Default, #{});
get_or(PathPattern, Default, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_or(StoreId, PathPattern, Default, Options).

-spec get_or(StoreId, PathPattern, Default, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:payload_ret(Default).
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% The returned `{ok, Payload}' tuple contains the payload of the targeted
%% tree node, or `{ok, Default}' if the tree node had no payload or was not
%% found.
%%
%% Example: query a tree node which holds the atom `value'
%% ```
%% %% Query the tree node at `/:foo/:bar'.
%% {ok, value} = khepri:get_or(StoreId, [foo, bar], default).
%% '''
%%
%% Example: query an existing tree node with no payload
%% ```
%% %% Query the tree node at `/:no_payload'.
%% {ok, default} = khepri:get_or(StoreId, [no_payload], default).
%% '''
%%
%% Example: query a non-existent tree node
%% ```
%% %% Query the tree node at `/:non_existent'.
%% {ok, default} = khepri:get_or(StoreId, [non_existent], default).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to get.
%% @param Default the default value to return in case the tree node has no
%%        payload or does not exist.
%% @param Options query options.
%%
%% @returns an `{ok, Payload | Default}' tuple or an `{error, Reason}' tuple.
%%
%% @see get/3.
%% @see get_many_or/4.
%% @see khepri_adv:get/3.

get_or(StoreId, PathPattern, Default, Options) ->
    case khepri_adv:get(StoreId, PathPattern, Options) of
        {ok, NodePropsMap} ->
            NodeProps = khepri_utils:get_single_node_props(NodePropsMap),
            Payload = khepri_utils:node_props_to_payload(NodeProps, Default),
            {ok, Payload};
        {error, ?khepri_error(node_not_found, _)} ->
            {ok, Default};
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% Calling this function is the same as calling `get_many(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see get_many/2.
%% @see get_many/3.

get_many(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_many(StoreId, PathPattern).

-spec get_many
(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:many_payloads_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`get_many(StoreId, PathPattern)'. Calling it is the same as calling
%% `get_many(StoreId, PathPattern, #{})'.</li>
%% <li>`get_many(PathPattern, Options)'. Calling it is the same as calling
%% `get_many(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see get_many/3.

get_many(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    get_many(StoreId, PathPattern, #{});
get_many(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_many(StoreId, PathPattern, Options).

-spec get_many(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% Calling this function is the same as calling `get_many_or(StoreId,
%% PathPattern, undefined, Options)'.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The returned `{ok, PayloadsMap}' tuple contains a map where keys correspond
%% to the path to a tree node matching the path pattern. Each key then points
%% to the payload of that matching tree node, or `Default' if the tree node
%% had no payload.
%%
%% Example: query all nodes in the tree
%% ```
%% %% Get all nodes in the tree. The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, #{[foo] := undefined,
%%        [foo, bar] := value}} = khepri:get_many(
%%                                  StoreId,
%%                                  [?KHEPRI_WILDCARD_STAR_STAR]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Options query options.
%%
%% @returns an `{ok, PayloadsMap}' tuple or an `{error, Reason}' tuple.
%%
%% @see get/3.
%% @see get_many_or/4.
%% @see khepri_adv:get_many/3.

get_many(StoreId, PathPattern, Options) ->
    get_many_or(StoreId, PathPattern, undefined, Options).

%% -------------------------------------------------------------------
%% get_many_or().
%% -------------------------------------------------------------------

-spec get_many_or(PathPattern, Default) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:many_payloads_ret(Default).
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% Calling this function is the same as calling `get_many_or(StoreId,
%% PathPattern, Default)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see get_many_or/3.
%% @see get_many_or/4.

get_many_or(PathPattern, Default) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_many_or(StoreId, PathPattern, Default).

-spec get_many_or
(StoreId, PathPattern, Default) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:many_payloads_ret(Default);
(PathPattern, Default, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret(Default).
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`get_many_or(StoreId, PathPattern, Default)'. Calling it is the same as
%% calling `get_many_or(StoreId, PathPattern, Default, #{})'.</li>
%% <li>`get_many_or(PathPattern, Default, Options)'. Calling it is the same as
%% calling `get_many_or(StoreId, PathPattern, Default, Options)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see get_many_or/4.

get_many_or(StoreId, PathPattern, Default) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    get_many_or(StoreId, PathPattern, Default, #{});
get_many_or(PathPattern, Default, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_many_or(StoreId, PathPattern, Default, Options).

-spec get_many_or(StoreId, PathPattern, Default, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret(Default).
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The returned `{ok, PayloadsMap}' tuple contains a map where keys correspond
%% to the path to a tree node matching the path pattern. Each key then points
%% to the payload of that matching tree node, or `Default' if the tree node
%% had no payload.
%%
%% Example: query all nodes in the tree
%% ```
%% %% Get all nodes in the tree. The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, #{[foo] := default,
%%        [foo, bar] := value}} = khepri:get_many_or(
%%                                  StoreId,
%%                                  [?KHEPRI_WILDCARD_STAR_STAR],
%%                                  default).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Default the default value to set in `PayloadsMap' for tree nodes
%%        with no payload.
%% @param Options query options.
%%
%% @returns an `{ok, PayloadsMap}' tuple or an `{error, Reason}' tuple.
%%
%% @see get_or/4.
%% @see get_many/3.
%% @see khepri_adv:get_many/3.

get_many_or(StoreId, PathPattern, Default, Options) ->
    Fun = fun(Path, NodeProps, Acc) ->
                  Payload = khepri_utils:node_props_to_payload(
                              NodeProps, Default),
                  Acc#{Path => Payload}
          end,
    khepri_machine:fold(StoreId, PathPattern, Fun, #{}, Options).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(PathPattern) -> Exists | Error when
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% Calling this function is the same as calling `exists(StoreId, PathPattern)'
%% with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see exists/2.
%% @see exists/3.

exists(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    exists(StoreId, PathPattern).

-spec exists
(StoreId, PathPattern) -> Exists | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean(),
      Error :: khepri:error();
(PathPattern, Options) -> Exists | Error when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Exists :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`exists(StoreId, PathPattern)'. Calling it is the same as calling
%% `exists(StoreId, PathPattern, #{})'.</li>
%% <li>`exists(PathPattern, Options)'. Calling it is the same as calling
%% `exists(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see exists/3.

exists(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    exists(StoreId, PathPattern, #{});
exists(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    exists(StoreId, PathPattern, Options).

-spec exists(StoreId, PathPattern, Options) -> Exists | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Exists :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to check.
%% @param Options query options such as `favor'.
%%
%% @returns `true' if the tree node exists, `false' if it does not, or an
%% `{error, Reason}' tuple.
%%
%% @see get/3.

exists(StoreId, PathPattern, Options) ->
    %% TODO: Use path condition instead.
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => []},
    case khepri_adv:get_many(StoreId, PathPattern, Options1) of
        {ok, _} ->
            true;
        {error, ?khepri_error(node_not_found, _)} ->
            false;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% has_data().
%% -------------------------------------------------------------------

-spec has_data(PathPattern) -> HasData | Error when
      PathPattern :: khepri_path:pattern(),
      HasData :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% Calling this function is the same as calling `has_data(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see has_data/2.
%% @see has_data/3.

has_data(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    has_data(StoreId, PathPattern).

-spec has_data
(StoreId, PathPattern) -> HasData | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      HasData :: boolean(),
      Error :: khepri:error();
(PathPattern, Options) -> HasData | Error when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      HasData :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`has_data(StoreId, PathPattern)'. Calling it is the same as calling
%% `has_data(StoreId, PathPattern, #{})'.</li>
%% <li>`has_data(PathPattern, Options)'. Calling it is the same as calling
%% `has_data(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see has_data/3.

has_data(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    has_data(StoreId, PathPattern, #{});
has_data(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    has_data(StoreId, PathPattern, Options).

-spec has_data(StoreId, PathPattern, Options) -> HasData | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      HasData :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to check.
%% @param Options query options such as `favor'.
%%
%% @returns `true' if tree the node holds data, `false' if it does not exist,
%% has no payload or holds a stored procedure, or an `{error, Reason}' tuple.
%%
%% @see get/3.

has_data(StoreId, PathPattern, Options) ->
    %% TODO: Use path condition instead.
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [has_payload]},
    case khepri_adv:get_many(StoreId, PathPattern, Options1) of
        {ok, NodePropsMap} ->
            [NodeProps] = maps:values(NodePropsMap),
            maps:get(has_data, NodeProps, false);
        {error, ?khepri_error(node_not_found, _)} ->
            false;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% is_sproc().
%% -------------------------------------------------------------------

-spec is_sproc(PathPattern) -> IsSproc | Error when
      PathPattern :: khepri_path:pattern(),
      IsSproc :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% Calling this function is the same as calling `is_sproc(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see is_sproc/2.
%% @see is_sproc/3.

is_sproc(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    is_sproc(StoreId, PathPattern).

-spec is_sproc
(StoreId, PathPattern) -> IsSproc | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      IsSproc :: boolean(),
      Error :: khepri:error();
(PathPattern, Options) -> IsSproc | Error when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      IsSproc :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`is_sproc(StoreId, PathPattern)'. Calling it is the same as calling
%% `is_sproc(StoreId, PathPattern, #{})'.</li>
%% <li>`is_sproc(PathPattern, Options)'. Calling it is the same as calling
%% `is_sproc(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see is_sproc/3.

is_sproc(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    is_sproc(StoreId, PathPattern, #{});
is_sproc(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    is_sproc(StoreId, PathPattern, Options).

-spec is_sproc(StoreId, PathPattern, Options) -> IsSproc | Error when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      IsSproc :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to check.
%% @param Options query options such as `favor'.
%%
%% @returns `true' if the tree node holds a stored procedure, `false' if it
%% does not exist, has no payload or holds data, or an `{error, Reason}'
%% tuple.
%%
%% @see get/3.

is_sproc(StoreId, PathPattern, Options) ->
    %% TODO: Use path condition instead.
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [has_payload]},
    case khepri_adv:get_many(StoreId, PathPattern, Options1) of
        {ok, NodePropsMap} ->
            [NodeProps] = maps:values(NodePropsMap),
            maps:get(is_sproc, NodeProps, false);
        {error, ?khepri_error(node_not_found, _)} ->
            false;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec count(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: ok(Count) | error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `count(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see count/2.
%% @see count/3.

count(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    count(StoreId, PathPattern).

-spec count
(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: ok(Count) | error(),
      Count :: non_neg_integer();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: ok(Count) | error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`count(StoreId, PathPattern)'. Calling it is the same as calling
%% `count(StoreId, PathPattern, #{})'.</li>
%% <li>`count(PathPattern, Options)'. Calling it is the same as calling
%% `count(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see count/3.

count(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    count(StoreId, PathPattern, #{});
count(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    count(StoreId, PathPattern, Options).

-spec count(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: ok(Count) | error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The root node is not included in the count.
%%
%% Example:
%% ```
%% %% Query the tree node at `/:foo/:bar'.
%% {ok, 3} = khepri:count(StoreId, [foo, ?KHEPRI_WILDCARD_STAR]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to count.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, Count}' tuple with the number of matching tree nodes, or
%% an `{error, Reason}' tuple.

count(StoreId, PathPattern, Options) ->
    Fun = fun khepri_tree:count_node_cb/3,
    Options1 = Options#{expect_specific_node => false},
    khepri_machine:fold(StoreId, PathPattern, Fun, 0, Options1).

%% -------------------------------------------------------------------
%% fold().
%% -------------------------------------------------------------------

-spec fold(PathPattern, Fun, Acc) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc.
%% @doc Calls `Fun' on successive tree nodes matching the given path pattern,
%% starting with `Acc'.
%%
%% Calling this function is the same as calling `fold(StoreId, PathPattern,
%% Fun, Acc)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see fold/4.
%% @see fold/5.

fold(PathPattern, Fun, Acc) ->
    StoreId = khepri_cluster:get_default_store_id(),
    fold(StoreId, PathPattern, Fun, Acc).

-spec fold
(StoreId, PathPattern, Fun, Acc) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc;
(PathPattern, Fun, Acc, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc.
%% @doc Calls `Fun' on successive tree nodes matching the given path pattern,
%% starting with `Acc'.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`fold(StoreId, PathPattern, Fun, Acc)'. Calling it is the same as
%% calling `fold(StoreId, PathPattern, Fun, Acc, #{})'.</li>
%% <li>`fold(PathPattern, Fun, Acc, Options)'. Calling it is the same as
%% calling `fold(StoreId, PathPattern, Fun, Acc, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see fold/5.

fold(StoreId, PathPattern, Fun, Acc) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    fold(StoreId, PathPattern, Fun, Acc, #{});
fold(PathPattern, Fun, Acc, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    fold(StoreId, PathPattern, Fun, Acc, Options).

-spec fold(StoreId, PathPattern, Fun, Acc, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc.
%% @doc Calls `Fun' on successive tree nodes matching the given path pattern,
%% starting with `Acc'.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% Like the function passed to {@link maps:fold/3}, `Fun' must accept the
%% following arguments:
%% <ol>
%% <li>the native path of the tree node being handled</li>
%% <li>the tree node properties map</li>
%% <li>an Erlang term which is either `Acc' for the first matched tree node, or
%% the return value of the previous call to `Fun'</li>
%% </ol>
%%
%% The returned `{ok, NewAcc}' tuple contains the return value of the last call
%% to `Fun', or `Acc' if no tree nodes matched the given path pattern.
%%
%% Example: list all nodes in the tree
%% ```
%% %% List all tree node paths in the tree. The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, [[foo], [foo, bar]]} = khepri:fold(
%%                               StoreId,
%%                               [?KHEPRI_WILDCARD_STAR_STAR],
%%                               fun(Path, _NodeProps, Acc) ->
%%                                   [Path | Acc]
%%                               end, []).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Fun the function to call for each matching tree node.
%% @param Acc the Erlang term to pass to the first call to `Fun'.
%% @param Options query options.
%%
%% @returns an `{ok, NewAcc}' tuple or an `{error, Reason}' tuple.

fold(StoreId, PathPattern, Fun, Acc, Options) ->
    Options1 = Options#{expect_specific_node => false},
    khepri_machine:fold(StoreId, PathPattern, Fun, Acc, Options1).

%% -------------------------------------------------------------------
%% foreach().
%% -------------------------------------------------------------------

-spec foreach(PathPattern, Fun) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Ret :: ok | khepri:error().
%% @doc Calls `Fun' for each tree node matching the given path pattern.
%%
%% Calling this function is the same as calling `foreach(StoreId, PathPattern,
%% Fun)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see foreach/3.
%% @see foreach/4.

foreach(PathPattern, Fun) ->
    StoreId = khepri_cluster:get_default_store_id(),
    foreach(StoreId, PathPattern, Fun).

-spec foreach
(StoreId, PathPattern, Fun) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Ret :: ok | khepri:error();
(PathPattern, Fun, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: ok | khepri:error().
%% @doc Calls `Fun' for each tree node matching the given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`foreach(StoreId, PathPattern, Fun)'. Calling it is the same as
%% calling `foreach(StoreId, PathPattern, Fun, #{})'.</li>
%% <li>`foreach(PathPattern, Fun, Options)'. Calling it is the same as
%% calling `foreach(StoreId, PathPattern, Fun, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see foreach/4.

foreach(StoreId, PathPattern, Fun) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    foreach(StoreId, PathPattern, Fun, #{});
foreach(PathPattern, Fun, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    foreach(StoreId, PathPattern, Fun, Options).

-spec foreach(StoreId, PathPattern, Fun, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: ok | khepri:error().
%% @doc Calls `Fun' for each tree node matching the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% Like the function passed to {@link maps:foreach/2}, `Fun' must accept the
%% following arguments:
%% <ol>
%% <li>the native path of the tree node being handled</li>
%% <li>the tree node properties map</li>
%% </ol>
%%
%% Example: print all nodes in the tree
%% ```
%% %% Print all tree node paths in the tree. The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% ok = khepri:foreach(
%%           StoreId,
%%           [?KHEPRI_WILDCARD_STAR_STAR],
%%           fun(Path, _NodeProps) ->
%%               io:format("Path ~0p~n", [Path])
%%           end).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Fun the function to call for each matching tree node.
%% @param Options query options.
%%
%% @returns `ok' or an `{error, Reason}' tuple.

foreach(StoreId, PathPattern, Fun, Options) when is_function(Fun, 2) ->
    FoldFun = fun(Path, NodeProps, Acc) ->
                      _ = Fun(Path, NodeProps),
                      Acc
              end,
    case fold(StoreId, PathPattern, FoldFun, ok, Options) of
        {ok, ok}                 -> ok;
        {error, _Reason} = Error -> Error
    end.

%% -------------------------------------------------------------------
%% map().
%% -------------------------------------------------------------------

-spec map(PathPattern, Fun) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:map_fun(),
      Ret :: khepri:ok(Map) | khepri:error(),
      Map :: #{khepri_path:native_path() => khepri:map_fun_ret()}.
%% @doc Produces a new map by calling `Fun' for each tree node matching the
%% given path pattern.
%%
%% Calling this function is the same as calling `map(StoreId, PathPattern,
%% Fun)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see map/3.
%% @see map/4.

map(PathPattern, Fun) ->
    StoreId = khepri_cluster:get_default_store_id(),
    map(StoreId, PathPattern, Fun).

-spec map
(StoreId, PathPattern, Fun) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:map_fun(),
      Ret :: khepri:ok(Map) | khepri:error(),
      Map :: #{khepri_path:native_path() => khepri:map_fun_ret()};
(PathPattern, Fun, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:map_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(Map) | khepri:error(),
      Map :: #{khepri_path:native_path() => khepri:map_fun_ret()}.
%% @doc Produces a new map by calling `Fun' for each tree node matching the
%% given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`map(StoreId, PathPattern, Fun)'. Calling it is the same as
%% calling `map(StoreId, PathPattern, Fun, #{})'.</li>
%% <li>`map(PathPattern, Fun, Options)'. Calling it is the same as
%% calling `map(StoreId, PathPattern, Fun, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see map/4.

map(StoreId, PathPattern, Fun) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    map(StoreId, PathPattern, Fun, #{});
map(PathPattern, Fun, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    map(StoreId, PathPattern, Fun, Options).

-spec map(StoreId, PathPattern, Fun, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:map_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:ok(Map) | khepri:error(),
      Map :: #{khepri_path:native_path() => khepri:map_fun_ret()}.
%% @doc Produces a new map by calling `Fun' for each tree node matching the
%% given path pattern.
%%
%% The produced map uses the tree node path as the key, like {@link get_many/3}
%% and the return value of `Fun' as the value.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% Like the function passed to {@link maps:map/2}, `Fun' must accept the
%% following arguments:
%% <ol>
%% <li>the native path of the tree node being handled</li>
%% <li>the tree node properties map</li>
%% </ol>
%%
%% Example: create a map of the form "native path => string path"
%% ```
%% %% The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, #{[foo] => "/:foo",
%%        [foo, bar] => "/:foo/:bar"}} = khepri:map(
%%                                         StoreId,
%%                                         [?KHEPRI_WILDCARD_STAR_STAR],
%%                                         fun(Path, _NodeProps) ->
%%                                             khepri_path:to_string(Path)
%%                                         end).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Fun the function to call for each matching tree node.
%% @param Options query options.
%%
%% @returns `{ok, Map}' or an `{error, Reason}' tuple.

map(StoreId, PathPattern, Fun, Options) when is_function(Fun, 2) ->
    FoldFun = fun(Path, NodeProps, Acc) ->
                      Ret = Fun(Path, NodeProps),
                      Acc#{Path => Ret}
              end,
    fold(StoreId, PathPattern, FoldFun, #{}, Options).

%% -------------------------------------------------------------------
%% filter().
%% -------------------------------------------------------------------

-spec filter(PathPattern, Pred) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Pred :: khepri:filter_fun(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns a map for which predicate `Pred' holds true in tree nodes
%% matching the given path pattern.
%%
%% Calling this function is the same as calling `filter(StoreId, PathPattern,
%% Pred)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see filter/3.
%% @see filter/4.

filter(PathPattern, Pred) ->
    StoreId = khepri_cluster:get_default_store_id(),
    filter(StoreId, PathPattern, Pred).

-spec filter
(StoreId, PathPattern, Pred) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Pred :: khepri:filter_fun(),
      Ret :: khepri:many_payloads_ret();
(PathPattern, Pred, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Pred :: khepri:filter_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns a map for which predicate `Pred' holds true in tree nodes
%% matching the given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`filter(StoreId, PathPattern, Pred)'. Calling it is the same as
%% calling `filter(StoreId, PathPattern, Pred, #{})'.</li>
%% <li>`filter(PathPattern, Pred, Options)'. Calling it is the same as
%% calling `filter(StoreId, PathPattern, Pred, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see filter/4.

filter(StoreId, PathPattern, Pred) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    filter(StoreId, PathPattern, Pred, #{});
filter(PathPattern, Pred, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    filter(StoreId, PathPattern, Pred, Options).

-spec filter(StoreId, PathPattern, Pred, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Pred :: khepri:filter_fun(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns a map for which predicate `Pred' holds true in tree nodes
%% matching the given path pattern.
%%
%% The produced map only contains tree nodes for which `Pred' returned true.
%% The map has the same form as the one returned by {@link get_many/3}
%% otherwise.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% Like the function passed to {@link maps:filter/2}, `Pred' must accept the
%% following arguments:
%% <ol>
%% <li>the native path of the tree node being handled</li>
%% <li>the tree node properties filter</li>
%% </ol>
%%
%% Example: only keep tree nodes under `/:foo'
%% ```
%% %% The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, #{[foo] => undefined,
%%        [foo, bar] => value}} = khepri:filter(
%%                                  StoreId,
%%                                  [?KHEPRI_WILDCARD_STAR_STAR],
%%                                  fun
%%                                    ([foo | _], _NodeProps) -> true;
%%                                    (_Path, _NodeProps)     -> false
%%                                  end).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Pred the function to call for each matching tree node.
%% @param Options query options.
%%
%% @returns `{ok, Map}' or an `{error, Reason}' tuple.

filter(StoreId, PathPattern, Pred, Options) when is_function(Pred, 2) ->
    FoldFun = fun(Path, NodeProps, Acc) ->
                      case Pred(Path, NodeProps) of
                          true ->
                              Payload = khepri_utils:node_props_to_payload(
                                          NodeProps, undefined),
                              Acc#{Path => Payload};
                          false ->
                              Acc
                      end
              end,
    fold(StoreId, PathPattern, FoldFun, #{}, Options).

%% -------------------------------------------------------------------
%% run_sproc().
%% -------------------------------------------------------------------

-spec run_sproc(PathPattern, Args) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Ret :: any().
%% @doc Runs the stored procedure pointed to by the given path and returns the
%% result.
%%
%% Calling this function is the same as calling `run_sproc(StoreId,
%% PathPattern, Args)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see run_sproc/3.
%% @see run_sproc/4.

run_sproc(PathPattern, Args) ->
    StoreId = khepri_cluster:get_default_store_id(),
    run_sproc(StoreId, PathPattern, Args).

-spec run_sproc
(StoreId, PathPattern, Args) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Ret :: any();
(PathPattern, Args, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: any().
%% @doc Runs the stored procedure pointed to by the given path and returns the
%% result.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`run_sproc(StoreId, PathPattern, Args)'. Calling it is the same as
%% calling `run_sproc(StoreId, PathPattern, Args, #{})'.</li>
%% <li>`run_sproc(PathPattern, Args, Options)'. Calling it is the same as
%% calling `run_sproc(StoreId, PathPattern, Args, Options)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see run_sproc/4.

run_sproc(StoreId, PathPattern, Args) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    run_sproc(StoreId, PathPattern, Args, #{});
run_sproc(PathPattern, Args, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    run_sproc(StoreId, PathPattern, Args, Options).

-spec run_sproc(StoreId, PathPattern, Args, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: khepri:query_options(),
      Ret :: any().
%% @doc Runs the stored procedure pointed to by the given path and returns the
%% result.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% The length of the `Args' list must match the number of arguments expected by
%% the stored procedure.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node holding the
%%        stored procedure.
%% @param Args the list of args to pass to the stored procedure; its length
%%        must be equal to the stored procedure arity.
%% @param Options query options.
%%
%% @returns the result of the stored procedure execution, or throws an
%% exception if the tree node does not exist, does not hold a stored procedure
%% or if there was an error.
%%
%% @see is_sproc/3.

run_sproc(StoreId, PathPattern, Args, Options) ->
    case khepri_adv:get(StoreId, PathPattern, Options) of
        {ok, NodePropsMap} ->
            NodeProps = khepri_utils:get_single_node_props(NodePropsMap),
            case NodeProps of
                #{sproc := StandaloneFun} ->
                    khepri_sproc:run(StandaloneFun, Args);
                _ ->
                    throw(?khepri_exception(
                             denied_execution_of_non_sproc_node,
                             #{path => PathPattern,
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

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec put(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `put(StoreId, PathPattern,
%% Data)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see put/3.
%% @see put/4.

put(PathPattern, Data) ->
    StoreId = khepri_cluster:get_default_store_id(),
    put(StoreId, PathPattern, Data).

-spec put(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `put(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see put/4.

put(StoreId, PathPattern, Data) ->
    put(StoreId, PathPattern, Data, #{}).

-spec put(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time.
%%
%% When using a simple path (i.e. without conditions), if the targeted tree
%% node does not exist, it is created using the given payload. If the
%% targeted tree node exists, it is updated with the given payload and its
%% payload version is increased by one. Missing parent nodes are created on
%% the way.
%%
%% When using a path pattern, the behavior is the same. However if a condition
%% in the path pattern is not met, an error is returned and the tree structure
%% is not modified.
%%
%% The payload must be one of the following form:
%% <ul>
%% <li>An explicit absence of payload ({@link khepri_payload:no_payload()}),
%% using the marker returned by {@link khepri_payload:none/0}, meaning there
%% will be no payload attached to the tree node and the existing payload will
%% be discarded if any</li>
%% <li>An anonymous function; it will be considered a stored procedure and
%% will be wrapped in a {@link khepri_payload:sproc()} record</li>
%% <li>Any other term; it will be wrapped in a {@link khepri_payload:data()}
%% record</li>
%% </ul>
%%
%% It is possible to wrap the payload in its internal structure explicitly
%% using the {@link khepri_payload} module directly.
%%
%% The `Options' map may specify command-level options; see {@link
%% khepri:command_options()}, {@link khepri:tree_options()} and {@link
%% khepri:put_options()}.
%%
%% When doing an asynchronous update, the {@link handle_async_ret/2}
%% function can be used to handle the message received from Ra.
%%
%% Example:
%% ```
%% %% Insert a tree node at `/:foo/:bar', overwriting the previous value.
%% ok = khepri:put(StoreId, [foo, bar], new_value).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to create or
%%        modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see create/4.
%% @see update/4.
%% @see compare_and_swap/5.
%% @see put_many/4.
%% @see khepri_adv:put/4.

put(StoreId, PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:put(StoreId, PathPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% put_many().
%% -------------------------------------------------------------------

-spec put_many(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `put_many(StoreId, PathPattern,
%% Data)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see put_many/3.
%% @see put_many/4.

put_many(PathPattern, Data) ->
    StoreId = khepri_cluster:get_default_store_id(),
    put_many(StoreId, PathPattern, Data).

-spec put_many(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `put_many(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see put_many/4.

put_many(StoreId, PathPattern, Data) ->
    put_many(StoreId, PathPattern, Data, #{}).

-spec put_many(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% When using a simple path (i.e. without conditions), if the targeted tree
%% node does not exist, it is created using the given payload. If the
%% targeted tree node exists, it is updated with the given payload and its
%% payload version is increased by one. Missing parent nodes are created on
%% the way.
%%
%% When using a path pattern, the behavior is the same. However if a condition
%% in the path pattern is not met, an error is returned and the tree structure
%% is not modified.
%%
%% The payload must be one of the following form:
%% <ul>
%% <li>An explicit absence of payload ({@link khepri_payload:no_payload()}),
%% using the marker returned by {@link khepri_payload:none/0}, meaning there
%% will be no payload attached to the tree node and the existing payload will
%% be discarded if any</li>
%% <li>An anonymous function; it will be considered a stored procedure and
%% will be wrapped in a {@link khepri_payload:sproc()} record</li>
%% <li>Any other term; it will be wrapped in a {@link khepri_payload:data()}
%% record</li>
%% </ul>
%%
%% It is possible to wrap the payload in its internal structure explicitly
%% using the {@link khepri_payload} module directly.
%%
%% The `Options' map may specify command-level options; see {@link
%% khepri:command_options()}, {@link khepri:tree_options()} and {@link
%% khepri:put_options()}.
%%
%% When doing an asynchronous update, the {@link handle_async_ret/2}
%% function can be used to handle the message received from Ra.
%%
%% Example:
%% ```
%% %% Insert a tree node at `/:foo/:bar', overwriting the previous value.
%% ok = khepri:put(StoreId, [foo, bar], new_value).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to create or
%%        modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see put/4.
%% @see khepri_adv:put_many/4.

put_many(StoreId, PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:put_many(StoreId, PathPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Creates a tree node with the given payload.
%%
%% Calling this function is the same as calling `create(StoreId, PathPattern,
%% Data)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see create/3.
%% @see create/4.

create(PathPattern, Data) ->
    StoreId = khepri_cluster:get_default_store_id(),
    create(StoreId, PathPattern, Data).

-spec create(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Creates a tree node with the given payload.
%%
%% Calling this function is the same as calling `create(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see create/4.

create(StoreId, PathPattern, Data) ->
    create(StoreId, PathPattern, Data, #{}).

-spec create(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Creates a tree node with the given payload.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple is
%% returned.
%%
%% Internally, the `PathPattern' is modified to include an
%% `#if_node_exists{exists = false}' condition on its last component.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to create.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see put/4.
%% @see update/4.
%% @see compare_and_swap/5.
%% @see khepri_adv:create/4.

create(StoreId, PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:create(StoreId, PathPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% Calling this function is the same as calling `update(StoreId, PathPattern,
%% Data)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see update/3.
%% @see update/4.

update(PathPattern, Data) ->
    StoreId = khepri_cluster:get_default_store_id(),
    update(StoreId, PathPattern, Data).

-spec update(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% Calling this function is the same as calling `update(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see update/4.

update(StoreId, PathPattern, Data) ->
    update(StoreId, PathPattern, Data, #{}).

-spec update(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple is
%% returned.
%%
%% Internally, the `PathPattern' is modified to include an
%% `#if_node_exists{exists = true}' condition on its last component.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see put/4.
%% @see create/4.
%% @see compare_and_swap/5.
%% @see khepri_adv:update/4.

update(StoreId, PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:update(StoreId, PathPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% compare_and_swap().
%% -------------------------------------------------------------------

-spec compare_and_swap(PathPattern, DataPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% Calling this function is the same as calling `compare_and_swap(StoreId,
%% PathPattern, DataPattern, Data)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see compare_and_swap/4.
%% @see compare_and_swap/5.

compare_and_swap(PathPattern, DataPattern, Data) ->
    StoreId = khepri_cluster:get_default_store_id(),
    compare_and_swap(StoreId, PathPattern, DataPattern, Data).

-spec compare_and_swap(StoreId, PathPattern, DataPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% Calling this function is the same as calling `compare_and_swap(StoreId,
%% PathPattern, DataPattern, Data, #{})'.
%%
%% @see compare_and_swap/5.

compare_and_swap(StoreId, PathPattern, DataPattern, Data) ->
    compare_and_swap(StoreId, PathPattern, DataPattern, Data, #{}).

-spec compare_and_swap(StoreId, PathPattern, DataPattern, Data, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple is
%% returned.
%%
%% Internally, the `PathPattern' is modified to include an
%% `#if_data_matches{pattern = DataPattern}' condition on its last component.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see put/4.
%% @see create/4.
%% @see update/4.
%% @see khepri_adv:compare_and_swap/5.

compare_and_swap(StoreId, PathPattern, DataPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:compare_and_swap(
            StoreId, PathPattern, DataPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% Calling this function is the same as calling `delete(StoreId, PathPattern)'
%% with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see delete/2.
%% @see delete/3.

delete(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete(StoreId, PathPattern).

-spec delete
(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`delete(StoreId, PathPattern)'. Calling it is the same as calling
%% `delete(StoreId, PathPattern, #{})'.</li>
%% <li>`delete(PathPattern, Options)'. Calling it is the same as calling
%% `delete(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see delete/3.

delete(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    delete(StoreId, PathPattern, #{});
delete(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete(StoreId, PathPattern, Options).

-spec delete(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words, deleting
%% many nodes is denied. That fact is checked before the tree node is looked
%% up: so if a condition in the path could potentially match several nodes, an
%% exception is raised, even though only one tree node would match at the time.
%% If you want to delete multiple nodes at once, use {@link delete_many/3}.
%%
%% Example:
%% ```
%% %% Delete the tree node at `/:foo/:bar'.
%% ok = khepri:delete(StoreId, [foo, bar]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the node to delete.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see delete_many/3.
%% @see khepri_adv:delete/3.

delete(StoreId, PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:delete(StoreId, PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete_many().
%% -------------------------------------------------------------------

-spec delete_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `delete_many(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see delete_many/2.
%% @see delete_many/3.

delete_many(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete_many(StoreId, PathPattern).

-spec delete_many
(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri:minimal_ret().

%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`delete_many(StoreId, PathPattern)'. Calling it is the same as calling
%% `delete(StoreId, PathPattern, #{})'.</li>
%% <li>`delete_many(PathPattern, Options)'. Calling it is the same as calling
%% `delete(StoreId, PathPattern, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see delete_many/3.

delete_many(StoreId, PathPattern) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    delete_many(StoreId, PathPattern, #{});
delete_many(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete_many(StoreId, PathPattern, Options).

-spec delete_many(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% Example:
%% ```
%% %% Delete all nodes in the tree.
%% ok = khepri:delete_many(StoreId, [?KHEPRI_WILDCARD_STAR]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to delete.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see delete/3.

delete_many(StoreId, PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:delete_many(StoreId, PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% clear_payload().
%% -------------------------------------------------------------------

-spec clear_payload(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `clear_payload(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see clear_payload/2.
%% @see clear_payload/3.

clear_payload(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    clear_payload(StoreId, PathPattern).

-spec clear_payload(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `clear_payload(StoreId,
%% PathPattern, #{})'.
%%
%% @see clear_payload/3.

clear_payload(StoreId, PathPattern) ->
    clear_payload(StoreId, PathPattern, #{}).

-spec clear_payload(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% In other words, the payload is set to {@link khepri_payload:no_payload()}.
%% Otherwise, the behavior is that of {@link put/4}.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see put/4.
%% @see khepri_adv:clear_payload/3.

clear_payload(StoreId, PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:clear_payload(StoreId, PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% clear_many_payloads().
%% -------------------------------------------------------------------

-spec clear_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `clear_many_payloads(StoreId,
%% PathPattern)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see clear_many_payloads/2.
%% @see clear_many_payloads/3.

clear_many_payloads(PathPattern) ->
    StoreId = khepri_cluster:get_default_store_id(),
    clear_many_payloads(StoreId, PathPattern).

-spec clear_many_payloads(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `clear_many_payloads(StoreId,
%% PathPattern, #{})'.
%%
%% @see clear_many_payloads/3.

clear_many_payloads(StoreId, PathPattern) ->
    clear_many_payloads(StoreId, PathPattern, #{}).

-spec clear_many_payloads(StoreId, PathPattern, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() | khepri_machine:async_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% In other words, the payload is set to {@link khepri_payload:no_payload()}.
%% Otherwise, the behavior is that of {@link put/4}.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to modify.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, `ok' or an `{error, Reason}'
%% tuple; in the case of an asynchronous call, always `ok' (the actual return
%% value may be sent by a message if a correlation ID was specified).
%%
%% @see delete_many/3.
%% @see put/4.
%% @see khepri_adv:clear_many_payloads/3.

clear_many_payloads(StoreId, PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_adv:clear_many_payloads(
            StoreId, PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% register_trigger().
%% -------------------------------------------------------------------

-spec register_trigger(TriggerId, EventFilter, StoredProcPath) -> Ret when
      TriggerId :: trigger_id(),
      EventFilter :: khepri_evf:event_filter() |
                     khepri_path:pattern(),
      StoredProcPath :: khepri_path:path(),
      Ret :: ok | error().
%% @doc Registers a trigger.
%%
%% Calling this function is the same as calling `register_trigger(StoreId,
%% TriggerId, EventFilter, StoredProcPath)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see register_trigger/4.

register_trigger(TriggerId, EventFilter, StoredProcPath) ->
    StoreId = khepri_cluster:get_default_store_id(),
    register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath).

-spec register_trigger
(StoreId, TriggerId, EventFilter, StoredProcPath) -> Ret when
      StoreId :: khepri:store_id(),
      TriggerId :: trigger_id(),
      EventFilter :: khepri_evf:event_filter() |
                     khepri_path:pattern(),
      StoredProcPath :: khepri_path:path(),
      Ret :: ok | error();
(TriggerId, EventFilter, StoredProcPath, Options) -> Ret when
      TriggerId :: trigger_id(),
      EventFilter :: khepri_evf:event_filter() |
                     khepri_path:pattern(),
      StoredProcPath :: khepri_path:path(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: ok | error().
%% @doc Registers a trigger.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath)'.
%% Calling it is the same as calling `register_trigger(StoreId, TriggerId,
%% EventFilter, StoredProcPath, #{})'.</li>
%% <li>`register_trigger(TriggerId, EventFilter, StoredProcPath, Options)'.
%% Calling it is the same as calling `register_trigger(StoreId, TriggerId,
%% EventFilter, StoredProcPath, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see register_trigger/5.

register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(TriggerId) ->
    register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath, #{});
register_trigger(TriggerId, EventFilter, StoredProcPath, Options)
  when is_atom(TriggerId) andalso is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    register_trigger(
      StoreId, TriggerId, EventFilter, StoredProcPath, Options).

-spec register_trigger(
        StoreId, TriggerId, EventFilter, StoredProcPath, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      TriggerId :: trigger_id(),
      EventFilter :: khepri_evf:event_filter() |
                     khepri_path:pattern(),
      StoredProcPath :: khepri_path:path(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: ok | error().
%% @doc Registers a trigger.
%%
%% A trigger is based on an event filter. It associates an event with a stored
%% procedure. When an event matching the event filter is emitted, the stored
%% procedure is executed.
%%
%% The following event filters are documented by {@link
%% khepri_evf:event_filter()}.
%%
%% Here are examples of event filters:
%%
%% ```
%% %% An event filter can be explicitly created using the `khepri_evf'
%% %% module. This is possible to specify properties at the same time.
%% EventFilter = khepri_evf:tree([stock, wood, <<"oak">>], %% Required
%%                               #{on_actions => [delete], %% Optional
%%                                 priority => 10}).       %% Optional
%% '''
%% ```
%% %% For ease of use, some terms can be automatically converted to an event
%% %% filter. In this example, a Unix-like path can be used as a tree event
%% %% filter.
%% EventFilter = "/:stock/:wood/oak".
%% '''
%%
%% The stored procedure is expected to accept a single argument. This argument
%% is a map containing the event properties. Here is an example:
%%
%% ```
%% my_stored_procedure(Props) ->
%%     #{path := Path},
%%       on_action => Action} = Props.
%% '''
%%
%% The stored procedure is executed on the leader's Erlang node.
%%
%% It is guaranteed to run at least once. It could be executed multiple times
%% if the Ra leader changes, therefore the stored procedure must be
%% idempotent.
%%
%% @param StoreId the name of the Khepri store.
%% @param TriggerId the name of the trigger.
%% @param EventFilter the event filter used to associate an event with a
%%        stored procedure.
%% @param StoredProcPath the path to the stored procedure to execute when the
%%        corresponding event occurs.
%%
%% @returns `ok' if the trigger was registered, an `{error, Reason}' tuple
%% otherwise.

register_trigger(StoreId, TriggerId, EventFilter, StoredProcPath, Options) ->
    khepri_machine:register_trigger(
      StoreId, TriggerId, EventFilter, StoredProcPath, Options).

%% -------------------------------------------------------------------
%% register_projection().
%% -------------------------------------------------------------------

-spec register_projection(PathPattern, Projection) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Projection :: khepri_projection:projection(),
      Ret :: ok | khepri:error().
%% @doc Registers a projection.
%%
%% Calling this function is the same as calling
%% `register_projection(StoreId, PathPattern, Projection)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see register_projection/3.

register_projection(PathPattern, Projection) ->
    StoreId = khepri_cluster:get_default_store_id(),
    register_projection(StoreId, PathPattern, Projection).

-spec register_projection
(StoreId, PathPattern, Projection) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Projection :: khepri_projection:projection(),
      Ret :: ok | khepri:error();
(PathPattern, Projection, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Projection :: khepri_projection:projection(),
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Registers a projection.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`register_projection(StoreId, PathPattern, Projection)'. Calling it is
%% the same as calling `register_projection(StoreId, PathPattern, Projection,
%% #{})'.</li>
%% <li>`register_projection(PathPattern, Projection, Options)'. Calling it is
%% the same as calling `register_projection(StoreId, PathPattern, Projection,
%% Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see register_projection/4.

register_projection(StoreId, PathPattern, Projection)
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    register_projection(StoreId, PathPattern, Projection, #{});
register_projection(PathPattern, Projection, Options)
  when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    register_projection(StoreId, PathPattern, Projection, Options).

-spec register_projection(StoreId, PathPattern, Projection, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Projection :: khepri_projection:projection(),
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Registers a projection.
%%
%% A projection is a replicated ETS cache which is kept up to date by
%% Khepri. See the {@link khepri_projection} module-docs for more information
%% about projections.
%%
%% This function associates a projection created with {@link
%% khepri_projection:new/3} with a pattern. Any changes to tree nodes matching
%% the provided pattern will be turned into records using the projection's
%% {@link khepri_projection:projection_fun()} and then applied to the
%% projection's ETS table.
%%
%% Registering a projection fills the projection's ETS table with records from
%% any tree nodes which match the `PathPattern' and are already in the store.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the pattern of tree nodes which the projection should
%%        watch.
%% @param Projection the projection resource, created with
%%        `khepri_projection:new/3'.
%% @param Options command options for registering the projection.
%% @returns `ok' if the projection was registered, an `{error, Reason}' tuple
%% otherwise.

register_projection(StoreId, PathPattern, Projection, Options) ->
    khepri_machine:register_projection(
      StoreId, PathPattern, Projection, Options).

%% -------------------------------------------------------------------
%% unregister_projections().
%% -------------------------------------------------------------------

-spec unregister_projections(Names) -> Ret when
      Names :: all | [khepri_projection:name()],
      Ret :: ok | khepri:error().
%% @doc Removes the given projections from the store.
%%
%% Calling this function is the same as calling
%% `unregister_projections(StoreId, Names)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see unregister_projections/2.

unregister_projections(Names) when Names =:= all orelse is_list(Names) ->
    StoreId = khepri_cluster:get_default_store_id(),
    unregister_projections(StoreId, Names).

-spec unregister_projections
(StoreId, Names) -> Ret when
      StoreId :: khepri:store_id(),
      Names :: all | [khepri_projection:name()],
      Ret :: ok | khepri:error();
(Names, Options) -> Ret when
      Names :: all | [khepri_projection:name()],
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Removes the given projections from the store.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`unregister_projections(StoreId, Names)'. Calling it is the same as
%% calling `unregister_projections(StoreId, Names, #{})'.</li>
%% <li>`unregister_projections(Names, Options)'. Calling it is the same as
%% calling `unregister_projections(StoreId, Names, Options)' with
%% the default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%% </li>
%% </ul>
%%
%% @see unregister_projections/3.

unregister_projections(StoreId, Names)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (Names =:= all orelse is_list(Names)) ->
    unregister_projections(StoreId, Names, #{});
unregister_projections(Names, Options)
  when (Names =:= all orelse is_list(Names)) andalso is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    unregister_projections(StoreId, Names, Options).

-spec unregister_projections(StoreId, Names, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      Names :: all | [khepri_projection:name()],
      Options :: khepri:command_options(),
      Ret :: ok | khepri:error().
%% @doc Removes the given projections from the store.
%%
%% `Names' may either be a list of projection names to remove or the atom
%% `all'. When `all' is passed, every projection in the store is removed.
%%
%% @param StoreId the name of the Khepri store.
%% @param Names the names of projections to unregister or the atom `all' to
%%        remove all projections.
%% @param Options command options for unregistering the projections.
%% @returns `ok' if the projections were unregistered, an `{error, Reason}'
%% tuple otherwise.
%%
%% @see khepri_adv:unregister_projections/3.

unregister_projections(StoreId, Names, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (Names =:= all orelse is_list(Names)) andalso
       is_map(Options) ->
    Ret = khepri_adv:unregister_projections(StoreId, Names, Options),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% has_projection().
%% -------------------------------------------------------------------

-spec has_projection(ProjectionName) -> Ret when
      ProjectionName :: khepri_projection:name(),
      Ret :: boolean() | khepri:error().
%% @doc Determines whether the store has a projection registered with the given
%% name.
%%
%% Calling this function is the same as calling
%% `has_projection(StoreId, ProjectionName)' with the default store ID
%% (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see has_projection/2.

has_projection(ProjectionName) when is_atom(ProjectionName) ->
    StoreId = khepri_cluster:get_default_store_id(),
    has_projection(StoreId, ProjectionName).

-spec has_projection
(StoreId, ProjectionName) -> Ret when
      StoreId :: khepri:store_id(),
      ProjectionName :: khepri_projection:name(),
      Ret :: boolean() | khepri:error();
(ProjectionName, Options) -> Ret when
      ProjectionName :: khepri_projection:name(),
      Options :: khepri:query_options(),
      Ret :: boolean() | khepri:error().
%% @doc Determines whether the store has a projection registered with the given
%% name.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`has_projection(StoreId, ProjectionName)'. Calling it is the same
%% as calling `has_projection(StoreId, ProjectionName, #{})'.</li>
%% <li>`has_projection(ProjectionName, Options)'. Calling it is the same
%% as calling `has_projection(StoreId, ProjectionName, Options)' with
%% the default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%% </li>
%% </ul>
%%
%% @see has_projection/3.

has_projection(StoreId, ProjectionName)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(ProjectionName) ->
    has_projection(StoreId, ProjectionName, #{});
has_projection(ProjectionName, Options)
  when is_atom(ProjectionName) andalso is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    has_projection(StoreId, ProjectionName, Options).

-spec has_projection(StoreId, ProjectionName, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      ProjectionName :: khepri_projection:name(),
      Options :: khepri:query_options(),
      Ret :: boolean() | khepri:error().
%% @doc Determines whether the store has a projection registered with the given
%% name.
%%
%% @param StoreId the name of the Khepri store.
%% @param ProjectionName the name of the projection to has as passed to
%%        {@link khepri_projection:new/3}.
%% @param Options query options.
%% @returns `true' if the store contains a projection registered with the given
%% name, `false' if it does not, or an `{error, Reason}' tuple if the query
%% failed.

has_projection(StoreId, ProjectionName, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(ProjectionName) andalso
       is_map(Options) ->
    case khepri_machine:get_projections_state(StoreId, Options) of
        {ok, ProjectionTree} ->
            khepri_machine:has_projection(ProjectionTree, ProjectionName);
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% transaction().
%% -------------------------------------------------------------------

-spec transaction(FunOrPath) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:tx_ret().
%% @doc Runs a transaction and returns its result.
%%
%% Calling this function is the same as calling `transaction(FunOrPath, [])'
%% with the default store ID.
%%
%% @see transaction/2.

transaction(FunOrPath) ->
    transaction(FunOrPath, []).

-spec transaction
(StoreId, FunOrPath) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:tx_ret();
(FunOrPath, Args) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Ret :: khepri_machine:tx_ret();
(FunOrPath, ReadWriteOrOptions) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      ReadWriteOrOptions :: ReadWrite | Options,
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().
%% @doc Runs a transaction and returns its result.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`transaction(StoreId, FunOrPath)'. Calling it is the same as calling
%% `transaction(StoreId, FunOrPath, [])'.</li>
%% <li>`transaction(FunOrPath, Args)'. Calling it is the same as calling
%% `transaction(StoreId, FunOrPath, Args)' with the default store ID.</li>
%% <li>`transaction(FunOrPath, ReadWriteOrOptions)'. Calling it is the same as
%% calling `transaction(StoreId, FunOrPath, [], ReadWriteOrOptions)' with the
%% default store ID.</li>
%% </ul>
%%
%% @see transaction/3.

transaction(FunOrPath, Args)
  when (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       is_list(Args) ->
    StoreId = khepri_cluster:get_default_store_id(),
    transaction(StoreId, FunOrPath, Args);
transaction(FunOrPath, ReadWriteOrOptions)
  when (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       (is_atom(ReadWriteOrOptions) orelse is_map(ReadWriteOrOptions)) ->
    StoreId = khepri_cluster:get_default_store_id(),
    transaction(StoreId, FunOrPath, ReadWriteOrOptions);
transaction(StoreId, FunOrPath)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) ->
    transaction(StoreId, FunOrPath, []).

-spec transaction
(StoreId, FunOrPath, Args) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Ret :: khepri_machine:tx_ret();
(StoreId, FunOrPath, ReadWriteOrOptions) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      ReadWriteOrOptions :: ReadWrite | Options,
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret();
(FunOrPath, Args, ReadWriteOrOptions) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      ReadWriteOrOptions :: ReadWrite | Options,
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret();
(FunOrPath, ReadWrite, Options) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().
%% @doc Runs a transaction and returns its result.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`transaction(StoreId, FunOrPath, Args)'. Calling it is the same as
%% calling `transaction(StoreId, FunOrPath, Args, auto)'.</li>
%% <li>`transaction(StoreId, FunOrPath, ReadWriteOrOptions)'. Calling it is
%% the same as calling `transaction(StoreId, FunOrPath, [],
%% ReadWriteOrOptions)'.</li>
%% <li>`transaction(FunOrPath, Args, ReadWriteOrOptions)'. Calling it is the
%% same as calling `transaction(StoreId, FunOrPath, Args, ReadWriteOrOptions)'
%% with the default store ID.</li>
%% </ul>
%%
%% @see transaction/4.

transaction(StoreId, FunOrPath, Args)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath))
       andalso is_list(Args) ->
    transaction(StoreId, FunOrPath, Args, auto);
transaction(StoreId, FunOrPath, ReadWriteOrOptions)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       (is_atom(ReadWriteOrOptions) orelse is_map(ReadWriteOrOptions)) ->
    transaction(StoreId, FunOrPath, [], ReadWriteOrOptions);
transaction(FunOrPath, Args, ReadWriteOrOptions)
  when (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath))
       andalso is_list(Args) andalso
       (is_atom(ReadWriteOrOptions) orelse is_map(ReadWriteOrOptions)) ->
    StoreId = khepri_cluster:get_default_store_id(),
    transaction(StoreId, FunOrPath, Args, ReadWriteOrOptions);
transaction(FunOrPath, ReadWrite, Options)
  when (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath))
       andalso is_atom(ReadWrite) andalso is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    transaction(StoreId, FunOrPath, ReadWrite, Options).

-spec transaction
(StoreId, FunOrPath, Args, ReadWrite) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      ReadWrite :: ro | rw | auto,
      Ret :: khepri_machine:tx_ret();
(StoreId, FunOrPath, Args, Options) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret();
(StoreId, FunOrPath, ReadWrite, Options) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret();
(FunOrPath, Args, ReadWrite, Options) -> Ret when
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      ReadWrite :: ro | rw | auto,
      Options :: command_options() | query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().
%% @doc Runs a transaction and returns its result.
%%
%% This function accepts the following three forms:
%% <ul>
%% <li>`transaction(StoreId, FunOrPath, Args, ReadWrite)'. Calling it is the
%% same as calling `transaction(StoreId, FunOrPath, Args, ReadWrite,
%% #{})'.</li>
%% <li>`transaction(StoreId, FunOrPath, Args, Options)'. Calling it is the
%% same as calling `transaction(StoreId, FunOrPath, Args, auto,
%% Options)'.</li>
%% <li>`transaction(FunOrPath, Args, ReadWrite, Options)'. Calling it is the
%% same as calling `transaction(StoreId, FunOrPath, Args, ReadWrite, Options)'
%% with the default store ID.</li>
%% </ul>
%%
%% @see transaction/5.

transaction(StoreId, FunOrPath, Args, ReadWrite)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       is_list(Args) andalso is_atom(ReadWrite) ->
    transaction(StoreId, FunOrPath, Args, ReadWrite, #{});
transaction(StoreId, FunOrPath, Args, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       is_list(Args) andalso is_map(Options) ->
    transaction(StoreId, FunOrPath, Args, auto, Options);
transaction(StoreId, FunOrPath, ReadWrite, Options)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso
       (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       is_atom(ReadWrite) andalso is_map(Options) ->
    transaction(StoreId, FunOrPath, [], ReadWrite, Options);
transaction(FunOrPath, Args, ReadWrite, Options)
  when (is_function(FunOrPath) orelse
        ?IS_KHEPRI_PATH_PATTERN(FunOrPath)) andalso
       is_list(Args) andalso
       is_atom(ReadWrite) andalso is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    transaction(StoreId, FunOrPath, Args, ReadWrite, Options).

-spec transaction(StoreId, FunOrPath, Args, ReadWrite, Options) -> Ret when
      StoreId :: store_id(),
      FunOrPath :: Fun | PathPattern,
      Fun :: khepri_tx:tx_fun(),
      PathPattern :: khepri_path:pattern(),
      Args :: list(),
      ReadWrite :: ro | rw | auto,
      Options :: khepri:command_options() | khepri:query_options(),
      Ret :: khepri_machine:tx_ret() | khepri_machine:async_ret().
%% @doc Runs a transaction and returns its result.
%%
%% `Fun' is an arbitrary anonymous function which takes the content of `Args'
%% as its arguments. In other words, the length of `Args' must correspond to
%% the arity of `Fun'.
%%
%% Instead of `Fun', `PathPattern' can be passed. It must point to an existing
%% stored procedure. The length to `Args' must correspond to the arity of that
%% stored procedure.
%%
%% The `ReadWrite' flag determines what the `Fun' anonymous function is
%% allowed to do and in which context it runs:
%%
%% <ul>
%% <li>If `ReadWrite' is `ro', `Fun' can do whatever it wants, except modify
%% the content of the store. In other words, uses of {@link khepri_tx:put/2}
%% or {@link khepri_tx:delete/1} are forbidden and will abort the function.
%% `Fun' is executed from a process on the leader Ra member.</li>
%% <li>If `ReadWrite' is `rw', `Fun' can use the {@link khepri_tx} transaction
%% API as well as any calls to other modules as long as those functions or what
%% they do is permitted. See {@link khepri_tx} for more details. If `Fun' does
%% or calls something forbidden, the transaction will be aborted. `Fun' is
%% executed in the context of the state machine process on each Ra
%% members.</li>
%% <li>If `ReadWrite' is `auto', `Fun' is analyzed to determine if it calls
%% {@link khepri_tx:put/2} or {@link khepri_tx:delete/1}, or uses any denied
%% operations for a read/write transaction. If it does, this is the same as
%% setting `ReadWrite' to true. Otherwise, this is the equivalent of setting
%% `ReadWrite' to false.</li>
%% </ul>
%%
%% When using `PathPattern', a `ReadWrite' of `auto' is synonymous of `rw'.
%%
%% `Options' is relevant for both read-only and read-write transactions
%% (including audetected ones). However note that both types expect different
%% options.
%%
%% The result of `FunOrPath' can be any term. That result is returned in an
%% `{ok, Result}' tuple if the transaction is synchronous. The result is sent
%% by message if the transaction is asynchronous and a correlation ID was
%% specified.
%%
%% @param StoreId the name of the Khepri store.
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

transaction(StoreId, FunOrPath, Args, ReadWrite, Options) ->
    khepri_machine:transaction(StoreId, FunOrPath, Args, ReadWrite, Options).

%% -------------------------------------------------------------------
%% fence().
%% -------------------------------------------------------------------

-spec fence() -> Ret when
      Ret :: ok | khepri:error().
%% @doc Blocks until all updates received by the cluster leader are applied
%% locally.
%%
%% Calling this function is the same as calling `fence(StoreId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see fence/1.
%% @see fence/2.

fence() ->
    StoreId = khepri_cluster:get_default_store_id(),
    fence(StoreId).

-spec fence(StoreId | Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Blocks until all updates received by the cluster leader are applied
%% locally.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`fence(StoreId)'. Calling it is the same as calling `fence(StoreId,
%% Timeout)' with the default timeout (see {@link
%% khepri_app:get_default_timeout/0}).</li>
%% <li>`fence(Timeout)'. Calling it is the same as calling `fence(StoreId,
%% Timeout)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see fence/2.

fence(Timeout) when Timeout =:= infinity orelse is_integer(Timeout) ->
    StoreId = khepri_cluster:get_default_store_id(),
    fence(StoreId, Timeout);
fence(StoreId) ->
    Timeout = khepri_app:get_default_timeout(),
    fence(StoreId, Timeout).

-spec fence(StoreId, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Timeout :: timeout(),
      Ret :: ok | khepri:error().
%% @doc Blocks until all updates received by the cluster leader are applied
%% locally.
%%
%% This ensures that a subsequent query will see the result of synchronous and
%% asynchronous updates.
%%
%% This can't work however if:
%% <ul>
%% <li>Asynchronous updates have a correlation ID, in which case the caller is
%% responsible for waiting for the replies.</li>
%% <li>The default `reply_from => local' command option is overridden by
%% something else.</li>
%% </ul>
%%
%% @param StoreId the name of the Khepri store.
%% @param Timeout the time limit after which the call returns with an error.
%%
%% @returns `ok' or an `{error, Reason}' tuple.

fence(StoreId, Timeout) ->
    khepri_machine:fence(StoreId, Timeout).

%% -------------------------------------------------------------------
%% handle_async_ret().
%% -------------------------------------------------------------------

-spec handle_async_ret(RaEvent) -> Ret when
      RaEvent :: ra_server_proc:ra_event(),
      Ret :: [{CorrelationId, AsyncRet}, ...],
      CorrelationId :: ra_server:command_correlation(),
      AsyncRet :: khepri:async_ret().
%% @doc Handles the Ra event sent for asynchronous call results.
%%
%% Calling this function is the same as calling
%% `handle_async_ret(StoreId, RaEvent)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see handle_async_ret/2.

handle_async_ret(RaEvent) ->
    StoreId = khepri_cluster:get_default_store_id(),
    handle_async_ret(StoreId, RaEvent).

-spec handle_async_ret(StoreId, RaEvent) -> Ret when
      StoreId :: khepri:store_id(),
      RaEvent :: ra_server_proc:ra_event(),
      Ret :: [{CorrelationId, AsyncRet}, ...],
      CorrelationId :: ra_server:command_correlation(),
      AsyncRet :: khepri:async_ret().
%% @doc Handles the Ra event sent for asynchronous call results.
%%
%% When sending commands with `async' {@link command_options()}, the calling
%% process will receive Ra events with the following structure:
%%
%% `{ra_event, CurrentLeader, {applied, [{Correlation1, Reply1}, ...]}}'
%%
%% or
%%
%% `{ra_event,
%%   FromId,
%%   {rejected, {not_leader, Leader | undefined, Correlation}}}'
%%
%% The first event acknowledges all commands handled in a batch while the
%% second is sent per-command when commands are sent against a non-leader
%% member.
%%
%% These events should be passed to this function in order to map the return
%% values from the async commands and to update leader information. This
%% function does not handle retrying rejected commands or return values from
%% applied commands - the caller is responsible for those tasks.
%%
%% Example:
%% ```
%% ok = khepri:put(StoreId, [stock, wood, <<"oak">>], 200, #{async => 1}),
%% ok = khepri:put(StoreId, [stock, wood, <<"maple">>], 150, #{async => 2}),
%% RaEvent = receive {ra_event, _, _} = Event -> Event end,
%% ?assertMatch(
%%   [{1, {ok, #{[stock, wood, <<"oak">>] => _}}},
%%    {2, {ok, #{[stock, wood, <<"maple">>] => _}}}],
%%   khepri:handle_async_ret(RaEvent)).
%% '''
%%
%% @see async_option().
%% @see ra:pipeline_command/4.

handle_async_ret(
  StoreId,
  {ra_event, _CurrentLeader, {applied, Correlations0}})
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    lists:map(
      fun({CorrelationId, Reply0}) ->
          Reply = case Reply0 of
                      {exception, _, _, _} = Exception ->
                          khepri_machine:handle_tx_exception(Exception);
                      ok ->
                          Reply0;
                      {ok, _} ->
                          Reply0;
                      {error, _} ->
                          Reply0
                  end,
          {CorrelationId, Reply}
      end, Correlations0);
handle_async_ret(
  StoreId,
  {ra_event, _RaServer,
   {rejected, {not_leader, LeaderId, CorrelationId}}})
  when ?IS_KHEPRI_STORE_ID(StoreId) ->
    [{CorrelationId, {error, {not_leader, LeaderId}}}].

%% -------------------------------------------------------------------
%% Bang functions.
%% -------------------------------------------------------------------

-include("khepri_bang.hrl").

%% -------------------------------------------------------------------
%% Import/export (backup & restore in Mnesia terms).
%% -------------------------------------------------------------------

-spec export(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @doc Exports a Khepri store using the `Module' callback module.
%%
%% Calling this function is the same as calling `export(StoreId, Module,
%% ModulePriv)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see export/3.
%% @see export/4.

export(Module, ModulePriv) when is_atom(Module) ->
    StoreId = khepri_cluster:get_default_store_id(),
    export(StoreId, Module, ModulePriv).

-spec export(StoreId | PathPattern, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @doc Exports a Khepri store using the `Module' callback module.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`export(StoreId, Module, ModulePriv)'. Calling it is the same as
%% calling `export(StoreId, "**", Module, ModulePriv)'.</li>
%% <li>`export(PathPattern, Module, ModulePriv)'. Calling it is the same as
%% calling `export(StoreId, PathPattern, Module, ModulePriv)' with the default
%% store ID (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see export/4.

export(StoreId, Module, ModulePriv)
  when?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(Module) ->
    export(StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Module, ModulePriv);
export(PathPattern, Module, ModulePriv)
  when is_atom(Module) ->
    StoreId = khepri_cluster:get_default_store_id(),
    export(StoreId, PathPattern, Module, ModulePriv).

-spec export(StoreId, PathPattern, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @doc Exports a Khepri store using the `Module' callback module.
%%
%% The `PathPattern' allows to filter which tree nodes are exported. The path
%% pattern can be provided as a native path pattern (a list of tree node names
%% and conditions) or as a string. See {@link khepri_path:from_string/1}.
%%
%% `Module' is the callback module called to perform the actual export. It
%% must conform to the Mnesia Backup &amp; Restore API. See {@link
%% khepri_import_export} for more details.
%%
%% `ModulePriv' is the term passed to `Module:open_write/1'.
%%
%% Example: export the full Khepri store using {@link khepri_export_erlang} as
%% the callback module
%% ```
%% ok = khepri:export(StoreId, khepri_export_erlang, "export-1.erl").
%% '''
%%
%% Example: export a subset of the Khepri store
%% ```
%% ok = khepri:export(
%%        StoreId,
%%        "/:stock/:wood/**",
%%        khepri_export_erlang,
%%        "export-wood-stock-1.erl").
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path pattern matching the tree nodes to export.
%% @param Module the callback module to use to export.
%% @param ModulePriv arguments passed to `Module:open_write/1'.
%%
%% @returns `ok' or an `{ok, Term}' tuple if the export succeeded (the actual
%% return value depends on whether the callback module wants to return anything
%% to the caller), or an `{error, Reason}' tuple if it failed.
%%
%% @see import/3.

export(StoreId, PathPattern, Module, ModulePriv)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(Module) ->
    khepri_import_export:export(StoreId, PathPattern, Module, ModulePriv).

-spec import(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @doc Imports a previously exported set of tree nodes using the `Module'
%% callback module.
%%
%% Calling this function is the same as calling `import(StoreId, Module,
%% ModulePriv)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see import/3.

import(Module, ModulePriv) when is_atom(Module) ->
    StoreId = khepri_cluster:get_default_store_id(),
    import(StoreId, Module, ModulePriv).

-spec import(StoreId, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @doc Imports a previously exported set of tree nodes using the `Module'
%% callback module.
%%
%% `Module' is the callback module called to perform the actual import. It
%% must conform to the Mnesia Backup &amp; Restore API. See {@link
%% khepri_import_export} for more details.
%%
%% `ModulePriv' is the term passed to `Module:open_read/1'.
%%
%% Importing something doesn't delete existing tree nodes. The caller is
%% responsible for deleting the existing content of a store if he needs to.
%%
%% Example: import a set of tree nodes using {@link khepri_export_erlang} as
%% the callback module
%% ```
%% ok = khepri:import(StoreId, khepri_export_erlang, "export-1.erl").
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param Module the callback module to use to import.
%% @param ModulePriv arguments passed to `Module:open_read/1'.
%%
%% @returns `ok' or an `{ok, Term}' tuple if the import succeeded (the actual
%% return value depends on whether the callback module wants to return anything
%% to the caller), or an `{error, Reason}' tuple if it failed.
%%
%% @see export/3.

import(StoreId, Module, ModulePriv)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(Module) ->
    khepri_import_export:import(StoreId, Module, ModulePriv).

%% -------------------------------------------------------------------
%% Public helpers.
%% -------------------------------------------------------------------

-spec info() -> ok.
%% @doc Lists the running stores on <em>stdout</em>.

info() ->
    StoreIds = get_store_ids(),
    case StoreIds of
        [] ->
            io:format("No stores running~n");
        _ ->
            io:format("Running stores:~n"),
            lists:foreach(
              fun(StoreId) ->
                      io:format("  ~ts~n", [StoreId])
              end, StoreIds)
    end,
    ok.

-spec info(StoreId) -> ok when
      StoreId :: store_id().
%% @doc Lists the content of specified store on <em>stdout</em>.
%%
%% @param StoreId the name of the Khepri store.

info(StoreId) ->
    info(StoreId, #{}).

-spec info(StoreId, Options) -> ok when
      StoreId :: khepri:store_id(),
      Options :: khepri:query_options().
%% @doc Lists the content of specified store on <em>stdout</em>.
%%
%% @param StoreId the name of the Khepri store.
%% @param Options query options.

info(StoreId, Options) ->
    io:format("~n\033[1;32m== CLUSTER MEMBERS ==\033[0m~n~n", []),
    case khepri_cluster:nodes(StoreId) of
        {ok, Nodes} ->
            Nodes1 = lists:sort(Nodes),
            lists:foreach(fun(Node) -> io:format("~ts~n", [Node]) end, Nodes1);
        {error, _} = Error ->
            io:format("Failed to query cluster members: ~p~n", [Error])
    end,

    case khepri_machine:get_keep_while_conds_state(StoreId, Options) of
        {ok, KeepWhileConds} when KeepWhileConds =/= #{} ->
            io:format("~n\033[1;32m== LIFETIME DEPS ==\033[0m~n", []),
            WatcherList = lists:sort(maps:keys(KeepWhileConds)),
            lists:foreach(
              fun(Watcher) ->
                      io:format("~n\033[1m~p depends on:\033[0m~n", [Watcher]),
                      WatchedsMap = maps:get(Watcher, KeepWhileConds),
                      Watcheds = lists:sort(maps:keys(WatchedsMap)),
                      lists:foreach(
                        fun(Watched) ->
                                Condition = maps:get(Watched, WatchedsMap),
                                io:format(
                                  "    ~p:~n"
                                  "        ~p~n",
                                  [Watched, Condition])
                        end, Watcheds)
              end, WatcherList);
        _ ->
            ok
    end,

    case khepri_machine:get_projections_state(StoreId, Options) of
        {ok, ProjectionTree} ->
            case khepri_pattern_tree:is_empty(ProjectionTree) of
                true ->
                    ok;
                false ->
                    io:format("~n\033[1;32m== PROJECTIONS ==\033[0m~n", []),
                    khepri_pattern_tree:foreach(
                      ProjectionTree,
                      fun(PathPattern, Projections) ->
                              [begin
                                   Name = khepri_projection:name(Projection),
                                   io:format(
                                     "~n~p:~n"
                                     "    ~p~n", [Name, PathPattern])
                               end || Projection <- Projections]
                      end)
            end;
        _ ->
            ok
    end,

    case khepri_adv:get_many(StoreId, [?KHEPRI_WILDCARD_STAR_STAR], Options) of
        {ok, Result} ->
            io:format("~n\033[1;32m== TREE ==\033[0m~n~n●~n", []),
            Tree = khepri_utils:flat_struct_to_tree(Result),
            khepri_utils:display_tree(Tree);
        _ ->
            ok
    end,
    ok.
