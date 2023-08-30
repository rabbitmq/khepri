%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Khepri database advanced API.
%%
%% This module exposes variants of the functions in {@link khepri} which
%% return more detailed return values for advanced use cases. Here are some
%% examples of what can be achieved with this module:
%%
%% <ul>
%% <li>Distinguish a tree node with the `undefined' atom as its payload, from
%% a tree node with no payload, from a non-existing tree node.</li>
%% <li>Know the payload version after a call to the functions based on {@link
%% put/4} which can be useful to perform transactional operations without
%% using {@link khepri:transaction/4}.</li>
%% </ul>
%%
%% Functions provided by {@link khepri} are implemented on top of this module
%% and simplify the return value for the more common use cases.

-module(khepri_adv).

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_ret.hrl").

-export([get/1, get/2, get/3,
         get_many/1, get_many/2, get_many/3,

         put/2, put/3, put/4,
         put_many/2, put_many/3, put_many/4,
         create/2, create/3, create/4,
         update/2, update/3, update/4,
         compare_and_swap/3, compare_and_swap/4, compare_and_swap/5,

         delete/1, delete/2, delete/3,
         delete_many/1, delete_many/2, delete_many/3,
         clear_payload/1, clear_payload/2, clear_payload/3,
         clear_many_payloads/1, clear_many_payloads/2,
         clear_many_payloads/3]).

%% For internal use only.
-export([process_command/3]).

-type node_props_map() :: #{khepri_path:native_path() => khepri:node_props()}.
%% Structure used to return a map of nodes and their associated properties,
%% payload and child nodes.

-type single_result() :: khepri:ok(khepri:node_props() | #{}) |
                         khepri:error().
%% Return value of a query or synchronous command targeting one specific tree
%% node.

-type many_results() :: khepri_machine:common_ret().
%% Return value of a query or synchronous command targeting many tree nodes.

-export_type([node_props_map/0,
              single_result/0,
              many_results/0]).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:single_result().
%% @doc Returns the properties and payload of the tree node pointed to by the
%% given path pattern.
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
      Ret :: khepri_adv:single_result();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri_adv:single_result().
%% @doc Returns the properties and payload of the tree node pointed to by the
%% given path pattern.
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

get(StoreId, PathPattern) when ?IS_STORE_ID(StoreId) ->
    get(StoreId, PathPattern, #{});
get(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get(StoreId, PathPattern, Options).

-spec get(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri_adv:single_result().
%% @doc Returns the properties and payload of the tree node pointed to by the
%% given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time. If you want to get multiple nodes at
%% once, use {@link get_many/3}.
%%
%% The returned `{ok, NodeProps}' tuple contains a map with the properties and
%% payload (if any) of the targeted tree node. If the tree node is not found,
%% `{error, ?khepri_error(node_not_found, Info)}' is returned.
%%
%% Example: query a tree node which holds the atom `value'
%% ```
%% %% Query the tree node at `/:foo/:bar'.
%% {ok, #{data := value,
%%        payload_version := 1}} = khepri_adv:get(StoreId, [foo, bar]).
%% '''
%%
%% Example: query an existing tree node with no payload
%% ```
%% %% Query the tree node at `/:no_payload'.
%% {ok, #{payload_version := 1}} = khepri_adv:get(StoreId, [no_payload]).
%% '''
%%
%% Example: query a non-existent tree node
%% ```
%% %% Query the tree node at `/:non_existent'.
%% {error, ?khepri_error(node_not_found, _)} = khepri_adv:get(
%%                                               StoreId, [non_existent]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to get.
%% @param Options query options.
%%
%% @returns an `{ok, NodeProps}' tuple or an `{error, Reason}' tuple.
%%
%% @see get_many/3.
%% @see khepri:get/3.

get(StoreId, PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true},
    Ret = get_many(StoreId, PathPattern, Options1),
    ?common_ret_to_single_result_ret(Ret).

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:many_results().
%% @doc Returns properties and payloads of all the tree nodes matching the
%% given path pattern.
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
      Ret :: khepri_adv:many_results();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri_adv:many_results().
%% @doc Returns properties and payloads of all the tree nodes matching the
%% given path pattern.
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

get_many(StoreId, PathPattern) when ?IS_STORE_ID(StoreId) ->
    get_many(StoreId, PathPattern, #{});
get_many(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    get_many(StoreId, PathPattern, Options).

-spec get_many(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options() | khepri:tree_options(),
      Ret :: khepri_adv:many_results().
%% @doc Returns properties and payloads of all the tree nodes matching the
%% given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The returned `{ok, NodePropsMap}' tuple contains a map where keys correspond
%% to the path to a tree node matching the path pattern. Each key then points
%% to a map containing the properties and payload (if any) of that matching
%% tree node.
%%
%% Example: query all nodes in the tree
%% ```
%% %% Get all nodes in the tree. The tree is:
%% %% <root>
%% %% `-- foo
%% %%     `-- bar = value
%% {ok, #{[foo] :=
%%        #{payload_version := 1},
%%        [foo, bar] :=
%%        #{data := value,
%%          payload_version := 1}}} = khepri_adv:get_many(
%%                                      StoreId,
%%                                      [?KHEPRI_WILDCARD_STAR_STAR]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to get.
%% @param Options query options such as `favor'.
%%
%% @returns an `{ok, NodePropsMap}' tuple or an `{error, Reason}' tuple.
%%
%% @see get/3.
%% @see khepri:get_many/3.

get_many(StoreId, PathPattern, Options) ->
    Fun = fun khepri_tree:collect_node_props_cb/3,
    khepri_machine:fold(StoreId, PathPattern, Fun, #{}, Options).

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec put(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret() |
             khepri_adv:single_result().
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
    {Command, CommandOptions} = khepri_command_adv:put(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec put(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret() |
             khepri_adv:single_result().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling `put(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see put/4.

put(StoreId, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:put(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec put(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:minimal_ret() |
             khepri_adv:single_result() |
             khepri_machine:async_ret().
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
%% The returned `{ok, NodeProps}' tuple contains a map with the properties and
%% payload (if any) of the targeted tree node: the payload was the one before
%% the update, other properties like the payload version correspond to the
%% updated node. If the targeted tree node didn't exist, `NodeProps' will be
%% an empty map.
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
%% When doing an asynchronous update, the {@link khepri:wait_for_async_ret/1}
%% function can be used to receive the message from Ra.
%%
%% The returned `{ok, NodeProps}' tuple contains a map with the properties and
%% payload (if any) of the targeted tree node as they were before the put.
%%
%% Example:
%% ```
%% %% Insert a tree node at `/:foo/:bar', overwriting the previous value.
%% {ok, #{data := value,
%%        payload_version := 1}} = khepri_adv:put(
%%                                   StoreId, [foo, bar], new_value).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to create or
%%        modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see create/4.
%% @see update/4.
%% @see compare_and_swap/5.
%% @see khepri:put/4.

put(StoreId, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:put(
                                  PathPattern, Data, Options),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

%% -------------------------------------------------------------------
%% put_many().
%% -------------------------------------------------------------------

-spec put_many(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:many_results().
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
    {Command, CommandOptions} = khepri_command_adv:put_many(PathPattern, Data),
    process_command(StoreId, Command, CommandOptions).

-spec put_many(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:many_results().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `put_many(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see put_many/4.

put_many(StoreId, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:put_many(PathPattern, Data),
    process_command(StoreId, Command, CommandOptions).

-spec put_many(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_adv:many_results() | khepri_machine:async_ret().
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
%% The returned `{ok, NodePropsMap}' tuple contains a map where keys
%% correspond to the path to a tree node matching the path pattern. Each key
%% then points to a map containing the properties and payload (if any) of the
%% targeted tree node: the payload was the one before the update, other
%% properties like the payload version correspond to the updated node.
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
%% When doing an asynchronous update, the {@link khepri:wait_for_async_ret/1}
%% function can be used to receive the message from Ra.
%%
%% Example:
%% ```
%% %% Set value of all tree nodes matching `/*/:bar', to `new_value'.
%% {ok, #{[foo, bar] :=
%%        #{data := value,
%%          payload_version := 1},
%%        [baz, bar] :=
%%        #{payload_version := 1}}} = khepri_adv:put_many(
%%                                      StoreId,
%%                                      [?KHEPRI_WILDCARD_STAR, bar],
%%                                      new_value).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to create or
%%        modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodePropsMap}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always `ok'
%% (the actual return value may be sent by a message if a correlation ID was
%% specified).
%%
%% @see put/4.
%% @see khepri:put_many/4.

put_many(StoreId, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:put_many(
                                  PathPattern, Data, Options),
    process_command(StoreId, Command, CommandOptions).

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
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
    {Command, CommandOptions} = khepri_command_adv:create(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec create(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
%% @doc Creates a tree node with the given payload.
%%
%% Calling this function is the same as calling `create(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see create/4.

create(StoreId, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:create(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec create(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_adv:single_result() | khepri_machine:async_ret().
%% @doc Creates a tree node with the given payload.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple
%% is returned.
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
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see put/4.
%% @see update/4.
%% @see khepri:create/4.

create(StoreId, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:create(
                                  PathPattern, Data, Options),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
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
    {Command, CommandOptions} = khepri_command_adv:update(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec update(StoreId, PathPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
%% @doc Updates an existing tree node with the given payload.
%%
%% Calling this function is the same as calling `update(StoreId, PathPattern,
%% Data, #{})'.
%%
%% @see update/4.

update(StoreId, PathPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:update(PathPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec update(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_adv:single_result() | khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple
%% is returned.
%%
%% Internally, the `PathPattern' is modified to include an
%% `#if_node_exists{exists = true}' condition on its last component.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see put/4.
%% @see create/4.
%% @see khepri:update/4.

update(StoreId, PathPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:update(
                                  PathPattern, Data, Options),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

%% -------------------------------------------------------------------
%% compare_and_swap().
%% -------------------------------------------------------------------

-spec compare_and_swap(PathPattern, DataPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
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
    {Command, CommandOptions} = khepri_command_adv:compare_and_swap(
                                  PathPattern, DataPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec compare_and_swap(StoreId, PathPattern, DataPattern, Data) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_adv:single_result().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% Calling this function is the same as calling `compare_and_swap(StoreId,
%% PathPattern, DataPattern, Data, #{})'.
%%
%% @see compare_and_swap/5.

compare_and_swap(StoreId, PathPattern, DataPattern, Data) ->
    {Command, CommandOptions} = khepri_command_adv:compare_and_swap(
                                  PathPattern, DataPattern, Data),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

-spec compare_and_swap(StoreId, PathPattern, DataPattern, Data, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_adv:single_result() | khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% The behavior is the same as {@link put/4} except that if the tree node
%% already exists, an `{error, ?khepri_error(mismatching_node, Info)}' tuple
%% is returned.
%%
%% Internally, the `PathPattern' is modified to include an
%% `#if_data_matches{pattern = DataPattern}' condition on its last component.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Data the Erlang term or function to store, or a {@link
%%        khepri_payload:payload()} structure.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see put/4.
%% @see khepri:compare_and_swap/5.

compare_and_swap(StoreId, PathPattern, DataPattern, Data, Options) ->
    {Command, CommandOptions} = khepri_command_adv:compare_and_swap(
                                  PathPattern, DataPattern, Data, Options),
    Ret = process_command(StoreId, Command, CommandOptions),
    ?common_ret_to_single_result_ret(Ret).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:single_result().
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
      Ret :: khepri_adv:single_result();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_adv:single_result().
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

delete(StoreId, PathPattern) when ?IS_STORE_ID(StoreId) ->
    delete(StoreId, PathPattern, #{});
delete(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete(StoreId, PathPattern, Options).

-spec delete(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_adv:single_result() | khepri_machine:async_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The `PathPattern' must target a specific tree node. In other words,
%% updating many nodes with the same payload is denied. That fact is checked
%% before the tree node is looked up: so if a condition in the path could
%% potentially match several nodes, an exception is raised, even though only
%% one tree node would match at the time. If you want to delete multiple nodes
%% at once, use {@link delete_many/3}.
%%
%% The returned `{ok, NodeProps}' tuple contains a map with the properties and
%% payload (if any) of the targeted tree node as they were before the delete.
%% If the targeted tree node didn't exist, `NodeProps' will be an empty map.
%%
%% When doing an asynchronous update, the {@link khepri:wait_for_async_ret/1}
%% function can be used to receive the message from Ra.
%%
%% Example:
%% ```
%% %% Delete the tree node at `/:foo/:bar'.
%% {ok, #{data := value,
%%        payload_version := 1}} = khepri_adv:delete(StoreId, [foo, bar]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to delete.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see delete_many/3.
%% @see khepri:delete/3.

delete(StoreId, PathPattern, Options) ->
    %% TODO: Not handled by khepri_machine:delete/3...
    Options1 = Options#{expect_specific_node => true},
    case khepri_machine:delete(StoreId, PathPattern, Options1) of
        {ok, NodePropsMap} ->
            %% It's ok to delete a non-existing tree node. The returned result
            %% will be an empty map, in which case we return `#{}' as the
            %% "node properties".
            NodeProps = case maps:values(NodePropsMap) of
                            [NP] -> NP;
                            []   -> #{}
                        end,
            {ok, NodeProps};
        Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% delete_many().
%% -------------------------------------------------------------------

-spec delete_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:many_results().
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
      Ret :: khepri_adv:many_results();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_adv:many_results().

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

delete_many(StoreId, PathPattern) when ?IS_STORE_ID(StoreId) ->
    delete_many(StoreId, PathPattern, #{});
delete_many(PathPattern, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    delete_many(StoreId, PathPattern, Options).

-spec delete_many(StoreId, PathPattern, Options) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() | khepri:tree_options(),
      Ret :: khepri_adv:many_results() | khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% The `PathPattern' can be provided as a native path pattern (a list of tree
%% node names and conditions) or as a string. See {@link
%% khepri_path:from_string/1}.
%%
%% The returned `{ok, NodePropsMap}' tuple contains a map where keys
%% correspond to the path to a deleted tree node. Each key then points to a
%% map containing the properties and payload (if any) of that deleted tree
%% node as they were before the delete.
%%
%% When doing an asynchronous update, the {@link khepri:wait_for_async_ret/1}
%% function can be used to receive the message from Ra.
%%
%% Example:
%% ```
%% %% Delete the tree node at `/:foo/:bar'.
%% {ok, #{[foo, bar] := #{data := value,
%%                        payload_version := 1},
%%        [baz, bar] := #{payload_version := 1}}} = khepri_adv:delete_many(
%%                                                    StoreId, [foo, bar]).
%% '''
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the nodes to delete.
%% @param Options command options such as the command type.
%%
%% @returns in the case of a synchronous call, an `{ok, NodePropsMap}' tuple
%% or an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see delete/3.
%% @see khepri:delete/3.

delete_many(StoreId, PathPattern, Options) ->
    khepri_machine:delete(StoreId, PathPattern, Options).

%% -------------------------------------------------------------------
%% clear_payload().
%% -------------------------------------------------------------------

-spec clear_payload(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:single_result().
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
      Ret :: khepri_adv:single_result().
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
      Ret :: khepri_adv:single_result() | khepri_machine:async_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% In other words, the payload is set to {@link khepri_payload:no_payload()}.
%% Otherwise, the behavior is that of {@link update/4}.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree node to modify.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodeProps}' tuple or
%% an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see update/4.
%% @see khepri:clear_payload/3.

clear_payload(StoreId, PathPattern, Options) ->
    Ret = update(StoreId, PathPattern, khepri_payload:none(), Options),
    case Ret of
        {error, ?khepri_error(node_not_found, _)} -> {ok, #{}};
        _                                         -> Ret
    end.

%% -------------------------------------------------------------------
%% clear_many_payloads().
%% -------------------------------------------------------------------

-spec clear_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:many_results().
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
    {Command, CommandOptions} = khepri_command_adv:clear_many_payloads(
                                  PathPattern),
    process_command(StoreId, Command, CommandOptions).

-spec clear_many_payloads(StoreId, PathPattern) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_adv:many_results().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling `clear_many_payloads(StoreId,
%% PathPattern, #{})'.
%%
%% @see clear_many_payloads/3.

clear_many_payloads(StoreId, PathPattern) ->
    {Command, CommandOptions} = khepri_command_adv:clear_many_payloads(
                                  PathPattern),
    process_command(StoreId, Command, CommandOptions).

-spec clear_many_payloads(StoreId, PathPattern, Options) ->
    Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri_adv:many_results() | khepri_machine:async_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% In other words, the payload is set to {@link khepri_payload:no_payload()}.
%% Otherwise, the behavior is that of {@link put/4}.
%%
%% @param StoreId the name of the Khepri store.
%% @param PathPattern the path (or path pattern) to the tree nodes to modify.
%% @param Extra extra options such as `keep_while' conditions.
%% @param Options command options.
%%
%% @returns in the case of a synchronous call, an `{ok, NodePropsMap}' tuple
%% or an `{error, Reason}' tuple; in the case of an asynchronous call, always
%% `ok' (the actual return value may be sent by a message if a correlation ID
%% was specified).
%%
%% @see delete_many/3.
%% @see put/4.
%% @see khepri:clear_many_payloads/3.

clear_many_payloads(StoreId, PathPattern, Options) ->
    {Command, CommandOptions} = khepri_command_adv:clear_many_payloads(
                                  PathPattern, Options),
    process_command(StoreId, Command, CommandOptions).

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

process_command(StoreId, Command, CommandOptions) ->
    khepri_machine:process_command(StoreId, Command, CommandOptions).
