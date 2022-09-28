%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Khepri API for transactional queries and updates.
%%
%% Transactions are anonymous functions which take no arguments, much like
%% what Mnesia supports. However, unlike with Mnesia, transaction functions in
%% Khepri are restricted:
%%
%% <ul>
%% <li>Calls to BIFs and other functions is limited to a set of whitelisted
%% APIs. See {@link is_remote_call_valid/3} for the complete list.</li>
%% <li>Sending or receiving messages is denied.</li>
%% </ul>
%%
%% The reason is that the transaction function must always have the exact same
%% outcome given its inputs. Indeed, the transaction function is executed on
%% every Ra cluster members participating in the consensus. The function must
%% therefore modify the Khepri state (the database) identically on all Ra
%% members. This is also true for Ra members joining the cluster later or
%% catching up after a network partition.
%%
%% To achieve that:
%% <ol>
%% <li>The code of the transaction function is extracted from the its initial
%% Erlang module. This way, the transaction function does not depend on the
%% initial module availability and is not affected by a module reload. See
%% {@link khepri_fun})</li>
%% <li>The code is verified to make sure it does not perform any denied
%% operations.</li>
%% <li>The extracted transaction function is stored as a Khepri state machine
%% command in the Ra journal to be replicated on all Ra members.</li>
%% </ol>
%%
%% Functions in this module have simplified return values to cover most
%% frequent use cases. If you need more details about the queried or modified
%% tree nodes, like the ability to distinguish a non-existent tree node from a
%% tree node with no payload, you can use the {@link khepri_tx_adv} module.

-module(khepri_tx).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").

%% IMPORTANT: When adding a new khepri_tx function to be used inside a
%% transaction function:
%%   1. The function must be added to the whitelist in
%%      `khepri_tx_adv:is_remote_call_valid()' in this file.
%%   2. If the function modifies the tree, it must be handled in
%%      `khepri_tx_adv:is_standalone_fun_still_needed()' as well.
-export([get/1, get/2,
         get_or/2, get_or/3,
         get_many/1, get_many/2,
         get_many_or/2, get_many_or/3,
         exists/1, exists/2,
         has_data/1, has_data/2,
         is_sproc/1, is_sproc/2,
         count/1, count/2,

         put/2, put/3,
         put_many/2, put_many/3,
         create/2, create/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         delete/1, delete/2,
         delete_many/1, delete_many/2,
         delete_payload/1, delete_payload/2,
         delete_many_payloads/1, delete_many_payloads/2,

         abort/1,
         is_transaction/0]).

-compile({no_auto_import, [get/1, put/2, erase/1]}).

%% FIXME: Dialyzer complains about several functions with "optional" arguments
%% (but not all). I believe the specs are correct, but can't figure out how to
%% please Dialyzer. So for now, let's disable this specific check for the
%% problematic functions.
-dialyzer({no_underspecs, [exists/1,
                           has_data/1, has_data/2]}).

-type tx_fun_result() :: any() | no_return().
%% Return value of a transaction function.

-type tx_fun() :: fun(() -> khepri_tx:tx_fun_result()).
%% Transaction function signature.

-type tx_abort() :: khepri:error(any()).
%% Return value after a transaction function aborted.

-export_type([tx_fun/0,
              tx_fun_result/0,
              tx_abort/0]).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:get/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:get/2.

get(PathPattern) ->
    get(PathPattern, #{}).

-spec get(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:get/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:get/3.

get(PathPattern, Options) ->
    case khepri_tx_adv:get(PathPattern, Options) of
        {ok, #{data := Data}}           -> {ok, Data};
        {ok, #{sproc := StandaloneFun}} -> {ok, StandaloneFun};
        {ok, _}                         -> {ok, undefined};
        Error                           -> Error
    end.

%% -------------------------------------------------------------------
%% get_or().
%% -------------------------------------------------------------------

-spec get_or(PathPattern, Default) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% This is the same as {@link khepri:get_or/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_or/3.

get_or(PathPattern, Default) ->
    get_or(PathPattern, Default, #{}).

-spec get_or(PathPattern, Default, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:tree_options(),
      Ret :: khepri:payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% This is the same as {@link khepri:get_or/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_or/4.

get_or(PathPattern, Default, Options) ->
    case khepri_tx_adv:get(PathPattern, Options) of
        {ok, #{data := Data}}           -> {ok, Data};
        {ok, #{sproc := StandaloneFun}} -> {ok, StandaloneFun};
        {ok, _}                         -> {ok, Default};
        {error, {node_not_found, _}}    -> {ok, Default};
        Error                           -> Error
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
%% This is the same as {@link khepri:get_many/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_many/2.

get_many(PathPattern) ->
    get_many(PathPattern, #{}).

-spec get_many(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% This is the same as {@link khepri:get_many/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_many/3.

get_many(PathPattern, Options) ->
    get_many_or(PathPattern, undefined, Options).

%% -------------------------------------------------------------------
%% get_many_or().
%% -------------------------------------------------------------------

-spec get_many_or(PathPattern, Default) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% This is the same as {@link khepri:get_many_or/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_many_or/3.

get_many_or(PathPattern, Default) ->
    get_many_or(PathPattern, Default, #{}).

-spec get_many_or(PathPattern, Default, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% This is the same as {@link khepri:get_many_or/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:get_many_or/4.

get_many_or(PathPattern, Default, Options) ->
    Ret = khepri_tx_adv:get_many(PathPattern, Options),
    ?many_results_ret_to_payloads_ret(Ret, Default).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(PathPattern) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% This is the same as {@link khepri:exists/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:exists/2.

exists(PathPattern) ->
    exists(PathPattern, #{}).

-spec exists(PathPattern, Options) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Exists :: boolean().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% This is the same as {@link khepri:exists/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:exists/3.

exists(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => []},
    case khepri_tx_adv:get_many(PathPattern, Options1) of
        {ok, _} ->
            true;
        {error, {node_not_found, _}} ->
            false;
        {error, {possibly_matching_many_nodes_denied, _}} ->
            ?reject_path_targetting_many_nodes(PathPattern);
        Error ->
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
%% This is the same as {@link khepri:has_data/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:has_data/2.

has_data(PathPattern) ->
    has_data(PathPattern, #{}).

-spec has_data(PathPattern, Options) -> HasData | Error when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      HasData :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% This is the same as {@link khepri:has_data/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:has_data/3.

has_data(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [has_payload]},
    case khepri_tx_adv:get_many(PathPattern, Options1) of
        {ok, NodePropsMap} ->
            [NodeProps] = maps:values(NodePropsMap),
            maps:get(has_data, NodeProps, false);
        {error, {node_not_found, _}} ->
            false;
        {error, {possibly_matching_many_nodes_denied, _}} ->
            ?reject_path_targetting_many_nodes(PathPattern);
        Error ->
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
%% This is the same as {@link khepri:is_sproc/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:is_sproc/2.

is_sproc(PathPattern) ->
    is_sproc(PathPattern, #{}).

-spec is_sproc(PathPattern, Options) -> IsSproc | Error when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      IsSproc :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% This is the same as {@link khepri:is_sproc/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:is_sproc/3.

is_sproc(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [has_payload]},
    case khepri_tx_adv:get_many(PathPattern, Options1) of
        {ok, NodePropsMap} ->
            [NodeProps] = maps:values(NodePropsMap),
            maps:get(is_sproc, NodeProps, false);
        {error, {node_not_found, _}} ->
            false;
        {error, {possibly_matching_many_nodes_denied, _}} ->
            ?reject_path_targetting_many_nodes(PathPattern);
        Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec count(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:ok(Count) | khepri:error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:count/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:count/2.

count(PathPattern) ->
    count(PathPattern, #{}).

-spec count(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri:ok(Count) | khepri:error(),
      Count :: non_neg_integer().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:count/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:count/3.

count(PathPattern, Options) ->
    PathPattern1 = khepri_tx_adv:path_from_string(PathPattern),
    {#khepri_machine{root = Root},
     _SideEffects} = khepri_tx_adv:get_tx_state(),
    khepri_machine:count_matching_nodes(Root, PathPattern1, Options).

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec put(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri:minimal_ret().
%% @doc Runs the stored procedure pointed to by the given path and returns the
%% result.
%%
%% This is the same as {@link khepri:put/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:put/3.

put(PathPattern, Data) ->
    put(PathPattern, Data, #{}).

-spec put(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Runs the stored procedure pointed to by the given path and returns the
%% result.
%%
%% This is the same as {@link khepri:put/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:put/4.

put(PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:put(PathPattern, Data, Options1),
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
%% This is the same as {@link khepri:put_many/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:put_many/3.

put_many(PathPattern, Data) ->
    put_many(PathPattern, Data, #{}).

-spec put_many(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:put_many/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:put_many/4.

put_many(PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:put_many(PathPattern, Data, Options1),
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
%% This is the same as {@link khepri:create/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:create/3.

create(PathPattern, Data) ->
    create(PathPattern, Data, #{}).

-spec create(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Creates a tree node with the given payload.
%%
%% This is the same as {@link khepri:create/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:create/4.

create(PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:create(PathPattern, Data, Options1),
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
%% This is the same as {@link khepri:update/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:update/3.

update(PathPattern, Data) ->
    update(PathPattern, Data, #{}).

-spec update(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% This is the same as {@link khepri:update/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:update/4.

update(PathPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:update(PathPattern, Data, Options1),
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
%% This is the same as {@link khepri:compare_and_swap/4} but inside the context
%% of a transaction function.
%%
%% @see khepri:compare_and_swap/4.

compare_and_swap(PathPattern, DataPattern, Data) ->
    compare_and_swap(PathPattern, DataPattern, Data, #{}).

-spec compare_and_swap(PathPattern, DataPattern, Data, Options) ->
    Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% This is the same as {@link khepri:compare_and_swap/5} but inside the context
%% of a transaction function.
%%
%% @see khepri:compare_and_swap/5.

compare_and_swap(PathPattern, DataPattern, Data, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:compare_and_swap(
            PathPattern, DataPattern, Data, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% This is the same as {@link khepri:delete/2} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete/2.

delete(PathPattern) ->
    delete(PathPattern, #{}).

-spec delete(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% This is the same as {@link khepri:delete/3} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete/3.

delete(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:delete(PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete_many().
%% -------------------------------------------------------------------

-spec delete_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:delete_many/2} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete_many/2.

delete_many(PathPattern) ->
    delete_many(PathPattern, #{}).

-spec delete_many(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:delete_many/3} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete_many/3.

delete_many(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:delete_many(PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete_payload().
%% -------------------------------------------------------------------

-spec delete_payload(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:delete_payload/2} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete_payload/2.

delete_payload(PathPattern) ->
    delete_payload(PathPattern, #{}).

-spec delete_payload(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:delete_payload/3} but inside the context
%% of a transaction function.
%%
%% @see khepri:delete_payload/3.

delete_payload(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:delete_payload(PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% delete_many_payloads().
%% -------------------------------------------------------------------

-spec delete_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:delete_many_payloads/2} but inside the
%% context of a transaction function.
%%
%% @see khepri:delete_many_payloads/2.

delete_many_payloads(PathPattern) ->
    delete_many_payloads(PathPattern, #{}).

-spec delete_many_payloads(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:delete_many_payloads/3} but inside the
%% context of a transaction function.
%%
%% @see khepri:delete_many_payloads/3.

delete_many_payloads(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:delete_many_payloads(PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% abort().
%% -------------------------------------------------------------------

-spec abort(Reason) -> no_return() when
      Reason :: any().
%% @doc Aborts the transaction.
%%
%% Any changes so far are not committed to the store.
%%
%% {@link khepri:transaction/1} and friends will return {@link tx_abort()}.
%%
%% @param Reason term to return to caller of the transaction.

abort(Reason) ->
    throw({aborted, Reason}).

%% -------------------------------------------------------------------
%% is_transaction().
%% -------------------------------------------------------------------

-spec is_transaction() -> boolean().
%% @doc Indicates if the calling function runs in the context of a transaction
%% function.
%%
%% @returns `true' if the calling code runs inside a transaction function,
%% `false' otherwise.

is_transaction() ->
    StateAndSideEffects = erlang:get(?TX_STATE_KEY),
    case StateAndSideEffects of
        {#khepri_machine{}, _SideEffects} -> true;
        _                                 -> false
    end.
