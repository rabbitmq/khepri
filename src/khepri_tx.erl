%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri API for transactional queries and updates.
%%
%% Transactions are anonymous functions which take no arguments, much like
%% what Mnesia supports. However, unlike with Mnesia, transaction functions in
%% Khepri are restricted:
%%
%% <ul>
%% <li>Calls to BIFs and other functions is limited to a set of whitelisted
%% APIs. See {@link khepri_tx_adv:is_remote_call_valid/3} for the complete
%% list.</li>
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
%% Horus documentation</li>
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
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").
-include("src/khepri_tx.hrl").

%% IMPORTANT: When adding a new khepri_tx function to be used inside a
%% transaction function:
%%   1. The function must be added to the whitelist in
%%      `khepri_tx_adv:is_remote_call_valid()' in this file.
%%   2. If the function modifies the tree, it must be handled in
%%      `khepri_tx_adv:is_standalone_fun_still_needed()' as well.
-export([is_empty/0, is_empty/1,

         get/1, get/2,
         get_or/2, get_or/3,
         get_many/1, get_many/2,
         get_many_or/2, get_many_or/3,
         exists/1, exists/2,
         has_data/1, has_data/2,
         is_sproc/1, is_sproc/2,
         count/1, count/2,
         fold/3, fold/4,
         foreach/2, foreach/3,
         map/2, map/3,
         filter/2, filter/3,

         put/2, put/3,
         put_many/2, put_many/3,
         create/2, create/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         delete/1, delete/2,
         delete_many/1, delete_many/2,
         clear_payload/1, clear_payload/2,
         clear_many_payloads/1, clear_many_payloads/2,

         abort/1,
         is_transaction/0,
         does_api_comply_with/1]).

-compile({no_auto_import, [get/1, put/2, erase/1]}).

%% FIXME: Dialyzer complains about several functions with "optional" arguments
%% (but not all). I believe the specs are correct, but can't figure out how to
%% please Dialyzer. So for now, let's disable this specific check for the
%% problematic functions.
-dialyzer({no_underspecs, [exists/1,
                           has_data/1, has_data/2]}).

-type tx_fun_result() :: any() | no_return().
%% Return value of a transaction function.

-type tx_fun() :: fun().
%% Transaction function signature.

-type tx_abort() :: khepri:error(any()).
%% Return value after a transaction function aborted.

-export_type([tx_fun/0,
              tx_fun_result/0,
              tx_abort/0]).

%% -------------------------------------------------------------------
%% is_empty().
%% -------------------------------------------------------------------

-spec is_empty() -> IsEmpty | Error when
      IsEmpty :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the store is empty or not.
%%
%% This is the same as {@link khepri:is_empty/1} but inside the context of a
%% transaction function.
%%
%% @see is_empty/1.
%% @see khepri:is_empty/2.

is_empty() ->
    is_empty(#{}).

-spec is_empty(Options) -> IsEmpty | Error when
      Options :: khepri:tree_options(),
      IsEmpty :: boolean(),
      Error :: khepri:error().
%% @doc Indicates if the store is empty or not.
%%
%% This is the same as {@link khepri:is_empty/2} but inside the context of a
%% transaction function.
%%
%% @see khepri:is_empty/2.

is_empty(Options) ->
    Path = [],
    Options1 = Options#{expect_specific_node => true,
                        props_to_return => [child_list_length]},
    case khepri_tx_adv:get_many(Path, Options1) of
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
        {ok, NodePropsMap} ->
            NodeProps = case does_api_comply_with(uniform_write_ret) of
                            true ->
                                khepri_utils:get_single_node_props(
                                  NodePropsMap);
                            false ->
                                NodePropsMap
                        end,
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
        {ok, NodePropsMap} ->
            NodeProps = case does_api_comply_with(uniform_write_ret) of
                            true ->
                                khepri_utils:get_single_node_props(
                                  NodePropsMap);
                            false ->
                                NodePropsMap
                        end,
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
    Fun = fun(Path, NodeProps, Acc) ->
                  Payload = khepri_utils:node_props_to_payload(
                              NodeProps, Default),
                  Acc#{Path => Payload}
          end,
    Acc = #{},
    khepri_tx_adv:do_get_many(PathPattern, Fun, Acc, Options).

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
        {ok, _}                                   -> true;
        {error, ?khepri_error(node_not_found, _)} -> false;
        Error                                     -> Error
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
        {error, ?khepri_error(node_not_found, _)} ->
            false;
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
        {error, ?khepri_error(node_not_found, _)} ->
            false;
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
    {State, _SideEffects} = khepri_tx_adv:get_tx_state(),
    StoreId = khepri_machine:get_store_id(State),
    Tree = khepri_machine:get_tree(State),
    Fun = fun khepri_tree:count_node_cb/3,
    {_QueryOptions, TreeOptions} =
    khepri_machine:split_query_options(StoreId, Options),
    TreeOptions1 = TreeOptions#{expect_specific_node => false},
    Ret = khepri_tree:fold(Tree, PathPattern1, Fun, 0, TreeOptions1),
    case Ret of
        {error, ?khepri_exception(_, _) = Exception} ->
            ?khepri_misuse(Exception);
        _ ->
            Ret
    end.

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
%% This is the same as {@link khepri:fold/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:fold/4.

fold(PathPattern, Fun, Acc) ->
    fold(PathPattern, Fun, Acc, #{}).

-spec fold(PathPattern, Fun, Acc, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      Options :: khepri:tree_options(),
      Ret :: khepri:ok(NewAcc) | khepri:error(),
      NewAcc :: Acc.
%% @doc Calls `Fun' on successive tree nodes matching the given path pattern,
%% starting with `Acc'.
%%
%% This is the same as {@link khepri:fold/5} but inside the context of a
%% transaction function.
%%
%% @see khepri:fold/5.

fold(PathPattern, Fun, Acc, Options) ->
    PathPattern1 = khepri_tx_adv:path_from_string(PathPattern),
    {State, _SideEffects} = khepri_tx_adv:get_tx_state(),
    StoreId = khepri_machine:get_store_id(State),
    Tree = khepri_machine:get_tree(State),
    {_QueryOptions, TreeOptions} =
    khepri_machine:split_query_options(StoreId, Options),
    TreeOptions1 = TreeOptions#{expect_specific_node => false},
    Ret = khepri_tree:fold(Tree, PathPattern1, Fun, Acc, TreeOptions1),
    case Ret of
        {error, ?khepri_exception(_, _) = Exception} ->
            ?khepri_misuse(Exception);
        _ ->
            Ret
    end.

%% -------------------------------------------------------------------
%% foreach().
%% -------------------------------------------------------------------

-spec foreach(PathPattern, Fun) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Ret :: ok | khepri:error().
%% @doc Calls `Fun' for each tree node matching the given path pattern.
%%
%% This is the same as {@link khepri:foreach/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:foreach/3.

foreach(PathPattern, Fun) ->
    foreach(PathPattern, Fun, #{}).

-spec foreach(PathPattern, Fun, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:foreach_fun(),
      Options :: khepri:tree_options(),
      Ret :: ok | khepri:error().
%% @doc Calls `Fun' for each tree node matching the given path pattern.
%%
%% This is the same as {@link khepri:foreach/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:foreach/4.

foreach(PathPattern, Fun, Options) when is_function(Fun, 2) ->
    FoldFun = fun(Path, NodeProps, Acc) ->
                      _ = Fun(Path, NodeProps),
                      Acc
              end,
    case fold(PathPattern, FoldFun, ok, Options) of
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
%% This is the same as {@link khepri:map/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:map/3.

map(PathPattern, Fun) ->
    map(PathPattern, Fun, #{}).

-spec map(PathPattern, Fun, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Fun :: khepri:map_fun(),
      Options :: khepri:tree_options(),
      Ret :: khepri:ok(Map) | khepri:error(),
      Map :: #{khepri_path:native_path() => khepri:map_fun_ret()}.
%% @doc Produces a new map by calling `Fun' for each tree node matching the
%% given path pattern.
%%
%% This is the same as {@link khepri:map/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:map/4.

map(PathPattern, Fun, Options) when is_function(Fun, 2) ->
    FoldFun = fun(Path, NodeProps, Acc) ->
                      Ret = Fun(Path, NodeProps),
                      Acc#{Path => Ret}
              end,
    fold(PathPattern, FoldFun, #{}, Options).

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
%% This is the same as {@link khepri:filter/3} but inside the context of a
%% transaction function.
%%
%% @see khepri:filter/3.

filter(PathPattern, Pred) ->
    filter(PathPattern, Pred, #{}).

-spec filter(PathPattern, Pred, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Pred :: khepri:filter_fun(),
      Options :: khepri:tree_options(),
      Ret :: khepri:many_payloads_ret().
%% @doc Returns a map for which predicate `Pred' holds true in tree nodes
%% matching the given path pattern.
%%
%% This is the same as {@link khepri:filter/4} but inside the context of a
%% transaction function.
%%
%% @see khepri:filter/4.

filter(PathPattern, Pred, Options) when is_function(Pred, 2) ->
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
    fold(PathPattern, FoldFun, #{}, Options).

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
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
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
%% clear_payload().
%% -------------------------------------------------------------------

-spec clear_payload(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:clear_payload/2} but inside the context
%% of a transaction function.
%%
%% @see khepri:clear_payload/2.

clear_payload(PathPattern) ->
    clear_payload(PathPattern, #{}).

-spec clear_payload(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri:clear_payload/3} but inside the context
%% of a transaction function.
%%
%% @see khepri:clear_payload/3.

clear_payload(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:clear_payload(PathPattern, Options1),
    ?result_ret_to_minimal_ret(Ret).

%% -------------------------------------------------------------------
%% clear_many_payloads().
%% -------------------------------------------------------------------

-spec clear_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:clear_many_payloads/2} but inside the
%% context of a transaction function.
%%
%% @see khepri:clear_many_payloads/2.

clear_many_payloads(PathPattern) ->
    clear_many_payloads(PathPattern, #{}).

-spec clear_many_payloads(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri:minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri:clear_many_payloads/3} but inside the
%% context of a transaction function.
%%
%% @see khepri:clear_many_payloads/3.

clear_many_payloads(PathPattern, Options) ->
    Options1 = Options#{props_to_return => []},
    Ret = khepri_tx_adv:clear_many_payloads(PathPattern, Options1),
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
    throw(?TX_ABORT(Reason)).

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
        {_State, _SideEffects} -> true;
        _                      -> false
    end.

%% -------------------------------------------------------------------
%% does_api_comply_with().
%% -------------------------------------------------------------------

-spec does_api_comply_with(Behaviour) -> DoesUse when
      Behaviour :: khepri_machine:api_behaviour(),
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

does_api_comply_with(Behaviour) ->
    MacVer = khepri_tx_adv:get_tx_effective_machine_version(),
    khepri_machine:does_api_comply_with(Behaviour, MacVer).
