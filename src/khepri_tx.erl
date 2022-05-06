%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

-module(khepri_tx).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").

%% IMPORTANT: When adding a new khepri_tx function to be used inside a
%% transaction function:
%%   1. The function must be added to the whitelist in
%%      `is_remote_call_valid()' in this file.
%%   2. If the function modifies the tree, it must be handled in
%%      `is_standalone_fun_still_needed()' is this file too.
-export([put/2, put/3,
         create/2, create/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         clear_payload/1, clear_payload/2,
         delete/1,

         exists/1, exists/2,
         get/1, get/2,
         get_node_props/1, get_node_props/2,
         has_data/1,
         get_data/1, get_data/2,
         get_data_or/2, get_data_or/3,

         list/1, list/2,
         find/2, find/3,

         abort/1,
         is_transaction/0]).

%% For internal user only.
-export([to_standalone_fun/2,
         run/3]).

-compile({no_auto_import, [get/1, put/2, erase/1]}).

%% FIXME: Dialyzer complains about several functions with "optional" arguments
%% (but not all). I believe the specs are correct, but can't figure out how to
%% please Dialyzer. So for now, let's disable this specific check for the
%% problematic functions.
-if(?OTP_RELEASE >= 24).
-dialyzer({no_underspecs, [exists/1,
                           has_data/1, has_data/2,
                           get_data/1, get_data/2,
                           get_data_or/2, get_data_or/3]}).
-endif.

-type tx_fun_result() :: any() | no_return().
-type tx_fun() :: fun(() -> tx_fun_result()).
-type tx_fun_bindings() :: #{Name :: atom() => Value :: any()}.
-type tx_abort() :: {aborted, any()}.

-type tx_props() :: #{allow_updates := boolean()}.

-export_type([tx_fun/0,
              tx_fun_bindings/0,
              tx_fun_result/0,
              tx_abort/0]).

%% -------------------------------------------------------------------
%% Equivalent of `khepri' functions allowed in transactions.
%% -------------------------------------------------------------------

-spec put(PathPattern, Data) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Result :: khepri:result().
%% @doc Creates or modifies a specific tree node in the tree structure.

put(PathPattern, Data) ->
    put(PathPattern, Data, #{}).

-spec put(PathPattern, Data, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri:result().
%% @doc Creates or modifies a specific tree node in the tree structure.

put(PathPattern, Data, Extra) ->
    ensure_updates_are_allowed(),
    PathPattern1 = path_from_string(PathPattern),
    Payload1 = khepri_payload:wrap(Data),
    Extra1 = case Extra of
                 #{keep_while := KeepWhile} ->
                     KeepWhile1 = khepri_condition:ensure_native_keep_while(
                                    KeepWhile),
                     Extra#{keep_while => KeepWhile1};
                 _ ->
                     Extra
             end,
    {State, SideEffects} = get_tx_state(),
    Ret = khepri_machine:insert_or_update_node(
            State, PathPattern1, Payload1, Extra1),
    case Ret of
        {NewState, Result, NewSideEffects} ->
            set_tx_state(NewState, SideEffects ++ NewSideEffects),
            Result;
        {NewState, Result} ->
            set_tx_state(NewState, SideEffects),
            Result
    end.

-spec create(PathPattern, Data) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Result :: khepri:result().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.

create(PathPattern, Data) ->
    create(PathPattern, Data, #{}).

-spec create(PathPattern, Data, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri:result().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.

create(PathPattern, Data, Extra) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = false}]),
    put(PathPattern2, Data, Extra).

-spec update(PathPattern, Data) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Result :: khepri:result().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.

update(PathPattern, Data) ->
    update(PathPattern, Data, #{}).

-spec update(PathPattern, Data, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri:result().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.

update(PathPattern, Data, Extra) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = true}]),
    put(PathPattern2, Data, Extra).

-spec compare_and_swap(PathPattern, DataPattern, Data) -> Result when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Result :: khepri:result().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.

compare_and_swap(PathPattern, DataPattern, Data) ->
    compare_and_swap(PathPattern, DataPattern, Data, #{}).

-spec compare_and_swap(PathPattern, DataPattern, Data, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri:result().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.

compare_and_swap(PathPattern, DataPattern, Data, Extra) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_data_matches{pattern = DataPattern}]),
    put(PathPattern2, Data, Extra).

-spec clear_payload(PathPattern) -> Result when
      PathPattern :: khepri_path:pattern(),
      Result :: khepri:result().
%% @doc Clears the payload of a specific tree node in the tree structure.

clear_payload(PathPattern) ->
    clear_payload(PathPattern, #{}).

-spec clear_payload(PathPattern, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri:result().
%% @doc Clears the payload of a specific tree node in the tree structure.

clear_payload(PathPattern, Extra) ->
    put(PathPattern, khepri_payload:none(), Extra).

-spec delete(PathPattern) -> Result when
      PathPattern :: khepri_path:pattern(),
      Result :: khepri:result().
%% @doc Deletes all tree nodes matching the path pattern.

delete(PathPattern) ->
    ensure_updates_are_allowed(),
    PathPattern1 = path_from_string(PathPattern),
    {State, SideEffects} = get_tx_state(),
    Ret = khepri_machine:delete_matching_nodes(State, PathPattern1),
    case Ret of
        {NewState, Result, NewSideEffects} ->
            set_tx_state(NewState, SideEffects ++ NewSideEffects),
            Result;
        {NewState, Result} ->
            set_tx_state(NewState, SideEffects),
            Result
    end.

-spec exists(PathPattern) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path exists,
%% otherwise `false'.

exists(PathPattern) ->
    exists(PathPattern, #{}).

-spec exists(PathPattern, Options) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Exists :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path exists,
%% otherwise `false'.

exists(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true},
    case get(PathPattern, Options1) of
        {ok, _} -> true;
        _       -> false
    end.

-spec get(PathPattern) -> Result when
      PathPattern :: khepri_path:pattern(),
      Result :: khepri:result().
%% @doc Returns all tree nodes matching the path pattern.

get(PathPattern) ->
    get(PathPattern, #{}).

-spec get(PathPattern, Options) -> Result when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:result().
%% @doc Returns all tree nodes matching the path pattern.

get(PathPattern, Options) ->
    PathPattern1 = path_from_string(PathPattern),
    {#khepri_machine{root = Root}, _SideEffects} = get_tx_state(),
    khepri_machine:find_matching_nodes(Root, PathPattern1, Options).

-spec get_node_props(PathPattern) -> NodeProps when
      PathPattern :: khepri_path:pattern(),
      NodeProps :: khepri:node_props().
%% @doc Returns the tree node properties associated with the given node path.

get_node_props(PathPattern) ->
    get_node_props(PathPattern, #{}).

-spec get_node_props(PathPattern, Options) -> NodeProps when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      NodeProps :: khepri:node_props().
%% @doc Returns the tree node properties associated with the given node path.

get_node_props(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true},
    case get(PathPattern, Options1) of
        {ok, Result} ->
            [{_Path, NodeProps}] = maps:to_list(Result),
            NodeProps;
        Error ->
            abort(Error)
    end.

-spec has_data(PathPattern) -> HasData when
      PathPattern :: khepri_path:pattern(),
      HasData :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path has data,
%% otherwise `false'.

has_data(PathPattern) ->
    has_data(PathPattern, #{}).

-spec has_data(PathPattern, Options) -> HasData when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      HasData :: boolean().
%% @doc Returns `true' if the tree node pointed to by the given path has data,
%% otherwise `false'.

has_data(PathPattern, Options) ->
    try
        NodeProps = get_node_props(PathPattern, Options),
        maps:is_key(data, NodeProps)
    catch
        throw:{aborted, _} ->
            false
    end.

-spec get_data(PathPattern) -> Data when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri:data().
%% @doc Returns the data associated with the given node path.

get_data(PathPattern) ->
    get_data(PathPattern, #{}).

-spec get_data(PathPattern, Options) -> Data when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Data :: khepri:data().
%% @doc Returns the data associated with the given node path.

get_data(PathPattern, Options) ->
    NodeProps = get_node_props(PathPattern, Options),
    case NodeProps of
        #{data := Data} -> Data;
        _               -> abort({error, {no_data, NodeProps}})
    end.

-spec get_data_or(PathPattern, Default) -> Data when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Data :: khepri:data().
%% @doc Returns the data associated with the given node path, or `Default' if
%% there is no data.

get_data_or(PathPattern, Default) ->
    get_data_or(PathPattern, Default, #{}).

-spec get_data_or(PathPattern, Default, Options) -> Data when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: khepri:query_options(),
      Data :: khepri:data().
%% @doc Returns the data associated with the given node path, or `Default' if
%% there is no data.

get_data_or(PathPattern, Default, Options) ->
    try
        NodeProps = get_node_props(PathPattern, Options),
        case NodeProps of
            #{data := Data} -> Data;
            _               -> Default
        end
    catch
        throw:{aborted, {error, {node_not_found, _}}} ->
            Default
    end.

-spec list(PathPattern) -> Result when
      PathPattern :: khepri_path:pattern(),
      Result :: khepri:result().
%% @doc Returns all direct child nodes under the given path.

list(PathPattern) ->
    list(PathPattern, #{}).

-spec list(PathPattern, Options) -> Result when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:query_options(),
      Result :: khepri:result().
%% @doc Returns all direct child nodes under the given path.

list(PathPattern, Options) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = [?ROOT_NODE | PathPattern1] ++ [?STAR],
    get(PathPattern2, Options).

-spec find(PathPattern, Condition) -> Result when
      PathPattern :: khepri_path:pattern(),
      Condition :: khepri_path:pattern_component(),
      Result :: khepri:result().
%% @doc Returns all tree nodes matching the path pattern.

find(PathPattern, Condition) ->
    find(PathPattern, Condition, #{}).

-spec find(PathPattern, Condition, Options) -> Result when
      PathPattern :: khepri_path:pattern(),
      Condition :: khepri_path:pattern_component(),
      Options :: khepri:query_options(),
      Result :: khepri:result().
%% @doc Finds tree nodes under `PathPattern' which match the given `Condition'.

find(PathPattern, Condition, Options) ->
    Condition1 = #if_all{conditions = [?STAR_STAR, Condition]},
    PathPattern1 = khepri_path:from_string(PathPattern),
    PathPattern2 = [?ROOT_NODE | PathPattern1] ++ [Condition1],
    get(PathPattern2, Options).

-spec abort(Reason) -> no_return() when
      Reason :: any().
%% @doc Aborts the transaction.
%%
%% Any changes so far are not committed to the store.
%%
%% {@link khepri:transaction/1} and friends will return `{atomic, Reason}'.
%%
%% @param Reason term to return to caller of the transaction.

abort(Reason) ->
    throw({aborted, Reason}).

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

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec to_standalone_fun(Fun, ReadWrite) -> StandaloneFun | no_return() when
      Fun :: fun(),
      ReadWrite :: ro | rw | auto,
      StandaloneFun :: khepri_fun:standalone_fun().
%% @private

to_standalone_fun(Fun, ReadWrite)
  when is_function(Fun, 0) andalso
       (ReadWrite =:= auto orelse ReadWrite =:= rw) ->
    Options =
    #{ensure_instruction_is_permitted =>
      fun ensure_instruction_is_permitted/1,
      should_process_function =>
      fun should_process_function/4,
      is_standalone_fun_still_needed =>
      fun(Params) -> is_standalone_fun_still_needed(Params, ReadWrite) end},
    try
        khepri_fun:to_standalone_fun(Fun, Options)
    catch
        throw:Error ->
            throw({invalid_tx_fun, Error})
    end;
to_standalone_fun(Fun, ro) ->
    Fun.

ensure_instruction_is_permitted({allocate, _, _}) ->
    ok;
ensure_instruction_is_permitted({allocate_zero, _, _}) ->
    ok;
ensure_instruction_is_permitted({allocate_heap, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({apply, _}) ->
    throw(dynamic_apply_denied);
ensure_instruction_is_permitted({apply_last, _, _}) ->
    throw(dynamic_apply_denied);
ensure_instruction_is_permitted({arithfbif, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({badmatch, _}) ->
    ok;
ensure_instruction_is_permitted({bif, Bif, _, Args, _}) ->
    Arity = length(Args),
    ensure_bif_is_valid(Bif, Arity);
ensure_instruction_is_permitted({bs_add, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_append, _, _, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_create_bin, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_init2, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_init_bits, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted(bs_init_writable) ->
    ok;
ensure_instruction_is_permitted({bs_private_append, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({BsPutSomething, _, _, _, _, _})
  when BsPutSomething =:= bs_put_binary orelse
       BsPutSomething =:= bs_put_integer ->
    ok;
ensure_instruction_is_permitted({bs_put_string, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_get_position, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_set_position, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_get_tail, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_start_match4, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({Call, _, _})
  when Call =:= call orelse Call =:= call_only orelse
       Call =:= call_ext orelse Call =:= call_ext_only ->
    ok;
ensure_instruction_is_permitted({Call, _, _, _})
  when Call =:= call_last orelse Call =:= call_ext_last ->
    ok;
ensure_instruction_is_permitted({call_fun, _}) ->
    ok;
ensure_instruction_is_permitted({call_fun2, {atom, safe}, _, _}) ->
    ok;
ensure_instruction_is_permitted({case_end, _}) ->
    ok;
ensure_instruction_is_permitted({'catch', _, _}) ->
    ok;
ensure_instruction_is_permitted({catch_end, _}) ->
    ok;
ensure_instruction_is_permitted({deallocate, _}) ->
    ok;
ensure_instruction_is_permitted({func_info, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({fconv, _, _}) ->
    ok;
ensure_instruction_is_permitted(fclearerror) ->
    ok;
ensure_instruction_is_permitted({fcheckerror, _}) ->
    ok;
ensure_instruction_is_permitted({fmove, _, _}) ->
    ok;
ensure_instruction_is_permitted({gc_bif, Bif, _, Arity, _, _}) ->
    ensure_bif_is_valid(Bif, Arity);
ensure_instruction_is_permitted({get_hd, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_tl, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_tuple_element, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_map_elements, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_list, _, _, _}) ->
    ok;
ensure_instruction_is_permitted(if_end) ->
    ok;
ensure_instruction_is_permitted({init, _}) ->
    ok;
ensure_instruction_is_permitted({init_yregs, _}) ->
    ok;
ensure_instruction_is_permitted({jump, _}) ->
    ok;
ensure_instruction_is_permitted({move, _, _}) ->
    ok;
ensure_instruction_is_permitted({loop_rec, _, _}) ->
    throw(receiving_message_denied);
ensure_instruction_is_permitted({loop_rec_env, _}) ->
    throw(receiving_message_denied);
ensure_instruction_is_permitted({make_fun2, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({make_fun3, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({put_list, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({put_map_assoc, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({put_tuple2, _, _}) ->
    ok;
ensure_instruction_is_permitted(raw_raise) ->
    ok;
ensure_instruction_is_permitted(remove_message) ->
    throw(receiving_message_denied);
ensure_instruction_is_permitted(return) ->
    ok;
ensure_instruction_is_permitted(send) ->
    throw(sending_message_denied);
ensure_instruction_is_permitted({select_tuple_arity, _, _, {list, _}}) ->
    ok;
ensure_instruction_is_permitted({select_val, _, _, {list, _}}) ->
    ok;
ensure_instruction_is_permitted({set_tuple_element, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({swap, _, _}) ->
    ok;
ensure_instruction_is_permitted({test, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({test, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({test, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({test_heap, _, _}) ->
    ok;
ensure_instruction_is_permitted({trim, _, _}) ->
    ok;
ensure_instruction_is_permitted({'try', _, _}) ->
    ok;
ensure_instruction_is_permitted({try_end, _}) ->
    ok;
ensure_instruction_is_permitted({try_case, _}) ->
    ok;
ensure_instruction_is_permitted(Unknown) ->
    throw({unknown_instruction, Unknown}).

should_process_function(Module, Name, Arity, FromModule) ->
    ShouldCollect = khepri_utils:should_collect_code_for_module(Module),
    case ShouldCollect of
        true ->
            case Module of
                FromModule ->
                    true;
                _ ->
                    _ = code:ensure_loaded(Module),
                    case erlang:function_exported(Module, Name, Arity) of
                        true ->
                            true;
                        false ->
                            throw({call_to_unexported_function,
                                   {Module, Name, Arity}})
                    end
            end;
        false ->
            ensure_call_is_valid(Module, Name, Arity),
            false
    end.

ensure_call_is_valid(Module, Name, Arity) ->
    case is_remote_call_valid(Module, Name, Arity) of
        true  -> ok;
        false -> throw({call_denied, {Module, Name, Arity}})
    end.

ensure_bif_is_valid(Bif, Arity) ->
    try
        ensure_call_is_valid(erlang, Bif, Arity)
    catch
        throw:{call_denied, {erlang, Bif, Arity}} ->
            throw({call_denied, {Bif, Arity}})
    end.

is_remote_call_valid(khepri_payload, no_payload, 0) -> true;
is_remote_call_valid(khepri_payload, data, 1) -> true;

is_remote_call_valid(khepri_tx, put, _) -> true;
is_remote_call_valid(khepri_tx, create, _) -> true;
is_remote_call_valid(khepri_tx, update, _) -> true;
is_remote_call_valid(khepri_tx, compare_and_swap, _) -> true;
is_remote_call_valid(khepri_tx, clear_payload, _) -> true;
is_remote_call_valid(khepri_tx, delete, _) -> true;
is_remote_call_valid(khepri_tx, exists, _) -> true;
is_remote_call_valid(khepri_tx, get, _) -> true;
is_remote_call_valid(khepri_tx, get_node_props, _) -> true;
is_remote_call_valid(khepri_tx, has_data, _) -> true;
is_remote_call_valid(khepri_tx, get_data, _) -> true;
is_remote_call_valid(khepri_tx, get_data_or, _) -> true;
is_remote_call_valid(khepri_tx, list, _) -> true;
is_remote_call_valid(khepri_tx, find, _) -> true;
is_remote_call_valid(khepri_tx, abort, _) -> true;
is_remote_call_valid(khepri_tx, is_transaction, _) -> true;

is_remote_call_valid(_, module_info, _) -> false;

is_remote_call_valid(erlang, abs, _) -> true;
is_remote_call_valid(erlang, adler32, _) -> true;
is_remote_call_valid(erlang, adler32_combine, _) -> true;
is_remote_call_valid(erlang, append_element, _) -> true;
is_remote_call_valid(erlang, 'and', _) -> true;
is_remote_call_valid(erlang, atom_to_binary, _) -> true;
is_remote_call_valid(erlang, atom_to_list, _) -> true;
is_remote_call_valid(erlang, 'band', _) -> true;
is_remote_call_valid(erlang, binary_part, _) -> true;
is_remote_call_valid(erlang, binary_to_atom, _) -> true;
is_remote_call_valid(erlang, binary_to_float, _) -> true;
is_remote_call_valid(erlang, binary_to_integer, _) -> true;
is_remote_call_valid(erlang, binary_to_list, _) -> true;
is_remote_call_valid(erlang, binary_to_term, _) -> true;
is_remote_call_valid(erlang, bit_size, _) -> true;
is_remote_call_valid(erlang, bitstring_to_list, _) -> true;
is_remote_call_valid(erlang, 'bnot', _) -> true;
is_remote_call_valid(erlang, 'bor', _) -> true;
is_remote_call_valid(erlang, 'bsl', _) -> true;
is_remote_call_valid(erlang, 'bsr', _) -> true;
is_remote_call_valid(erlang, 'bxor', _) -> true;
is_remote_call_valid(erlang, byte_size, _) -> true;
is_remote_call_valid(erlang, ceil, _) -> true;
is_remote_call_valid(erlang, crc32, _) -> true;
is_remote_call_valid(erlang, crc32_combine, _) -> true;
is_remote_call_valid(erlang, delete_element, _) -> true;
is_remote_call_valid(erlang, 'div', _) -> true;
is_remote_call_valid(erlang, element, _) -> true;
is_remote_call_valid(erlang, error, _) -> true;
is_remote_call_valid(erlang, exit, _) -> true;
is_remote_call_valid(erlang, external_size, _) -> true;
is_remote_call_valid(erlang, fadd, _) -> true;
is_remote_call_valid(erlang, fdiv, _) -> true;
is_remote_call_valid(erlang, fmul, _) -> true;
is_remote_call_valid(erlang, fnegate, _) -> true;
is_remote_call_valid(erlang, fsub, _) -> true;
is_remote_call_valid(erlang, float, _) -> true;
is_remote_call_valid(erlang, float_to_binary, _) -> true;
is_remote_call_valid(erlang, float_to_list, _) -> true;
is_remote_call_valid(erlang, hd, _) -> true;
is_remote_call_valid(erlang, insert_element, _) -> true;
is_remote_call_valid(erlang, integer_to_binary, _) -> true;
is_remote_call_valid(erlang, integer_to_list, _) -> true;
is_remote_call_valid(erlang, iolist_size, _) -> true;
is_remote_call_valid(erlang, iolist_to_binary, _) -> true;
is_remote_call_valid(erlang, iolist_to_iovec, _) -> true;
is_remote_call_valid(erlang, is_atom, _) -> true;
is_remote_call_valid(erlang, is_binary, _) -> true;
is_remote_call_valid(erlang, is_bitstring, _) -> true;
is_remote_call_valid(erlang, is_boolean, _) -> true;
is_remote_call_valid(erlang, is_float, _) -> true;
is_remote_call_valid(erlang, is_integer, _) -> true;
is_remote_call_valid(erlang, is_list, _) -> true;
is_remote_call_valid(erlang, is_map, _) -> true;
is_remote_call_valid(erlang, is_map_key, _) -> true;
is_remote_call_valid(erlang, is_number, _) -> true;
is_remote_call_valid(erlang, is_pid, _) -> true;
is_remote_call_valid(erlang, is_record, _) -> true;
is_remote_call_valid(erlang, is_reference, _) -> true;
is_remote_call_valid(erlang, is_tuple, _) -> true;
is_remote_call_valid(erlang, length, _) -> true;
is_remote_call_valid(erlang, list_to_atom, _) -> true;
is_remote_call_valid(erlang, list_to_binary, _) -> true;
is_remote_call_valid(erlang, list_to_bitstring, _) -> true;
is_remote_call_valid(erlang, list_to_float, _) -> true;
is_remote_call_valid(erlang, list_to_integer, _) -> true;
is_remote_call_valid(erlang, list_to_pid, _) -> true;
is_remote_call_valid(erlang, list_to_tuple, _) -> true;
is_remote_call_valid(erlang, make_tuple, _) -> true;
is_remote_call_valid(erlang, map_get, _) -> true;
is_remote_call_valid(erlang, map_size, _) -> true;
is_remote_call_valid(erlang, max, _) -> true;
is_remote_call_valid(erlang, md5, _) -> true;
is_remote_call_valid(erlang, md5_final, _) -> true;
is_remote_call_valid(erlang, md5_init, _) -> true;
is_remote_call_valid(erlang, md5_update, _) -> true;
is_remote_call_valid(erlang, min, _) -> true;
is_remote_call_valid(erlang, 'not', _) -> true;
is_remote_call_valid(erlang, 'or', _) -> true;
is_remote_call_valid(erlang, phash2, _) -> true;
is_remote_call_valid(erlang, pid_to_list, _) -> true;
is_remote_call_valid(erlang, raise, _) -> true;
is_remote_call_valid(erlang, 'rem', _) -> true;
is_remote_call_valid(erlang, round, _) -> true;
is_remote_call_valid(erlang, setelement, _) -> true;
is_remote_call_valid(erlang, size, _) -> true;
is_remote_call_valid(erlang, split_binary, _) -> true;
%% FIXME: What about changes to the marshalling code between versions of
%% Erlang?
is_remote_call_valid(erlang, term_to_binary, _) -> true;
is_remote_call_valid(erlang, term_to_iovec, _) -> true;
is_remote_call_valid(erlang, throw, _) -> true;
is_remote_call_valid(erlang, tl, _) -> true;
is_remote_call_valid(erlang, tuple_size, _) -> true;
is_remote_call_valid(erlang, tuple_to_list, _) -> true;
is_remote_call_valid(erlang, 'xor', _) -> true;
is_remote_call_valid(erlang, '++', _) -> true;
is_remote_call_valid(erlang, '--', _) -> true;
is_remote_call_valid(erlang, '+', _) -> true;
is_remote_call_valid(erlang, '-', _) -> true;
is_remote_call_valid(erlang, '*', _) -> true;
is_remote_call_valid(erlang, '>=', _) -> true;
is_remote_call_valid(erlang, '=<', _) -> true;
is_remote_call_valid(erlang, '>', _) -> true;
is_remote_call_valid(erlang, '<', _) -> true;
is_remote_call_valid(erlang, '==', _) -> true;
is_remote_call_valid(erlang, '/=', _) -> true;
is_remote_call_valid(erlang, '=:=', _) -> true;
is_remote_call_valid(erlang, '=/=', _) -> true;

is_remote_call_valid(dict, _, _) -> true;
is_remote_call_valid(io_lib, format, _) -> true;
is_remote_call_valid(lists, _, _) -> true;
is_remote_call_valid(logger, alert, _) -> true;
is_remote_call_valid(logger, critical, _) -> true;
is_remote_call_valid(logger, debug, _) -> true;
is_remote_call_valid(logger, emergency, _) -> true;
is_remote_call_valid(logger, error, _) -> true;
is_remote_call_valid(logger, info, _) -> true;
is_remote_call_valid(logger, notice, _) -> true;
is_remote_call_valid(logger, warning, _) -> true;
is_remote_call_valid(maps, _, _) -> true;
is_remote_call_valid(orddict, _, _) -> true;
is_remote_call_valid(ordsets, _, _) -> true;
is_remote_call_valid(proplists, _, _) -> true;
is_remote_call_valid(re, compile, _) -> true;
is_remote_call_valid(re, inspect, _) -> true;
is_remote_call_valid(re, replace, _) -> true;
is_remote_call_valid(re, run, _) -> true;
is_remote_call_valid(re, split, _) -> true;
is_remote_call_valid(sets, _, _) -> true;
is_remote_call_valid(string, _, _) -> true;
is_remote_call_valid(unicode, _, _) -> true;

is_remote_call_valid(_, _, _) -> false.

is_standalone_fun_still_needed(_, rw) ->
    true;
is_standalone_fun_still_needed(#{calls := Calls}, auto) ->
    ReadWrite = case Calls of
                    #{{khepri_tx, put, 2} := _}              -> rw;
                    #{{khepri_tx, put, 3} := _}              -> rw;
                    #{{khepri_tx, create, 2} := _}           -> rw;
                    #{{khepri_tx, create, 3} := _}           -> rw;
                    #{{khepri_tx, update, 2} := _}           -> rw;
                    #{{khepri_tx, update, 3} := _}           -> rw;
                    #{{khepri_tx, compare_and_swap, 3} := _} -> rw;
                    #{{khepri_tx, compare_and_swap, 4} := _} -> rw;
                    #{{khepri_tx, clear_payload, 1} := _}    -> rw;
                    #{{khepri_tx, clear_payload, 2} := _}    -> rw;
                    #{{khepri_tx, delete, 1} := _}           -> rw;
                    _                                        -> ro
                end,
    ReadWrite =:= rw.

-spec run(State, Fun, AllowUpdates) -> Ret when
      State :: khepri_machine:state(),
      Fun :: tx_fun(),
      AllowUpdates :: boolean(),
      Ret :: {State, tx_fun_result() | Exception, SideEffects},
      Exception :: {exception, Class, Reason, Stacktrace},
      Class :: error | exit | throw,
      Reason :: any(),
      Stacktrace :: list(),
      SideEffects :: ra_machine:effects().
%% @private

run(State, Fun, AllowUpdates) ->
    SideEffects = [],
    TxProps = #{allow_updates => AllowUpdates},
    NoState = erlang:put(?TX_STATE_KEY, {State, SideEffects}),
    NoProps = erlang:put(?TX_PROPS, TxProps),
    ?assertEqual(undefined, NoState),
    ?assertEqual(undefined, NoProps),
    try
        Ret = Fun(),

        {NewState, NewSideEffects} = erlang:erase(?TX_STATE_KEY),
        NewTxProps = erlang:erase(?TX_PROPS),
        ?assert(is_record(NewState, khepri_machine)),
        ?assertEqual(TxProps, NewTxProps),
        {NewState, Ret, NewSideEffects}
    catch
        Class:Reason:Stacktrace ->
            _ = erlang:erase(?TX_STATE_KEY),
            _ = erlang:erase(?TX_PROPS),
            Exception = {exception, Class, Reason, Stacktrace},
            {State, Exception, []}
    end.

-spec get_tx_state() -> {State, SideEffects} when
      State :: khepri_machine:state(),
      SideEffects :: ra_machine:effects().
%% @private

get_tx_state() ->
    StateAndSideEffects =
    {#khepri_machine{}, _SideEffects} = erlang:get(?TX_STATE_KEY),
    StateAndSideEffects.

-spec set_tx_state(State, SideEffects) -> ok when
      State :: khepri_machine:state(),
      SideEffects :: ra_machine:effects().
%% @private

set_tx_state(#khepri_machine{} = NewState, SideEffects) ->
     _ = erlang:put(?TX_STATE_KEY, {NewState, SideEffects}),
     ok.

-spec get_tx_props() -> TxProps when
      TxProps :: tx_props().
%% @private

get_tx_props() ->
    erlang:get(?TX_PROPS).

-spec path_from_string(PathPattern) -> NativePathPattern | no_return() when
      PathPattern :: khepri_path:pattern(),
      NativePathPattern :: khepri_path:native_pattern().
%% @doc Converts a string to a path (if necessary) and validates it.
%%
%% This is the same as calling {@link khepri_path:from_string/1} then {@link
%% khepri_path:is_valid/1}, but the exception is caught to abort the
%% transaction instead.
%%
%% @private

path_from_string(PathPattern) ->
    try
        PathPattern1 = khepri_path:from_string(PathPattern),
        khepri_path:ensure_is_valid(PathPattern1),
        PathPattern1
    catch
        throw:{invalid_path, _} = Reason ->
            abort(Reason)
    end.

-spec ensure_updates_are_allowed() -> ok | no_return().
%% @private

ensure_updates_are_allowed() ->
    case get_tx_props() of
        #{allow_updates := true}  -> ok;
        #{allow_updates := false} -> abort(store_update_denied)
    end.
