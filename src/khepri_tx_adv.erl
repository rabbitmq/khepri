%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri advanced API for transactional queries and updates.
%%
%% This module exposes variants of the functions in {@link khepri_tx} which
%% return more detailed return values for advanced use cases. See {@link
%% khepri_adv} for examples of use cases where this module could be useful.

-module(khepri_tx_adv).

-include_lib("stdlib/include/assert.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_ret.hrl").
-include("src/khepri_tx.hrl").

%% IMPORTANT: When adding a new khepri_tx_adv function to be used inside a
%% transaction function:
%%   1. The function must be added to the whitelist in
%%      `khepri_tx_adv:is_remote_call_valid()' in this file.
%%   2. If the function modifies the tree, it must be handled in
%%      `khepri_tx_adv:is_standalone_fun_still_needed()' as well.
-export([get/1, get/2,
         get_many/1, get_many/2,

         put/2, put/3,
         put_many/2, put_many/3,
         create/2, create/3,
         update/2, update/3,
         compare_and_swap/3, compare_and_swap/4,

         delete/1, delete/2,
         delete_many/1, delete_many/2,
         clear_payload/1, clear_payload/2,
         clear_many_payloads/1, clear_many_payloads/2]).

%% For internal use only.
-export([do_get_many/4,
         to_standalone_fun/2,
         run/5,
         ensure_instruction_is_permitted/1,
         should_process_function/4,
         is_standalone_fun_still_needed/2,
         get_tx_state/0,
         get_tx_props/0,
         get_tx_effective_machine_version/0,
         path_from_string/1]).

-compile({no_auto_import, [get/1, put/2, erase/1]}).

-type tx_props() :: #{allow_updates := boolean(),
                      ra_meta => ra_machine:command_meta_data()}.

-type legacy_ret() :: khepri:ok(khepri:node_props() | #{}) |
                      khepri:error().
%% Return value when a single tree node could be returned. This is no longer
%% the case with the transaction API version 1 (all functions return a tree
%% node props map). But it can happen when the same transaction is executed on
%% another Khepri cluster member that runs an older version of Khepri.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:get/2} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:get/2.

get(PathPattern) ->
    get(PathPattern, #{}).

-spec get(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:get/3} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:get/3.

get(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true},
    Ret = get_many(PathPattern, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:get_many/2} but inside the context of
%% a transaction function.
%%
%% @see khepri_adv:get_many/2.

get_many(PathPattern) ->
    get_many(PathPattern, #{}).

-spec get_many(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri_machine:write_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:get_many/3} but inside the context of
%% a transaction function.
%%
%% @see khepri_adv:get_many/3.

get_many(PathPattern, Options) ->
    Fun = fun khepri_tree:collect_node_props_cb/3,
    Acc = #{},
    do_get_many(PathPattern, Fun, Acc, Options).

do_get_many(PathPattern, Fun, Acc, Options) ->
    PathPattern1 = path_from_string(PathPattern),
    {State, _SideEffects} = get_tx_state(),
    StoreId = khepri_machine:get_store_id(State),
    Tree = khepri_machine:get_tree(State),
    {_QueryOptions, TreeOptions} =
    khepri_machine:split_query_options(StoreId, Options),
    Ret = khepri_tree:fold(Tree, PathPattern1, Fun, Acc, TreeOptions),
    case Ret of
        {error, ?khepri_exception(_, _) = Exception} ->
            ?khepri_misuse(Exception);
        _ ->
            Ret
    end.

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec put(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:put/3} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:put/3.

put(PathPattern, Data) ->
    put(PathPattern, Data, #{}).

-spec put(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:put/4} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:put/4.

put(PathPattern, Data, Options) ->
    Options1 = Options#{expect_specific_node => true},
    Ret = put_many(PathPattern, Data, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% put_many().
%% -------------------------------------------------------------------

-spec put_many(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_machine:write_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:put_many/3} but inside the context of
%% a transaction function.
%%
%% @see khepri_adv:put_many/3.

put_many(PathPattern, Data) ->
    put_many(PathPattern, Data, #{}).

-spec put_many(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:put_many/4} but inside the context of
%% a transaction function.
%%
%% @see khepri_adv:put_many/4.

put_many(PathPattern, Data, Options) ->
    ensure_updates_are_allowed(),
    PathPattern1 = path_from_string(PathPattern),
    Payload1 = khepri_payload:wrap(Data),
    {State, _SideEffects} = get_tx_state(),
    StoreId = khepri_machine:get_store_id(State),
    {_CommandOptions, TreeAndPutOptions} =
    khepri_machine:split_command_options(StoreId, Options),
    {TreeOptions, PutOptions} =
    khepri_machine:split_put_options(TreeAndPutOptions),
    TreeOptions1 = maybe_return_indirect_deletes(TreeOptions),
    %% TODO: Ensure `CommandOptions' is unset.
    Fun = fun(State1, SideEffects) ->
                  khepri_machine:insert_or_update_node(
                    State1, PathPattern1, Payload1, PutOptions, TreeOptions1,
                    SideEffects)
          end,
    handle_state_for_call(Fun).

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Creates a tree node with the given payload.
%%
%% This is the same as {@link khepri_adv:create/3} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:create/3.

create(PathPattern, Data) ->
    create(PathPattern, Data, #{}).

-spec create(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Creates a tree node with the given payload.
%%
%% This is the same as {@link khepri_adv:create/4} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:create/4.

create(PathPattern, Data, Options) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = false}]),
    Options1 = Options#{expect_specific_node => true},
    Ret = put_many(PathPattern2, Data, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(PathPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% This is the same as {@link khepri_adv:update/3} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:update/3.

update(PathPattern, Data) ->
    update(PathPattern, Data, #{}).

-spec update(PathPattern, Data, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% This is the same as {@link khepri_adv:update/4} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:update/4.

update(PathPattern, Data, Options) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_node_exists{exists = true}]),
    Options1 = Options#{expect_specific_node => true},
    Ret = put_many(PathPattern2, Data, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% compare_and_swap().
%% -------------------------------------------------------------------

-spec compare_and_swap(PathPattern, DataPattern, Data) -> Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% This is the same as {@link khepri_adv:compare_and_swap/4} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:compare_and_swap/4.

compare_and_swap(PathPattern, DataPattern, Data) ->
    compare_and_swap(PathPattern, DataPattern, Data, #{}).

-spec compare_and_swap(PathPattern, DataPattern, Data, Options) ->
    Ret when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | khepri:data() | fun(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% This is the same as {@link khepri_adv:compare_and_swap/5} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:compare_and_swap/5.

compare_and_swap(PathPattern, DataPattern, Data, Options) ->
    PathPattern1 = path_from_string(PathPattern),
    PathPattern2 = khepri_path:combine_with_conditions(
                     PathPattern1, [#if_data_matches{pattern = DataPattern}]),
    Options1 = Options#{expect_specific_node => true},
    Ret = put_many(PathPattern2, Data, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% This is the same as {@link khepri_adv:delete/2} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:delete/2.

delete(PathPattern) ->
    delete(PathPattern, #{}).

-spec delete(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% This is the same as {@link khepri_adv:delete/3} but inside the context of a
%% transaction function.
%%
%% @see khepri_adv:delete/3.

delete(PathPattern, Options) ->
    Options1 = Options#{expect_specific_node => true},
    Ret = delete_many(PathPattern, Options1),
    maybe_write_ret_to_legacy_ret(PathPattern, Ret).

%% -------------------------------------------------------------------
%% delete_many().
%% -------------------------------------------------------------------

-spec delete_many(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:delete_many/2} but inside the context
%% of a transaction function.
%%
%% @see khepri_adv:delete_many/2.

delete_many(PathPattern) ->
    delete_many(PathPattern, #{}).

-spec delete_many(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:delete_many/3} but inside the context
%% of a transaction function.
%%
%% @see khepri_adv:delete_many/3.

delete_many(PathPattern, Options) ->
    ensure_updates_are_allowed(),
    PathPattern1 = path_from_string(PathPattern),
    {State, _SideEffects} = get_tx_state(),
    StoreId = khepri_machine:get_store_id(State),
    {_CommandOptions, TreeOptions} =
    khepri_machine:split_command_options(StoreId, Options),
    TreeOptions1 = maybe_return_indirect_deletes(TreeOptions),
    %% TODO: Ensure `CommandOptions' is empty and `TreeOptions' doesn't
    %% contains put options.
    Fun = fun(State1, SideEffects) ->
                  khepri_machine:delete_matching_nodes(
                    State1, PathPattern1, TreeOptions1, SideEffects)
          end,
    handle_state_for_call(Fun).

%% -------------------------------------------------------------------
%% clear_payload().
%% -------------------------------------------------------------------

-spec clear_payload(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:clear_payload/2} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:clear_payload/2.

clear_payload(PathPattern) ->
    clear_payload(PathPattern, #{}).

-spec clear_payload(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret() | legacy_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% This is the same as {@link khepri_adv:clear_payload/3} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:clear_payload/3.

clear_payload(PathPattern, Options) ->
    Ret = update(PathPattern, khepri_payload:none(), Options),
    case Ret of
        {error, ?khepri_error(node_not_found, _)} ->
            {ok, #{}};
        _ ->
            maybe_write_ret_to_legacy_ret(PathPattern, Ret)
    end.

%% -------------------------------------------------------------------
%% clear_many_payloads().
%% -------------------------------------------------------------------

-spec clear_many_payloads(PathPattern) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:clear_many_payloads/2} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:clear_many_payloads/2.

clear_many_payloads(PathPattern) ->
    clear_many_payloads(PathPattern, #{}).

-spec clear_many_payloads(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:tree_options() | khepri:put_options(),
      Ret :: khepri_machine:write_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% This is the same as {@link khepri_adv:clear_many_payloads/3} but inside the
%% context of a transaction function.
%%
%% @see khepri_adv:clear_many_payloads/3.

clear_many_payloads(PathPattern, Options) ->
    put_many(PathPattern, khepri_payload:none(), Options).

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

-spec to_standalone_fun(Fun, ReadWrite) -> StandaloneFun | no_return() when
      Fun :: fun(),
      ReadWrite :: ro | rw | auto,
      StandaloneFun :: horus:horus_fun().
%% @private

to_standalone_fun(Fun, ReadWrite)
  when is_function(Fun) andalso
       (ReadWrite =:= auto orelse ReadWrite =:= rw) ->
    Options =
    #{ensure_instruction_is_permitted =>
      fun ensure_instruction_is_permitted/1,
      should_process_function =>
      fun should_process_function/4,
      is_standalone_fun_still_needed =>
      fun(Params) -> is_standalone_fun_still_needed(Params, ReadWrite) end},
    try
        horus:to_standalone_fun(Fun, Options)
    catch
        throw:Error:Stacktrace ->
            erlang:error(
              ?khepri_exception(
                 failed_to_prepare_tx_fun,
                 #{'fun' => Fun,
                   error => Error,
                   stacktrace => Stacktrace}))
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
ensure_instruction_is_permitted({badrecord, _}) ->
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
ensure_instruction_is_permitted({bs_match, _, _, _}) ->
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
ensure_instruction_is_permitted({call_fun2, {atom, unsafe}, _, _}) ->
    ok;
ensure_instruction_is_permitted({call_fun2, {f, _}, _, _}) ->
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
ensure_instruction_is_permitted({update_record, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted(Unknown) ->
    throw({unknown_instruction, Unknown}).

should_process_function(Module, Name, Arity, FromModule) ->
    ShouldProcess = khepri_utils:should_process_module(Module),
    case ShouldProcess of
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

is_remote_call_valid(khepri_path, compile, _) -> true;
is_remote_call_valid(khepri_path, from_string, _) -> true;
is_remote_call_valid(khepri_path, from_binary, _) -> true;
is_remote_call_valid(khepri_path, to_string, _) -> true;
is_remote_call_valid(khepri_path, to_binary, _) -> true;
is_remote_call_valid(khepri_path, combine_with_conditions, _) -> true;
is_remote_call_valid(khepri_path, targets_specific_node, _) -> true;
is_remote_call_valid(khepri_path, component_targets_specific_node, _) -> true;
is_remote_call_valid(khepri_path, is_valid, _) -> true;
is_remote_call_valid(khepri_path, ensure_is_valid, _) -> true;
is_remote_call_valid(khepri_path, abspath, _) -> true;
is_remote_call_valid(khepri_path, realpath, _) -> true;
is_remote_call_valid(khepri_path, pattern_includes_root_node, _) -> true;

is_remote_call_valid(khepri_tx, is_empty, _) -> true;
is_remote_call_valid(khepri_tx, get, _) -> true;
is_remote_call_valid(khepri_tx, get_or, _) -> true;
is_remote_call_valid(khepri_tx, get_many, _) -> true;
is_remote_call_valid(khepri_tx, get_many_or, _) -> true;
is_remote_call_valid(khepri_tx, exists, _) -> true;
is_remote_call_valid(khepri_tx, has_data, _) -> true;
is_remote_call_valid(khepri_tx, is_sproc, _) -> true;
is_remote_call_valid(khepri_tx, count, _) -> true;
is_remote_call_valid(khepri_tx, fold, _) -> true;
is_remote_call_valid(khepri_tx, foreach, _) -> true;
is_remote_call_valid(khepri_tx, map, _) -> true;
is_remote_call_valid(khepri_tx, filter, _) -> true;
is_remote_call_valid(khepri_tx, put, _) -> true;
is_remote_call_valid(khepri_tx, put_many, _) -> true;
is_remote_call_valid(khepri_tx, create, _) -> true;
is_remote_call_valid(khepri_tx, update, _) -> true;
is_remote_call_valid(khepri_tx, compare_and_swap, _) -> true;
is_remote_call_valid(khepri_tx, delete, _) -> true;
is_remote_call_valid(khepri_tx, delete_many, _) -> true;
is_remote_call_valid(khepri_tx, clear_payload, _) -> true;
is_remote_call_valid(khepri_tx, clear_many_payloads, _) -> true;
is_remote_call_valid(khepri_tx, abort, _) -> true;
is_remote_call_valid(khepri_tx, is_transaction, _) -> true;
is_remote_call_valid(khepri_tx, does_api_comply_with, _) -> true;

is_remote_call_valid(khepri_tx_adv, get, _) -> true;
is_remote_call_valid(khepri_tx_adv, get_many, _) -> true;
is_remote_call_valid(khepri_tx_adv, put, _) -> true;
is_remote_call_valid(khepri_tx_adv, put_many, _) -> true;
is_remote_call_valid(khepri_tx_adv, create, _) -> true;
is_remote_call_valid(khepri_tx_adv, update, _) -> true;
is_remote_call_valid(khepri_tx_adv, compare_and_swap, _) -> true;
is_remote_call_valid(khepri_tx_adv, delete, _) -> true;
is_remote_call_valid(khepri_tx_adv, delete_many, _) -> true;
is_remote_call_valid(khepri_tx_adv, clear_payload, _) -> true;
is_remote_call_valid(khepri_tx_adv, clear_many_payloads, _) -> true;

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
is_remote_call_valid(timer, sleep, _) -> true;
is_remote_call_valid(unicode, _, _) -> true;

is_remote_call_valid(_, _, _) -> false.

is_standalone_fun_still_needed(_, rw) ->
    true;
is_standalone_fun_still_needed(#{calls := Calls}, auto) ->
    ReadWrite = case Calls of
                    #{{khepri_tx, put, 2} := _}                     -> rw;
                    #{{khepri_tx, put, 3} := _}                     -> rw;
                    #{{khepri_tx, put, 4} := _}                     -> rw;
                    #{{khepri_tx, put_many, 2} := _}                -> rw;
                    #{{khepri_tx, put_many, 3} := _}                -> rw;
                    #{{khepri_tx, put_many, 4} := _}                -> rw;
                    #{{khepri_tx, create, 2} := _}                  -> rw;
                    #{{khepri_tx, create, 3} := _}                  -> rw;
                    #{{khepri_tx, create, 4} := _}                  -> rw;
                    #{{khepri_tx, update, 2} := _}                  -> rw;
                    #{{khepri_tx, update, 3} := _}                  -> rw;
                    #{{khepri_tx, update, 4} := _}                  -> rw;
                    #{{khepri_tx, compare_and_swap, 3} := _}        -> rw;
                    #{{khepri_tx, compare_and_swap, 4} := _}        -> rw;
                    #{{khepri_tx, compare_and_swap, 5} := _}        -> rw;
                    #{{khepri_tx, delete, 1} := _}                  -> rw;
                    #{{khepri_tx, delete, 2} := _}                  -> rw;
                    #{{khepri_tx, delete_many, 1} := _}             -> rw;
                    #{{khepri_tx, delete_many, 2} := _}             -> rw;
                    #{{khepri_tx, clear_payload, 1} := _}           -> rw;
                    #{{khepri_tx, clear_payload, 2} := _}           -> rw;
                    #{{khepri_tx, clear_payload, 3} := _}           -> rw;
                    #{{khepri_tx, clear_many_payloads, 1} := _}     -> rw;
                    #{{khepri_tx, clear_many_payloads, 2} := _}     -> rw;
                    #{{khepri_tx, clear_many_payloads, 3} := _}     -> rw;

                    #{{khepri_tx_adv, put, 2} := _}                 -> rw;
                    #{{khepri_tx_adv, put, 3} := _}                 -> rw;
                    #{{khepri_tx_adv, put, 4} := _}                 -> rw;
                    #{{khepri_tx_adv, put_many, 2} := _}            -> rw;
                    #{{khepri_tx_adv, put_many, 3} := _}            -> rw;
                    #{{khepri_tx_adv, put_many, 4} := _}            -> rw;
                    #{{khepri_tx_adv, create, 2} := _}              -> rw;
                    #{{khepri_tx_adv, create, 3} := _}              -> rw;
                    #{{khepri_tx_adv, create, 4} := _}              -> rw;
                    #{{khepri_tx_adv, update, 2} := _}              -> rw;
                    #{{khepri_tx_adv, update, 3} := _}              -> rw;
                    #{{khepri_tx_adv, update, 4} := _}              -> rw;
                    #{{khepri_tx_adv, compare_and_swap, 2} := _}    -> rw;
                    #{{khepri_tx_adv, compare_and_swap, 3} := _}    -> rw;
                    #{{khepri_tx_adv, compare_and_swap, 4} := _}    -> rw;
                    #{{khepri_tx_adv, delete, 1} := _}              -> rw;
                    #{{khepri_tx_adv, delete, 2} := _}              -> rw;
                    #{{khepri_tx_adv, delete_many, 1} := _}         -> rw;
                    #{{khepri_tx_adv, delete_many, 2} := _}         -> rw;
                    #{{khepri_tx_adv, clear_payload, 1} := _}       -> rw;
                    #{{khepri_tx_adv, clear_payload, 2} := _}       -> rw;
                    #{{khepri_tx_adv, clear_payload, 3} := _}       -> rw;
                    #{{khepri_tx_adv, clear_many_payloads, 1} := _} -> rw;
                    #{{khepri_tx_adv, clear_many_payloads, 2} := _} -> rw;
                    #{{khepri_tx_adv, clear_many_payloads, 3} := _} -> rw;
                    _                                               -> ro
                end,
    ReadWrite =:= rw.

-spec run(State, StandaloneFun, Args, AllowUpdates, RaMeta) -> Ret when
      State :: khepri_machine:state(),
      StandaloneFun :: horus:horus_fun(),
      Args :: list(),
      AllowUpdates :: boolean(),
      RaMeta :: ra_machine:command_meta_data(),
      Ret :: {State, khepri_tx:tx_fun_result() | Exception, SideEffects},
      Exception :: {exception, Class, Reason, Stacktrace},
      Class :: error | exit | throw,
      Reason :: any(),
      Stacktrace :: list(),
      SideEffects :: ra_machine:effects().
%% @private

run(State, StandaloneFun, Args, AllowUpdates, RaMeta)
  when ?IS_HORUS_FUN(StandaloneFun) ->
    SideEffects = [],
    TxProps0 = #{allow_updates => AllowUpdates},
    TxProps = case RaMeta of
                  _ when is_map(RaMeta) -> TxProps0#{ra_meta => RaMeta};
                  undefined             -> TxProps0
              end,
    NoState = erlang:put(?TX_STATE_KEY, {State, SideEffects}),
    NoProps = erlang:put(?TX_PROPS, TxProps),
    ?assertEqual(undefined, NoState),
    ?assertEqual(undefined, NoProps),
    try
        Ret = horus:exec(StandaloneFun, Args),

        {NewState, NewSideEffects} = erlang:erase(?TX_STATE_KEY),
        NewTxProps = erlang:erase(?TX_PROPS),
        khepri_machine:ensure_is_state(NewState),
        ?assertEqual(TxProps, NewTxProps),
        %% The side effect list is built using prepends so the list needs to
        %% be reversed to process the effects in order.
        NewSideEffects1 = lists:reverse(NewSideEffects),
        {NewState, Ret, NewSideEffects1}
    catch
        Class:Reason:Stacktrace ->
            _ = erlang:erase(?TX_STATE_KEY),
            _ = erlang:erase(?TX_PROPS),
            Exception = {exception, Class, Reason, Stacktrace},
            {State, Exception, []}
    end.

handle_state_for_call(Fun) ->
    {State, SideEffects} = get_tx_state(),
    {NewState, Ret, SideEffects1} = Fun(State, SideEffects),
    set_tx_state(NewState, SideEffects1),
    ?raise_exception_if_any(Ret).

-spec get_tx_state() -> {State, SideEffects} when
      State :: khepri_machine:state(),
      SideEffects :: ra_machine:effects().
%% @private

get_tx_state() ->
    case erlang:get(?TX_STATE_KEY) of
        {State, _SideEffects} = StateAndSideEffects ->
            khepri_machine:ensure_is_state(State),
            StateAndSideEffects;
        undefined ->
            ?khepri_misuse(invalid_use_of_khepri_tx_outside_transaction, #{})
    end.

-spec set_tx_state(State, SideEffects) -> ok when
      State :: khepri_machine:state(),
      SideEffects :: ra_machine:effects().
%% @private

set_tx_state(NewState, SideEffects) ->
    khepri_machine:ensure_is_state(NewState),
    _ = erlang:put(?TX_STATE_KEY, {NewState, SideEffects}),
    ok.

-spec get_tx_props() -> TxProps when
      TxProps :: tx_props().
%% @private

get_tx_props() ->
    case erlang:get(?TX_PROPS) of
        TxProps when is_map(TxProps) ->
            TxProps;
        undefined ->
            ?khepri_misuse(invalid_use_of_khepri_tx_outside_transaction, #{})
    end.

-spec get_tx_effective_machine_version() -> MacVer when
      MacVer :: ra_machine:version().
%% @private

get_tx_effective_machine_version() ->
    TxProps = get_tx_props(),
    #{ra_meta := #{machine_version := MacVer}} = TxProps,
    MacVer.

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
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    PathPattern1.

-spec ensure_updates_are_allowed() -> ok | no_return().
%% @private

ensure_updates_are_allowed() ->
    case get_tx_props() of
        #{allow_updates := true} ->
            ok;
        #{allow_updates := false} ->
            ?khepri_misuse(denied_update_in_readonly_tx, #{})
    end.

-spec maybe_return_indirect_deletes(TreeOptions) -> NewTreeOptions when
      TreeOptions :: khepri:tree_options(),
      NewTreeOptions :: khepri:tree_options().
%% @doc Sets options if indirect deletes should be returned or not.
%%
%% This depends on the effective machive version.
%%
%% @private

maybe_return_indirect_deletes(TreeOptions) ->
    case khepri_tx:does_api_comply_with(indirect_deletes_in_ret) of
        true  -> TreeOptions;
        false -> TreeOptions#{return_indirect_deletes => false}
    end.

-spec maybe_write_ret_to_legacy_ret(PathPattern, Ret) -> NewRet when
      PathPattern :: khepri_path:pattern(),
      Ret :: khepri_machine:write_ret(),
      NewRet :: khepri_machine:write_ret() | legacy_ret().
%% @doc Converts the return value to the legacy format, if relevant.
%%
%% The legacy format was used by Khepri 0.16.0.
%%
%% This function will always return the new return value, except in unit tests
%% because we don't set the `tx_api_version' process dict key.
%%
%% The reason for its existence is the same function may return the legacy
%% format when the transaction is executed on another Khepri cluster member
%% that still runs an older version of Khepri. The caller should be aware of
%% that. It also heps with Dialyzer reports.
%%
%% @private

maybe_write_ret_to_legacy_ret(PathPattern, {ok, NodePropsMap} = Ret) ->
    case khepri_tx:does_api_comply_with(uniform_write_ret) of
        true ->
            Ret;
        false ->
            {true, Path} = khepri_path:targets_specific_node(PathPattern),
            NodeProps = maps:get(Path, NodePropsMap),
            {ok, NodeProps}
    end;
maybe_write_ret_to_legacy_ret(_PathPattern, {error, _} = Error) ->
    Error.
