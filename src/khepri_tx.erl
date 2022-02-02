%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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
         get/1, get/2,
         exists/1,
         has_data/1,
         list/1,
         find/2,
         delete/1,
         abort/1,
         is_transaction/0]).

%% For internal user only.
-export([to_standalone_fun/2,
         run/3]).

-compile({no_auto_import, [get/1, put/2, erase/1]}).

-type tx_fun_result() :: any() | no_return().
-type tx_fun() :: fun(() -> tx_fun_result()).
-type tx_fun_bindings() :: #{Name :: atom() => Value :: any()}.
-type tx_abort() :: {aborted, any()}.

-type tx_props() :: #{allow_updates := boolean()}.

-export_type([tx_fun/0,
              tx_fun_bindings/0,
              tx_fun_result/0,
              tx_abort/0]).

-spec put(PathPattern, Payload) -> Result when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_machine:payload(),
      Result :: khepri_machine:result().
%% @doc Creates or modifies a specific tree node in the tree structure.

put(PathPattern, Payload) ->
    put(PathPattern, Payload, #{}).

-spec put(PathPattern, Payload, Extra) -> Result when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri_machine:payload(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Result :: khepri_machine:result().
%% @doc Creates or modifies a specific tree node in the tree structure.

put(PathPattern, Payload, Extra) when ?IS_KHEPRI_PAYLOAD(Payload) ->
    ensure_path_pattern_is_valid(PathPattern),
    ensure_updates_are_allowed(),
    State = get_tx_state(),
    {NewState, Result} = khepri_machine:insert_or_update_node(
                           State, PathPattern, Payload, Extra),
    set_tx_state(NewState),
    Result;
put(PathPattern, Payload, _Extra) ->
    abort({invalid_payload, PathPattern, Payload}).

get(PathPattern) ->
    get(PathPattern, #{}).

get(PathPattern, Options) ->
    ensure_path_pattern_is_valid(PathPattern),
    #khepri_machine{root = Root} = get_tx_state(),
    khepri_machine:find_matching_nodes(Root, PathPattern, Options).

-spec exists(Path) -> Exists when
      Path :: khepri_path:pattern(),
      Exists :: boolean().

exists(Path) ->
    case get(Path, #{expect_specific_node => true}) of
        {ok, _} -> true;
        _       -> false
    end.

-spec has_data(Path) -> HasData when
      Path :: khepri_path:pattern(),
      HasData :: boolean().

has_data(Path) ->
    case get(Path, #{expect_specific_node => true}) of
        {ok, Result} ->
            [NodeProps] = maps:values(Result),
            maps:is_key(data, NodeProps);
        _ ->
            false
    end.

list(Path) ->
    Path1 = Path ++ [?STAR],
    get(Path1).

find(Path, Condition) ->
    Condition1 = #if_all{conditions = [?STAR_STAR, Condition]},
    Path1 = Path ++ [Condition1],
    get(Path1).

delete(PathPattern) ->
    ensure_path_pattern_is_valid(PathPattern),
    ensure_updates_are_allowed(),
    State = get_tx_state(),
    {NewState, Result} = khepri_machine:delete_matching_nodes(
                           State, PathPattern),
    set_tx_state(NewState),
    Result.

-spec abort(Reason) -> no_return() when
      Reason :: any().

abort(Reason) ->
    throw({aborted, Reason}).

-spec is_transaction() -> boolean().

is_transaction() ->
    State = erlang:get(?TX_STATE_KEY),
    is_record(State, khepri_machine).

-spec to_standalone_fun(Fun, ReadWrite) -> StandaloneFun | no_return() when
      Fun :: fun(),
      ReadWrite :: ro | rw | auto,
      StandaloneFun :: khepri_fun:standalone_fun().

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
        throw:readonly_tx_fun_detected ->
            Fun;
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
ensure_instruction_is_permitted({badmatch, _}) ->
    ok;
ensure_instruction_is_permitted({bif, Bif, _, Args, _}) ->
    Arity = length(Args),
    ensure_bif_is_valid(Bif, Arity);
ensure_instruction_is_permitted({bs_add, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_append, _, _, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_init2, _, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_put_binary, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_put_integer, _, _, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({bs_put_string, _, _}) ->
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
ensure_instruction_is_permitted({gc_bif, Bif, _, Arity, _, _}) ->
    ensure_bif_is_valid(Bif, Arity);
ensure_instruction_is_permitted({get_tuple_element, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_map_elements, _, _, _}) ->
    ok;
ensure_instruction_is_permitted({get_list, _, _, _}) ->
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
ensure_instruction_is_permitted(remove_message) ->
    throw(receiving_message_denied);
ensure_instruction_is_permitted(return) ->
    ok;
ensure_instruction_is_permitted(send) ->
    throw(sending_message_denied);
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
ensure_instruction_is_permitted({test_heap, _, _}) ->
    ok;
ensure_instruction_is_permitted({trim, _, _}) ->
    ok;
ensure_instruction_is_permitted(Unknown) ->
    throw({unknown_instruction, Unknown}).

should_process_function(Module, Name, Arity, FromModule) ->
    ShouldCollect = should_collect_code_for_module(Module),
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

should_collect_code_for_module(Module) ->
    Modules = get_list_of_module_to_skip(),
    not maps:is_key(Module, Modules).

get_list_of_module_to_skip() ->
    Key = {?MODULE, skipped_modules_in_code_collection},
    case persistent_term:get(Key, undefined) of
        Modules when Modules =/= undefined ->
            Modules;
        undefined ->
            InitialModules = #{erlang => true},
            Applications = [erts,
                            kernel,
                            stdlib,
                            mnesia,
                            sasl,
                            ssl,
                            khepri],
            Modules = lists:foldl(
                        fun(App, Modules0) ->
                                _ = application:load(App),
                                case application:get_key(App, modules) of
                                    {ok, Mods} ->
                                        lists:foldl(
                                          fun(Mod, Modules1) ->
                                                  Modules1#{Mod => true}
                                          end, Modules0, Mods);
                                    undefined ->
                                        Modules0
                                end
                        end, InitialModules, Applications),
            persistent_term:put(Key, Modules),

            %% Applications which were not loaded before this function are not
            %% unloaded now: we have no way to determine if another process
            %% could have loaded them in parallel.

            Modules
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

is_remote_call_valid(khepri, no_payload, 0) -> true;
is_remote_call_valid(khepri, data_payload, 1) -> true;

is_remote_call_valid(khepri_tx, put, _) -> true;
is_remote_call_valid(khepri_tx, get, _) -> true;
is_remote_call_valid(khepri_tx, exists, _) -> true;
is_remote_call_valid(khepri_tx, has_data, _) -> true;
is_remote_call_valid(khepri_tx, list, _) -> true;
is_remote_call_valid(khepri_tx, find, _) -> true;
is_remote_call_valid(khepri_tx, delete, _) -> true;
is_remote_call_valid(khepri_tx, abort, _) -> true;
is_remote_call_valid(khepri_tx, is_transaction, _) -> true;

is_remote_call_valid(_, module_info, _) -> false;

is_remote_call_valid(erlang, abs, _) -> true;
is_remote_call_valid(erlang, adler32, _) -> true;
is_remote_call_valid(erlang, adler32_combine, _) -> true;
is_remote_call_valid(erlang, append_element, _) -> true;
is_remote_call_valid(erlang, atom_to_binary, _) -> true;
is_remote_call_valid(erlang, atom_to_list, _) -> true;
is_remote_call_valid(erlang, binary_part, _) -> true;
is_remote_call_valid(erlang, binary_to_atom, _) -> true;
is_remote_call_valid(erlang, binary_to_float, _) -> true;
is_remote_call_valid(erlang, binary_to_integer, _) -> true;
is_remote_call_valid(erlang, binary_to_list, _) -> true;
is_remote_call_valid(erlang, binary_to_term, _) -> true;
is_remote_call_valid(erlang, bit_size, _) -> true;
is_remote_call_valid(erlang, bitstring_to_list, _) -> true;
is_remote_call_valid(erlang, byte_size, _) -> true;
is_remote_call_valid(erlang, ceil, _) -> true;
is_remote_call_valid(erlang, crc32, _) -> true;
is_remote_call_valid(erlang, crc32_combine, _) -> true;
is_remote_call_valid(erlang, delete_element, _) -> true;
is_remote_call_valid(erlang, element, _) -> true;
is_remote_call_valid(erlang, error, _) -> true;
is_remote_call_valid(erlang, exit, _) -> true;
is_remote_call_valid(erlang, external_size, _) -> true;
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
is_remote_call_valid(erlang, phash2, _) -> true;
is_remote_call_valid(erlang, pid_to_list, _) -> true;
is_remote_call_valid(erlang, raise, _) -> true;
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
is_remote_call_valid(sets, _, _) -> true;
is_remote_call_valid(string, _, _) -> true;
is_remote_call_valid(unicode, _, _) -> true;

is_remote_call_valid(_, _, _) -> false.

is_standalone_fun_still_needed(_, rw) ->
    true;
is_standalone_fun_still_needed(#{calls := Calls}, auto) ->
    ReadWrite = case Calls of
                    #{{khepri_tx, put, 2} := _}    -> rw;
                    #{{khepri_tx, put, 3} := _}    -> rw;
                    #{{khepri_tx, delete, 1} := _} -> rw;
                    _                              -> ro
                end,
    ReadWrite =:= rw.

-spec run(State, Fun, AllowUpdates) -> Ret when
      State :: khepri_machine:state(),
      Fun :: tx_fun(),
      AllowUpdates :: boolean(),
      Ret :: {khepri_machine:state(), tx_fun_result() | Exception},
      Exception :: {exception, Class, Reason, Stacktrace},
      Class :: error | exit | throw,
      Reason :: any(),
      Stacktrace :: list().
%% @private

run(State, Fun, AllowUpdates) ->
    TxProps = #{allow_updates => AllowUpdates},
    NoState = erlang:put(?TX_STATE_KEY, State),
    NoProps = erlang:put(?TX_PROPS, TxProps),
    ?assertEqual(undefined, NoState),
    ?assertEqual(undefined, NoProps),
    try
        Ret = Fun(),

        NewState = erlang:erase(?TX_STATE_KEY),
        NewTxProps = erlang:erase(?TX_PROPS),
        ?assert(is_record(NewState, khepri_machine)),
        ?assertEqual(TxProps, NewTxProps),
        {NewState, Ret}
    catch
        Class:Reason:Stacktrace ->
            _ = erlang:erase(?TX_STATE_KEY),
            _ = erlang:erase(?TX_PROPS),
            Exception = {exception, Class, Reason, Stacktrace},
            {State, Exception}
    end.

-spec get_tx_state() -> State when
      State :: khepri_machine:state().
%% @private

get_tx_state() ->
    #khepri_machine{} = State = erlang:get(?TX_STATE_KEY),
    State.

-spec set_tx_state(State) -> ok when
      State :: khepri_machine:state().
%% @private

set_tx_state(#khepri_machine{} = NewState) ->
     _ = erlang:put(?TX_STATE_KEY, NewState),
     ok.

-spec get_tx_props() -> TxProps when
      TxProps :: tx_props().
%% @private

get_tx_props() ->
    erlang:get(?TX_PROPS).

-spec ensure_path_pattern_is_valid(PathPattern) -> ok | no_return() when
      PathPattern :: khepri_path:pattern().

ensure_path_pattern_is_valid(PathPattern) ->
    case khepri_path:is_valid(PathPattern) of
        true          -> ok;
        {false, Path} -> abort({invalid_path, Path})
    end.

-spec ensure_updates_are_allowed() -> ok | no_return().
%% @private

ensure_updates_are_allowed() ->
    case get_tx_props() of
        #{allow_updates := true}  -> ok;
        #{allow_updates := false} -> abort(store_update_denied)
    end.
