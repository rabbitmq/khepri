%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(tx_funs).

-include_lib("eunit/include/eunit.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_error.hrl").

-dialyzer([{no_return, [allowed_khepri_tx_api_test/0,
                        allowed_erlang_module_api_test/0]}]).

-define(make_standalone_fun(Expression),
        fun() ->
            helpers:init_list_of_modules_to_skip(),
            __Fun = fun() -> Expression end,
            khepri_tx_adv:to_standalone_fun(__Fun, rw)
        end()).

-define(assertStandaloneFun(Expression),
        ?assertMatch(
           Fun when ?IS_HORUS_STANDALONE_FUN(Fun),
           ?make_standalone_fun(Expression))).

-define(assertToFunError(Expected, Expression),
        ?assertError(Expected, ?make_standalone_fun(Expression))).

%% The compiler is smart enough to optimize away many instructions by
%% inspecting types and values. `mask/1' confuses the compiler by sending
%% and receiving the value.
mask(Value) ->
    self() ! Value,
    receive Msg -> Msg end.

allowed_khepri_tx_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = khepri_tx:put([foo], khepri_payload:data(value)),
           _ = khepri_tx:put([foo], khepri_payload:data(value), #{}),
           _ = khepri_tx:get([foo]),
           _ = khepri_tx:get([foo], #{}),
           _ = khepri_tx:exists([foo]),
           _ = khepri_tx:has_data([foo]),
           _ = khepri_tx:delete([foo]),
           _ = khepri_tx:abort(error),
           _ = khepri_tx:is_transaction()
       end).

denied_khepri_tx_adv_run_4_test() ->
    MachineState = #khepri_machine{
                      config = #config{store_id = ?FUNCTION_NAME,
                                       member = {?FUNCTION_NAME, node()}}
                     },
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {khepri_tx_adv, run, 4}}})}),
       _ = khepri_tx_adv:run(MachineState, fun() -> ok end, [], true)).

denied_receive_block_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := receiving_message_denied})}),
       begin
           receive
               Msg -> Msg
           end
       end).

denied_receive_after_block_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := receiving_message_denied})}),
       begin
           receive
               Msg -> Msg
           after 0 ->
                     ok
           end
       end).

denied_module_info_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {lists, module_info, 0}}})}),
       begin
           _ = lists:module_info()
       end).

denied_module_info_1_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {lists, module_info, 1}}})}),
       begin
           _ = lists:module_info(compile)
       end).

-record(record, {field, other_field}).

allowed_erlang_module_api_test() ->
    %% The compiler optimization will replace many of the following calls
    %% directly by their result, so the testing is a bit limited... I tried to
    %% use some counter-measures but it's not every effective.
    self() ! erlang:phash2(dict:new()),
    Term = receive Msg -> Msg end,
    ?assertStandaloneFun(
       begin
           Atom = list_to_atom(binary_to_list(term_to_binary(Term))),
           Binary = term_to_binary(Term),
           String = binary_to_list(Binary),
           Int = size(term_to_binary(Term)),
           Float = float(Int),
           List = [Term, Term],
           Map = maps:from_list(List),
           Pid = list_to_pid(String),

           _ = erlang:abs(Int),
           _ = erlang:adler32(Binary),
           _ = erlang:adler32(Term, Binary),
           _ = erlang:adler32_combine(Term, Term, Term),
           _ = erlang:append_element({Term, Term}, Term),
           _ = erlang:atom_to_binary(Atom),
           _ = erlang:atom_to_list(Atom),
           _ = erlang:binary_to_atom(Binary),
           _ = erlang:binary_to_float(Binary),
           _ = erlang:binary_to_integer(Binary),
           _ = erlang:binary_to_list(Binary),
           _ = erlang:binary_to_term(Binary),
           _ = erlang:bitstring_to_list(Binary),
           _ = erlang:ceil(Int),
           _ = erlang:crc32(Binary),
           _ = erlang:crc32(Term, Binary),
           _ = erlang:crc32_combine(Term, Term, Term),
           _ = erlang:delete_element(Term, {Term, Term}),
           _ = erlang:element(Term, {Term, Term}),
           _ = erlang:external_size(Term),
           _ = erlang:float(Int),
           _ = erlang:float_to_binary(Float),
           _ = erlang:float_to_list(Float),
           _ = erlang:hd([Term, Term]),
           _ = erlang:insert_element(Term, {Term, Term}, Term),
           _ = erlang:integer_to_binary(Term),
           _ = erlang:integer_to_list(Term),
           _ = erlang:iolist_size(Binary),
           _ = erlang:iolist_to_binary(Binary),
           _ = erlang:iolist_to_iovec(Binary),
           _ = erlang:is_atom(Term),
           _ = erlang:is_binary(Term),
           _ = erlang:is_bitstring(Term),
           _ = erlang:is_boolean(Term),
           _ = erlang:is_float(Term),
           _ = erlang:is_integer(Term),
           _ = erlang:is_list(Term),
           _ = erlang:is_map(Term),
           _ = erlang:is_map_key(Term, #{a => b}),
           _ = erlang:is_number(Term),
           _ = erlang:is_pid(Term),
           _ = erlang:is_record(Term, record),
           _ = erlang:is_reference(Term),
           _ = erlang:is_tuple({Term, Term}),
           _ = erlang:list_to_atom(String),
           _ = erlang:list_to_binary(String),
           _ = erlang:list_to_bitstring(String),
           _ = erlang:list_to_float(String),
           _ = erlang:list_to_integer(String),
           _ = erlang:list_to_pid(String),
           _ = erlang:list_to_tuple(String),
           _ = erlang:make_tuple(Term, Term),
           _ = erlang:max(Int, Int),
           _ = erlang:md5(Binary),
           _ = erlang:md5_final(Binary),
           _ = erlang:md5_init(),
           _ = erlang:md5_update(Binary, Binary),
           _ = erlang:min(Term, Term),
           _ = erlang:phash2(Term),
           _ = erlang:phash2(Term, Term),
           _ = erlang:pid_to_list(Pid),
           _ = erlang:raise(error, Term, []),
           _ = erlang:round(Term),
           _ = erlang:setelement(Term, {Term, Term}, Term),
           _ = erlang:split_binary(Binary, Int),
           _ = erlang:term_to_binary(Term),
           _ = erlang:term_to_iovec(Term),
           _ = erlang:tl([Term, Term]),
           _ = erlang:tuple_size({Term, Term}),
           _ = erlang:tuple_to_list({Term, Term}),

           _ = erlang:binary_part(Binary, 0, 10),
           _ = erlang:bit_size(Binary),
           _ = erlang:byte_size(Binary),
           _ = erlang:error(Term),
           _ = erlang:exit(Term),
           _ = erlang:length(List),
           _ = erlang:map_get(key, Map),
           _ = erlang:map_size(Map),
           _ = erlang:size(Binary),
           _ = erlang:throw(Term)
       end).

denied_builtin_make_ref_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, make_ref, 0}}})}),
       _ = make_ref()).

denied_erlang_make_ref_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, make_ref, 0}}})}),
       _ = erlang:make_ref()).

denied_builtin_node_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {node, 0}}})}),
       _ = node()).

denied_erlang_node_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {node, 0}}})}),
       _ = erlang:node()).

denied_builtin_node_1_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {node, 1}}})}),
       _ = node(list_to_pid("<0.0.0>"))).

denied_erlang_node_1_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {node, 1}}})}),
       _ = erlang:node(list_to_pid("<0.0.0>"))).

denied_builtin_nodes_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, nodes, 0}}})}),
       _ = nodes()).

denied_erlang_nodes_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, nodes, 0}}})}),
       _ = erlang:nodes()).

denied_builtin_nodes_1_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, nodes, 1}}})}),
       _ = nodes(visible)).

denied_erlang_nodes_1_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, nodes, 1}}})}),
       _ = erlang:nodes(visible)).

denied_builtin_self_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {self, 0}}})}),
       _ = self()).

denied_erlang_self_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {self, 0}}})}),
       _ = erlang:self()).

denied_builtin_send_2_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := sending_message_denied})}),
       list_to_pid("<0.0.0>") ! msg).

denied_erlang_send_2_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, send, 2}}})}),
       _ = erlang:send(list_to_pid("<0.0.0>"), msg)).

denied_erlang_send_3_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {erlang, send, 3}}})}),
       _ = erlang:send(list_to_pid("<0.0.0>"), msg, [nosuspend])).

%% `apply_last' instruction is used when the apply is the last call
%% in the function.
denied_apply_last_test() ->
    self() ! erlang,
    Module = receive Msg -> Msg end,
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := dynamic_apply_denied})}),
       _ = Module:now()).
denied_apply_test() ->
    self() ! erlang,
    Module = receive Msg -> Msg end,
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := dynamic_apply_denied})}),
       c = hd(Module:tl([[a, b], c]))).

allowed_dict_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = dict:new()
       end).

allowed_io_lib_format_test() ->
    ?assertStandaloneFun(
       begin
           _ = io_lib:format("", [])
       end).

denied_io_api_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {io, format, 1}}})}),
       begin
           _ = io:format("")
       end).

allowed_lists_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = lists:reverse([])
       end).

allowed_logger_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = logger:debug(""),
           _ = logger:info(""),
           _ = logger:notice(""),
           _ = logger:warning(""),
           _ = logger:error(""),
           _ = logger:critical(""),
           _ = logger:alert(""),
           _ = logger:emergency("")
       end).

denied_logger_get_config_0_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {logger, get_config, 0}}})}),
       begin
           _ = logger:get_config()
       end).

allowed_maps_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = maps:keys(#{})
       end).

allowed_orddict_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = orddict:new()
       end).

allowed_ordsets_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = ordsets:new()
       end).

allowed_proplists_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = proplists:get_keys([])
       end).

allowed_sets_api_test() ->
    ListA = mask([a, b, c]),
    ListB = mask([b, d]),
    StandaloneFun = ?make_standalone_fun(
                      begin
                          SetA = sets:from_list(ListA),
                          SetB = sets:from_list(ListB),
                          sets:subtract(SetA, SetB)
                      end),
    SetC = horus:exec(StandaloneFun, []),
    ?assert(sets:is_element(a, SetC)),
    ?assertNot(sets:is_element(b, SetC)),
    ?assert(sets:is_element(c, SetC)),
    ?assertNot(sets:is_element(d, SetC)).

allowed_string_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = string:length("")
       end).

allowed_unicode_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = unicode:characters_to_binary("")
       end).

allowed_re_test() ->
    ?assertStandaloneFun(
       begin
           {ok, MP} = re:compile("abcd"),
           _ = re:inspect(MP, namelist),
           _ = re:run("abcd", "ab.*"),
           _ = re:replace("abcd", "ab", "ef"),
           _ = re:split("abcab", "a")
       end).

denied_re_version_test() ->
    ?assertToFunError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {re, version, 0}}})}),
       begin
           re:version()
       end).

when_readwrite_mode_is_true_test() ->
    helpers:init_list_of_modules_to_skip(),
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun() ->
                    khepri_tx:get([foo])
            end,
            rw))),
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun() ->
                    khepri_tx:put([foo], khepri_payload:data(value))
            end,
            rw))),
    ?assertError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {self, 0}}})}),
       khepri_tx_adv:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:get([foo]),
                 self() ! message
         end,
         rw)),
    ?assertError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {self, 0}}})}),
       khepri_tx_adv:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:put([foo], khepri_payload:data(value)),
                 self() ! message
         end,
         rw)),
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun mod_used_for_transactions:exported/0,
            rw))),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun dict:new/0,
                     rw),
                   0)),

    Fun = fun() -> khepri_tx:delete([foo]) end,
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun() -> Fun() end,
            rw))).

when_readwrite_mode_is_false_test() ->
    helpers:init_list_of_modules_to_skip(),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             khepri_tx:get([foo])
                     end,
                     ro),
                   0)),
    %% In the following case, `to_standalone()' works, but the transaction
    %% will abort once executed.
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             khepri_tx:put(
                               [foo], khepri_payload:data(value))
                     end,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:get([foo]),
                             self() ! message
                     end,
                     ro),
                   0)),
    %% In the following case, `to_standalone()' works, but the transaction
    %% will abort once executed.
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:put(
                                   [foo], khepri_payload:data(value)),
                             self() ! message
                     end,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun mod_used_for_transactions:exported/0,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun dict:new/0,
                     ro),
                   0)),

    Fun = fun() -> khepri_tx:delete([foo]) end,
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() -> Fun() end,
                     ro),
                   0)).

when_readwrite_mode_is_auto_test() ->
    helpers:init_list_of_modules_to_skip(),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             khepri_tx:get([foo])
                     end,
                     auto),
                   0)),
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun() ->
                    khepri_tx:put([foo], khepri_payload:data(value))
            end,
            auto))),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:get([foo]),
                             self() ! message
                     end,
                     auto),
                   0)),
    ?assertError(
       ?khepri_exception(
          failed_to_prepare_tx_fun,
          #{error :=
            ?horus_error(
               extraction_denied,
               #{error := {call_denied, {self, 0}}})}),
       khepri_tx_adv:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:put([foo], khepri_payload:data(value)),
                 self() ! message
         end,
         auto)),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun mod_used_for_transactions:exported/0,
                     auto),
                   0)),
    ?assert(
       is_function(khepri_tx_adv:to_standalone_fun(
                     fun dict:new/0,
                     auto),
                   0)),

    Fun0 = fun() -> khepri_tx:delete([foo]) end,
    self() ! Fun0,
    Fun = receive F -> F end,
    ?assert(
       ?IS_HORUS_STANDALONE_FUN(
          khepri_tx_adv:to_standalone_fun(
            fun() -> Fun() end,
            auto))).
