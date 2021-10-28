%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(tx_funs).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").

-define(make_standalone_fun(Expression),
        begin
            __Fun = fun() -> Expression end,
            khepri_tx:to_standalone_fun(__Fun)
        end).

-define(assertStandaloneFun(Expression),
        ?assert(is_record(?make_standalone_fun(Expression), standalone_fun))).

-define(assertToFunThrow(Expected, Expression),
        ?assertThrow(Expected, ?make_standalone_fun(Expression))).

noop_ok_test() ->
    ?assertStandaloneFun(ok).

allowed_khepri_tx_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = khepri_tx:put([foo], ?DATA_PAYLOAD(value)),
           _ = khepri_tx:put([foo], ?DATA_PAYLOAD(value), #{}),
           _ = khepri_tx:get([foo]),
           _ = khepri_tx:get([foo], #{}),
           _ = khepri_tx:exists([foo]),
           _ = khepri_tx:has_data([foo]),
           _ = khepri_tx:list([foo]),
           _ = khepri_tx:find([foo], ?STAR),
           _ = khepri_tx:delete([foo]),
           _ = khepri_tx:abort(error)
       end).

denied_khepri_tx_run_3_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {khepri_tx, run, 3}}},
       _ = khepri_tx:run(#khepri_machine{}, fun() -> ok end, true)).

allowed_erlang_expressions_test() ->
    ?assertStandaloneFun(
       begin
           _Var = 1,

           _ = + 1,

           _ = 1 + 1,
           _ = 1 - 1,
           _ = 1 * 1,
           _ = 1 / 1,
           _ = 1 div 1,
           _ = 1 rem 1,

           _ = bnot 1,
           _ = 1 band 1,
           _ = 1 bor 1,
           _ = 1 bxor 1,
           _ = 1 bsl 1,
           _ = 1 bsr 1,

           _ = 1 == 1,
           _ = 1 /= 1,
           _ = 1 =:= 1,
           _ = 1 =/= 1,
           _ = 1 > 1,
           _ = 1 >= 1,
           _ = 1 < 1,
           _ = 1 =< 1,

           _ = not 1,
           _ = 1 and 1,
           _ = 1 or 1,
           _ = 1 xor 1,

           _ = true andalso true,
           _ = true orelse true,

           _ = [a] ++ [b],
           _ = [a] -- [b],

           M = #{a => b},
           #{a := _} = M,
           _ = M#{a => c}
       end).

allowed_erlang_types_test() ->
    ?assertStandaloneFun(
       begin
           _ = 1,
           _ = 1.0,
           _ = atom,
           _ = <<"binary">>,
           _ = [l, i, s, t],
           _ = #{a => b},
           _ = $c,
           _ = "string"
       end).

allowed_list_comprehension_test() ->
    ?assertStandaloneFun(
       begin
           [erlang:abs(I) || I <- [1, 2, 3]]
       end).

allowed_list_comprehension_with_funs_test() ->
    ?assertStandaloneFun(
       begin
           [begin
                F = fun(I1) -> I1 end,
                F(I)
            end || I <- (fun(L) -> L end)([1, 2, 3])]
       end).

allowed_list_comprehension_with_multiple_qualifiers_test() ->
    ?assertStandaloneFun(
       begin
           {ok, Nodes} = khepri_tx:list([?ROOT_NODE]),
           [Data ||
            Path <- lists:sort(maps:keys(Nodes)),
            #{data := Data} <- maps:get(Path, Nodes)]
       end).

allowed_begin_block_test() ->
    ?assertStandaloneFun(
       begin
           F1 = fun() -> ok end,
           F2 = fun() -> ok end,
           _F3 = fun() -> {F1, F2} end
       end).

allowed_if_block_test() ->
    ?assertStandaloneFun(
       begin
           L = lists:max([1]),
           if
               L >= 0 -> fun() -> ok end;
               true   -> ok
           end
       end).

allowed_try_catch_block_test() ->
    ?assertStandaloneFun(
       begin
           try
               1 + 1
           catch
               C:R:S ->
                   erlang:raise(C, R, S)
           end
       end).

denied_receive_block_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, receiving_message_denied},
       begin
           receive
               Msg -> Msg
           end
       end).

denied_receive_after_block_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, receiving_message_denied},
       begin
           receive
               Msg -> Msg
           after 0 ->
                     ok
           end
       end).

denied_module_info_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {lists, module_info, 0}}},
       begin
           _ = lists:module_info()
       end).

denied_module_info_1_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {lists, module_info, 1}}},
       begin
           _ = lists:module_info(compile)
       end).

-record(record, {}).

allowed_erlang_module_api_test() ->
    %% The compiler optimization will replace many of the following calls
    %% directly by their result, so the testing is a bit limited... I tried to
    %% use some counter-measures but it's not every effective.
    self() ! erlang:phash2(dict:new()),
    Term = receive Msg -> Msg end,
    ?assertStandaloneFun(
       begin
           _ = erlang:abs(Term),
           _ = erlang:adler32(Term),
           _ = erlang:adler32(Term, Term),
           _ = erlang:adler32_combine(Term, Term, Term),
           _ = erlang:append_element({Term, Term}, Term),
           _ = erlang:atom_to_binary(Term),
           _ = erlang:atom_to_list(Term),
           _ = erlang:binary_to_atom(Term),
           _ = erlang:binary_to_float(Term),
           _ = erlang:binary_to_integer(Term),
           _ = erlang:binary_to_list(Term),
           _ = erlang:binary_to_term(Term),
           _ = erlang:bitstring_to_list(Term),
           _ = erlang:ceil(Term),
           _ = erlang:crc32(Term),
           _ = erlang:crc32(Term, Term),
           _ = erlang:crc32_combine(Term, Term, Term),
           _ = erlang:delete_element(Term, {Term, Term}),
           _ = erlang:element(Term, {Term, Term}),
           _ = erlang:external_size(Term),
           _ = erlang:float(Term),
           _ = erlang:float_to_binary(Term),
           _ = erlang:float_to_list(Term),
           _ = erlang:hd([Term, Term]),
           _ = erlang:insert_element(Term, {Term, Term}, Term),
           _ = erlang:integer_to_binary(Term),
           _ = erlang:integer_to_list(Term),
           _ = erlang:iolist_size(Term),
           _ = erlang:iolist_to_binary(term_to_binary(Term)),
           _ = erlang:iolist_to_iovec(term_to_binary(Term)),
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
           _ = erlang:list_to_atom(Term),
           _ = erlang:list_to_binary(binary_to_list(Term)),
           _ = erlang:list_to_bitstring(binary_to_list(Term)),
           _ = erlang:list_to_float(Term),
           _ = erlang:list_to_integer(Term),
           _ = erlang:list_to_pid(Term),
           _ = erlang:list_to_tuple(Term),
           _ = erlang:make_tuple(Term, Term),
           _ = erlang:max(Term, Term),
           _ = erlang:md5(Term),
           _ = erlang:md5_final(Term),
           _ = erlang:md5_init(),
           _ = erlang:md5_update(Term, Term),
           _ = erlang:min(Term, Term),
           _ = erlang:phash2(Term),
           _ = erlang:phash2(Term, Term),
           _ = erlang:pid_to_list(Term),
           _ = erlang:raise(Term, Term, Term),
           _ = erlang:round(Term),
           _ = erlang:setelement(Term, {Term, Term}, Term),
           _ = erlang:split_binary(Term, Term),
           _ = erlang:term_to_binary(Term),
           _ = erlang:term_to_iovec(Term),
           _ = erlang:tl([Term, Term]),
           _ = erlang:tuple_size({Term, Term}),
           _ = erlang:tuple_to_list({Term, Term}),

           _ = erlang:binary_part(Term, 0, 10),
           _ = erlang:bit_size(Term),
           _ = erlang:byte_size(Term),
           _ = erlang:error(Term),
           _ = erlang:exit(Term),
           _ = erlang:length(Term),
           _ = erlang:map_get(key, maps:from_list(Term)),
           _ = erlang:map_size(Term),
           _ = erlang:size(Term),
           _ = erlang:throw(Term)
       end).

denied_builtin_make_ref_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, make_ref, 0}}},
       _ = make_ref()).

denied_erlang_make_ref_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, make_ref, 0}}},
       _ = erlang:make_ref()).

denied_builtin_node_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {node, 0}}},
       _ = node()).

denied_erlang_node_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {node, 0}}},
       _ = erlang:node()).

denied_builtin_node_1_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {node, 1}}},
       _ = node(list_to_pid("<0.0.0>"))).

denied_erlang_node_1_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {node, 1}}},
       _ = erlang:node(list_to_pid("<0.0.0>"))).

denied_builtin_nodes_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, nodes, 0}}},
       _ = nodes()).

denied_erlang_nodes_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, nodes, 0}}},
       _ = erlang:nodes()).

denied_builtin_nodes_1_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, nodes, 1}}},
       _ = nodes(visible)).

denied_erlang_nodes_1_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, nodes, 1}}},
       _ = erlang:nodes(visible)).

denied_builtin_self_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {self, 0}}},
       _ = self()).

denied_erlang_self_0_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {self, 0}}},
       _ = erlang:self()).

denied_builtin_send_2_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, sending_message_denied},
       list_to_pid("<0.0.0>") ! msg).

denied_erlang_send_2_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, send, 2}}},
       _ = erlang:send(list_to_pid("<0.0.0>"), msg)).

denied_erlang_send_3_test() ->
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {erlang, send, 3}}},
       _ = erlang:send(list_to_pid("<0.0.0>"), msg, [nosuspend])).

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
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {io, format, 1}}},
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
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {logger, get_config, 0}}},
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
    ?assertStandaloneFun(
       begin
           _ = sets:new()
       end).

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
