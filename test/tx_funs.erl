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

-dialyzer([{no_return, [allowed_khepri_tx_api_test/0,
                        allowed_erlang_expressions_test/0,
                        allowed_erlang_module_api_test/0]},
           {no_missing_calls,
            [extracting_unexported_external_function_test/0]},
           {no_match,
            [matches_type/2]}]).

-define(make_standalone_fun(Expression),
        begin
            __Fun = fun() -> Expression end,
            khepri_tx:to_standalone_fun(__Fun, rw)
        end).

-define(assertStandaloneFun(Expression),
        ?assertMatch(#standalone_fun{}, ?make_standalone_fun(Expression))).

-define(assertToFunThrow(Expected, Expression),
        ?assertThrow(Expected, ?make_standalone_fun(Expression))).

noop_ok_test() ->
    ?assertStandaloneFun(ok).

allowed_khepri_tx_api_test() ->
    ?assertStandaloneFun(
       begin
           _ = khepri_tx:put([foo], #kpayload_data{data = value}),
           _ = khepri_tx:put([foo], #kpayload_data{data = value}, #{}),
           _ = khepri_tx:get([foo]),
           _ = khepri_tx:get([foo], #{}),
           _ = khepri_tx:exists([foo]),
           _ = khepri_tx:has_data([foo]),
           _ = khepri_tx:list([foo]),
           _ = khepri_tx:find([foo], ?STAR),
           _ = khepri_tx:delete([foo]),
           _ = khepri_tx:abort(error),
           _ = khepri_tx:is_transaction()
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

           _ = not true,
           _ = true and false,
           _ = true or false,
           _ = true xor false,

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

allowed_case_block_test() ->
    ?assertStandaloneFun(
       begin
           case khepri_tx:get([foo]) of
               {ok, #{[foo] := _}} -> {ok, found};
               {ok, #{}}           -> {ok, not_found};
               _                   -> error
           end
       end).

allowed_binary_handling_test() ->
    ?assertStandaloneFun(
       begin
           _ = name_concat(<<"prefix">>, <<"name2">>),
           _ = name_concat(<<"name1">>, 0)
       end).

name_concat(<<"prefix">>, Name2) ->
    <<"prefix_", Name2/binary>>;
name_concat(Name1, Name2) ->
    <<Name1/binary, "_", Name2/signed>>.

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
            #{data := Data} <- [maps:get(Path, Nodes)]]
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

allowed_catch_test() ->
    ?assertStandaloneFun(
       begin
           case catch (exit(a)) of
               {'EXIT', _Exit} -> true;
               _ -> false
           end
       end).

matches_type(exchange, <<"exchanges">>) -> true;
matches_type(queue,    <<"queues">>)    -> true;
matches_type(exchange, <<"all">>)       -> true;
matches_type(queue,    <<"all">>)       -> true;
matches_type(_,        _)               -> false.

allowed_bs_match_test() ->
    List = [{'apply-to', <<"queues">>}],
    ?assertStandaloneFun(
       begin
           matches_type(queue, proplists:get_value('apply-to', List))
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
    ?assertToFunThrow(
       {invalid_tx_fun, {call_denied, {re, version, 0}}},
       begin
           re:version()
       end).

when_readwrite_mode_is_true_test() ->
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun() ->
                           khepri_tx:get([foo])
                   end,
                   rw),
                 standalone_fun)),
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun() ->
                           khepri_tx:put([foo], #kpayload_data{data = value})
                   end,
                   rw),
                 standalone_fun)),
    ?assertThrow(
       {invalid_tx_fun, {call_denied, {self, 0}}},
       khepri_tx:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:get([foo]),
                 self() ! message
         end,
         rw)),
    ?assertThrow(
       {invalid_tx_fun, {call_denied, {self, 0}}},
       khepri_tx:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:put([foo], #kpayload_data{data = value}),
                 self() ! message
         end,
         rw)),
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun mod_used_for_transactions:exported/0,
                   rw),
                 standalone_fun)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun dict:new/0,
                     rw),
                   0)),

    Fun = fun() -> khepri_tx:delete([foo]) end,
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun() -> Fun() end,
                   rw),
                 standalone_fun)).

when_readwrite_mode_is_false_test() ->
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             khepri_tx:get([foo])
                     end,
                     ro),
                   0)),
    %% In the following case, `to_standalone()' works, but the transaction
    %% will abort once executed.
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             khepri_tx:put(
                               [foo], #kpayload_data{data = value})
                     end,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:get([foo]),
                             self() ! message
                     end,
                     ro),
                   0)),
    %% In the following case, `to_standalone()' works, but the transaction
    %% will abort once executed.
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:put(
                                   [foo], #kpayload_data{data = value}),
                             self() ! message
                     end,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun mod_used_for_transactions:exported/0,
                     ro),
                   0)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun dict:new/0,
                     ro),
                   0)),

    Fun = fun() -> khepri_tx:delete([foo]) end,
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() -> Fun() end,
                     ro),
                   0)).

when_readwrite_mode_is_auto_test() ->
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             khepri_tx:get([foo])
                     end,
                     auto),
                   0)),
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun() ->
                           khepri_tx:put([foo], #kpayload_data{data = value})
                   end,
                   auto),
                 standalone_fun)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun() ->
                             _ = khepri_tx:get([foo]),
                             self() ! message
                     end,
                     auto),
                   0)),
    ?assertThrow(
       {invalid_tx_fun, {call_denied, {self, 0}}},
       khepri_tx:to_standalone_fun(
         fun() ->
                 _ = khepri_tx:put([foo], #kpayload_data{data = value}),
                 self() ! message
         end,
         auto)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun mod_used_for_transactions:exported/0,
                     auto),
                   0)),
    ?assert(
       is_function(khepri_tx:to_standalone_fun(
                     fun dict:new/0,
                     auto),
                   0)),

    Fun = fun() -> khepri_tx:delete([foo]) end,
    ?assert(
       is_record(khepri_tx:to_standalone_fun(
                   fun() -> Fun() end,
                   auto),
                 standalone_fun)).

make_list() -> [a, b].
make_map() -> #{a => b}.
make_tuple() -> {a, b}.
make_binary() -> <<"ab">>.

make_fun(0)  -> fun() -> result end;
make_fun(1)  -> fun(_) -> result end;
make_fun(2)  -> fun(_, _) -> result end;
make_fun(3)  -> fun(_, _, _) -> result end;
make_fun(4)  -> fun(_, _, _, _) -> result end;
make_fun(5)  -> fun(_, _, _, _, _) -> result end;
make_fun(6)  -> fun(_, _, _, _, _, _) -> result end;
make_fun(7)  -> fun(_, _, _, _, _, _, _) -> result end;
make_fun(8)  -> fun(_, _, _, _, _, _, _, _) -> result end;
make_fun(9)  -> fun(_, _, _, _, _, _, _, _, _) -> result end;
make_fun(10) -> fun(_, _, _, _, _, _, _, _, _, _) -> result end.

list_in_fun_env_test() ->
    List = make_list(),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> List end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(List, khepri_fun:exec(StandaloneFun, [])).

map_in_fun_env_test() ->
    Map = make_map(),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Map end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(Map, khepri_fun:exec(StandaloneFun, [])).

tuple_in_fun_env_test() ->
    Tuple = make_tuple(),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Tuple end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(Tuple, khepri_fun:exec(StandaloneFun, [])).

binary_in_fun_env_test() ->
    Binary = make_binary(),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Binary end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(Binary, khepri_fun:exec(StandaloneFun, [])).

fun0_in_fun_env_test() ->
    Fun = make_fun(0),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun() end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun1_in_fun_env_test() ->
    Fun = make_fun(1),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun2_in_fun_env_test() ->
    Fun = make_fun(2),
    self() ! Fun,
    receive Fun -> ok end,
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun3_in_fun_env_test() ->
    Fun = make_fun(3),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun4_in_fun_env_test() ->
    Fun = make_fun(4),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun5_in_fun_env_test() ->
    Fun = make_fun(5),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun6_in_fun_env_test() ->
    Fun = make_fun(6),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5, 6) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun7_in_fun_env_test() ->
    Fun = make_fun(7),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5, 6, 7) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun8_in_fun_env_test() ->
    Fun = make_fun(8),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5, 6, 7, 8) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun9_in_fun_env_test() ->
    Fun = make_fun(9),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5, 6, 7, 8, 9) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

fun10_in_fun_env_test() ->
    Fun = make_fun(10),
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> Fun(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertNotEqual([], StandaloneFun#standalone_fun.env),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

exec_with_regular_fun_test() ->
    Fun = khepri_tx:to_standalone_fun(
            fun() -> result end,
            ro),
    ?assert(is_function(Fun)),
    ?assertEqual(result, khepri_fun:exec(Fun, [])).

exec_standalone_fun_multiple_times_test() ->
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> result end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

exec_with_standalone_fun_test() ->
    StandaloneFun = khepri_tx:to_standalone_fun(
                      fun() -> result end,
                      rw),
    ?assertMatch(#standalone_fun{}, StandaloneFun),
    %% This is to make sure it still works after the generated module was
    %% loaded once.
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])),
    ?assertEqual(result, khepri_fun:exec(StandaloneFun, [])).

record_matching_fun_clause_test() ->
    StandaloneFun = khepri_fun:to_standalone_fun(
                      fun mod_used_for_transactions:outer_function/2,
                      #{}),
    %% Dialyzer doesn't like that we ?assertMatch(#standalone_fun{},
    %% StandaloneFun), I don't know why... Let's verify we don't have a
    %% function object instead.
    ?assertNot(is_function(StandaloneFun)),
    MyRecord1 = mod_used_for_transactions:make_record(hash_term),
    ?assertEqual(true, khepri_fun:exec(StandaloneFun, [MyRecord1, a])),
    MyRecord2 = mod_used_for_transactions:make_record(non_existing),
    ?assertEqual(false, khepri_fun:exec(StandaloneFun, [MyRecord2, a])),
    ?assertEqual(false, khepri_fun:exec(StandaloneFun, [not_my_record, a])),
    ok.

extracting_unexported_external_function_test() ->
    ?assertThrow(
       {call_to_unexported_function,
        {mod_used_for_transactions, inner_function, 2}},
       khepri_fun:to_standalone_fun(
         fun mod_used_for_transactions:inner_function/2,
         #{})).
