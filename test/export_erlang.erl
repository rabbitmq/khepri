%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(export_erlang).

-include_lib("eunit/include/eunit.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-define(
   FILENAME,
   lists:flatten(io_lib:format("test-export-~s.erl", [?FUNCTION_NAME]))).

export_import_empty_store_to_file_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:delete(Filename)
     end,
     [?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual({ok, []}, file:consult(Filename)),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**"))]}.

export_import_full_store_to_file_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    Cond = #if_any{conditions =
                   [#if_child_list_length{count = {gt, 0}},
                    #if_has_payload{has_payload = true}]},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:delete(Filename)
     end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value,
                [foo, bar, baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual(
         {ok, [#put{path = [foo],
                    options = #{keep_while => #{[foo] => Cond}}},
               #put{path = [foo, bar],
                    payload = khepri_payload:data(bar_value)},
               #put{path = [foo, bar, baz],
                    payload = khepri_payload:data(baz_value)}]},
         file:consult(Filename)),

      ?_assertEqual(ok, khepri:delete_many(?FUNCTION_NAME, "**")),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value,
                [foo, bar, baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, "**"))]}.

fail_to_open_file_test_() ->
    Module = khepri_export_erlang,
    {ok, Filename} = file:get_cwd(),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv)
     end,
     [?_assertEqual(
         {error, eisdir},
         khepri:export(?FUNCTION_NAME, Module, Filename))]}.

fail_to_write_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    {ok, Fd1} = file:open(Filename, [write]),
    ok = file:close(Fd1),
    {ok, Fd2} = file:open(Filename, [read]),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:close(Fd2),
             ok = file:delete(Filename)
     end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value,
                [foo, bar, baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertMatch(
         {error, _},
         khepri:export(?FUNCTION_NAME, Module, Fd2))]}.

export_import_empty_store_to_fd_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    {ok, Fd} = file:open(Filename, [read, write]),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:close(Fd),
             ok = file:delete(Filename)
     end,
     [?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Fd)),
      ?_assertEqual({ok, []}, file:consult(Filename)),

      ?_assertEqual({ok, 0}, file:position(Fd, 0)),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Fd)),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**"))]}.

export_import_full_store_to_fd_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    {ok, Fd} = file:open(Filename, [read, write]),
    Cond = #if_any{conditions =
                   [#if_child_list_length{count = {gt, 0}},
                    #if_has_payload{has_payload = true}]},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:close(Fd),
             ok = file:delete(Filename)
     end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar, baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value,
                [foo, bar, baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Fd)),
      ?_assertEqual(
         {ok, [#put{path = [foo],
                    options = #{keep_while => #{[foo] => Cond}}},
               #put{path = [foo, bar],
                    payload = khepri_payload:data(bar_value)},
               #put{path = [foo, bar, baz],
                    payload = khepri_payload:data(baz_value)}]},
         file:consult(Filename)),

      ?_assertEqual(ok, khepri:delete_many(?FUNCTION_NAME, "**")),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),

      ?_assertEqual({ok, 0}, file:position(Fd, 0)),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Fd)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value,
                [foo, bar, baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, "**"))]}.

export_import_standalone_function_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    Fun = fun() -> "It works!" end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:delete(Filename)
     end,
     [?_assertEqual(ok, khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertMatch(
         {ok, #{[sproc] := StoredFun}}
           when ?IS_HORUS_STANDALONE_FUN(StoredFun),
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual(
         {ok, [#put{path = [sproc],
                    payload =
                    khepri_payload:prepare(khepri_payload:sproc(Fun))}]},
         file:consult(Filename)),

      ?_assertEqual(ok, khepri:delete_many(?FUNCTION_NAME, "**")),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Filename)),
      ?_assertMatch(
         {ok, #{[sproc] := StoredFun}}
           when ?IS_HORUS_STANDALONE_FUN(StoredFun),
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(Fun(), khepri:run_sproc(?FUNCTION_NAME, [sproc], []))]}.

export_import_function_ref_test_() ->
    Module = khepri_export_erlang,
    Filename = ?FILENAME,
    Fun = fun lists:last/1,
    List = [a, b],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) ->
             test_ra_server_helpers:cleanup(Priv),
             ok = file:delete(Filename)
     end,
     [?_assertEqual(ok, khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertMatch(
         {ok, #{[sproc] := Fun}},
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(ok, khepri:export(?FUNCTION_NAME, Module, Filename)),
      ?_assertEqual(
         {ok, [#put{path = [sproc],
                    payload =
                    khepri_payload:prepare(khepri_payload:sproc(Fun))}]},
         file:consult(Filename)),

      ?_assertEqual(ok, khepri:delete_many(?FUNCTION_NAME, "**")),
      ?_assertEqual({ok, #{}}, khepri:get_many(?FUNCTION_NAME, "**")),

      ?_assertEqual(ok, khepri:import(?FUNCTION_NAME, Module, Filename)),
      ?_assertMatch(
         {ok, #{[sproc] := Fun}},
         khepri:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(
         Fun(List),
         khepri:run_sproc(?FUNCTION_NAME, [sproc], [List]))]}.
