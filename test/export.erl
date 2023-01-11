%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(export).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_fun.hrl").
-include("src/khepri_machine.hrl").

export_empty_store_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, []},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

export_full_store_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Cond = #if_any{conditions =
                   [#if_child_list_length{count = {gt, 0}},
                    #if_has_payload{has_payload = true}]},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, [#put{path = [foo],
                    options = #{keep_while => #{[foo] => Cond}}},
               #put{path = [foo, bar],
                    payload = khepri_payload:data(bar_value)}]},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

export_standalone_function_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Fun = fun() -> "It works!" end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertEqual(
         {ok, [#put{path = [sproc],
                    payload =
                    khepri_payload:prepare(khepri_payload:sproc(Fun))}]},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

export_function_ref_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Fun = fun lists:last/1,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertEqual(
         {ok, [#put{path = [sproc],
                    payload =
                    khepri_payload:prepare(khepri_payload:sproc(Fun))}]},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

export_root_tree_node_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [], root_value)),
      ?_assertEqual(
         {ok, [#put{path = [],
                    payload = khepri_payload:data(root_value)}]},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

export_non_existing_node_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, []},
         khepri:export(?FUNCTION_NAME, [non_existing], Module, ModulePriv))]}.

export_keep_while_conds_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Cond1 = #if_any{conditions =
                    [#if_child_list_length{count = {gt, 0}},
                     #if_has_payload{has_payload = true}]},
    Cond2 = #if_child_list_length{count = 0},
    KeepWhile = #{[?THIS_KHEPRI_NODE] => Cond2},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [foo, bar], ?NO_PAYLOAD,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, [#put{path = [foo],
                    options = #{keep_while => #{[foo] => Cond1}}},
               #put{path = [foo, bar],
                    payload = ?NO_PAYLOAD,
                    options = #{keep_while => #{[foo, bar] => Cond2}}}]},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

error_to_export_during_open_write_test_() ->
    Module = export_helper,
    ModulePriv = error_during_open_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {error, open_write},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_export_during_open_write_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_open_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertThrow(
         open_write,
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

error_to_export_during_write_test_() ->
    Module = export_helper,
    ModulePriv = error_during_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {error, write},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_export_during_write_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertThrow(
         write,
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

error_to_export_during_commit_write_test_() ->
    Module = export_helper,
    ModulePriv = error_during_commit_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {error, commit_write},
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_export_during_commit_write_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_commit_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertThrow(
         commit_write,
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_export_during_abort_write_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_abort_write,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertThrow(
         abort_write,
         khepri:export(?FUNCTION_NAME, Module, ModulePriv))]}.

import_empty_store_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertEqual(
         {ok, #{}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

import_full_store_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             ok = khepri:delete_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => bar_value}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR]))]}.

import_standalone_function_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Fun = fun() -> "It works!" end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertMatch(
         {ok, #{[sproc] := #standalone_fun{}}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             ok = khepri:delete_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertMatch(
         {ok, #{[sproc] := #standalone_fun{}}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR]))]}.

import_function_ref_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Fun = fun lists:last/1,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [sproc], Fun)),
      ?_assertMatch(
         {ok, #{[sproc] := Fun}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             ok = khepri:delete_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertMatch(
         {ok, #{[sproc] := Fun}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR]))]}.

import_root_tree_node_test_() ->
    Module = export_helper,
    ModulePriv = [],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(?FUNCTION_NAME, [], root_value)),
      ?_assertMatch(
         {ok, #{[] := root_value}},
         khepri:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR],
           #{include_root_props => true})),
      ?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             ok = khepri:delete_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertMatch(
         {ok, #{[] := root_value}},
         khepri:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR],
           #{include_root_props => true}))]}.

import_keep_while_conds_test_() ->
    Module = export_helper,
    ModulePriv = [],
    Cond1 = #if_any{conditions =
                    [#if_child_list_length{count = {gt, 0}},
                     #if_has_payload{has_payload = true}]},
    Cond2 = #if_child_list_length{count = 0},
    KeepWhile = #{[?THIS_KHEPRI_NODE] => Cond2},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [foo, bar], ?NO_PAYLOAD,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => undefined}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         {ok, #{[foo] => #{[foo] => Cond1},
                [foo, bar] => #{[foo, bar] => Cond2}}},
         khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{})),
      ?_assertEqual(
         ok,
         begin
             {ok, ModulePriv1} = khepri:export(
                                   ?FUNCTION_NAME, Module, ModulePriv),
             ok = khepri:delete_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]),
             khepri:import(?FUNCTION_NAME, Module, ModulePriv1)
         end),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [foo, bar] => undefined}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR])),
      ?_assertEqual(
         {ok, #{[foo] => #{[foo] => Cond1},
                [foo, bar] => #{[foo, bar] => Cond2}}},
         khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))]}.

error_to_import_during_open_read_test_() ->
    Module = export_helper,
    ModulePriv = error_during_open_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error, open_read},
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_import_during_open_read_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_open_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         open_read,
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.

error_to_import_during_read_test_() ->
    Module = export_helper,
    ModulePriv = error_during_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error, read},
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_import_during_read_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         read,
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.

error_to_import_during_close_read_test_() ->
    Module = export_helper,
    ModulePriv = error_during_close_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error, close_read},
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.

throw_to_import_during_close_read_test_() ->
    Module = export_helper,
    ModulePriv = throw_during_close_read,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         close_read,
         khepri:import(?FUNCTION_NAME, Module, ModulePriv))]}.
