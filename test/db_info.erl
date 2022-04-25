%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(db_info).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

get_store_ids_with_no_running_stores_test_() ->
    [?_assertEqual(
        [],
        khepri:get_store_ids())].

get_store_ids_with_running_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         [?FUNCTION_NAME],
         khepri:get_store_ids())]}.

%% FIXME: There is no way yet to stop a store. Therefore this testcase is
%% polluted by other testcases.
%get_global_info_with_no_running_stores_test_() ->
%    [?_assertEqual(
%        "No stores running\n",
%        begin
%            khepri:info(),
%            ?capturedOutput
%        end)].

%% FIXME: There is no way yet to stop a store. Therefore this testcase is
%% polluted by other testcases.
%get_global_info_with_running_store_test_() ->
%    {setup,
%     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
%     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
%     [?_assertEqual(
%         {ok, #{}},
%         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
%      ?_assertEqual(
%         {ok, #{}},
%         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
%      ?_assertEqual(
%         begin
%             khepri:info(),
%             ?capturedOutput
%         end)]}.

get_store_info_on_non_existing_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, bar] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         "\n"
         "\033[1;32m== CLUSTER MEMBERS ==\033[0m\n"
         "\n",
         begin
             khepri:info(non_existing_store),
             ?capturedOutput
         end)]}.

get_store_info_on_running_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo, <<"bar">>] => #{}}},
         khepri:create(?FUNCTION_NAME, [foo, <<"bar">>], bar_value)),
      ?_assertEqual(
         {ok, #{[baz] => #{}}},
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         "\n"
         "\033[1;32m== CLUSTER MEMBERS ==\033[0m\n"
         "\n"
         "nonode@nohost\n"
         "\n"
         "\033[1;32m== TREE ==\033[0m\n"
         "\n"
         "●\n"
         "├── baz\n"
         "│     \033[38;5;246mData: baz_value\033[0m\n"
         "│\n"
         "╰── foo\n"
         "    ╰── <<\"bar\">>\n"
         "          \033[38;5;246mData: bar_value\033[0m\n"
         "\n",
         begin
             khepri:info(?FUNCTION_NAME),
             ?capturedOutput
         end)]}.

get_store_info_with_keep_while_conds_test_() ->
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{[foo] => #{}}},
         khepri:put(
           ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value),
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         "\n"
         "\033[1;32m== CLUSTER MEMBERS ==\033[0m\n"
         "\n"
         "nonode@nohost\n"
         "\n"
         "\033[1;32m== LIFETIME DEPS ==\033[0m\n"
         "\n"
         "\033[1m[foo] depends on:\033[0m\n"
         "    [foo]:\n"
         "        {if_child_list_length,{gt,0}}\n"
         "\n"
         "\033[1;32m== TREE ==\033[0m\n"
         "\n"
         "●\n"
         "╰── foo\n"
         "      \033[38;5;246mData: foo_value\033[0m\n"
         "\n",
         begin
             khepri:info(?FUNCTION_NAME),
             ?capturedOutput
         end)]}.
