%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(db_info).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
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

get_store_ids_after_killing_ra_server_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         [?FUNCTION_NAME],
         khepri:get_store_ids()),
      ?_assertEqual(
         true,
         khepri_cluster:is_store_running(?FUNCTION_NAME)),

      ?_test(terminate_ra_server(?FUNCTION_NAME)),

      ?_assertEqual(
         [],
         khepri:get_store_ids()),
      ?_assertEqual(
         false,
         khepri_cluster:is_store_running(?FUNCTION_NAME))]}.

terminate_ra_server(ClusterName) ->
    RaSystem = ClusterName,
    Member = {ClusterName, node()},
    _ = ra:stop_server(RaSystem, Member),
    khepri_cluster:wait_for_ra_server_exit(Member).

is_store_running_with_no_running_stores_test_() ->
    [?_assertEqual(
        false,
        khepri_cluster:is_store_running(?FUNCTION_NAME))].

is_store_running_with_running_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         true,
         khepri_cluster:is_store_running(?FUNCTION_NAME))]}.

is_store_empty_on_an_empty_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         true,
         khepri:is_empty(?FUNCTION_NAME))]}.

is_store_empty_on_a_non_empty_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         false,
         khepri:is_empty(?FUNCTION_NAME))]}.

is_store_empty_in_a_tx_on_an_empty_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_empty()
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

is_store_empty_in_a_tx_on_a_non_empty_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           ok = khepri_tx:create([foo], foo_value),
                           khepri_tx:is_empty()
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
%         ok,
%         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
%      ?_assertEqual(
%         ok,
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
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         "\n"
         "\033[1;32m== CLUSTER MEMBERS ==\033[0m\n"
         "\n"
         "Failed to query cluster members: {error,noproc}\n",
         begin
             #{level := OldLogLevel} = logger:get_primary_config(),
             _ = logger:set_primary_config(level, debug),
             Log = helpers:capture_log(
                     fun() ->
                             khepri:info(
                               non_existing_store, #{timeout => 1000})
                     end),
             _ = logger:set_primary_config(level, OldLogLevel),
             ?assertSubString(<<"Cannot query members in store">>, Log),
             ?capturedOutput
         end)]}.

get_store_info_on_running_store_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, <<"bar">>], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
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
         "        {if_any,[{if_child_list_length,{gt,0}},{if_has_payload,true}]}\n"
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
    KeepWhile = #{[?THIS_KHEPRI_NODE] =>
                  #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
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

get_all_stores_info_with_no_store_test() ->
    ?assertEqual(
       "No stores running\n",
       begin
           khepri:info(),
           ?capturedOutput
       end).

get_all_stores_info_with_one_store_test_() ->
    StoreId = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         "Running stores:\n"
         "  " ++ atom_to_list(StoreId) ++ "\n",
         begin
             khepri:info(),
             ?capturedOutput
         end)]}.
