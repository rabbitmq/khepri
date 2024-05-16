%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(machine_code_called_from_ra).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_payload.hrl").
-include("test/helpers.hrl").

-dialyzer([{nowarn_function,
            [use_an_invalid_path_test_/0,
             use_an_invalid_payload_test_/0,
             submit_unknown_command_test_/0]}]).

insert_a_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:put(
           ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value)))]}.

query_a_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, foo_value},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value)),
             khepri:get(?FUNCTION_NAME, [foo])
         end)]}.

delete_a_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Adding and deleting key/value",
         ?_assertEqual(
            ok,
            begin
                _ = khepri:put(
                      ?FUNCTION_NAME, [foo],
                      khepri_payload:data(foo_value)),
                khepri:delete(?FUNCTION_NAME, [foo])
            end)},
        {"Checking the deleted key is gone",
         ?_assertMatch(
            {error, ?khepri_error(node_not_found, _)},
            khepri:get(?FUNCTION_NAME, [foo]))}]}
     ]}.

query_keep_while_conds_state_test_() ->
    KeepWhile = #{[?THIS_KHEPRI_NODE] =>
                  #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           ok,
           khepri:put(
             ?FUNCTION_NAME,
             [foo],
             khepri_payload:data(foo_value),
             #{keep_while => KeepWhile})),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))
       ]}]}.

use_unix_string_path_in_keep_while_cond_test_() ->
    KeepWhile = #{"." => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           ok,
           khepri:put(
             ?FUNCTION_NAME,
             [foo],
             khepri_payload:data(foo_value),
             #{keep_while => KeepWhile})),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))
       ]}]}.

use_unix_binary_path_in_keep_while_cond_test_() ->
    KeepWhile = #{<<".">> => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           ok,
           khepri:put(
             ?FUNCTION_NAME,
             [foo],
             khepri_payload:data(foo_value),
             #{keep_while => KeepWhile})),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))
       ]}]}.

use_an_invalid_path_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(invalid_path, #{path := not_a_list}),
         khepri:put(
           ?FUNCTION_NAME,
           not_a_list,
           ?NO_PAYLOAD)),
      ?_assertError(
         ?khepri_exception(invalid_path, #{path := ["not_a_component"]}),
         khepri:put(
           ?FUNCTION_NAME,
           ["not_a_component"],
           ?NO_PAYLOAD))]}.

use_an_invalid_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(invalid_payload, #{path := [foo],
                                              payload := invalid_payload}),
         khepri_machine:put(
           ?FUNCTION_NAME,
           [foo],
           invalid_payload,
           #{})),
      ?_assertError(
         ?khepri_exception(
            invalid_payload,
            #{path := [foo],
              payload := {invalid_payload, in_a_tuple}}),
         khepri_machine:put(
           ?FUNCTION_NAME,
           [foo],
           {invalid_payload, in_a_tuple},
           #{}))]}.

submit_unknown_command_test_() ->
    UnknownCommand = unknown_command,
    MacVer = khepri_machine:version(),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            unknown_khepri_state_machine_command,
            #{command := UnknownCommand,
              machine_version := MacVer}),
         khepri_machine:process_command(
           ?FUNCTION_NAME, unknown_command, #{}))]}.
