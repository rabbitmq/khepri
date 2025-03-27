%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(return_indirect_deletes_option).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

put_with_default_return_indirect_deletes_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = false}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1},
                [bar] => #{data => bar_value,
                           payload_version => 1,
                           delete_reason => keep_while}}},
         khepri_adv:create(?FUNCTION_NAME, [foo], foo_value))]}.

put_with_return_indirect_deletes_true_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = false}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1},
                [bar] => #{data => bar_value,
                           payload_version => 1,
                           delete_reason => keep_while}}},
         khepri_adv:create(
           ?FUNCTION_NAME, [foo], foo_value,
           #{return_indirect_deletes => true}))]}.

put_with_return_indirect_deletes_false_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = false}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{payload_version => 1}}},
         khepri_adv:create(
           ?FUNCTION_NAME, [foo], foo_value,
           #{return_indirect_deletes => false}))]}.

delete_with_default_return_indirect_deletes_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = true}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           delete_reason => explicit},
                [bar] => #{data => bar_value,
                           payload_version => 1,
                           delete_reason => keep_while}}},
         khepri_adv:delete(?FUNCTION_NAME, [foo]))]}.

delete_with_return_indirect_deletes_true_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = true}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           delete_reason => explicit},
                [bar] => #{data => bar_value,
                           payload_version => 1,
                           delete_reason => keep_while}}},
         khepri_adv:delete(
           ?FUNCTION_NAME, [foo], #{return_indirect_deletes => true}))]}.

delete_with_return_indirect_deletes_false_test_() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = true}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:create(
           ?FUNCTION_NAME, [bar], bar_value,
           #{keep_while => KeepWhile})),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1,
                           delete_reason => explicit}}},
         khepri_adv:delete(
           ?FUNCTION_NAME, [foo], #{return_indirect_deletes => false}))]}.
