%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(transactions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

-dialyzer([{no_match,
            [exception_in_ro_transaction_test_/0,
             exception_in_rw_transaction_test_/0]},
           {no_missing_calls,
            [calling_unexported_remote_function_as_fun_term_test_/0]},
           {no_return,
            [aborted_transaction_test_/0,
             exception_in_ro_transaction_test_/0,
             exception_in_rw_transaction_test_/0]},
           {nowarn_function,
            [fun_taking_args_in_ro_transaction_test_/0,
             fun_taking_args_in_rw_transaction_test_/0,
             not_a_function_as_ro_transaction_test_/0,
             not_a_function_as_rw_transaction_test_/0,
             use_an_invalid_path_in_tx_test_/0]}]).

%% Used internally for a testcase.
-export([really_do_get_root_path/0,
         really_do_get_node_name/0]).

fun_extraction_test() ->
    Parent = self(),

    %% We load the `mod_used_for_transactions' and do the function extraction
    %% from a separate process. This is required so that we can unload the
    %% module.
    %%
    %% If we were to do that from the test process, it would have a reference
    %% to the module's code and it would be impossible to unload and purge it.
    Child = spawn(
              fun() ->
                      Fun = mod_used_for_transactions:get_lambda(),
                      ExpectedRet = Fun(Parent),
                      StandaloneFun = khepri_fun:to_standalone_fun(Fun, #{}),
                      Parent ! {standalone_fun,
                                StandaloneFun,
                                [Parent],
                                ExpectedRet}
              end),
    MRef = erlang:monitor(process, Child),
    receive
        {'DOWN', MRef, process, Child, _Reason} ->
            %% At this point, we are sure the child process is gone. We can
            %% unload the code.
            true = code:delete(mod_used_for_transactions),
            false = code:purge(mod_used_for_transactions),
            ?assertEqual(false, code:is_loaded(mod_used_for_transactions)),

            receive
                {standalone_fun, StandaloneFun, Args, ExpectedRet} ->
                    ?assertMatch({ok, _, _}, ExpectedRet),
                    ?assertEqual(
                       ExpectedRet,
                       khepri_fun:exec(StandaloneFun, Args))
            end
    end.

is_transaction_test_() ->
    [?_assertNot(khepri_tx:is_transaction()),
     {setup,
      fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
      fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
      [?_assertEqual(
          {atomic, true},
          begin
              Fun = fun() ->
                            khepri_tx:is_transaction()
                    end,
              khepri:transaction(?FUNCTION_NAME, Fun, ro)
          end),
       ?_assertEqual(
          {atomic, true},
          begin
              Fun = fun() ->
                            khepri_tx:is_transaction()
                    end,
              khepri:transaction(?FUNCTION_NAME, Fun, rw)
          end)]}
    ].

noop_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, ok},
         begin
             Fun = fun() ->
                           ok
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

noop_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, ok},
         begin
             Fun = fun() ->
                           ok
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_node_props_on_non_existing_node_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted,
          {error, {node_not_found, #{node_name => foo,
                                     node_path => [foo],
                                     node_is_target => true}}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_node_props([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_node_props_on_existing_node_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          #{data => value1,
            payload_version => 1,
            child_list_version => 1,
            child_list_length => 0}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_node_props([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_node_props_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          #{data => value1,
            payload_version => 1,
            child_list_version => 1,
            child_list_length => 0}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_node_props([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_data_on_non_existing_node_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted,
          {error, {node_not_found, #{node_name => foo,
                                     node_path => [foo],
                                     node_is_target => true}}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_on_existing_node_with_data_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, value1},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_on_existing_node_without_data_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted,
          {error, {no_data, #{payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:none()),

             Fun = fun() ->
                           khepri_tx:get_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, value1},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_data_on_many_nodes_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted,
          {error,
           {possibly_matching_many_nodes_denied,
            #if_name_matches{regex = any}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:none()),

             Fun = fun() ->
                           khepri_tx:get_data([?STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_or_default_on_non_existing_node_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, default},
         begin
             Fun = fun() ->
                           khepri_tx:get_data_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_or_default_on_existing_node_with_data_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, value1},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_data_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_or_default_on_many_nodes_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted,
          {error,
           {possibly_matching_many_nodes_denied,
            #if_name_matches{regex = any}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:none()),

             Fun = fun() ->
                           khepri_tx:get_data_or([?STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_or_default_on_existing_node_without_data_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, default},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:none()),

             Fun = fun() ->
                           khepri_tx:get_data_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

get_data_or_default_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, value1},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:get_data_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

put_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:get(Path) of
                               {ok, #{Path := #{data := value1}}} ->
                                   khepri_tx:put(
                                     Path, khepri_payload:data(value2));
                               Other ->
                                   Other
                           end
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

put_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:get(Path) of
                               {ok, #{Path := #{data := value1}}} ->
                                   khepri_tx:put(
                                     Path, khepri_payload:data(value2));
                               Other ->
                                   Other
                           end
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

put_with_native_keep_while_cond_test_() ->
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           {atomic,
            {ok, #{[foo] => #{}}}},
           begin
               Fun = fun() ->
                             khepri_tx:put(
                               [foo], value,
                               #{keep_while => KeepWhile})
                     end,
               khepri:transaction(?FUNCTION_NAME, Fun, rw)
           end),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))]}]}.

put_with_unix_string_keep_while_cond_test_() ->
    KeepWhile = #{"." => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           {atomic,
            {ok, #{[foo] => #{}}}},
           begin
               Fun = fun() ->
                             khepri_tx:put(
                               [foo], value,
                               #{keep_while => KeepWhile})
                     end,
               khepri:transaction(?FUNCTION_NAME, Fun, rw)
           end),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))]}]}.

put_with_unix_binary_keep_while_cond_test_() ->
    KeepWhile = #{<<".">> => #if_child_list_length{count = {gt, 0}}},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [?_assertEqual(
           {atomic,
            {ok, #{[foo] => #{}}}},
           begin
               Fun = fun() ->
                             khepri_tx:put(
                               [foo], value,
                               #{keep_while => KeepWhile})
                     end,
               khepri:transaction(?FUNCTION_NAME, Fun, rw)
           end),
        ?_assertEqual(
           {ok, #{[foo] =>
                  #{[foo] => #if_child_list_length{count = {gt, 0}}}}},
           khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME, #{}))]}]}.

create_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:create(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

create_on_non_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{}}}},
         begin
             Fun = fun() ->
                           khepri_tx:create(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

create_on_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {error,
           {mismatching_node,
            #{condition => #if_node_exists{exists = false},
              node_name => foo,
              node_path => [foo],
              node_is_target => true,
              node_props => #{data => value1,
                              payload_version => 1,
                              child_list_version => 1,
                              child_list_length => 0}}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:create(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

update_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:update(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

update_on_non_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {error,
           {node_not_found,
            #{condition => #if_all{conditions =
                                   [foo,
                                    #if_node_exists{exists = true}]},
              node_name => foo,
              node_path => [foo],
              node_is_target => true}}}},
         begin
             Fun = fun() ->
                           khepri_tx:update(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

update_on_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:update(
                             [foo], khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], value1, khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

compare_and_swap_on_non_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertMatch(
         {atomic,
          {error,
           {node_not_found,
            #{condition := #if_all{conditions =
                                   [foo,
                                    #if_data_matches{pattern = value1}]},
              node_name := foo,
              node_path := [foo],
              node_is_target := true}}}},
         begin
             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], value1, khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_on_existing_matching_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], value1, khepri_payload:data(value2))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

compare_and_swap_on_existing_non_matching_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertMatch(
         {atomic,
          {error,
           {mismatching_node,
            #{condition := #if_data_matches{pattern = value2},
              node_name := foo,
              node_path := [foo],
              node_is_target := true,
              node_props := #{data := value1,
                              payload_version := 1,
                              child_list_version := 1,
                              child_list_length := 0}}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:compare_and_swap(
                             [foo], value2, khepri_payload:data(value3))
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

clear_payload_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:clear_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

clear_payload_on_non_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{}}}},
         begin
             Fun = fun() ->
                           khepri_tx:clear_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

clear_payload_on_existing_node_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           khepri_tx:clear_payload([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

delete_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, store_update_denied},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:get(Path) of
                               {ok, #{Path := #{data := value1}}} ->
                                   khepri_tx:delete(Path);
                               Other ->
                                   Other
                           end
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

delete_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:get(Path) of
                               {ok, #{Path := #{data := value1}}} ->
                                   khepri_tx:delete(Path);
                               Other ->
                                   Other
                           end
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

exists_api_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {false, true, false}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo, bar],
                   khepri_payload:data(bar_value)),

             Fun = fun() ->
                           {khepri_tx:has_data([foo]),
                            khepri_tx:has_data([foo, bar]),
                            khepri_tx:has_data([foo, bar, baz])}
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

has_data_api_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {true, false}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value)),

             Fun = fun() ->
                           {khepri_tx:exists([foo]),
                            khepri_tx:exists([bar])}
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

find_api_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => foo_value,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value)),

             Fun = fun() ->
                           khepri_tx:find([], #if_data_matches{pattern = '_'})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

simple_api_test_() ->
    %% This is in the `khepri` module which provides the "simple API", but in
    %% the case of transactions, the API is the same as the "advanced API"
    %% from `khepri_machine`.
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:get(Path) of
                               {ok, #{Path := #{data := value1}}} ->
                                   khepri_tx:put(
                                     Path, khepri_payload:data(value2));
                               Other ->
                                   Other
                           end
                   end,
             %% Let Khepri detect if the transaction is R/W or R/O.
             khepri:transaction(?FUNCTION_NAME, Fun)
         end)]}.

case_abort_jump_instruction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, {created, [foo]}},
         begin
             Fun = fun() ->
                           Path = [foo],
                           case khepri_tx:put(Path, khepri_payload:data(value2)) of
                               {ok, _} ->
                                   ok;
                               Error ->
                                   khepri_tx:abort(Error)
                           end,
                           {created, Path}
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun)
         end)]}.

list_comprehension_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, [bar_value, foo_value]},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(foo_value)),
             _ = khepri:put(
                   ?FUNCTION_NAME, [bar], khepri_payload:data(bar_value)),

             Fun = fun() ->
                           {ok, Nodes} = khepri_tx:list([?ROOT_NODE]),
                           [Data ||
                            Path <- lists:sort(maps:keys(Nodes)),
                            #{data := Data} <- [maps:get(Path, Nodes)]]
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

aborted_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, abort_transaction},
         begin
             Fun = fun() ->
                           khepri_tx:abort(abort_transaction)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

fun_taking_args_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {requires_args, 1}},
         begin
             Fun = fun(Arg) ->
                           Arg
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

fun_taking_args_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {requires_args, 1}},
         begin
             Fun = fun(Arg) ->
                           Arg
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

not_a_function_as_ro_transaction_test_() ->
    Term = an_atom,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, Term},
         khepri:transaction(?FUNCTION_NAME, Term, ro))]}.

not_a_function_as_rw_transaction_test_() ->
    Term = an_atom,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, Term},
         khepri:transaction(?FUNCTION_NAME, Term, rw))]}.

exception_in_ro_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {badmatch, {ok, #{[] := #{payload_version := 1,
                                   child_list_version := 1,
                                   child_list_length := 0}}}},
         begin
             Fun = fun() ->
                           bad_return_value = khepri_tx:list([])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end)]}.

exception_in_rw_transaction_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         {badmatch, {ok, #{[] := #{payload_version := 1,
                                   child_list_version := 1,
                                   child_list_length := 0}}}},
         begin
             Fun = fun() ->
                           bad_return_value = khepri_tx:list([])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

external_variable_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, {ok, #{[] => #{payload_version => 1,
                                 child_list_version => 1,
                                 child_list_length => 0}}}},
         begin
             Path = [],
             Fun = fun() ->
                           khepri_tx:get(Path)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_root_path() -> do_get_root_path().
do_get_root_path() -> ?MODULE:really_do_get_root_path().
really_do_get_root_path() -> seriously_do_get_root_path().
seriously_do_get_root_path() -> [].

calling_valid_local_function_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, {ok, #{[] => #{payload_version => 1,
                                 child_list_version => 1,
                                 child_list_length => 0}}}},
         begin
             Fun = fun() ->
                           Path = get_root_path(),
                           khepri_tx:get(Path)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_node_name() -> do_get_node_name().
do_get_node_name() -> ?MODULE:really_do_get_node_name().
really_do_get_node_name() -> node().

calling_invalid_local_function_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, {node, 0}}},
         begin
             Fun = fun() ->
                           Path = [node, get_node_name()],
                           khepri_tx:get(Path)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

noop() -> ok.

calling_local_function_as_fun_term_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, ok},
         begin
             Fun = fun noop/0,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

calling_stdlib_function_as_fun_term_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, dict:new()},
         begin
             Fun = fun dict:new/0,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

calling_remote_function_as_fun_term_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, mod_used_for_transactions:exported()},
         begin
             Fun = fun mod_used_for_transactions:exported/0,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

calling_unexported_remote_function_as_fun_term_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun,
          {call_to_unexported_function,
           {mod_used_for_transactions, unexported, 0}}},
         begin
             Fun = fun mod_used_for_transactions:unexported/0,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

with(Arg, Fun) ->
    fun() -> Fun(Arg) end.

nested_funs_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic, {nested, arg1}},
         begin
             Fun = fun(Arg) ->
                           erlang:list_to_tuple([nested, Arg])
                   end,
             khepri:transaction(?FUNCTION_NAME, with(arg1, Fun), rw)
         end)]}.

trying_to_send_msg_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, sending_message_denied},
         begin
             Pid = self(),
             Fun = fun() ->
                           Pid ! msg
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_receive_msg_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, receiving_message_denied},
         begin
             Fun = fun() ->
                           receive
                               Msg ->
                                   Msg
                           after 100 ->
                                     ok
                           end
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_use_process_dict_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, {erlang, get, 0}}},
         begin
             Fun = fun() ->
                           get()
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_use_persistent_term_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, {persistent_term, put, 2}}},
         begin
             Fun = fun() ->
                           persistent_term:put(key, value)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_use_mnesia_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, {mnesia, read, 2}}},
         begin
             Fun = fun() ->
                           mnesia:read(table, key)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_run_http_request_test_() ->
    %% In this case, Khepri will try to copy the code of the `httpc' module
    %% and will eventually find a forbidden instruction or call.
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, _},
         begin
             Fun = fun() ->
                           httpc:request("url://")
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

trying_to_use_ssl_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, {ssl, connect, 4}}},
         begin
             Fun = fun() ->
                           ssl:connect("localhost", 1234, [], infinity)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

use_an_invalid_path_in_tx_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {aborted, {invalid_path, #{path => not_a_list}}},
         begin
             Fun = fun() ->
                           khepri_tx:put(not_a_list, ?NO_PAYLOAD)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun)
         end),
      ?_assertEqual(
         {aborted, {invalid_path, #{path => ["not_a_component"],
                                    tail => ["not_a_component"]}}},
         begin
             Fun = fun() ->
                           khepri_tx:put(["not_a_component"], ?NO_PAYLOAD)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun)
         end)]}.

-define(TX_CODE, "Fun = fun() ->
                          Path = [foo],
                          case khepri_tx:get(Path) of
                              {ok, #{Path := #{data := value1}}} ->
                                  khepri_tx:put(
                                    Path,
                                    khepri_payload:data(value2));
                              Other ->
                                  Other
                          end
                  end,
                  khepri:transaction(
                    " ++ atom_to_list(?FUNCTION_NAME) ++ ",
                    Fun).
                  ").

tx_from_the_shell_test_() ->
    %% We simuate the use of a transaction from the Erlang shell by using
    %% `erl_parse' and `erl_eval'. The transaction is the same as
    %% `put_tx_test_()'.
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {atomic,
          {ok, #{[foo] => #{data => value1,
                            payload_version => 1,
                            child_list_version => 1,
                            child_list_length => 0}}}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             Bindings = erl_eval:new_bindings(),
             {ok, Tokens, _EndLocation} = erl_scan:string(?TX_CODE),
             {ok, Exprs} = erl_parse:parse_exprs(Tokens),
             {value, Value, _NewBindings} = erl_eval:exprs(Exprs, Bindings),
             Value
         end)]}.

local_fun_using_erl_eval() ->
    Bindings = erl_eval:new_bindings(),
    {ok, Tokens, _EndLocation} = erl_scan:string(?TX_CODE),
    {ok, Exprs} = erl_parse:parse_exprs(Tokens),
    {value, Value, _NewBindings} = erl_eval:exprs(Exprs, Bindings),
    Value.

tx_using_erl_eval_test_() ->
    %% Unlike the previous testcase, the transaction calls a function using
    %% `erl_eval'. This situation shouldn't happen in an Erlang shell. It is
    %% unsupported and rejected.
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertThrow(
         {invalid_tx_fun, {call_denied, _}},
         begin
             _ = khepri:put(
                   ?FUNCTION_NAME, [foo], khepri_payload:data(value1)),

             khepri:transaction(
               ?FUNCTION_NAME,
               fun local_fun_using_erl_eval/0,
               rw)
         end)]}.
