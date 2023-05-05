%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(batch_command).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

batch_zero_command_test() ->
    Batch0 = khepri_batch:new(),
    Command = khepri_batch:to_command(Batch0),
    ?assertEqual(none, Command).

batch_single_command_test() ->
    Batch0 = khepri_batch:new(),
    Batch1 = khepri_batch_adv:put(
               Batch0,
               [foo], value,
               #{props_to_return => [payload_version]}),
    Command = khepri_batch:to_command(Batch1),

    S0 = khepri_machine:init(?MACH_PARAMS()),
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               props = ?INIT_NODE_PROPS,
               payload = khepri_payload:data(value)}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{payload_version => 1}}}, Ret),
    ?assertEqual([], SE).

batch_many_commands_test() ->
    Count = 5,
    Batch0 = khepri_batch:new(),
    Batch1 = lists:foldl(
               fun(I, B) ->
                       khepri_batch_adv:put(
                         B,
                         [foo], I,
                         #{props_to_return => [payload_version]})
               end, Batch0, lists:seq(1, Count)),
    Command = khepri_batch:to_command(Batch1),

    S0 = khepri_machine:init(?MACH_PARAMS()),
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               props = #{payload_version => 5,
                         child_list_version => 1},
               payload = khepri_payload:data(Count)}}},
       Root),
    ?assertEqual(
       {ok, [{ok, #{[foo] => #{payload_version => 1}}},
             {ok, #{[foo] => #{payload_version => 2}}},
             {ok, #{[foo] => #{payload_version => 3}}},
             {ok, #{[foo] => #{payload_version => 4}}},
             {ok, #{[foo] => #{payload_version => 5}}}]},
       Ret),
    ?assertEqual([], SE).

batch_zero_commands_in_real_state_machine_test_() ->
    Batch0 = khepri_batch:new(),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, []},
         khepri_batch:commit(Batch0))]}.

batch_single_command_in_real_state_machine_test_() ->
    Batch0 = khepri_batch:new(?FUNCTION_NAME),
    Batch1 = khepri_batch:put(
               Batch0,
               [foo], value,
               #{props_to_return => [payload_version]}),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual({ok, [ok]}, khepri_batch:commit(Batch1)),
      ?_assertEqual({ok, value}, khepri:get(?FUNCTION_NAME, [foo]))]}.

batch_single_adv_command_in_real_state_machine_test_() ->
    Batch0 = khepri_batch:new(?FUNCTION_NAME),
    Batch1 = khepri_batch_adv:put(
               Batch0,
               [foo], value,
               #{props_to_return => [payload_version]}),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, [{ok, #{payload_version => 1}}]},
         khepri_batch:commit(Batch1)),
      ?_assertEqual({ok, value}, khepri:get(?FUNCTION_NAME, [foo]))]}.

batch_many_commands_in_real_state_machine_test_() ->
    Count = 5,
    Batch0 = khepri_batch:new(?FUNCTION_NAME),
    Batch1 = lists:foldl(
               fun(I, B) ->
                       khepri_batch:put(
                         B,
                         [foo], I,
                         #{props_to_return => [payload_version]})
               end, Batch0, lists:seq(1, Count)),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual({ok, [ok, ok, ok, ok, ok]}, khepri_batch:commit(Batch1)),
      ?_assertEqual({ok, Count}, khepri:get(?FUNCTION_NAME, [foo]))]}.

batch_many_adv_commands_in_real_state_machine_test_() ->
    Count = 5,
    Batch0 = khepri_batch:new(?FUNCTION_NAME),
    Batch1 = lists:foldl(
               fun(I, B) ->
                       khepri_batch_adv:put(
                         B,
                         [foo], I,
                         #{props_to_return => [payload_version]})
               end, Batch0, lists:seq(1, Count)),
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, [{ok, #{payload_version => 1}},
               {ok, #{payload_version => 2}},
               {ok, #{payload_version => 3}},
               {ok, #{payload_version => 4}},
               {ok, #{payload_version => 5}}]},
         khepri_batch:commit(Batch1)),
      ?_assertEqual({ok, Count}, khepri:get(?FUNCTION_NAME, [foo]))]}.
