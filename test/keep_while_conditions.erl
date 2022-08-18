%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(keep_while_conditions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'. Likewise
%% for:
%%   - `khepri_machine:get_keep_while_conds/1'
%%   - `khepri_machine:get_keep_while_conds_revidx/1'
%%   - `khepri_machine:are_keep_while_conditions_met/2'
-dialyzer(no_missing_calls).

are_keep_while_conditions_met_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Root = khepri_machine:get_root(S0),

    %% TODO: Add more testcases.
    ?assert(
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{})),
    ?assert(
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{[foo] => #if_node_exists{exists = true}})),
    ?assertEqual(
       {false, {pattern_matches_no_nodes, [baz]}},
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{[baz] => #if_node_exists{exists = true}})),
    ?assert(
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{[foo, bar] => #if_node_exists{exists = true}})),
    ?assert(
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{[foo, bar] => #if_child_list_length{count = 0}})),
    ?assertEqual(
       {false, #if_child_list_length{count = 1}},
       khepri_machine:are_keep_while_conditions_met(
         Root,
         #{[foo, bar] => #if_child_list_length{count = 1}})).

%% TODO: Add checks for the internal structures, `keep_while_conds` and
%% `keep_while_conds_revidx`.

insert_when_keep_while_true_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    KeepWhile = #{[foo] => #if_node_exists{exists = true}},
    Command = #put{path = [baz],
                   payload = khepri_payload:data(baz_value),
                   extra = #{keep_while => KeepWhile}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),
    KeepWhileConds = khepri_machine:get_keep_while_conds(S1),
    KeepWhileCondsRevIdx = khepri_machine:get_keep_while_conds_revidx(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes =
          #{foo =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = khepri_payload:data(foo_value)},
            baz =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = khepri_payload:data(baz_value)}}},
       Root),
    ?assertEqual(
       #{[baz] => KeepWhile},
       KeepWhileConds),
    ?assertEqual(
       #{[foo] => #{[baz] => ok}},
       KeepWhileCondsRevIdx),
    ?assertEqual({ok, #{[baz] => #{}}}, Ret),
    ?assertEqual([], SE).

insert_when_keep_while_false_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    %% The targeted keep_while node does not exist.
    KeepWhile1 = #{[foo, bar] => #if_node_exists{exists = true}},
    Command1 = #put{path = [baz],
                    payload = khepri_payload:data(baz_value),
                    extra = #{keep_while => KeepWhile1}},
    {S1, Ret1, SE1} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual({error,
                  {keep_while_conditions_not_met,
                   #{node_name => baz,
                     node_path => [baz],
                     keep_while_reason =>
                     {pattern_matches_no_nodes, [foo, bar]}}}},
                 Ret1),
    ?assertEqual([], SE1),

    %% The targeted keep_while node exists but the condition is not verified.
    KeepWhile2 = #{[foo] => #if_child_list_length{count = 10}},
    Command2 = #put{path = [baz],
                    payload = khepri_payload:data(baz_value),
                    extra =
                    #{keep_while => KeepWhile2}},
    {S2, Ret2, SE2} = khepri_machine:apply(?META, Command2, S0),

    ?assertEqual(S0#khepri_machine.root, S2#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual({error,
                  {keep_while_conditions_not_met,
                   #{node_name => baz,
                     node_path => [baz],
                     keep_while_reason =>
                     #if_child_list_length{count = 10}}}},
                 Ret2),
    ?assertEqual([], SE2).

insert_when_keep_while_true_on_self_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = 0}},
    Command = #put{path = [foo],
                   payload = khepri_payload:data(foo_value),
                   extra = #{keep_while => KeepWhile}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret),
    ?assertEqual([], SE).

insert_when_keep_while_false_on_self_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = 1}},
    Command = #put{path = [foo],
                   payload = khepri_payload:data(foo_value),
                   extra = #{keep_while => KeepWhile}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = khepri_payload:data(foo_value)}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret),
    ?assertEqual([], SE).

keep_while_still_true_after_command_test() ->
    KeepWhile = #{[foo] => #if_child_list_length{count = 0}},
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value),
                     extra = #{keep_while => KeepWhile}}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    Command = #put{path = [foo],
                   payload = khepri_payload:data(new_foo_value),
                   options = #{props_to_return => [payload,
                                                   payload_version,
                                                   child_list_version,
                                                   child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes =
          #{foo =>
            #node{
               stat = #{payload_version => 2,
                        child_list_version => 1},
               payload = khepri_payload:data(new_foo_value)},
            baz =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = khepri_payload:data(baz_value)}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{data => foo_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

keep_while_now_false_after_command_test() ->
    KeepWhile = #{[foo] => #if_child_list_length{count = 0}},
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value),
                     extra = #{keep_while => KeepWhile}}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    Command = #put{path = [foo, bar],
                   payload = khepri_payload:data(bar_value)},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 4},
          child_nodes =
          #{foo =>
            #node{
               stat = #{payload_version => 1,
                        child_list_version => 2},
               payload = khepri_payload:data(foo_value),
               child_nodes =
               #{bar =>
                 #node{stat = ?INIT_NODE_STAT,
                       payload = khepri_payload:data(bar_value)}}}}},
       Root),
    ?assertEqual({ok, #{[foo, bar] => #{}}}, Ret),
    ?assertEqual([], SE).

recursive_automatic_cleanup_test() ->
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = {gt, 0}}},
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value),
                     extra = #{keep_while => KeepWhile}},
                #put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value),
                     extra = #{keep_while => KeepWhile}},
                #put{path = [foo, bar, baz],
                     payload = khepri_payload:data(baz_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    Command = #delete{path = [foo, bar, baz],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes = #{}},
       Root),
    ?assertEqual({ok, #{[foo, bar, baz] => #{data => baz_value,
                                             payload_version => 1,
                                             child_list_version => 1,
                                             child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).

keep_while_now_false_after_delete_command_test() ->
    KeepWhile = #{[foo] => #if_node_exists{exists = true}},
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value),
                     extra = #{keep_while => KeepWhile}}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),

    Command = #delete{path = [foo],
                      options = #{props_to_return => [payload,
                                                      payload_version,
                                                      child_list_version,
                                                      child_list_length]}},
    {S1, Ret, SE} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 5},
          child_nodes = #{}},
       Root),
    ?assertEqual({ok, #{[foo] => #{data => foo_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret),
    ?assertEqual([], SE).
