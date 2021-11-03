%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(keep_until_conditions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

are_keep_until_conditions_met_test() ->
    Commands = [#put{path = [foo, bar],
                     payload = ?DATA_PAYLOAD(bar_value)}],
    S0 = khepri_machine:init(#{commands => Commands}),
    Root = khepri_machine:get_root(S0),

    %% TODO: Add more testcases.
    ?assert(
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{})),
    ?assert(
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{[foo] => #if_node_exists{exists = true}})),
    ?assertEqual(
       {false, {pattern_matches_no_nodes, [baz]}},
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{[baz] => #if_node_exists{exists = true}})),
    ?assert(
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{[foo, bar] => #if_node_exists{exists = true}})),
    ?assert(
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{[foo, bar] => #if_child_list_length{count = 0}})),
    ?assertEqual(
       {false, #if_child_list_length{count = 1}},
       khepri_machine:are_keep_until_conditions_met(
         Root,
         #{[foo, bar] => #if_child_list_length{count = 1}})).

%% TODO: Add checks for the internal structures, `keep_untils` and
%% `keep_untils_revidx`.

insert_when_keep_until_true_test() ->
    Commands = [#put{path = [foo],
                     payload = ?DATA_PAYLOAD(foo_value)}],
    S0 = khepri_machine:init(#{commands => Commands}),

    KeepUntil = #{[foo] => #if_node_exists{exists = true}},
    Command = #put{path = [baz],
                   payload = ?DATA_PAYLOAD(baz_value),
                   extra = #{keep_until => KeepUntil}},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),
    KeepUntils = khepri_machine:get_keep_untils(S1),
    KeepUntilsRevIdx = khepri_machine:get_keep_untils_revidx(S1),

    ?assertEqual(
       #node{
          stat =
          #{payload_version => 1,
            child_list_version => 3},
          child_nodes =
          #{foo =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = {data, foo_value}},
            baz =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = {data, baz_value}}}},
       Root),
    ?assertEqual(
       #{[baz] => KeepUntil},
       KeepUntils),
    ?assertEqual(
       #{[foo] => #{[baz] => ok}},
       KeepUntilsRevIdx),
    ?assertEqual({ok, #{[baz] => #{}}}, Ret).

insert_when_keep_until_false_test() ->
    Commands = [#put{path = [foo],
                     payload = ?DATA_PAYLOAD(foo_value)}],
    S0 = khepri_machine:init(#{commands => Commands}),

    %% The targeted keep_until node does not exist.
    KeepUntil1 = #{[foo, bar] => #if_node_exists{exists = true}},
    Command1 = #put{path = [baz],
                    payload = ?DATA_PAYLOAD(baz_value),
                    extra = #{keep_until => KeepUntil1}},
    {S1, Ret1} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual({error,
                  {keep_until_conditions_not_met,
                   #{node_name => baz,
                     node_path => [baz],
                     keep_until_reason =>
                     {pattern_matches_no_nodes, [foo, bar]}}}},
                 Ret1),

    %% The targeted keep_until node exists but the condition is not verified.
    KeepUntil2 = #{[foo] => #if_child_list_length{count = 10}},
    Command2 = #put{path = [baz],
                    payload = ?DATA_PAYLOAD(baz_value),
                    extra =
                    #{keep_until => KeepUntil2}},
    {S2, Ret2} = khepri_machine:apply(?META, Command2, S0),

    ?assertEqual(S0#khepri_machine.root, S2#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual({error,
                  {keep_until_conditions_not_met,
                   #{node_name => baz,
                     node_path => [baz],
                     keep_until_reason =>
                     #if_child_list_length{count = 10}}}},
                 Ret2).

insert_when_keep_until_true_on_self_test() ->
    S0 = khepri_machine:init(#{}),
    KeepUntil = #{[?THIS_NODE] => #if_child_list_length{count = 0}},
    Command = #put{path = [foo],
                   payload = ?DATA_PAYLOAD(foo_value),
                   extra = #{keep_until => KeepUntil}},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
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
               payload = {data, foo_value}}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret).

insert_when_keep_until_false_on_self_test() ->
    S0 = khepri_machine:init(#{}),
    KeepUntil = #{[?THIS_NODE] => #if_child_list_length{count = 1}},
    Command = #put{path = [foo],
                   payload = ?DATA_PAYLOAD(foo_value),
                   extra = #{keep_until => KeepUntil}},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
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
               payload = {data, foo_value}}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret).

keep_until_still_true_after_command_test() ->
    KeepUntil = #{[foo] => #if_child_list_length{count = 0}},
    Commands = [#put{path = [foo],
                     payload = ?DATA_PAYLOAD(foo_value)},
                #put{path = [baz],
                     payload = ?DATA_PAYLOAD(baz_value),
                     extra = #{keep_until => KeepUntil}}],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{path = [foo],
                   payload = ?DATA_PAYLOAD(new_foo_value)},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
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
               payload = {data, new_foo_value}},
            baz =>
            #node{
               stat = ?INIT_NODE_STAT,
               payload = {data, baz_value}}}},
       Root),
    ?assertEqual({ok, #{[foo] => #{data => foo_value,
                                   payload_version => 1,
                                   child_list_version => 1,
                                   child_list_length => 0}}}, Ret).

keep_until_now_false_after_command_test() ->
    KeepUntil = #{[foo] => #if_child_list_length{count = 0}},
    Commands = [#put{path = [foo],
                     payload = ?DATA_PAYLOAD(foo_value)},
                #put{path = [baz],
                     payload = ?DATA_PAYLOAD(baz_value),
                     extra = #{keep_until => KeepUntil}}],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{path = [foo, bar],
                   payload = ?DATA_PAYLOAD(bar_value)},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
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
               payload = {data, foo_value},
               child_nodes =
               #{bar =>
                 #node{stat = ?INIT_NODE_STAT,
                       payload = {data, bar_value}}}}}},
       Root),
    ?assertEqual({ok, #{[foo, bar] => #{}}}, Ret).

recursive_automatic_cleanup_test() ->
    KeepUntil = #{[?THIS_NODE] => #if_child_list_length{count = {gt, 0}}},
    Commands = [#put{path = [foo],
                     payload = ?DATA_PAYLOAD(foo_value),
                     extra = #{keep_until => KeepUntil}},
                #put{path = [foo, bar],
                     payload = ?DATA_PAYLOAD(bar_value),
                     extra = #{keep_until => KeepUntil}},
                #put{path = [foo, bar, baz],
                     payload = ?DATA_PAYLOAD(baz_value)}],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #delete{path = [foo, bar, baz]},
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
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
                                             child_list_length => 0}}}, Ret).
