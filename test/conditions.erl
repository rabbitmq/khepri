%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(conditions).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

%% -------------------------------------------------------------------
%% Compilation & optimization.
%% -------------------------------------------------------------------

optimize_if_all_test() ->
    ?assertEqual(
       #if_all{conditions = []},
       khepri_condition:compile(#if_all{conditions = []})),
    ?assertEqual(
       foo,
       khepri_condition:compile(#if_all{conditions = [foo]})),
    ?assertEqual(
       #if_child_list_version{version = 3},
       khepri_condition:compile(
         #if_all{conditions = [#if_child_list_version{version = 3}]})),
    ?assertEqual(
       #if_all{conditions = [foo,
                             #if_child_list_version{version = 3}]},
       khepri_condition:compile(
         #if_all{conditions = [foo,
                               #if_child_list_version{version = 3}]})),
    %% The exact match on the component name becomes the first tested
    %% condition.
    ?assertEqual(
       #if_all{conditions = [foo,
                             #if_child_list_version{version = 3}]},
       khepri_condition:compile(
         #if_all{conditions = [#if_child_list_version{version = 3},
                               foo]})).

optimize_if_any_test() ->
    ?assertEqual(
       #if_any{conditions = []},
       khepri_condition:compile(#if_any{conditions = []})),
    ?assertEqual(
       foo,
       khepri_condition:compile(#if_any{conditions = [foo]})),
    ?assertEqual(
       #if_child_list_version{version = 3},
       khepri_condition:compile(
         #if_any{conditions = [#if_child_list_version{version = 3}]})),
    ?assertEqual(
       #if_any{conditions = [foo,
                             #if_child_list_version{version = 3}]},
       khepri_condition:compile(
         #if_any{conditions = [foo,
                               #if_child_list_version{version = 3}]})),
    ?assertEqual(
       #if_any{conditions = [#if_child_list_version{version = 3},
                             foo]},
       khepri_condition:compile(
         #if_any{conditions = [#if_child_list_version{version = 3},
                               foo]})).

%% -------------------------------------------------------------------
%% Using conditions.
%% -------------------------------------------------------------------

eval_regex_test() ->
    ?assert(khepri_condition:eval_regex(?STAR, "a", undefined, atom)),
    ?assert(khepri_condition:eval_regex(?STAR, "b", undefined, <<"bin">>)),
    ?assert(khepri_condition:eval_regex(?STAR, "a", re:compile("a"), atom)),
    ?assertEqual(
       {false, ?STAR},
       khepri_condition:eval_regex(?STAR, "b", undefined, atom)),
    ?assertEqual(
       {false, ?STAR},
       khepri_condition:eval_regex(?STAR, "b", undefined, atom)),
    ?assertEqual(
       {false, {?STAR,
                {error,
                 {"missing terminating ] for character class", 3}}}},
       khepri_condition:eval_regex(?STAR, "[a-", undefined, atom)),
    ?assertEqual(
       {false, {?STAR,
                {error,
                 {"missing terminating ] for character class", 3}}}},
       khepri_condition:eval_regex(?STAR, "[a-", re:compile("[a-"), atom)).

compare_numerical_values_test() ->
    ?assert(khepri_condition:compare_numerical_values(1, 1)),
    ?assertNot(khepri_condition:compare_numerical_values(1, 2)),

    ?assert(khepri_condition:compare_numerical_values(1, {eq, 1})),
    ?assertNot(khepri_condition:compare_numerical_values(1, {eq, 2})),

    ?assert(khepri_condition:compare_numerical_values(1, {ne, 2})),
    ?assertNot(khepri_condition:compare_numerical_values(1, {ne, 1})),

    ?assert(khepri_condition:compare_numerical_values(1, {lt, 2})),
    ?assertNot(khepri_condition:compare_numerical_values(1, {lt, 1})),
    ?assertNot(khepri_condition:compare_numerical_values(2, {lt, 1})),

    ?assert(khepri_condition:compare_numerical_values(1, {le, 2})),
    ?assert(khepri_condition:compare_numerical_values(1, {le, 1})),
    ?assertNot(khepri_condition:compare_numerical_values(2, {le, 1})),

    ?assert(khepri_condition:compare_numerical_values(2, {gt, 1})),
    ?assertNot(khepri_condition:compare_numerical_values(2, {gt, 2})),
    ?assertNot(khepri_condition:compare_numerical_values(2, {gt, 3})),

    ?assert(khepri_condition:compare_numerical_values(2, {ge, 1})),
    ?assert(khepri_condition:compare_numerical_values(2, {ge, 2})),
    ?assertNot(khepri_condition:compare_numerical_values(2, {ge, 3})).

exact_name_matching_test() ->
    ?assert(khepri_condition:is_met(foo, foo, #node{})),
    ?assertEqual({false, foo}, khepri_condition:is_met(foo, bar, #node{})).

if_node_exists_matching_test() ->
    %% is_met/3 is called when we are sure the node already exists. If the
    %% node doesn't exist, the condition is verified by
    %% can_continue_update_after_node_not_found/1 in khepri_machine.
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_node_exists{exists = true}),
         foo, #node{})),
    ?assertEqual(
       {false, #if_node_exists{exists = false}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_node_exists{exists = false}),
         foo, #node{})).

if_name_matches_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_name_matches{regex = any}),
         foo, #node{})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_name_matches{regex = "o"}),
         foo, #node{})),
    CompiledCond = khepri_condition:compile(#if_name_matches{regex = "a"}),
    ?assertEqual(
       {false, CompiledCond},
       khepri_condition:is_met(CompiledCond, foo, #node{})).

if_path_matches_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_path_matches{regex = any}),
         foo, #node{})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_path_matches{regex = "o"}),
         foo, #node{})),
    CompiledCond = khepri_condition:compile(#if_path_matches{regex = "a"}),
    ?assertEqual(
       {false, CompiledCond},
       khepri_condition:is_met(CompiledCond, foo, #node{})).

if_has_data_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_has_data{has_data = false}),
         foo, #node{})),
    ?assertEqual(
       {false, #if_has_data{has_data = true}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_has_data{has_data = true}),
         foo, #node{})),
    ?assertEqual(
       {false, #if_has_data{has_data = false}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_has_data{has_data = false}),
         foo, #node{payload = khepri_payload:data(foo)})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_has_data{has_data = true}),
         foo, #node{payload = khepri_payload:data(foo)})).

if_data_matches_matching_test() ->
    CompiledCond1 = khepri_condition:compile(
                      #if_data_matches{pattern = '_'}),
    ?assertEqual(
       {false, CompiledCond1},
       khepri_condition:is_met(
         CompiledCond1, foo, #node{})),
    ?assert(
       khepri_condition:is_met(
         CompiledCond1, foo, #node{payload = khepri_payload:data({a, b})})),

    CompiledCond2 = khepri_condition:compile(
                      #if_data_matches{pattern = {a, '_'}}),
    ?assert(
       khepri_condition:is_met(
         CompiledCond2, foo, #{data => {a, b}})),
    ?assertEqual(
       {false, CompiledCond2},
       khepri_condition:is_met(
         CompiledCond2, foo, #{})),
    ?assertEqual(
       {false, CompiledCond2},
       khepri_condition:is_met(
         CompiledCond2, foo, #{data => other})),

    ?assert(
       khepri_condition:is_met(
         CompiledCond2, foo, #node{payload = khepri_payload:data({a, b})})),
    ?assert(
       khepri_condition:is_met(
         CompiledCond2, foo, #node{payload = khepri_payload:data({a, c})})),
    ?assertEqual(
       {false, CompiledCond2},
       khepri_condition:is_met(
         CompiledCond2, foo, #node{payload = khepri_payload:data({b, c})})),
    ?assertEqual(
       {false, CompiledCond2},
       khepri_condition:is_met(
         CompiledCond2, foo, #node{payload = khepri_payload:data(other)})),

    CompiledCond3 = khepri_condition:compile(
                      #if_data_matches{pattern = {a, '$1'},
                                       conditions = [{is_integer, '$1'},
                                                     {'>=', '$1', 10}]}),
    ?assert(
       khepri_condition:is_met(
         CompiledCond3, foo, #{data => {a, 10}})),
    ?assertEqual(
       {false, CompiledCond3},
       khepri_condition:is_met(
         CompiledCond3, foo, #{})),
    ?assertEqual(
       {false, CompiledCond3},
       khepri_condition:is_met(
         CompiledCond3, foo, #{data => {a, 9}})),
    ?assertEqual(
       {false, CompiledCond3},
       khepri_condition:is_met(
         CompiledCond3, foo, #{data => {a, not_integer}})),
    ?assertEqual(
       {false, CompiledCond3},
       khepri_condition:is_met(
         CompiledCond3, foo, #{data => other})),
    ok.

if_payload_version_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_payload_version{version = 2}),
         foo, #{payload_version => 2})),

    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_payload_version{version = 2}),
         foo, #node{stat = #{payload_version => 2,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_payload_version{version = 2}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_payload_version{version = 2}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_payload_version{version = {ge, 1}}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_payload_version{version = {ge, 2}}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_payload_version{version = {ge, 2}}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})).

if_child_list_version_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_version{version = 2}),
         foo, #{child_list_version => 2})),

    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_version{version = 2}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 2}})),
    ?assertEqual(
       {false, #if_child_list_version{version = 2}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_version{version = 2}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_version{version = {ge, 1}}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_child_list_version{version = {ge, 2}}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_version{version = {ge, 2}}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})).

if_child_list_length_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = 2}),
         foo, #{child_list_length => 2})),

    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = 2}),
         foo, #node{child_nodes = #{a => #node{}, b => #node{}}})),
    ?assertEqual(
       {false, #if_child_list_length{count = 3}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = 3}),
         foo, #node{child_nodes = #{a => #node{}, b => #node{}}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = {ge, 1}}),
         foo, #node{child_nodes = #{a => #node{}, b => #node{}}})),
    ?assertEqual(
       {false, #if_child_list_length{count = {eq, 1}}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = {eq, 1}}),
         foo, #node{child_nodes = #{a => #node{}, b => #node{}}})).

if_not_matching_test() ->
    ?assertEqual(
       {false, #if_not{condition =
                       #if_child_list_length{count = 2}}},
       khepri_condition:is_met(
         khepri_condition:compile(#if_not{condition =
                                          #if_child_list_length{count = 2}}),
         foo, #{child_list_length => 2})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_not{condition =
                   #if_any{conditions = []}}),
         foo, #node{})),

    ?assertEqual(
       {false, #if_not{condition =
                       #if_any{conditions = [foo,
                                             #if_payload_version{version = 1}]}}},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_not{condition =
                   #if_any{conditions = [foo,
                                         #if_payload_version{version = 1}]}}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})).

if_all_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_all{conditions = []}),
         foo, #node{})),

    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_all{conditions = [foo,
                                 #if_payload_version{version = 1}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, bar},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_all{conditions = [bar,
                                 #if_payload_version{version = 1}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_payload_version{version = 2}},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_all{conditions = [foo,
                                 #if_payload_version{version = 2}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, bar},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_all{conditions = [bar,
                                 #if_payload_version{version = 2}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})).

if_any_matching_test() ->
    ?assertEqual(
       {false, #if_any{conditions = []}},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions = []}),
         foo, #node{})),

    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions = [foo,
                                 #if_payload_version{version = 1}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions = [bar,
                                 #if_payload_version{version = 1}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions = [foo,
                                 #if_payload_version{version = 2}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_any{conditions = [bar,
                                     #if_payload_version{version = 2}]}},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions = [bar,
                                 #if_payload_version{version = 2}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})).

complex_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions =
                   [#if_all{conditions =
                            [foo,
                             #if_child_list_length{count = {lt, 10}}]},
                    #if_payload_version{version = 1000}]}),
         foo, #node{stat = #{payload_version => 1,
                             child_list_version => 1}})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions =
                   [#if_all{conditions =
                            [bar,
                             #if_child_list_length{count = {lt, 10}}]},
                    #if_payload_version{version = 1000}]}),
         foo, #node{stat = #{payload_version => 1000,
                             child_list_version => 1}})),
    ?assertEqual(
       {false, #if_any{conditions =
                       [#if_all{conditions =
                                [bar,
                                 #if_child_list_length{count = {lt, 10}}]},
                        #if_payload_version{version = 1}]}},
       khepri_condition:is_met(
         khepri_condition:compile(
           #if_any{conditions =
                   [#if_all{conditions =
                            [bar,
                             #if_child_list_length{count = {lt, 10}}]},
                    #if_payload_version{version = 1}]}),
         foo, #node{stat = #{payload_version => 1000,
                             child_list_version => 1}})).


path_matching_test() ->
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_child_list_length{count = 0}),
         [], #node{})),
    ?assert(
       khepri_condition:is_met(
         khepri_condition:compile(#if_name_matches{regex = "a"}),
         [foo, bar], #node{})),
    ?assertEqual(
       {false, baz},
       khepri_condition:is_met(
         khepri_condition:compile(baz),
         [foo, bar], #node{})).

applies_to_grandchildren_test() ->
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_name_matches{regex = any})),
    ?assert(
       khepri_condition:applies_to_grandchildren(
         #if_path_matches{regex = any})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_data_matches{pattern = '_'})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_node_exists{exists = true})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_payload_version{version = 1})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_child_list_version{version = 1})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_child_list_length{count = 1})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_all{conditions = []})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_all{conditions = [#if_name_matches{regex = any},
                               #if_data_matches{pattern = '_'}]})),
    ?assert(
       khepri_condition:applies_to_grandchildren(
         #if_all{conditions = [#if_path_matches{regex = any},
                               #if_data_matches{pattern = '_'}]})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_any{conditions = []})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_any{conditions = [#if_name_matches{regex = any},
                               #if_data_matches{pattern = '_'}]})),
    ?assert(
       khepri_condition:applies_to_grandchildren(
         #if_any{conditions = [#if_path_matches{regex = any},
                               #if_data_matches{pattern = '_'}]})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_not{condition = #if_child_list_length{count = 1}})),
    ?assertNot(
       khepri_condition:applies_to_grandchildren(
         #if_not{condition =
                 #if_all{conditions = [#if_name_matches{regex = any},
                                       #if_data_matches{pattern = '_'}]}})),
    ?assert(
       khepri_condition:applies_to_grandchildren(
         #if_not{condition =
                 #if_all{conditions = [#if_path_matches{regex = any},
                                       #if_data_matches{pattern = '_'}]}})).
