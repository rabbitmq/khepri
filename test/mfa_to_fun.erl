%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(mfa_to_fun).

-include_lib("eunit/include/eunit.hrl").

-include("src/khepri_error.hrl").
-include("src/khepri_mfa.hrl").

-export([args_to_list/0,
         args_to_list/1,
         args_to_list/2,
         args_to_list/3,
         args_to_list/4,
         args_to_list/5,
         args_to_list/6,
         args_to_list/7,
         args_to_list/8,
         args_to_list/9,
         args_to_list/10]).

-dialyzer([{nowarn_function, [mfa_to_fun_0_11_test/0]}]).

mfa_to_fun_0_0_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(),
       Fun()).

mfa_to_fun_0_1_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1),
       Fun(arg1)).

mfa_to_fun_0_2_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2),
       Fun(arg1, arg2)).

mfa_to_fun_0_3_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3),
       Fun(arg1, arg2, arg3)).

mfa_to_fun_0_4_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4),
       Fun(arg1, arg2, arg3, arg4)).

mfa_to_fun_0_5_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun(arg1, arg2, arg3, arg4, arg5)).

mfa_to_fun_0_6_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 6),
    ?assert(is_function(Fun, 6)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg1, arg2, arg3, arg4, arg5, arg6)).

mfa_to_fun_0_7_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 7),
    ?assert(is_function(Fun, 7)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg1, arg2, arg3, arg4, arg5, arg6, arg7)).

mfa_to_fun_0_8_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 8),
    ?assert(is_function(Fun, 8)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)).

mfa_to_fun_0_9_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 9),
    ?assert(is_function(Fun, 9)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)).

mfa_to_fun_0_10_test() ->
    MFA = {?MODULE, args_to_list, []},
    Fun = khepri_mfa:to_fun(MFA, 10),
    ?assert(is_function(Fun, 10)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_0_11_test() ->
    MFA = {?MODULE, args_to_list, []},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 11}),
      khepri_mfa:to_fun(MFA, 11)).

mfa_to_fun_1_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1),
       Fun()).

mfa_to_fun_1_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2),
       Fun(arg2)).

mfa_to_fun_1_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3),
       Fun(arg2, arg3)).

mfa_to_fun_1_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4),
       Fun(arg2, arg3, arg4)).

mfa_to_fun_1_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun(arg2, arg3, arg4, arg5)).

mfa_to_fun_1_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg2, arg3, arg4, arg5, arg6)).

mfa_to_fun_1_6_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 6),
    ?assert(is_function(Fun, 6)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg2, arg3, arg4, arg5, arg6, arg7)).

mfa_to_fun_1_7_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 7),
    ?assert(is_function(Fun, 7)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg2, arg3, arg4, arg5, arg6, arg7, arg8)).

mfa_to_fun_1_8_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 8),
    ?assert(is_function(Fun, 8)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)).

mfa_to_fun_1_9_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    Fun = khepri_mfa:to_fun(MFA, 9),
    ?assert(is_function(Fun, 9)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_1_10_test() ->
    MFA = {?MODULE, args_to_list, [arg1]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 10}),
      khepri_mfa:to_fun(MFA, 10)).

mfa_to_fun_2_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2),
       Fun()).

mfa_to_fun_2_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3),
       Fun(arg3)).

mfa_to_fun_2_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4),
       Fun(arg3, arg4)).

mfa_to_fun_2_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun(arg3, arg4, arg5)).

mfa_to_fun_2_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg3, arg4, arg5, arg6)).

mfa_to_fun_2_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg3, arg4, arg5, arg6, arg7)).

mfa_to_fun_2_6_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 6),
    ?assert(is_function(Fun, 6)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg3, arg4, arg5, arg6, arg7, arg8)).

mfa_to_fun_2_7_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 7),
    ?assert(is_function(Fun, 7)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg3, arg4, arg5, arg6, arg7, arg8, arg9)).

mfa_to_fun_2_8_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    Fun = khepri_mfa:to_fun(MFA, 8),
    ?assert(is_function(Fun, 8)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_2_9_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 9}),
      khepri_mfa:to_fun(MFA, 9)).

mfa_to_fun_3_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3),
       Fun()).

mfa_to_fun_3_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4),
       Fun(arg4)).

mfa_to_fun_3_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun(arg4, arg5)).

mfa_to_fun_3_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg4, arg5, arg6)).

mfa_to_fun_3_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg4, arg5, arg6, arg7)).

mfa_to_fun_3_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg4, arg5, arg6, arg7, arg8)).

mfa_to_fun_3_6_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 6),
    ?assert(is_function(Fun, 6)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg4, arg5, arg6, arg7, arg8, arg9)).

mfa_to_fun_3_7_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    Fun = khepri_mfa:to_fun(MFA, 7),
    ?assert(is_function(Fun, 7)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg4, arg5, arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_3_8_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 8}),
      khepri_mfa:to_fun(MFA, 8)).

mfa_to_fun_4_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4),
       Fun()).

mfa_to_fun_4_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun(arg5)).

mfa_to_fun_4_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg5, arg6)).

mfa_to_fun_4_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg5, arg6, arg7)).

mfa_to_fun_4_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg5, arg6, arg7, arg8)).

mfa_to_fun_4_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg5, arg6, arg7, arg8, arg9)).

mfa_to_fun_4_6_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    Fun = khepri_mfa:to_fun(MFA, 6),
    ?assert(is_function(Fun, 6)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg5, arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_4_7_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 7}),
      khepri_mfa:to_fun(MFA, 7)).

mfa_to_fun_5_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5),
       Fun()).

mfa_to_fun_5_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun(arg6)).

mfa_to_fun_5_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg6, arg7)).

mfa_to_fun_5_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg6, arg7, arg8)).

mfa_to_fun_5_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg6, arg7, arg8, arg9)).

mfa_to_fun_5_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    Fun = khepri_mfa:to_fun(MFA, 5),
    ?assert(is_function(Fun, 5)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg6, arg7, arg8, arg9, arg10)).

mfa_to_fun_5_6_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 6}),
      khepri_mfa:to_fun(MFA, 6)).

mfa_to_fun_6_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6),
       Fun()).

mfa_to_fun_6_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun(arg7)).

mfa_to_fun_6_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg7, arg8)).

mfa_to_fun_6_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg7, arg8, arg9)).

mfa_to_fun_6_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    Fun = khepri_mfa:to_fun(MFA, 4),
    ?assert(is_function(Fun, 4)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg7, arg8, arg9, arg10)).

mfa_to_fun_6_5_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 5}),
      khepri_mfa:to_fun(MFA, 5)).

mfa_to_fun_7_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7),
       Fun()).

mfa_to_fun_7_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun(arg8)).

mfa_to_fun_7_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg8, arg9)).

mfa_to_fun_7_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7]},
    Fun = khepri_mfa:to_fun(MFA, 3),
    ?assert(is_function(Fun, 3)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg8, arg9, arg10)).

mfa_to_fun_7_4_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 4}),
      khepri_mfa:to_fun(MFA, 4)).

mfa_to_fun_8_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8),
       Fun()).

mfa_to_fun_8_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun(arg9)).

mfa_to_fun_8_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8]},
    Fun = khepri_mfa:to_fun(MFA, 2),
    ?assert(is_function(Fun, 2)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg9, arg10)).

mfa_to_fun_8_3_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 3}),
      khepri_mfa:to_fun(MFA, 3)).

mfa_to_fun_9_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9),
       Fun()).

mfa_to_fun_9_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9]},
    Fun = khepri_mfa:to_fun(MFA, 1),
    ?assert(is_function(Fun, 1)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun(arg10)).

mfa_to_fun_9_2_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 2}),
      khepri_mfa:to_fun(MFA, 2)).

mfa_to_fun_10_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9, arg10]},
    Fun = khepri_mfa:to_fun(MFA, 0),
    ?assert(is_function(Fun, 0)),
    ?assertEqual(
       args_to_list(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
                    arg10),
       Fun()).

mfa_to_fun_10_1_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9, arg10]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 1}),
      khepri_mfa:to_fun(MFA, 1)).

mfa_to_fun_11_0_test() ->
    MFA = {?MODULE, args_to_list, [arg1, arg2, arg3, arg4, arg5, arg6, arg7,
                                   arg8, arg9, arg10, arg11]},
    ?assertError(
      ?khepri_exception(
        tx_mfa_with_arity_too_great,
        #{mfa := MFA,
          arity := 0}),
      khepri_mfa:to_fun(MFA, 0)).

args_to_list() ->
    [].

args_to_list(Arg1) ->
    [Arg1].

args_to_list(Arg1, Arg2) ->
    [Arg1, Arg2].

args_to_list(Arg1, Arg2, Arg3) ->
    [Arg1, Arg2, Arg3].

args_to_list(Arg1, Arg2, Arg3, Arg4) ->
    [Arg1, Arg2, Arg3, Arg4].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9].

args_to_list(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
    [Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10].
