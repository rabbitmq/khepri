%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(utils).

-include_lib("eunit/include/eunit.hrl").

sleep_with_no_timeout_test() ->
    Sleep = 50,
    Timeout = infinity,
    T0 = erlang:monotonic_time(),
    ?assertEqual(Timeout, khepri_utils:sleep(Sleep, Timeout)),
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    ?assert(TDiff >= Sleep).

sleep_with_zero_timeout_test() ->
    Sleep = 50,
    Timeout = 0,
    T0 = erlang:monotonic_time(),
    NewTimeout = khepri_utils:sleep(Sleep, Timeout),
    ?assertEqual(0, NewTimeout),
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    ?assert(TDiff =< Sleep),
    ?assert(TDiff >= Timeout).

sleep_with_low_timeout_test() ->
    Sleep = 50,
    Timeout = 10,
    T0 = erlang:monotonic_time(),
    NewTimeout = khepri_utils:sleep(Sleep, Timeout),
    ?assertEqual(0, NewTimeout),
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    ?assert(TDiff =< Sleep),
    ?assert(TDiff >= Timeout).

sleep_with_high_timeout_test() ->
    Sleep = 50,
    Timeout = 1000,
    T0 = erlang:monotonic_time(),
    NewTimeout = khepri_utils:sleep(Sleep, Timeout),
    ?assert(NewTimeout =< Timeout - Sleep),
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    ?assert(TDiff >= Sleep).

timeout_window_with_no_timeout_test() ->
    Sleep = 50,
    Timeout = infinity,
    Window = khepri_utils:start_timeout_window(Timeout),
    timer:sleep(Sleep),
    NewTimeout = khepri_utils:end_timeout_window(Timeout, Window),
    ?assertEqual(Timeout, NewTimeout).

timeout_window_with_low_timeout_test() ->
    Sleep = 50,
    Timeout = 10,
    Window = khepri_utils:start_timeout_window(Timeout),
    timer:sleep(Sleep),
    NewTimeout = khepri_utils:end_timeout_window(Timeout, Window),
    ?assertEqual(0, NewTimeout).

timeout_window_with_high_timeout_test() ->
    Sleep = 50,
    Timeout = 1000,
    Window = khepri_utils:start_timeout_window(Timeout),
    timer:sleep(Sleep),
    NewTimeout = khepri_utils:end_timeout_window(Timeout, Window),
    ?assert(NewTimeout =< Timeout - Sleep).
