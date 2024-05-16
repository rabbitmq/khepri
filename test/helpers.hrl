%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-define(META, #{term => 1,
                index => ?LINE,
                system_time => erlang:system_time(millisecond),
                machine_version => khepri_machine:version()}).

-define(MACH_PARAMS(),
        #{store_id => ?FUNCTION_NAME,
          member => khepri_cluster:this_member(?FUNCTION_NAME)}).
-define(MACH_PARAMS(Commands),
        #{store_id => ?FUNCTION_NAME,
          member => khepri_cluster:this_member(?FUNCTION_NAME),
          commands => Commands}).

%% Asserts that `SubString' is contained in `String'.
%%
%% `SubString' and `String' may either be lists or binaries but both must be
%% the same type in any call.
-define(assertSubString(SubString, String),
        ?assertNotMatch(nomatch, string:find(String, SubString))).

%% Asserts that a message matching the pattern `Pattern' <em>is</em> received
%% by the current process in the time window `Timeout'.
-define(assertReceive(Pattern, Timeout),
        receive
            Pattern ->
                ok
        after
            Timeout ->
                error(
                  "Did not receive a message matching the given pattern "
                  "within the timeout")
        end).

%% Asserts that a message matching the pattern `Pattern' <em>is</em> in the
%% mailbox of the current process.
-define(assertReceived(Pattern), ?assertReceive(Pattern, 0)).

%% Asserts that a message matching the pattern `Pattern' is <em>not</em>
%% received by the current process in the time window `Timeout'.
-define(assertNotReceive(Pattern, Timeout),
        receive
            Pattern = Msg ->
                error(
                  lists:flatten(
                    io_lib:format(
                      "Expected no messages matching the given pattern but "
                      "received ~p", [Msg])))
        after
            Timeout ->
                ok
        end).

%% Asserts that a message matching the pattern `Pattern' is <em>not</em> in the
%% mailbox of the current process.
-define(assertNotReceived(Pattern), ?assertNotReceive(Pattern, 0)).

%% Asserts that the `Expected' list is equal to the `Actual' list when sorted.
-define(assertListsEqual(Expected, Actual),
        fun() ->
                ExpectedSorted = lists:sort(Expected),
                ?assertEqual(ExpectedSorted, lists:sort(Actual))
        end()).
