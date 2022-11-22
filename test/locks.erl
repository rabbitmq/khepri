%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(locks).

-include_lib("eunit/include/eunit.hrl").

-include("src/khepri_error.hrl").

-define(SPAWNED(Expr),
        fun() ->
                TestProc__ = self(),
                Tag__ = erlang:make_ref(),
                spawn(fun() -> TestProc__ ! {Tag__, Expr} end),
                receive {Tag__, Result__} -> Result__ end
        end()).

-define(assertSpawnedEqual(Expected, Expr),
        ?assertEqual(Expected, ?SPAWNED(Expr))).

with_lock_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,
    TestProc = self(),

    spawn(fun() ->
                  {true, 42} = khepri_lock:with_lock(
                                 StoreId,
                                 LockId,
                                 fun() ->
                                         TestProc ! acquired,
                                         timer:sleep(20),
                                         42
                                 end),
                  TestProc ! released
          end),

    %% The test process holds the lock while the function is being executed.
    receive acquired -> ok end,

    ?assertEqual(
      false,
      khepri_lock:with_lock(
        StoreId, LockId, #{retries => 0},
        %% If the lock is unavailable, the inner function is not called.
        fun() -> throw(unreachable) end)),

    %% When the inner function is done, the lock is released.
    receive released -> ok end,
    ?assertEqual(0, count_holders(StoreId, LockId)),

    %% If the function exits, the lock is released anyways.
    ?assertThrow(
      crash,
      khepri_lock:with_lock(StoreId, LockId, fun() -> throw(crash) end)),
    ?assertEqual(0, count_holders(StoreId, LockId)),

    test_ra_server_helpers:cleanup(Props).

exclusive_lock_cannot_be_acquired_twice_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,

    {true, Lock} = khepri_lock:acquire(StoreId, LockId),

    %% Cannot be acquired recursively be the same process by default.
    ?assertEqual(
      false,
      khepri_lock:acquire(StoreId, LockId, #{retries => 2})),

    %% Cannot be acquired by any other process.
    ?assertSpawnedEqual(
      false,
      khepri_lock:acquire(StoreId, LockId, #{retries => 2})),

    true = khepri_lock:release(Lock),
    test_ra_server_helpers:cleanup(Props).

lock_is_released_when_holder_exits_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,
    TestProc = self(),

    {_Pid, MonitorRef} =
    spawn_monitor(fun() ->
                          {true, _Lock} = khepri_lock:acquire(StoreId, LockId),
                          TestProc ! acquired,
                          timer:sleep(20)
                  end),

    receive acquired -> ok end,
    ?assertEqual(1, count_holders(StoreId, LockId)),

    receive {'DOWN', MonitorRef, process, _, _} -> ok end,
    %% The lock can be acquired by another process.
    {true, Lock} = khepri_lock:acquire(StoreId, LockId, #{retries => 3}),
    ?assertEqual(1, count_holders(StoreId, LockId)),

    true = khepri_lock:release(Lock),
    ?assertEqual(0, count_holders(StoreId, LockId)),

    test_ra_server_helpers:cleanup(Props).

recursive_exclusive_lock_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,
    Options = #{recursive => true},

    {true, Lock} = khepri_lock:attempt(StoreId, LockId, Options),
    ?assertEqual(1, count_holders(StoreId, LockId)),
    %% The same process can acquire the lock again.
    {true, Lock1} = khepri_lock:attempt(StoreId, LockId, Options),
    ?assertEqual(2, count_holders(StoreId, LockId)),

    %% The lock is exclusive: other processes cannot also acquire the lock.
    ?assertSpawnedEqual(
      false,
      khepri_lock:attempt(StoreId, LockId, Options)),

    %% The lock must be released as many times as it is acquired.
    ?assertEqual(true, khepri_lock:release(Lock1)),
    ?assertEqual(1, count_holders(StoreId, LockId)),

    ?assertEqual(true, khepri_lock:release(Lock)),
    ?assertEqual(0, count_holders(StoreId, LockId)),

    test_ra_server_helpers:cleanup(Props).

non_exclusive_lock_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,
    Options = #{group => ?FUNCTION_NAME, recursive => true},

    {true, Lock} = khepri_lock:attempt(StoreId, LockId, Options),

    {true, Lock1} = khepri_lock:attempt(StoreId, LockId, Options),
    ?assertEqual(2, count_holders(StoreId, LockId)),

    ?assertSpawnedEqual(
      3,
      begin
          {true, Lock2} = khepri_lock:attempt(
                            StoreId,
                            LockId,
                            Options#{release_on_disconnect => true}),
          Holders = count_holders(StoreId, LockId),
          true = khepri_lock:release(Lock2),
          Holders
      end),

    %% The lock must be released by all holders.
    ?assertEqual(2, count_holders(StoreId, LockId)),
    ?assertEqual(true, khepri_lock:release(Lock1)),
    ?assertEqual(1, count_holders(StoreId, LockId)),
    ?assertEqual(true, khepri_lock:release(Lock)),
    ?assertEqual(0, count_holders(StoreId, LockId)),

    test_ra_server_helpers:cleanup(Props).

cannot_change_recursivity_test() ->
    Props = test_ra_server_helpers:setup(?FUNCTION_NAME),
    LockId = ?FUNCTION_NAME,
    StoreId = ?FUNCTION_NAME,

    {true, Lock} = khepri_lock:attempt(StoreId, LockId),

    ?assertEqual(
      {error,
       ?khepri_error(
          mismatching_recursive_option,
          #{expected => false,
            got => true,
            lock_id => LockId})},
      khepri_lock:attempt(StoreId, LockId, #{recursive => true})),

    true = khepri_lock:release(Lock),

    test_ra_server_helpers:cleanup(Props).

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

count_holders(StoreId, LockId) ->
    case khepri_lock:info(StoreId, LockId, #{favor => consistency}) of
        {ok, undefined} ->
            0;
        {ok, #{holds := Holds}} ->
            length(Holds);
        {error, _} = Error ->
            Error
    end.
