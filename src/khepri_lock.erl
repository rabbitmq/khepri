%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Cluster-wide locking API.
%%
%% The locks in this module provide a way to ensure that only one or a group
%% of processes has access to some resource at a time. These locks are not
%% related to database locks - they are not involved in transactions and do not
%% prevent modifications of a Khepri store. Instead, these locks are similar
%% to locks created with {@link global:set_lock/3}.
%%
%% === Recursive locks ===
%%
%% Locks can be <em>recursive</em>: a holder of the lock can acquire the lock
%% again while holding the lock. Recursive locks must be released as many times
%% as they are acquired. Locks are non-recursive by default. A lock can be
%% acquired as recursive by passing the `recursive' option as `true'. See
%% {@link options()}.
%%
%% ```
%% LockId = my_lock,
%% Options = #{recursive => true},
%% {true, Lock1} = khepri_lock:acquire(LockId, Options),
%% {true, Lock2} = khepri_lock:acquire(LockId, Options),
%% true = khepri_lock:release(Lock2),
%% true = khepri_lock:release(Lock1)
%% '''
%%
%% === Exclusive locks ===
%%
%% Locks can be <em>exclusive</em>: only one process can acquire it at a time.
%% A lock can be acquired by multiple processes if all processes call {@link
%% acquire/4} with the same `LockId' and pass the same `group' option in
%% {@link options()}. If a lock is non-exclusive, it must also be recursive.
%% Locks are exclusive by default.
%%
%% ```
%% LockId = my_lock,
%% Options = #{group => my_group},
%% {true, Lock1} = khepri_lock:acquire(LockId, Options),
%% %% Another process can also acquire the lock if it provides the same LockId
%% %% and `group' term.
%% spawn(fun() ->
%%               {true, Lock2} = khepri_lock:acquire(LockId, Options),
%%               true = khepri_lock:release(Lock2)
%%       end),
%% true = khepri_lock:release(Lock1)
%% '''

-module(khepri_lock).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_cluster.hrl").
-include("src/khepri_lock.hrl").

-export([attempt/1, attempt/2, attempt/3,
         acquire/1, acquire/2, acquire/3,
         release/1,
         force_release/1, force_release/2,
         with_lock/2, with_lock/3, with_lock/4,
         info/1, info/2, info/3]).

%% Dialyzer complains about functions with "overlapping contracts." The specs
%% are correct but `khepri:store_id()' and `khepri_lock:lock_options()' are
%% both valid `khepri_lock:lock_id()'s, so the Dialyzer says that the
%% contracts overlap.
-dialyzer({no_contracts, [attempt/2,
                          acquire/2,
                          with_lock/3,
                          info/2]}).

-type lock_id() :: term().
%% A term used to represent a lockable resource.

-type group() :: pid() | term().
%% An identifier that unique to a process or group of processes acquiring a
%% lock.
%%
%% This term is used to ensure that a lock is only acquired by a single process
%% or a controllable group of processes. Exclusive locks use the acquirer's PID
%% for this term. When acquiring a non-exclusive lock with {@link attempt/3} or
%% {@link acquire/3}, the `group' option passed in {@link lock_options()} is
%% used.

-type lock() :: #khepri_lock{}.
%% A term representing an acquired lock.
%%
%% {@link attempt/3} and {@link acquire/3} return this type if a lock can
%% successfully be acquired. Locks should be passed to {@link release/1} to
%% make the lock available to other processes or groups.

-type lock_options() :: #{recursive => boolean(),
                          group => term(),
                          release_on_disconnect => boolean()}.
%% Options that control the behavior of a lock acquired with {@link attempt/3}
%% or {@link acquire/3}.
%%
%% The following keys are supported:
%%
%% <ul>
%% <li>`recursive' controls whether the lock should be recursive. Recursive
%% locks may be acquired by the same process multiple times. By default, this
%% option is `false'.</li>
%% <li>`group' controls whether the lock should be exclusive. When omitted,
%% the lock can only be acquired by the current process. Multiple processes may
%% acquire a lock if they all provide the same `StoreId', `LockId' and `group'
%% values to {@link attempt/3} or {@link acquire/3}.</li>
%% <li>`release_on_disconnect' controls whether the lock should be
%% automatically released if the holder of the lock exits with a `noconnection'
%% reason. This defaults to `false'. If a cluster member holds a lock and
%% becomes unavailable because of a network partition, the process may think it
%% still holds the lock but the cluster could allow another process to acquire
%% the lock if `release_on_disconnect' is `true'. When `release_on_disconnect'
%% is `false', locks must be released explicitly with {@link release/1} if the
%% holder process becomes disconnected from the cluster.</li>
%% </ul>

-type acquire_options() :: lock_options() |
                           #{retries => non_neg_integer() | infinity}.
%% Options for acquiring a lock with {@link acquire/3}.
%%
%% {@link lock_options()} are all valid options for {@link acquire/3}.
%% {@link acquire/3} also takes a `retries' option which controls the number
%% of times that {@link acquire/3} will attempt to acquire the lock before
%% giving up.

-type lock_hold() :: #{holder := pid(),
                       acquired_at := calendar:datetime1970(),
                       release_on_disconnect := boolean()}.
%% A map of information about a single hold on a lock by a process.

-type lock_info() :: #{group := group(),
                       recursive := boolean(),
                       holds := [lock_hold(), ...]}.
%% A map of information about a lock.

-export_type([lock_id/0,
              group/0,
              lock/0,
              lock_options/0,
              acquire_options/0,
              lock_hold/0,
              lock_info/0]).

%% -------------------------------------------------------------------
%% attempt().
%% -------------------------------------------------------------------

-spec attempt(LockId) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock once.
%%
%% Calling this function is the same as calling `attempt(StoreId, LockId, #{})'
%% with the default Store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see attempt/2.
%% @see attempt/3.

attempt(LockId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    attempt(StoreId, LockId, #{}).

-spec attempt
(StoreId, LockId) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock();
(LockId, Options) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:lock_options(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock once.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`attempt(StoreId, LockId)'. Calling it is the same as calling
%% `attempt(StoreId, LockId, #{})'.</li>
%% <li>`attempt(LockId, Options)'. Calling it is the same as calling
%% `attempt(StoreId, LockId, Options)' with the default Store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%% </ul>
%%
%% @see attempt/3.

attempt(StoreId, LockId) when ?IS_STORE_ID(StoreId) ->
    attempt(StoreId, LockId, #{});
attempt(LockId, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    attempt(StoreId, LockId, Options).

-spec attempt(StoreId, LockId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:lock_options(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock once.
%%
%% This function is the same as {@link acquire/3} but only attempts to acquire
%% the lock once.
%%
%% @param StoreId the name of the Khepri store.
%% @param LockId the term representing the lock.
%% @param Options options for acquiring the lock.
%%
%% @returns `{true, Lock}' if the lock is successfully acquired, `false' if
%% the lock is held by another process or group, and `{error, Reason}' if the
%% attempt failed for unexpected reasons.
%%
%% @see acquire/3.

attempt(StoreId, LockId, Options)
  when ?IS_STORE_ID(StoreId) andalso is_map(Options) ->
    Lock = lock(StoreId, LockId, Options),
    case khepri_machine:attempt_lock(Lock) of
        true ->
            {true, Lock};
        false ->
            false;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% acquire().
%% -------------------------------------------------------------------

-spec acquire(LockId) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock.
%%
%% Calling this function is the same as calling `acquire(StoreId, LockId, #{})'
%% with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see acquire/2.
%% @see acquire/3.

acquire(LockId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    acquire(StoreId, LockId, #{}).

-spec acquire
(StoreId, LockId) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock();
(LockId, Options) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:acquire_options(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`acquire(StoreId, LockId)'. Calling it is the same as calling
%% `acquire(StoreId, LockId, #{})'.</li>
%% <li>`acquire(LockId, Options)'. Calling it is the same as calling
%% `acquire(StoreId, LockId, Options)' with the default store ID (see
%% {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see acquire/3.

acquire(StoreId, LockId) when ?IS_STORE_ID(StoreId) ->
    acquire(StoreId, LockId, #{});
acquire(LockId, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    acquire(StoreId, LockId, Options).

-spec acquire(StoreId, LockId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:acquire_options(),
      Ret :: {true, Lock} | false | khepri:error(),
      Lock :: khepri_lock:lock().
%% @doc Attempts to acquire a lock.
%%
%% This function attempts to acquire a lock, returning `{true, Lock}' if it is
%% successful. If the lock can't be acquired and there are retries remaining,
%% this function will sleep at random increasing intervals. The first sleep
%% is at most 1/4 of a second. The maximum value doubles with each failed
%% attempt to acquire the lock up to a maximum of 8 seconds. The actual time
%% slept is randomized with {@link random:uniform/1}.
%%
%% If the number of retries is exhausted, this function returns `false'.
%% The number of retries can be configured with the `retries' option in {@link
%% acquire_options()}. By default, this value is `infinity' representing
%% unlimited retries.
%%
%% @param StoreId the name of the Khepri store.
%% @param LockId a term representing the lock.
%% @param Options options for acquiring the lock.
%%
%% @returns `{true, Lock}' if the lock is successfully acquired or `false'
%% if the lock is unavailable and the number of retries are exhausted,
%% and `{error, Reason}' with any other reason if the acquisition failed for
%% expected reasons.

acquire(StoreId, LockId, Options)
  when ?IS_STORE_ID(StoreId) andalso is_map(Options) ->
    Retries = maps:get(retries, Options, infinity),
    Lock = lock(StoreId, LockId, Options),
    acquire_with_backoff(Lock, Retries, 1).

%% -------------------------------------------------------------------
%% release().
%% -------------------------------------------------------------------

-spec release(Lock) -> Ret when
      Lock :: khepri_lock:lock(),
      Ret :: boolean() | khepri:error().
%% @doc Releases the given lock if it exists, synchronously.
%%
%% A lock may be held multiple times. This function returns `ok' if the lock
%% is successfully released once but the lock may still be held - either by the
%% same process if the lock is recursive or by other processes if the lock is
%% non-exclusive. See {@link group()} for more information.
%%
%% @param Lock the lock resource returned by {@link attempt/3} or
%% {@link acquire/3}.
%%
%% @returns `true' if the lock existed and was released, `false' if the lock
%% was not released or `{error, Reason}' if the release fails for unexpected
%% reasons.

release(Lock) ->
    khepri_machine:release_lock(Lock).

%% -------------------------------------------------------------------
%% force_release().
%% -------------------------------------------------------------------

-spec force_release(LockId) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Ret :: khepri:ok(TimesReleased) | khepri:error(),
      TimesReleased :: non_neg_integer().
%% @doc Synchronously and forcefully releases all holds on a lock.
%%
%% This is the same as calling `force_release(StoreId, LockId)' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see force_release/2.

force_release(LockId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    force_release(StoreId, LockId).

-spec force_release(StoreId, LockId) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Ret :: khepri:ok(TimesReleased) | khepri:error(),
      TimesReleased :: non_neg_integer().
%% @doc Synchronously and forcefully releases all holds on a lock.
%%
%% This function should only be used to manually remove any locks which are
%% stuck. Any holders of the lock are not notified that the lock has been
%% released.
%%
%% @returns `{ok, N}' where `N' is the number of times the lock was released
%% or `{error, Reason}' if the deletion failed for unexpected reasons. If the
%% lock does not exist, `{ok, 0}' is returned.

force_release(StoreId, LockId) when ?IS_STORE_ID(StoreId) ->
    khepri_machine:force_release_lock(StoreId, LockId).

%% -------------------------------------------------------------------
%% with_lock().
%% -------------------------------------------------------------------

-spec with_lock(LockId, Fun) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Fun :: fun(() -> FunRet),
      Ret :: {true, FunRet} | false | khepri:error().
%% @doc Executes a function after the given locks has been acquired, and
%% releases the lock afterwards.
%%
%% Calling this function is the same as calling `with_lock(StoreId, LockId,
%% #{}, Fun)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).
%%
%% @see with_lock/3.
%% @see with_lock/4.

with_lock(LockId, Fun) when is_function(Fun, 0) ->
    StoreId = khepri_cluster:get_default_store_id(),
    with_lock(StoreId, LockId, #{}, Fun).

-spec with_lock
(StoreId, LockId, Fun) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Fun :: fun(() -> FunRet),
      Ret :: {true, FunRet} | false | khepri:error();
(LockId, Options, Fun) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:acquire_options(),
      Fun :: fun(() -> FunRet),
      Ret :: {true, FunRet} | false | khepri:error().
%% @doc Executes a function after the given locks has been acquired, and
%% releases the lock afterwards.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`with_lock(StoreId, LockId, Fun)'. Calling it is the same as
%% calling `with_lock(StoreId, LockId, #{}, Fun)'.</li>
%% <li>`with_lock(LockId, Options, Fun)'. Calling it is the same as
%% calling `with_lock(StoreId, Options, Fun)' with the default store ID
%% (see {@link khepri_cluster:get_default_store_id/0}).</li>
%% </ul>
%%
%% @see with_lock/4.

with_lock(StoreId, LockId, Fun)
  when ?IS_STORE_ID(StoreId) andalso is_function(Fun, 0) ->
    with_lock(StoreId, LockId, #{}, Fun);
with_lock(LockId, Options, Fun)
  when is_map(Options) andalso is_function(Fun, 0) ->
    StoreId = khepri_cluster:get_default_store_id(),
    with_lock(StoreId, LockId, Options, Fun).

-spec with_lock(StoreId, LockId, Options, Fun) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Options :: khepri_lock:acquire_options(),
      Fun :: fun(() -> FunRet),
      Ret :: {true, FunRet} | false | khepri:error().
%% @doc Executes a function after the given locks has been acquired, and
%% releases the lock afterwards.
%%
%% This function wraps {@link acquire/3} and {@link release/1} in a `try' block
%% so that even if the function fails, the lock is released afterwards.
%%
%% @param StoreId the name of the Khepri store.
%% @param LockId the term representing the lock.
%% @param Options options for acquiring the lock.
%% @param Fun the function to execute with the lock acquired.
%%
%% @returns `{true, FunRet}' if the lock was acquired successfully and the
%% function returns `FunRet', `false' if the lock could not be acquired and the
%% number of retries has been exhausted, `{error, Reason}' if acquiring the
%% lock fails for unexpected reasons, or crashes if the given `Fun' crashes.

with_lock(StoreId, LockId, Options, Fun)
  when ?IS_STORE_ID(StoreId) andalso
       is_map(Options) andalso
       is_function(Fun, 0) ->
    case acquire(StoreId, LockId, Options) of
        {true, Lock} ->
            try
                {true, Fun()}
            after
                release(Lock)
            end;
        false ->
            false;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% info().
%% -------------------------------------------------------------------

-spec info(LockId) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Ret :: khepri:ok(LockInfo) | khepri:error(),
      LockInfo :: khepri_lock:lock_info() | undefined.
%% @doc Returns information about a lock.
%%
%% This is the same as calling `info(StoreId, LockId, #{})' with the
%% default store ID (see {@link khepri_cluster:get_default_store_id/0}).
%%
%% @see info/2.
%% @see info/3.

info(LockId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    info(StoreId, LockId, #{}).

-spec info
(LockId, Options) -> Ret when
      LockId :: khepri_lock:lock_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(LockInfo) | khepri:error(),
      LockInfo :: khepri_lock:lock_info() | undefined;
(StoreId, LockId) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Ret :: khepri:ok(LockInfo) | khepri:error(),
      LockInfo :: khepri_lock:lock_info() | undefined.
%% @doc Returns information about a lock.
%%
%% This function accepts the following two forms:
%% <ul>
%% <li>`info(LockId, Options)'. Calling it is the same as calling
%% `info(StoreId, LockId, Options)' with the default store ID (see {@link
%% khepri_cluster:get_default_store_id/0}).</li>
%% <li>`info(StoreId, LockId)'. Calling it is the same as calling
%% `info(StoreId, LockId, #{})'.</li>
%% </ul>
%%
%% @see info/3.

info(LockId, Options) when is_map(Options) ->
    StoreId = khepri_cluster:get_default_store_id(),
    info(StoreId, LockId, Options);
info(StoreId, LockId) when ?IS_STORE_ID(StoreId) ->
    info(StoreId, LockId, #{}).

-spec info(StoreId, LockId, Options) -> Ret when
      StoreId :: khepri:store_id(),
      LockId :: khepri_lock:lock_id(),
      Options :: khepri:query_options(),
      Ret :: khepri:ok(LockInfo) | khepri:error(),
      LockInfo :: khepri_lock:lock_info() | undefined.
%% @doc Returns information about a lock.
%%
%% @returns `{ok, LockInfo}' where `LockInfo' is `undefined' if the lock does
%% not exist and a map of information if the lock does exist.

info(StoreId, LockId, Options) ->
    khepri_machine:get_lock_info(StoreId, LockId, Options).

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

lock(StoreId, LockId, Options) ->
    #khepri_lock{store_id = StoreId,
                 lock_id = LockId,
                 group = maps:get(group, Options, self()),
                 recursive = maps:get(recursive, Options, false),
                 release_on_disconnect =
                 maps:get(release_on_disconnect, Options, false)}.

acquire_with_backoff(Lock, Retries, Attempts) ->
    case khepri_machine:attempt_lock(Lock) of
        true ->
            {true, Lock};
        false when Retries > 0 ->
            backoff_sleep(Attempts),
            acquire_with_backoff(Lock, decrement(Retries), Attempts + 1);
        false ->
            false;
        {error, _} = Error ->
            Error
    end.

backoff_sleep(Attempts) ->
    %% See `global:random_sleep/1'. We use the same jitter/backoff numbers so
    %% that existing codebases can use Khepri locks without noticeable changes
    %% to the time taken to acquire locks.
    _ = case Attempts rem 10 of
            0 -> rand:seed(exsplus);
            _ -> ok
        end,
    %% First time 1/4 seconds, then doubling each time up to 8 seconds max.
    Tmax = case Attempts > 5 of
               true ->
                   8000;
               false ->
                   ((1 bsl Attempts) * 1000) div 8
           end,
    T = rand:uniform(Tmax),
    receive after T -> ok end.

decrement(infinity) ->
    infinity;
decrement(N) when is_integer(N) ->
    N - 1.
