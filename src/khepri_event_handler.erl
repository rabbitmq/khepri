%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @hidden

-module(khepri_event_handler).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-export([start_link/0,
         handle_triggered_actions/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type trigger_action() :: {sproc, khepri_path:native_path()}.
%% The type of action associated with a trigger.
%%
%% It must be the path to a stored procedure.

-type triggered_action() :: {sproc, horus:horus_fun()}.
%% The action associated with a trigger once it is triggered.
%%
%% It is the stored procedure pointed to by the path in the triggered action's
%% arguments.

-type trigger_exec_loc() :: leader.
%% Where to execute the triggered action.
%%
%% It supports the following locations:
%% <ul>
%% <li>`leader': the action is executed on the leader node at the time the
%% action is triggered.</li>
%% </ul>

-export_type([trigger_action/0,
              triggered_action/0,
              trigger_exec_loc/0]).

-define(SERVER, ?MODULE).

-define(SILENCE_ERROR_FOR, 10000).

-record(?MODULE, {trigger_crashes = #{}}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

handle_triggered_actions(_StoreId, []) ->
    ok;
handle_triggered_actions(StoreId, TriggeredActions) ->
    gen_server:cast(?SERVER, {?FUNCTION_NAME, StoreId, TriggeredActions}).

init(_) ->
    erlang:process_flag(trap_exit, true),
    State = #?MODULE{},
    {ok, State}.

handle_call(Request, From, State) ->
    ?LOG_WARNING(
       "Unhandled handle_call request from ~0p: ~p",
       [From, Request]),
    {State1, Timeout} = log_accumulated_trigger_crashes(State),
    {reply, ok, State1, Timeout}.

handle_cast(
  {handle_triggered_actions, StoreId, TriggeredActions}, State) ->
    State1 =
    lists:foldl(
      fun
          (#triggered{id = TriggerId,
                      sproc = StoredProc,
                      event_filter = EventFilter,
                      props = Props},
           S) ->
              run_triggered_sproc(
                StoreId, TriggerId, StoredProc, EventFilter, Props, S);
          (#triggered_v2{id = TriggerId,
                         action = {sproc, StoredProc},
                         event_filter = EventFilter,
                         props = Props},
           S) ->
              run_triggered_sproc(
                StoreId, TriggerId, StoredProc, EventFilter, Props, S)
      end, State, TriggeredActions),
    _ = khepri_machine:ack_triggers_execution(StoreId, TriggeredActions),
    {State2, Timeout} = log_accumulated_trigger_crashes(State1),
    {noreply, State2, Timeout};
handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled handle_cast request: ~p", [Request]),
    {State1, Timeout} = log_accumulated_trigger_crashes(State),
    {noreply, State1, Timeout}.

handle_info(timeout, State) ->
    {State1, Timeout} = log_accumulated_trigger_crashes(State),
    {noreply, State1, Timeout};
handle_info(Msg, State) ->
    ?LOG_WARNING("Unhandled handle_info message: ~p", [Msg]),
    {State1, Timeout} = log_accumulated_trigger_crashes(State),
    {noreply, State1, Timeout}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

run_triggered_sproc(
  StoreId, TriggerId, StoredProc, EventFilter, Props,
  #?MODULE{trigger_crashes = Crashes} = State) ->
    Args = [Props],
    try
        %% TODO: Be flexible and accept a function with an arity of 0.
        _ = khepri_sproc:run(StoredProc, Args),
        State
    catch
        Class:Reason:Stacktrace ->
            Key = {Class, Reason, Stacktrace},
            case Crashes of
                #{Key := {Timestamp, Count, Msg}} ->
                    Crashes1 = Crashes#{Key => {Timestamp, Count + 1, Msg}},
                    State#?MODULE{trigger_crashes = Crashes1};
                _ ->
                    Msg = io_lib:format(
                            "Triggered stored procedure crash~n"
                            "  Store ID: ~s~n"
                            "  Trigger ID: ~s~n"
                            "  Event filter:~n"
                            "    ~p~n"
                            "  Event props:~n"
                            "    ~p~n"
                            "  Crash:~n"
                            "    ~ts",
                            [StoreId, TriggerId, EventFilter, Props,
                             khepri_utils:format_exception(
                               Class, Reason, Stacktrace,
                               #{column => 4})]),
                    ?LOG_ERROR(Msg, []),

                    Timestamp = erlang:monotonic_time(millisecond),
                    Crashes1 = Crashes#{Key => {Timestamp, 1, Msg}},
                    State#?MODULE{trigger_crashes = Crashes1}
            end
    end.

log_accumulated_trigger_crashes(
  #?MODULE{trigger_crashes = Crashes} = State)
  when Crashes =:= #{} ->
    {State, infinity};
log_accumulated_trigger_crashes(
  #?MODULE{trigger_crashes = Crashes} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Crashes1 = maps:filter(
                 fun
                     (_Key, {Timestamp, Count, Msg})
                       when Now - Timestamp >= ?SILENCE_ERROR_FOR andalso
                            Msg > 1 ->
                         ?LOG_ERROR(
                            "~ts~n"
                            "(this crash occurred ~b times in the last ~b "
                            "seconds)",
                            [Msg, Count, (Now - Timestamp) div 1000]),
                         false;
                     (_Key, _Value) ->
                         true
                 end, Crashes),
    State1 = State#?MODULE{trigger_crashes = Crashes1},
    case Crashes =:= #{} of
        true  -> {State1, infinity};
        false -> {State1, ?SILENCE_ERROR_FOR}
    end.
