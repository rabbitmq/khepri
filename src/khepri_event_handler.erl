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
-include("src/khepri_evf.hrl").
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
%%
%% The action will get a trigger desccriptor as argument to describe a specific
%% execution of that trigger. See {@link trigger_descriptor/0}.

-type triggered_action() :: {sproc, horus:horus_fun()}.
%% The action associated with a trigger once it is triggered.
%%
%% It is the stored procedure pointed to by the path in the triggered action's
%% arguments.

-type trigger_exec_loc() :: leader | {member, node()} | all_members.
%% Where to execute the triggered action.
%%
%% It supports the following locations:
%% <ul>
%% <li>`leader': the action is executed on the leader node at the time the
%% action is triggered.</li>
%% <li>`{member, Member}': the action is executed on the given node if it is a
%% member of the cluster, or the leader otherwise.</li>
%% <li>`all_members': the action is executed on all members of the
%% cluster.</li>
%% </ul>

-type trigger_descriptor() :: #khepri_trigger{}.
%% Descriptor of the trigger used as an argument to the trigger action.
%%
%% The descriptor contains all the properties of a specific instance of that
%% trigger.
%%
%% When the action is a stored procedure or an MFA tuple, the descriptor is
%% appended to the list of arguments.
%%
%% When the action is a PID, the descriptor is send to the PID as a standalone
%% message. The `action' properties map will contain the `Priv' element of the
%% `send' tuple under the `priv' field.

-export_type([trigger_action/0,
              triggered_action/0,
              trigger_exec_loc/0,
              trigger_descriptor/0]).

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
      fun(TriggeredAction, S) ->
              ActionArg = prepare_action_arg(StoreId, TriggeredAction),
              run_triggered_action(StoreId, TriggeredAction, ActionArg, S)
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

prepare_action_arg(_StoreId, #triggered{props = Props}) ->
    Arg = Props,
    Arg;
prepare_action_arg(
  StoreId,
  #triggered_v2{id = TriggerId,
                event = Event,
                action = Action}) ->
    EventType = event_type(Event),
    EventProps = event_to_props(Event),
    ActionProps = action_to_props(Action),
    Arg = #khepri_trigger{type = EventType,
                          store_id = StoreId,
                          trigger_id = TriggerId,
                          event = EventProps,
                          action = ActionProps},
    Arg.

event_type(#ev_tree{}) ->
    tree;
event_type(#ev_process{}) ->
    process.

event_to_props(#ev_tree{path = Path, change = Change}) ->
    #{path => Path,
      change => Change};
event_to_props(#ev_process{pid = Pid, change = Change}) ->
    #{pid => Pid,
      change => Change}.

action_to_props({sproc, _StoredProc}) ->
    #{}.

run_triggered_action(
  StoreId,
  #triggered{sproc = StoredProc} = TriggeredAction,
  ActionArg, State) ->
    run_triggered_sproc(
      StoreId, TriggeredAction, StoredProc, ActionArg, State);
run_triggered_action(
  StoreId,
  #triggered_v2{action = {sproc, StoredProc}} = TriggeredAction,
  ActionArg, State) ->
    run_triggered_sproc(
      StoreId, TriggeredAction, StoredProc, ActionArg, State).

run_triggered_sproc(StoreId, TriggeredAction, StoredProc, ActionArg, State) ->
    Args = [ActionArg],
    try
        %% TODO: Be flexible and accept a function with an arity of 0.
        _ = khepri_sproc:run(StoredProc, Args),
        State
    catch
        Class:Reason:Stacktrace ->
            handle_action_crash(
              StoreId, TriggeredAction, ActionArg,
              Class, Reason, Stacktrace, State)
    end.

handle_action_crash(
  StoreId, TriggeredAction, ActionArg, Class, Reason, Stacktrace,
  #?MODULE{trigger_crashes = Crashes} = State) ->
    Key = {Class, Reason, Stacktrace},
    case Crashes of
        #{Key := {Timestamp, Count, Msg}} ->
            Crashes1 = Crashes#{Key => {Timestamp, Count + 1, Msg}},
            State#?MODULE{trigger_crashes = Crashes1};
        _ ->
            TriggerId = case TriggeredAction of
                            #triggered{id = Id}    -> Id;
                            #triggered_v2{id = Id} -> Id
                        end,
            EventFilter = case TriggeredAction of
                              #triggered{event_filter = EF}    -> EF;
                              #triggered_v2{event_filter = EF} -> EF
                          end,
            Msg = io_lib:format(
                    "Triggered stored procedure crash~n"
                    "  Store ID: ~s~n"
                    "  Trigger ID: ~s~n"
                    "  Event filter:~n"
                    "    ~p~n"
                    "  Action arg:~n"
                    "    ~p~n"
                    "  Crash:~n"
                    "    ~ts",
                    [StoreId, TriggerId, EventFilter, ActionArg,
                     khepri_utils:format_exception(
                       Class, Reason, Stacktrace,
                       #{column => 4})]),
            ?LOG_ERROR(Msg, []),

            Timestamp = erlang:monotonic_time(millisecond),
            Crashes1 = Crashes#{Key => {Timestamp, 1, Msg}},
            State#?MODULE{trigger_crashes = Crashes1}
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
