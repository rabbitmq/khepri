%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_batch_proxy).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-export([start_link/1,
         proxy_command/2,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(?MODULE, {store_id :: khepri:store_id(),
                  callers :: [any()],
                  batch :: khepri_batch:draft()}).

-define(PT_SERVER_PID(StoreId), {?MODULE, StoreId}).

start_link(StoreId) ->
    gen_server:start_link(?MODULE, #{store_id => StoreId}, []).

get_pid(StoreId) ->
    persistent_term:get(?PT_SERVER_PID(StoreId)).

proxy_command(StoreId, Command) ->
    ServerPid = get_pid(StoreId),
    gen_server:call(ServerPid, {?FUNCTION_NAME, Command}).

stop(StoreId) ->
    ServerPid = get_pid(StoreId),
    gen_server:stop(ServerPid).

init(#{store_id := StoreId}) ->
    ThisPid = self(),
    ?LOG_DEBUG(
       "Starting batch proxy ~p for store \"~s\"",
       [ThisPid, StoreId]),
    erlang:process_flag(trap_exit, true),
    persistent_term:put(?PT_SERVER_PID(StoreId), ThisPid),
    Callers = [],
    Draft = khepri_batch:new(StoreId),
    State = #?MODULE{store_id = StoreId,
                     callers = Callers,
                     batch = Draft},
    {ok, State}.

handle_call(
  {proxy_command, Command},
  From,
  #?MODULE{callers = Callers, batch = Draft} = State) ->
    Callers1 = [From | Callers],
    Draft1 = khepri_batch:append(Draft, Command),
    State1 = State#?MODULE{callers = Callers1,
                           batch = Draft1},
    Timeout = get_new_timeout(State1),
    {noreply, State1, Timeout};
handle_call(Request, From, State) ->
    ?LOG_WARNING(
       "Unhandled handle_call request from ~0p: ~p",
       [From, Request]),
    Timeout = get_new_timeout(State),
    {reply, ok, State, Timeout}.

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled handle_cast request: ~p", [Request]),
    Timeout = get_new_timeout(State),
    {noreply, State, Timeout}.

handle_info(timeout, State) ->
    State1 = process_batch(State),
    Timeout = get_new_timeout(State1),
    {noreply, State1, Timeout};
handle_info(Msg, State) ->
    ?LOG_WARNING("Unhandled handle_info message: ~p", [Msg]),
    Timeout = get_new_timeout(State),
    {noreply, State, Timeout}.

terminate(_Reason, #?MODULE{store_id = StoreId}) ->
    ThisPid = self(),
    ?LOG_DEBUG(
       "Terminating batch proxy ~p for store \"~s\"",
       [ThisPid, StoreId]),
    %% TODO: Handle pending batch.
    persistent_term:erase(?PT_SERVER_PID(StoreId)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_new_timeout(#?MODULE{batch = Draft}) ->
    case khepri_batch:count_pending(Draft) of
        0 -> infinity;
        _ -> 0
    end.

process_batch(
  #?MODULE{store_id = StoreId,
           callers = Callers,
           batch = Draft} = State) ->
    Ret = khepri_batch:commit(Draft),
    case Ret of
        {ok, Rets} ->
            ?assertEqual(length(Callers), length(Rets)),
            Callers1 = lists:reverse(Callers),
            send_replies(Callers1, Rets),

            NewCallers = [],
            NewDraft = khepri_batch:new(StoreId),
            State1 = State#?MODULE{callers = NewCallers,
                                   batch = NewDraft},
            State1;
        Error ->
            %% TODO
            ?LOG_ERROR("Error = ~p", [Error]),
            State
    end.

send_replies([From | Callers], [Ret | Rets]) ->
    gen_server:reply(From, Ret),
    send_replies(Callers, Rets);
send_replies([], []) ->
    ok.
