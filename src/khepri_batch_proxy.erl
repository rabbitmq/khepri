%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @hidden

-module(khepri_batch_proxy).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-export([start_link/1,
         proxy_command/3,
         flush_commands/1,
         stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(?MODULE, {store_id :: khepri:store_id(),
                  callers = [] :: [gen_server:from()],
                  commands = [] :: [khepri_machine:command()]}).

-define(PT_SERVER_PID(StoreId), {?MODULE, StoreId}).

start_link(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    gen_server:start_link(?MODULE, #{store_id => StoreId}, []).

get_pid(StoreId) ->
    persistent_term:get(?PT_SERVER_PID(StoreId)).

-spec proxy_command(StoreId, Command, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Command :: khepri_machine:command(),
      Timeout :: timeout(),
      Ret :: khepri_machine:write_ret() | khepri_machine:tx_ret().
%% @private

proxy_command(StoreId, Command, Timeout) ->
    try
        ServerPid = get_pid(StoreId),
        gen_server:call(ServerPid, {?FUNCTION_NAME, Command}, Timeout)
    catch
        error:badarg ->
            {error, noproc}
    end.

-spec flush_commands(StoreId) -> ok when
      StoreId :: khepri:store_id().

%% @private

flush_commands(StoreId) ->
    %% Flush pending commands without any timeout; we want to block here.
    ServerPid = get_pid(StoreId),
    Timeout = infinity,
    gen_server:call(ServerPid, ?FUNCTION_NAME, Timeout).

stop(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    try
        ServerPid = get_pid(StoreId),
        gen_server:stop(ServerPid)
    catch
        error:badarg ->
            ok
    end.

init(#{store_id := StoreId}) ->
    ThisPid = self(),
    ?LOG_DEBUG(
       "Starting batch proxy ~p for store \"~s\"",
       [ThisPid, StoreId]),
    erlang:process_flag(trap_exit, true),
    persistent_term:put(?PT_SERVER_PID(StoreId), ThisPid),
    State = #?MODULE{store_id = StoreId},
    {ok, State}.

handle_call(
  {proxy_command, Command},
  From,
  #?MODULE{callers = Callers, commands = Commands} = State) ->
    Callers1 = [From | Callers],
    Commands1 = [Command | Commands],
    State1 = State#?MODULE{callers = Callers1,
                           commands = Commands1},
    Timeout = get_new_timeout(State1),
    {noreply, State1, Timeout};
handle_call(flush_commands, _From, State) ->
    State1 = process_batch(State),
    Timeout = get_new_timeout(State1),
    {reply, ok, State1, Timeout};
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

terminate(_Reason, #?MODULE{store_id = StoreId} = State) ->
    _State = process_batch(State),
    ThisPid = self(),
    ?LOG_DEBUG(
       "Terminating batch proxy ~p for store \"~s\"",
       [ThisPid, StoreId]),
    persistent_term:erase(?PT_SERVER_PID(StoreId)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_new_timeout(#?MODULE{commands = Commands}) ->
    case length(Commands) of
        0 -> infinity;
        _ -> 0
    end.

process_batch(
  #?MODULE{callers = [],
           commands = []} = State) ->
    State;
process_batch(
  #?MODULE{store_id = StoreId,
           callers = Callers,
           commands = Commands} = State) ->
    Commands1 = lists:reverse(Commands),
    case length(Commands1) of
        N when N > 1 ->
            logger:alert("BATCH: ~b commands", [N]);
        _ ->
            ok
    end,
    Ret = khepri_machine:batch(StoreId, Commands1, #{atomic => false}),
    case Ret of
        {ok, Rets} ->
            ?assertEqual(length(Callers), length(Rets)),
            Callers1 = lists:reverse(Callers),
            LeaderId = ra_leaderboard:lookup_leader(StoreId),
            send_replies(Callers1, Rets, LeaderId),

            State1 = State#?MODULE{callers = [],
                                   commands = []},
            State1;
        Error ->
            %% TODO
            ?LOG_ERROR("Error = ~p", [Error]),
            State
    end.

send_replies([From | Callers], [Ret | Rets], LeaderId) ->
    gen_server:reply(From, {ok, Ret, LeaderId}),
    send_replies(Callers, Rets, LeaderId);
send_replies([], [], _LeaderId) ->
    ok.
