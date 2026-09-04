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

-include("include/khepri.hrl").

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
                  batch :: khepri_batch:batch(),
                  batch_age = undefined,
                  submitters = [] :: [pid()]}).

-define(MAX_SIZE, 20).
-define(MAX_AGE, 10).
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
        Ret = gen_server:call(
                ServerPid, {?FUNCTION_NAME, Command}, Timeout),
        case Ret of
            {error, {timeout, _}} ->
                {error, timeout};
            _ ->
                Ret
        end
    catch
        error:badarg ->
            erlang:error(no_batch_proxy);
        exit:{timeout, {gen_server, call, _}}:Stacktrace ->
            erlang:raise(exit, timeout, Stacktrace)
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
    Batch = khepri_batch:new(#{machine_version_from_store => StoreId}),
    State = #?MODULE{store_id = StoreId,
                     batch = Batch},
    {ok, State}.

handle_call(
  {proxy_command, Command},
  From,
  #?MODULE{batch = Batch, batch_age = Age} = State) ->
    {Command1, _} = khepri_machine:maybe_set_reply_to(
                      Command, #{reply_to => {gen_statem, From}}),
    Batch1 = khepri_batch:add(Batch, Command1),
    Age1 = case Age of
               _ when is_integer(Age) ->
                   Age;
               undefined ->
                   erlang:monotonic_time(millisecond)
           end,
    State1 = State#?MODULE{batch = Batch1,
                           batch_age = Age1},
    State2 = maybe_process_batch(State1),
    Timeout = get_new_timeout(State2),
    {noreply, State2, Timeout};
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
    State1 = maybe_process_batch(State),
    Timeout = get_new_timeout(State1),
    {noreply, State1, Timeout};
handle_info({'EXIT', Pid, _Reason}, #?MODULE{submitters = Submitters} = State) ->
    Submitters1 = Submitters -- [Pid],
    State1 = State#?MODULE{submitters = Submitters1},
    Timeout = get_new_timeout(State1),
    {noreply, State1, Timeout};
handle_info(Msg, State) ->
    ?LOG_WARNING("Unhandled handle_info message: ~p", [Msg]),
    Timeout = get_new_timeout(State),
    {noreply, State, Timeout}.

terminate(_Reason, #?MODULE{store_id = StoreId} = State) ->
    _State = process_batch(State), % FIXME asynchronous
    ThisPid = self(),
    ?LOG_DEBUG(
       "Terminating batch proxy ~p for store \"~s\"",
       [ThisPid, StoreId]),
    persistent_term:erase(?PT_SERVER_PID(StoreId)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_new_timeout(#?MODULE{batch = Batch}) ->
    case khepri_batch:is_empty(Batch) of
        true  -> infinity;
        false -> 0
    end.

% maybe_process_batch(#?MODULE{submitter = Pid} = State)
%   when is_pid(Pid) ->
%     State;
maybe_process_batch(#?MODULE{batch = Batch, batch_age = Age} = State) ->
    ShoudProcessBatch = (
      khepri_batch:size(Batch) >= ?MAX_SIZE orelse
      begin
          Now = erlang:monotonic_time(millisecond),
          Elapsed = Now - Age,
          Elapsed >= ?MAX_AGE
      end),
    case ShoudProcessBatch of
        true  -> process_batch(State);
        false -> State
    end.

process_batch(
  #?MODULE{store_id = StoreId,
           batch = Batch,
           submitters = Submitters} = State) ->
    Submitters1 = case khepri_batch:is_empty(Batch) of
                      false ->
                          Pid = spawn_link(
                                  fun() ->
                                          do_process_batch(StoreId, Batch)
                                  end),
                          [Pid | Submitters];
                      true ->
                          Submitters
                  end,
    NewBatch = khepri_batch:new(#{machine_version_from_store => StoreId}),
    State1 = State#?MODULE{batch = NewBatch,
                           batch_age = undefined,
                           submitters = Submitters1},
    State1.

% do_process_batch(StoreId, Commands) ->
%     Commands1 = optimize_batch(Commands),
%     OnlyCommands = [Command || {_From, Command} <- Commands1],
%     case length(OnlyCommands) of
%         N when N > 0 ->
%             logger:alert("BATCH: ~b commands", [N]);
%         _ ->
%             ok
%     end,
%     Ret = khepri_machine:batch(StoreId, OnlyCommands, #{atomic => false}),
%     case Ret of
%         {ok, Rets} ->
%             ?assertEqual(length(Commands1), length(Rets)),
%             LeaderId = ra_leaderboard:lookup_leader(StoreId),
%             send_replies(Commands1, Rets, LeaderId);
%         Error ->
%             %% TODO
%             ?LOG_ERROR("Error = ~p", [Error]),
%             ok
%     end.
do_process_batch(StoreId, Batch) ->
    Options = #{reply_from => {member, {StoreId, node()}},
                %% FIXME: How to manage timeout, especially if commands have
                %% very different timeouts? How to be sure the callers are
                %% still waiting?
                timeout => infinity},
    Ret = khepri_batch:submit(StoreId, Batch, Options),
    case Ret of
        {ok, _} ->
            ok;
        Error ->
            Commands = khepri_batch:get_commands(Batch),
            lists:foreach(
              fun(Command) ->
                      {gen_statem, From} = khepri_machine:get_reply_to_option(Command),
                      ?LOG_ERROR("Error = ~0p -> ~0p", [Error, From]),
                      gen_server:reply(From, Error)
              end, Commands),
            ok
    end.
