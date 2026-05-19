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
                  commands = [] :: [{gen_server:from(),
                                     khepri_machine:command()}],
                  batch_size = 0,
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
        Ret = gen_server:call(ServerPid, {?FUNCTION_NAME, Command}, Timeout),
        case Ret of
            {error, {timeout, _}} ->
                {error, timeout};
            _ ->
                Ret
        end
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
  #?MODULE{commands = Commands, batch_size = Size, batch_age = Age} = State) ->
    % logger:alert("PROXY ~p", [Command]),
    Commands1 = [{From, Command} | Commands],
    Size1 = Size + 1,
    Age1 = case Age of
               _ when is_integer(Age) ->
                   Age;
               undefined ->
                   erlang:monotonic_time(millisecond)
           end,
    State1 = State#?MODULE{commands = Commands1,
                           batch_size = Size1,
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
    _State = process_batch(State), % FIXME synchronous
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

% maybe_process_batch(#?MODULE{submitter = Pid} = State)
%   when is_pid(Pid) ->
%     State;
maybe_process_batch(#?MODULE{batch_size = Size} = State)
  when Size >= ?MAX_SIZE ->
    process_batch(State);
maybe_process_batch(#?MODULE{batch_age = Age} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Elapsed = Now - Age,
    case Elapsed of
        _ when Elapsed > ?MAX_AGE ->
            process_batch(State);
        _ ->
            State
    end;
maybe_process_batch(State) ->
    State.

process_batch(
  #?MODULE{commands = []} = State) ->
    State;
% process_batch(
%   #?MODULE{store_id = StoreId, commands = [_] = Commands} = State) ->
%     do_process_batch(StoreId, Commands),
%     State1 = State#?MODULE{commands = [],
%                            batch_size = 0,
%                            batch_age = undefined},
%     State1;
process_batch(
  #?MODULE{store_id = StoreId,
           commands = Commands,
           submitters = Submitters} = State) ->
    Pid = spawn_link(
            fun() ->
                    do_process_batch(StoreId, Commands)
            end),
    Submitters1 = [Pid | Submitters],
    State1 = State#?MODULE{commands = [],
                           batch_size = 0,
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
do_process_batch(StoreId, Commands) when Commands =/= [] ->
    Commands1 = lists:reverse(Commands),
    _Commands2 = optimize_batch(StoreId, Commands1),
    Commands2 = Commands1,
    % L1 = length(Commands1),
    % L2 = length(Commands2),
    % if
    %     L2 < L1 ->
    %         logger:alert("BATCH: ~b commands (optimised from ~b commands)~n~p", [L2, L1, Commands2]);
    %     L2 =:= L1 ->
    %         logger:alert("BATCH: ~b commands", [L1])
    % end,
    Ret = khepri_machine:batch(StoreId, Commands2, #{atomic => false, reply_from => {member, {StoreId, node()}}}),
    case Ret of
        ok ->
            % logger:alert("BATCH: returned ok"),
            ok;
        Error ->
            lists:foreach(
              fun({From, _Command}) ->
                      ?LOG_ERROR("Error = ~0p -> ~0p", [Error, From]),
                      gen_server:reply(From, Error)
              end, Commands1),
            ok
    end.

optimize_batch(_StoreId, [_] = Commands) ->
    Commands;
optimize_batch(StoreId, Commands) ->
    Commands1 = simplify_specific_deletes(StoreId, Commands),
    case Commands1 of
        Commands ->
            % logger:alert("No optimisation"),
            Commands;
        _ ->
            % logger:alert("Optimised:~n  Before: ~p~n  After:  ~p", [Commands, Commands1]),
            Commands1
    end.

simplify_specific_deletes(StoreId, Commands) ->
    simplify_specific_deletes(StoreId, Commands, #{other_commands => []}).

simplify_specific_deletes(
  StoreId,
  [{_From, #delete{options = Options}} = Command | Commands],
  DeletesPerOptions) ->
    % logger:alert("(delete command: ~0p)", [Command]),
    Cmds = maps:get(Options, DeletesPerOptions, []),
    Cmds1 = [Command | Cmds],
    DeletesPerOptions1 = DeletesPerOptions#{Options => Cmds1},
    simplify_specific_deletes(StoreId, Commands, DeletesPerOptions1);
simplify_specific_deletes(
  StoreId,
  [Command | Commands],
  DeletesPerOptions) ->
    % logger:alert("(other command: ~0p)", [Command]),
    Cmds = maps:get(other_commands, DeletesPerOptions),
    Cmds1 = [Command | Cmds],
    DeletesPerOptions1 = DeletesPerOptions#{other_commands => Cmds1},
    simplify_specific_deletes(StoreId, Commands, DeletesPerOptions1);
simplify_specific_deletes(
  _StoreId,
  [],
  DeletesPerOptions) ->
    maps:fold(
      fun
          (#{expect_specific_node := true} = Options, Cmds, Acc) ->
              Paths = [Path || {_From, #delete{path = Path}} <- Cmds],
              Patterns = khepri_path:paths_to_patterns(Paths),
              case Patterns of
                  [Pattern] ->
                      Froms = [From || {From, _Cmd} <- Cmds],
                      Options1 = Options#{expect_specific_node => false},
                      Cmd1 = #delete{path = Pattern, options = Options1},
                      Cmd2 = {Froms, Cmd1},
                      % logger:alert("OPTIMISE: ~0p", [Froms]),
                      % logger:alert("OPTIMIZE:~n  1: ~p~n  2: ~p", [Cmds, Cmd3]),
                      Acc ++ [Cmd2];
                  _ ->
                      Acc ++ lists:reverse(Cmds)
              end;
          (_, Cmds, Acc) ->
              Acc ++ lists:reverse(Cmds)
      end, [], DeletesPerOptions).

    % DeletesPerOptions = lists:foldl(
    %                       fun
    %                           (#delete{options = Options} = Command, Acc) ->
    %                               Cmds = maps:get(Options, Acc, []),
    %                               Cmds1 = [Command | Cmds],
    %                               Acc#{Options => Cmds1};
    %                           (Command, Acc) ->
    %                               Cmds = maps:get(other_commands, Acc),
    %                               Cmds1 = [Command | Cmds],
    %                               Acc#{other_commands => Cmds1}
    %                       end, #{other_commands => []}, Commands),
    % maps:fold(
    %   fun
    %       (other_commands, Cmds, {Acc1, Acc2}) ->
    %           Acc ++ Cmds;
    %       (#{expect_specific_node := true} = Options, Cmds, Acc) ->
    %           Paths = [Path || #delete{path = Path} <- Cmds],
    %           Patterns = khepri_path:paths_to_patterns(Paths),
    %           Cmds1 = [#delete{path = Pattern, options = Options}
    %                    || Pattern <- Patterns],
    %           logger:alert("OPTIMIZE:~n  1: ~p~n  2: ~p", [Cmds, Cmds1]),
    %           Acc ++ Cmds1
    %   end, {[], #{}}, DeletesPerOptions).

% send_replies([{Froms, _} | Commands], [Ret | Rets], LeaderId) ->
%     % logger:alert("REPLY ~p = ~p", [Command, Ret]),
%     case is_list(Froms) of
%         false ->
%             gen_server:reply(Froms, {ok, Ret, LeaderId});
%         true ->
%             lists:foreach(
%               fun(From) ->
%                       gen_server:reply(From, {ok, Ret, LeaderId})
%               end, Froms)
%     end,
%     send_replies(Commands, Rets, LeaderId);
% send_replies([], [], _LeaderId) ->
%     ok.
