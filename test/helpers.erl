%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(helpers).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-export([start_epmd/0,
         init_list_of_modules_to_skip/0,
         start_ra_system/1,
         stop_ra_system/1,
         store_dir_name/1,
         remove_store_dir/1,
         with_log/1,
         capture_log/1,
         silence_default_logger/0,
         restore_default_logger/1,

         setup_node/0,
         basic_logger_config/0,
         start_n_nodes/3,
         stop_erlang_node/2,
         call/5,
         get_ra_system_name/1,
         get_leader_in_store/3,

         %% For internal use only.
         log/2,
         format/2]).

-define(CAPTURE_LOGGER_ID, capture_logger).

start_epmd() ->
    RootDir = code:root_dir(),
    ErtsVersion = erlang:system_info(version),
    ErtsDir = lists:flatten(io_lib:format("erts-~ts", [ErtsVersion])),
    EpmdPath0 = filename:join([RootDir, ErtsDir, "bin", "epmd"]),
    EpmdPath = case os:type() of
                   {win32, _} -> EpmdPath0 ++ ".exe";
                   _          -> EpmdPath0
               end,
    Port = erlang:open_port(
             {spawn_executable, EpmdPath},
             [{args, ["-daemon"]}]),
    erlang:port_close(Port),
    ok.

init_list_of_modules_to_skip() ->
    _ = application:load(khepri),
    khepri_utils:init_list_of_modules_to_skip().

start_ra_system(RaSystem) ->
    {ok, _} = application:ensure_all_started(ra),
    StoreDir = store_dir_name(RaSystem),
    _ = remove_store_dir(StoreDir),
    Default = ra_system:default_config(),
    RaSystemConfig = Default#{name => RaSystem,
                              data_dir => StoreDir,
                              wal_data_dir => StoreDir,
                              wal_max_size_bytes => 16 * 1024,
                              names => ra_system:derive_names(RaSystem)},
    case ra_system:start(RaSystemConfig) of
        {ok, RaSystemPid} ->
            #{ra_system => RaSystem,
              ra_system_pid => RaSystemPid,
              store_dir => StoreDir};
        {error, _} = Error ->
            throw(Error)
    end.

stop_ra_system(#{ra_system := RaSystem,
                 store_dir := StoreDir}) ->
    ?assertEqual(ok, ra_system:stop(RaSystem)),
    _ = remove_store_dir(StoreDir),
    ok.

store_dir_name(RaSystem) ->
    Node = node(),
    lists:flatten(
      io_lib:format("_test.~s.~s", [RaSystem, Node])).

remove_store_dir(StoreDir) ->
    OnWindows = case os:type() of
                    {win32, _} -> true;
                    _          -> false
                end,
    case file:del_dir_r(StoreDir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, eexist} when OnWindows ->
            %% FIXME: Some files are not deleted on Windows... Are they still
            %% open in Ra?
            io:format(
              standard_error,
              "Files remaining in ~ts: ~p~n",
              [StoreDir, file:list_dir_all(StoreDir)]),
            ok;
        Error ->
            throw(Error)
    end.

silence_default_logger() ->
    {ok, #{level := OldDefaultLoggerLevel}} =
      logger:get_handler_config(default),
    ok = logger:set_handler_config(default, level, none),
    OldDefaultLoggerLevel.

restore_default_logger(OldDefaultLoggerLevel) ->
    ok = logger:set_handler_config(default, level, OldDefaultLoggerLevel).

-spec with_log(Fun) -> {Result, Log}
    when
      Fun :: fun(() -> Result),
      Result :: any(),
      Log :: binary().

%% @doc Returns the value of executing the given `Fun' and any log messages
%% produced while executing it, concatenated into a binary.
with_log(Fun) ->
    FormatterConfig = #{},
    HandlerConfig = #{config => self(),
                      formatter => {?MODULE, FormatterConfig}},
    {ok, #{level := OldDefaultLogLevel}} = logger:get_handler_config(default),
    ok = logger:set_handler_config(default, level, none),
    ok = logger:add_handler(?CAPTURE_LOGGER_ID, ?MODULE, HandlerConfig),
    try
        Result = Fun(),
        Log = collect_logs(),
        {Result, Log}
    after
        _ = logger:remove_handler(?CAPTURE_LOGGER_ID),
        _ = logger:set_handler_config(default, level, OldDefaultLogLevel)
    end.

-spec capture_log(Fun) -> Log
    when
      Fun :: fun(() -> any()),
      Log :: binary().

%% @doc Returns the logger messages produced while executing the given `Fun'
%% concatenated into a binary.
capture_log(Fun) ->
    {_Result, Log} = with_log(Fun),
    Log.

%% Implements the `log/2' callback for logger handlers
log(LogEvent, Config) ->
    #{config := TestPid} = Config,
    Msg = case maps:get(msg, LogEvent) of
              {report, Report} ->
                  {Format, Args} = logger:format_report(Report),
                  iolist_to_binary(io_lib:format(Format, Args));
              {string, Chardata} ->
                  unicode:characters_to_binary(Chardata);
              {Format, Args} ->
                  iolist_to_binary(io_lib:format(Format, Args))
          end,
    TestPid ! {?MODULE, Msg},
    ok.

%% Implements the `format/2' callback for logger formatters
format(_LogEvent, _FormatConfig) ->
    %% No-op: print nothing to the console.
    ok.

collect_logs() ->
    collect_logs(<<>>).

collect_logs(Acc) ->
    receive
        {?MODULE, Msg} -> collect_logs(<<Msg/binary, Acc/binary>>)
    after
        50 -> Acc
    end.

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

setup_node() ->
    basic_logger_config(),

    %% We use an additional logger handler for messages tagged with a non-OTP
    %% domain because by default, `cth_log_redirect' drops them.
    GL = erlang:group_leader(),
    GLNode = node(GL),
    Ret = logger:add_handler(
            cth_log_redirect_any_domains, cth_log_redirect_any_domains,
            #{config => #{group_leader => GL,
                          group_leader_node => GLNode}}),
    case Ret of
        ok                          -> ok;
        {error, {already_exist, _}} -> ok
    end,
    ok = logger:set_handler_config(
           cth_log_redirect_any_domains, formatter,
           {logger_formatter, ?LOGFMT_CONFIG}),
    ?LOG_INFO(
       "Extended logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),

    ok.

basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),

    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              ok = logger:update_handler_config(
                    HandlerId, config, #{burst_limit_enable => false}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ?LOG_INFO(
       "Basic logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok.

start_n_nodes(Module, NamePrefix, Count) ->
    ct:pal("Start ~b Erlang nodes:", [Count]),
    Nodes = [begin
                 Name = lists:flatten(
                          io_lib:format(
                            "~s-~s-~b", [Module, NamePrefix, I])),
                 ct:pal("- ~s", [Name]),
                 start_erlang_node(Name)
             end || I <- lists:seq(1, Count)],
    ct:pal("Started nodes: ~p", [[Node || {Node, _Peer} <- Nodes]]),

    %% We add all nodes to the test coverage report.
    CoveredNodes = [Node || {Node, _Peer} <- Nodes],
    {ok, _} = cover:start(CoveredNodes),

    CodePath = code:get_path(),
    lists:foreach(
      fun({_Node, Peer}) ->
              peer:call(Peer, code, add_pathsz, [CodePath], infinity),
              ok = peer:call(Peer, ?MODULE, setup_node, [], infinity),

              %% Load this module on peer nodes.
              ?assertMatch(
                 MI when is_list(MI),
                 peer:call(Peer, Module, module_info, []))
      end, Nodes),
    Nodes.

start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, Peer, Node} = peer:start(#{name => Name1,
                                    wait_boot => infinity,
                                    connection => standard_io}),
    {Node, Peer}.

stop_erlang_node(Node, Peer) ->
    ok = cover:stop([Node]),
    ok = peer:stop(Peer).

get_ra_system_name(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node | _] = maps:keys(PropsPerNode),
    #{props := #{ra_system := RaSystem}} = maps:get(Node, PropsPerNode),
    RaSystem.

call(Config, Node, Module, Func, Args) ->
    PropsPerNode = ?config(ra_system_props, Config),
    #{peer := Peer} = maps:get(Node, PropsPerNode),
    peer:call(Peer, Module, Func, Args, infinity).

get_leader_in_store(Config, StoreId, [Node | _] = _RunningNodes) ->
    %% Query members; this is used to make sure there is an elected leader.
    ct:pal("Trying to figure out who the leader is in \"~s\"", [StoreId]),
    {ok, Members} = call(Config, Node, khepri_cluster, members, [StoreId]),
    Pids = [[Member, catch call(Config, N, erlang, whereis, [RegName])]
            || {RegName, N} = Member <- Members],
    LeaderId = call(Config, Node, ra_leaderboard, lookup_leader, [StoreId]),
    ?assertNotEqual(undefined, LeaderId),
    ct:pal(
      "Leader: ~0p~n"
      "Members:~n" ++
      string:join([" - ~0p -> ~0p" || _ <- Pids], "\n"),
      [LeaderId] ++ lists:flatten(Pids)),
    LeaderId.
