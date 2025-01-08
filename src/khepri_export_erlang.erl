%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri import/export callback module using Erlang terms formatted as
%% plain text as its external format.
%%
%% The exported file could be read using {@link file:consult/1} to get back
%% the list of backup items.
%%
%% This callback module takes a filename or an opened file descriptor as its
%% private data passed to {@link khepri:export/4} and {@link khepri:import/3}.
%%
%% Example:
%% ```
%% ok = khepri:put(StoreId, "/:stock/:wood/Oak", 100).
%% ok = khepri:put(StoreId, "/:stock/:wood/Mapple", 55).
%% ok = khepri:export(StoreId, khepri_export_erlang, "export.erl").
%% '''
%%
%% Content of `export.erl':
%% ```
%% {put,[stock,wood,<<"Mapple">>],{p_data,55},#{},#{}}.
%% {put,[stock,wood,<<"Oak">>],{p_data,100},#{},#{}}.
%% '''

-module(khepri_export_erlang).

-behavior(khepri_import_export).

-include_lib("kernel/include/logger.hrl").

%% How many bytes to read from the file descriptor before `read/1' returns a
%% list of backup items. `khepri_import_export' will call this module again to
%% read the next batch.
-define(IMPORT_APPROX_LIMIT_PER_CALL, 100 * 1024). %% In bytes.

%% This macro should match all the types that are accepted for
%% `file:name_all/0'.
-define(IS_FILENAME(Filename), (is_list(Filename) orelse
                                is_atom(Filename) orelse
                                is_binary(Filename))).

-export([open_write/1,
         write/2,
         commit_write/1,
         abort_write/1,

         open_read/1,
         read/1,
         close_read/1]).

-type write_state() :: #{fd := file:io_device(),
                         filename => file:name_all()}.

-type read_state() :: #{fd := file:io_device(),
                        max_chunk := non_neg_integer(),
                        location := erl_anno:location(),
                        start_pos => integer(),
                        filename => file:name_all()}.

-spec open_write(Filename | Fd) -> Ret when
      Filename :: file:name_all(),
      Fd :: file:io_device(),
      Ret :: {ok, WriteState} | {error, any()},
      WriteState :: write_state().
%% @private

open_write(Filename) when ?IS_FILENAME(Filename) ->
    ?LOG_DEBUG(
       "~s: opening file \"~ts\" for export",
       [?MODULE, Filename],
       #{domain => [khepri, import_export, ?MODULE]}),
    case file:open(Filename, [write]) of
        {ok, Fd} ->
            State = #{fd => Fd,
                      filename => Filename},
            {ok, State};
        {error, _} = Error ->
            Error
    end;
open_write(Fd) ->
    ?LOG_DEBUG(
       "~s: using existing file descriptor ~p",
       [?MODULE, Fd],
       #{domain => [khepri, import_export, ?MODULE]}),
    State = #{fd => Fd},
    {ok, State}.

-spec write(WriteState, BackupItems) -> Ret when
      WriteState :: write_state(),
      BackupItems :: [khepri_import_export:backup_item()],
      Ret :: {ok, WriteState} | {error, any()}.
%% @private

write(#{fd := Fd} = State, [BackupItem | Rest]) ->
    ?LOG_DEBUG(
       "~s: exporting: ~p",
       [?MODULE, BackupItem],
       #{domain => [khepri, import_export, ?MODULE]}),
    Binary = io_lib:format("~0p.~n", [BackupItem]),
    case file:write(Fd, Binary) of
        ok                 -> write(State, Rest);
        {error, _} = Error -> Error
    end;
write(State, []) ->
    ?LOG_DEBUG(
       "~s: finished exporting",
       [?MODULE],
       #{domain => [khepri, import_export, ?MODULE]}),
    {ok, State}.

-spec commit_write(WriteState) -> Ret when
      WriteState :: write_state(),
      Ret :: ok | {error, any()}.
%% @private

commit_write(#{fd := Fd, filename := Filename}) ->
    ?LOG_DEBUG(
       "~s: closing file \"~ts\" after export",
       [?MODULE, Filename],
       #{domain => [khepri, import_export, ?MODULE]}),
    case file:close(Fd) of
        ok                 -> ok;
        {error, _} = Error -> Error
    end;
commit_write(_State) ->
    ?LOG_DEBUG(
       "~s: used existing file descriptor; nothing to close after export",
       [?MODULE],
       #{domain => [khepri, import_export, ?MODULE]}),
    ok.

-spec abort_write(WriteState) -> Ret when
      WriteState :: write_state(),
      Ret :: ok.
%% @private

abort_write(#{fd := Fd, filename := Filename}) ->
    ?LOG_DEBUG(
       "~s: aborting export to file \"~ts\"",
       [?MODULE, Filename],
       #{domain => [khepri, import_export, ?MODULE]}),
    _ = file:close(Fd),
    _ = file:delete(Filename),
    ok;
abort_write(#{fd := Fd}) ->
    ?LOG_DEBUG(
       "~s: aborting export to existing file descriptor ~p",
       [?MODULE, Fd],
       #{domain => [khepri, import_export, ?MODULE]}),
    ok.

-spec open_read(Filename | Fd) -> Ret when
      Filename :: file:name_all(),
      Fd :: file:io_device(),
      Ret :: {ok, ReadState} | {error, any()},
      ReadState :: read_state().
%% @private

open_read(Filename) when ?IS_FILENAME(Filename) ->
    ?LOG_DEBUG(
       "~s: opening file \"~ts\" for import",
       [?MODULE, Filename],
       #{domain => [khepri, import_export, ?MODULE]}),
    case file:open(Filename, [read]) of
        {ok, Fd} ->
            _ = epp:set_encoding(Fd),
            State = #{fd => Fd,
                      max_chunk => ?IMPORT_APPROX_LIMIT_PER_CALL,
                      location => 1,
                      filename => Filename},
            {ok, State};
        {error, _} = Error ->
            Error
    end;
open_read(Fd) ->
    ?LOG_DEBUG(
       "~s: using existing file descriptor ~p for import",
       [?MODULE, Fd],
       #{domain => [khepri, import_export, ?MODULE]}),
    State = #{fd => Fd,
              max_chunk => ?IMPORT_APPROX_LIMIT_PER_CALL,
              location => 1},
    {ok, State}.

-spec read(ReadState) -> Ret when
      ReadState :: read_state(),
      Ret :: {ok, BackupItems, ReadState} | {error, any()},
      BackupItems :: [khepri_import_export:backup_item()].
%% @private

read(#{fd := Fd} = State) ->
    StartPos = case file:position(Fd, cur) of
                   {ok, Pos} -> Pos;
                   _         -> undefined
               end,
    State1 = State#{start_pos => StartPos},
    do_read(State1, []).

-spec do_read(ReadState, BackupItems) -> Ret when
      ReadState :: read_state(),
      BackupItems :: [khepri_import_export:backup_item()],
      Ret :: {ok, BackupItems, ReadState} | {error, any()}.
%% @private

do_read(
  #{fd := Fd,
    location := Location,
    start_pos := StartPos,
    max_chunk := MaxChunk} = State,
  BackupItems) ->
    case io:read(Fd, <<>>, Location) of
        {ok, BackupItem, EndLocation} ->
            ?LOG_DEBUG(
               "~s: importing: ~p",
               [?MODULE, BackupItem],
               #{domain => [khepri, import_export, ?MODULE]}),
            BackupItems1 = [BackupItem | BackupItems],
            State1 = State#{location => EndLocation},

            CurPos = case file:position(Fd, cur) of
                         {ok, Pos} -> Pos;
                         _         -> undefined
                     end,
            MustStop = (StartPos =:= undefined orelse
                        CurPos =:= undefined orelse
                        CurPos - StartPos >= MaxChunk),
            case MustStop of
                false -> do_read(State1, BackupItems1);
                true  -> {ok, lists:reverse(BackupItems1), State1}
            end;
        {eof, EndLocation} ->
            ?LOG_DEBUG(
               "~s: finished importing",
               [?MODULE],
               #{domain => [khepri, import_export, ?MODULE]}),
            State1 = State#{location => EndLocation},
            {ok, lists:reverse(BackupItems), State1};
        {error, _} = Error ->
            Error
    end.

-spec close_read(ReadState) -> Ret when
      ReadState :: read_state(),
      Ret :: ok | {error, any()}.
%% @private

close_read(#{fd := Fd, filename := Filename}) ->
    ?LOG_DEBUG(
       "~s: closing file \"~ts\" after import",
       [?MODULE, Filename],
       #{domain => [khepri, import_export, ?MODULE]}),
    file:close(Fd);
close_read(_State) ->
    ?LOG_DEBUG(
       "~s: used existing file descriptor; nothing to close after import",
       [?MODULE],
       #{domain => [khepri, import_export, ?MODULE]}),
    ok.
