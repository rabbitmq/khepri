%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(export_helper).

-export([open_write/1,
         write/2,
         commit_write/1,
         abort_write/1,

         open_read/1,
         read/1,
         close_read/1]).

open_write(ModulePriv) when is_list(ModulePriv) ->
    {ok, ModulePriv};
open_write(error_during_open_write) ->
    {error, ?FUNCTION_NAME};
open_write(throw_during_open_write) ->
    throw(?FUNCTION_NAME);
open_write(error_during_write = ModulePriv) ->
    {ok, ModulePriv};
open_write(throw_during_write = ModulePriv) ->
    {ok, ModulePriv};
open_write(error_during_commit_write = ModulePriv) ->
    {ok, ModulePriv};
open_write(throw_during_commit_write = ModulePriv) ->
    {ok, ModulePriv};
open_write(throw_during_abort_write = ModulePriv) ->
    {ok, ModulePriv}.

write(ModulePriv, BackupItems)
  when is_list(ModulePriv) andalso is_list(BackupItems) ->
    ModulePriv1 = ModulePriv ++ BackupItems,
    {ok, ModulePriv1};
write(error_during_write, _BackupItems) ->
    {error, ?FUNCTION_NAME};
write(throw_during_write, _BackupItems) ->
    throw(?FUNCTION_NAME);
write(error_during_commit_write = ModulePriv, _BackupItems) ->
    {ok, ModulePriv};
write(throw_during_commit_write = ModulePriv, _BackupItems) ->
    {ok, ModulePriv};
write(throw_during_abort_write, _BackupItems) ->
    {error, ?FUNCTION_NAME}.

commit_write(ModulePriv) when is_list(ModulePriv) ->
    {ok, ModulePriv};
commit_write(error_during_commit_write) ->
    {error, ?FUNCTION_NAME};
commit_write(throw_during_commit_write) ->
    throw(?FUNCTION_NAME).

abort_write(ModulePriv) when is_list(ModulePriv) ->
    ok;
abort_write(error_during_write) ->
    ok;
abort_write(throw_during_write) ->
    ok;
abort_write(throw_during_abort_write) ->
    throw(?FUNCTION_NAME).

open_read(ModulePriv) when is_list(ModulePriv) ->
    {ok, ModulePriv};
open_read(error_during_open_read) ->
    {error, ?FUNCTION_NAME};
open_read(throw_during_open_read) ->
    throw(?FUNCTION_NAME);
open_read(error_during_read = ModulePriv) ->
    {ok, ModulePriv};
open_read(throw_during_read = ModulePriv) ->
    {ok, ModulePriv};
open_read(error_during_close_read = ModulePriv) ->
    {ok, ModulePriv};
open_read(throw_during_close_read = ModulePriv) ->
    {ok, ModulePriv}.

read([BackupItem | Rest]) ->
    {ok, [BackupItem], Rest};
read([]) ->
    {ok, [], []};
read(error_during_read) ->
    {error, ?FUNCTION_NAME};
read(throw_during_read) ->
    throw(?FUNCTION_NAME);
read(error_during_close_read = ModulePriv) ->
    {ok, [], ModulePriv};
read(throw_during_close_read = ModulePriv) ->
    {ok, [], ModulePriv}.

close_read(ModulePriv) when is_list(ModulePriv) ->
    ok;
close_read(error_during_read) ->
    ok;
close_read(throw_during_read) ->
    ok;
close_read(error_during_close_read) ->
    {error, ?FUNCTION_NAME};
close_read(throw_during_close_read) ->
    throw(?FUNCTION_NAME).
