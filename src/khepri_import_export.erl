%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Wrapper around Mnesia Backup &amp; Restore callback modules.
%%
%% Khepri uses the same callback module as Mnesia to implement its import and
%% export feature. The <a
%% href="https://www.erlang.org/doc/apps/mnesia/mnesia_app_a">Mnesia Backup
%% &amp; Restore API is described in the Mnesia documentation</a>.
%%
%% Mnesia doesn't provide an Erlang behavior module to ease development. This
%% module can be used as the behavior instead. For example:
%% ```
%% -module(my_export_module).
%% -behavior(khepri_import_export).
%%
%% -export([open_write/1,
%%          write/2,
%%          commit_write/1,
%%          abort_write/1,
%% 
%%          open_read/1,
%%          read/1,
%%          close_read/1]).
%% '''
%%
%% There are two sets of callback functions to implement: one used during
%% export (or "backup" in Mnesia terms), one used during import (or "restore"
%% in Mnesia terms).
%%
%% == Export callback API ==
%%
%% <ol>
%% <li>
%% <p>`Module:open_write(Args)'</p>
%% <p>This function is called at the beginning of an export. It is responsible
%% for any initialization and must return an `{ok, ModulePriv}' tuple.
%% `ModulePriv' is a private state passed to the following functions.</p>
%% </li>
%% <li>
%% <p>`Module:write(ModulePriv, BackupItems)'</p>
%% <p>This function is called for each subset of the items to export.</p>
%% <p>`BackupItems' is a list of opaque Erlang terms. The callback module
%% can't depend on the structure of these Erlang terms. Only the fact that it
%% is a list is guarantied.</p>
%% <p>An empty list of `BackupItems' means this is the last call to
%% `Module:write/2'. Otherwise, the way all items to export are split into
%% several subsets and thus several calls to `Module:write/2' is
%% undefined.</p>
%% <p>At the end of each call, the function must return `{ok, NewModulePriv}'.
%% This new private state is passed to the subsequent calls.</p>
%% </li>
%% <li>
%% <p>`Module:commit_write(ModulePriv)'</p>
%% <p>This function is called after successful calls to `Module:write/2'. It
%% is responsible for doing any cleanup.</p>
%% <p>This function can return `ok' or `{ok, Ret}' if it wants to return some
%% Erlang terms to the caller.</p>
%% </li>
%% <li>
%% <p>`Module:abort_write(ModulePriv)'</p>
%% <p>This function is called after failed call to `Module:write/2', or if
%% reading from the Khepri store fails. It is responsible for doing any
%% cleanup.</p>
%% <p>This function can return `ok' or `{ok, Ret}' if it wants to return some
%% Erlang terms to the caller.</p>
%% </li>
%% </ol>
%%
%% == Import callback API ==
%%
%% <ol>
%% <li>
%% <p>`Module:open_read(Args)'</p>
%% <p>This function is called at the beginning of an import. It is responsible
%% for any initialization and must return an `{ok, ModulePriv}' tuple.
%% `ModulePriv' is a private state passed to the following functions.</p>
%% </li>
%% <li>
%% <p>`Module:read(ModulePriv)'</p>
%% <p>This function is one or more times until there is nothing left to
%% import.</p>
%% <p>The function must return `{ok, BackupItems, NewModulePriv}'. This new
%% private state is passed to the subsequent calls.</p>
%% <p>`BackupItems' is a list of opaque Erlang terms. The callback module
%% can't depend on the structure of these Erlang terms. The backup items must
%% be returned exactly as they were at the time of the export and in the same
%% order.</p>
%% <p>An empty list of `BackupItems' means this is the last batch.
%% `Module:read/1' won't be called anymore. `Module:close_read/1' will be
%% called next instead.</p>
%% </li>
%% <li>
%% <p>`Module:close_read(ModulePriv)'</p>
%% <p>This function is called after the last call to `Module:read/1',
%% successful or not, or if the actual import into the Khepri store fails. It
%% is responsible for doing any cleanup.</p>
%% <p>This function can return `ok' or `{ok, Ret}' if it wants to return some
%% Erlang terms to the caller.</p>
%% </li>
%% </ol>
%%
%% == Available callback modules ==
%%
%% Khepri comes with {@link khepri_export_erlang}. This module offers to
%% export Khepri tree nodes in a plain text file where backup items are
%% formatted as Erlang terms.

-module(khepri_import_export).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-export([export/4,
         import/3]).

-type module_priv() :: any().
%% Private data passed to `Module:open_write/1' or `Module:open_read/1'
%% initially.
%%
%% The actual term is specific to the callback module implementation. The
%% callback can use this term however it wants. It is returned from most
%% callback functions and passed to the next one.

-type backup_item() :: term().
%% An opaque term passed in a list to `Module:write/2' and returned by
%% `Module:read/1'.
%%
%% The callback module must not depend on the content of this term.

-export_type([module_priv/0,
              backup_item/0]).

-callback open_write(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, ModulePriv} | {error, any()}.

-callback write(ModulePriv, BackupItems) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      BackupItems :: [khepri_import_export:backup_item()],
      Ret :: {ok, ModulePriv} | {error, any()}.

-callback commit_write(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.

-callback abort_write(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {error, any()}.

-callback open_read(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, ModulePriv} | {error, any()}.

-callback read(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, BackupItems, ModulePriv} | {error, any()},
      BackupItems :: [khepri_import_export:backup_item()].

-callback close_read(ModulePriv) -> Ret when
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.

%% -------------------------------------------------------------------
%% Export (backup).
%% -------------------------------------------------------------------

-spec export(StoreId, PathPattern, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      PathPattern :: khepri_path:pattern(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

export(StoreId, PathPattern, Module, ModulePriv)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(Module) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    khepri_path:ensure_is_valid(PathPattern1),
    Query = fun(State) ->
                    try
                        do_export(State, PathPattern, Module, ModulePriv)
                    catch
                        Class:Exception:Stacktrace ->
                            {error, {exception, Class, Exception, Stacktrace}}
                    end
            end,
    Ret = khepri_machine:process_query(StoreId, Query, #{}),
    case Ret of
        {error, {exception, Class, Exception, Stacktrace}} ->
            erlang:raise(Class, Exception, Stacktrace);
        _ ->
            Ret
    end.

-spec do_export(MachineState, PathPattern, Module, ModulePriv) -> Ret when
      MachineState :: khepri_machine:state(),
      PathPattern :: khepri_path:native_pattern(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

do_export(State, PathPattern, Module, ModulePriv) ->
    %% Initialize export using the callback module.
    Tree = khepri_machine:get_tree(State),
    case open_write(Module, ModulePriv) of
        {ok, ModulePriv1} ->
            %% Prepare the traversal.
            Fun =
            fun(Path, Node, ModulePriv2) ->
                    Ret =
                    try
                        write(State, Path, Node, Module, ModulePriv2)
                    catch
                        Class:Exception:Stacktrace ->
                            _ = catch abort_write(Module, ModulePriv2),
                            erlang:raise(Class, Exception, Stacktrace)
                    end,
                    case Ret of
                        {ok, ModulePriv3} ->
                            {ok, keep, ModulePriv3};
                        {error, _} = Error ->
                            _ = abort_write(Module, ModulePriv2),
                            Error
                    end
            end,
            TreeOptions = #{expect_specific_node => false,
                            include_root_props => true},
            Ret1 = khepri_tree:walk_down_the_tree(
                     Tree, PathPattern, TreeOptions, Fun, ModulePriv1),
            case Ret1 of
                {ok, Tree1, _AppliedChanges, FinalModulePriv} ->
                    khepri_tree:assert_equal(Tree, Tree1),
                    commit_write(Module, FinalModulePriv);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec open_write(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, ModulePriv} | {error, any()}.
%% @private

open_write(Module, ModulePriv) ->
    ?LOG_DEBUG(
       "Khepri export: calling ~s:open_write/1:~n"
       "  opaque data: ~p",
       [Module, ModulePriv],
       #{domain => [khepri, import_export]}),
    case Module:open_write(ModulePriv) of
        {ok, _} = Ret      -> Ret;
        {error, _} = Error -> Error
    end.

-spec write(MachineState, Path, Node, Module, ModulePriv) -> Ret when
      MachineState :: khepri_machine:state(),
      Path :: khepri_path:native_path(),
      Node :: khepri_tree:tree_node(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, ModulePriv} | {error, any()}.
%% @private

write(State, Path, #node{payload = Payload}, Module, ModulePriv) ->
    KeepWhileConds = khepri_machine:get_keep_while_conds(State),
    PutOptions = case KeepWhileConds of
                     #{Path := KeepWhile} -> #{keep_while => KeepWhile};
                     _                    -> #{}
                 end,
    HasPayload = Payload =/= ?NO_PAYLOAD,
    HasPutOptions = PutOptions =/= #{},
    case HasPayload orelse HasPutOptions of
        true ->
            Command = #put{path = Path,
                           payload = Payload,
                           options = PutOptions},
            ?LOG_DEBUG(
               "Khepri export: calling ~s:write/2:~n"
               "  opaque data: ~p~n"
               "  backup items: ~p",
               [Module, ModulePriv, [Command]],
               #{domain => [khepri, import_export]}),
            case Module:write(ModulePriv, [Command]) of
                {ok, _} = Ret      -> Ret;
                {error, _} = Error -> Error
            end;
        false ->
            {ok, ModulePriv}
    end;
write(_State, _Path, {interrupted, _, _}, _Module, ModulePriv) ->
    {ok, ModulePriv}.

-spec commit_write(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

commit_write(Module, ModulePriv) ->
    ?LOG_DEBUG(
       "Khepri export: calling ~s:commit_write/1:~n"
       "  opaque data: ~p",
       [Module, ModulePriv],
       #{domain => [khepri, import_export]}),
    case Module:commit_write(ModulePriv) of
        ok = Ret           -> Ret;
        {ok, _} = Ret      -> Ret;
        {error, _} = Error -> Error
    end.

-spec abort_write(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {error, any()}.
%% @private

abort_write(Module, ModulePriv) ->
    ?LOG_DEBUG(
       "Khepri export: calling ~s:abort_write/1:~n"
       "  opaque data: ~p",
       [Module, ModulePriv],
       #{domain => [khepri, import_export]}),
    Module:abort_write(ModulePriv).

%% -------------------------------------------------------------------
%% Import (restore).
%% -------------------------------------------------------------------

-spec import(StoreId, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

import(StoreId, Module, ModulePriv)
  when ?IS_KHEPRI_STORE_ID(StoreId) andalso is_atom(Module) ->
    case open_read(Module, ModulePriv) of
        {ok, ModulePriv1}  -> do_import(StoreId, Module, ModulePriv1);
        {error, _} = Error -> Error
    end.

-spec do_import(StoreId, Module, ModulePriv) -> Ret when
      StoreId :: khepri:store_id(),
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

do_import(StoreId, Module, ModulePriv) ->
    Ret = try
              read(Module, ModulePriv)
          catch
              Class:Exception:Stacktrace ->
                  _ = catch close_read(Module, ModulePriv),
                  erlang:raise(Class, Exception, Stacktrace)
          end,
    case Ret of
        {ok, BackupItems, ModulePriv1} when BackupItems =/= [] ->
            case handle_backup_items(StoreId, BackupItems) of
                ok ->
                    do_import(StoreId, Module, ModulePriv1);
                {error, _} = Error ->
                    _ = close_read(Module, ModulePriv1),
                    Error
            end;
        {ok, [], ModulePriv1} ->
            close_read(Module, ModulePriv1);
        {error, _} = Error ->
            _ = close_read(Module, ModulePriv),
            Error
    end.

-spec handle_backup_items(StoreId, BackupItems) -> Ret when
      StoreId :: khepri:store_id(),
      BackupItems :: [khepri_import_export:backup_item()],
      Ret :: ok | {error, any()}.
%% @private

handle_backup_items(StoreId, [Command | Rest]) ->
    Options = #{},
    case khepri_machine:process_command(StoreId, Command, Options) of
        {ok, _}            -> handle_backup_items(StoreId, Rest);
        {error, _} = Error -> Error
    end;
handle_backup_items(_StoreId, []) ->
    ok.

-spec open_read(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, ModulePriv} | {error, any()}.
%% @private

open_read(Module, ModulePriv) ->
    case Module:open_read(ModulePriv) of
        {ok, _} = Ret      -> Ret;
        {error, _} = Error -> Error
    end.

-spec read(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: {ok, BackupItems, ModulePriv} | {error, any()},
      BackupItems :: [khepri_import_export:backup_item()].
%% @private

read(Module, ModulePriv) ->
    case Module:read(ModulePriv) of
        {ok, BackupItems, _ModulePriv1} = Ret when is_list(BackupItems) ->
            Ret;
        {error, _Reason} = Error ->
            Error
    end.

-spec close_read(Module, ModulePriv) -> Ret when
      Module :: module(),
      ModulePriv :: khepri_import_export:module_priv(),
      Ret :: ok | {ok, ModulePriv} | {error, any()}.
%% @private

close_read(Module, ModulePriv) ->
    case Module:close_read(ModulePriv) of
        ok = Ret                 -> Ret;
        {ok, _ModulePriv1} = Ret -> Ret;
        {error, _Reason} = Error -> Error
    end.
