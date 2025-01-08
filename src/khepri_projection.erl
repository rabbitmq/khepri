%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri projections
%%
%% Projections build a replicated ETS table using tree nodes from the store
%% which match a {@link khepri_path:pattern()}. When a tree node matching a
%% projection's pattern is changed in the store, the tree node is passed
%% through the projection's {@link projection_fun()} to create record(s).
%% These records are then stored in the projection's ETS table for all members
%% in a Khepri cluster.
%%
%% Projections provide a way to query the store as fast as possible and are
%% appropriate for lookups which require low latency and/or high throughput.
%% Projection tables contain all records matching the pattern, though, so the
%% memory footprint of a projection table grows with the number of tree nodes
%% in the store matching the pattern.
%%
%% Projection ETS tables are owned by the Khepri cluster and are deleted when
%% the cluster stops.
%%
%% Updates to projection tables are immediately consistent for the member of
%% the cluster on which the change to the store is made and the leader member
%% but are eventually consistent for all other followers.

-module(khepri_projection).

-include_lib("kernel/include/logger.hrl").

-include_lib("horus/include/horus.hrl").

-include("src/khepri_projection.hrl").
-include("src/khepri_machine.hrl").

-export([new/2, new/3, name/1]).

 %% For internal use only
-export([init/1, trigger/4, delete/1]).

-type name() :: atom().
%% The name of a projection.
%%
%% @see khepri_projection:new/3.

-type projection() :: #khepri_projection{}.
%% A projection resource.
%%
%% @see khepri_projection:new/3.
%% @see khepri:register_projection/4.

-type simple_projection_fun() ::
fun((Path :: khepri_path:native_path(),
     Payload :: khepri:data()) -> Record :: tuple()).
%% A simple projection function.
%%
%% Simple projection functions only take the path and payload for a tree node
%% in the store. The record produced by the function is used to create and
%% delete objects in the ETS table.
%%
%% This function is compiled the same way as a transaction function: all
%% side-effects are not allowed. Additionally, for any `Path' and `Payload'
%% inputs, this function must consistently return the same `Record'.

-type extended_projection_fun() ::
fun((Table :: ets:tid(),
     Path :: khepri_path:native_path(),
     OldPayload :: khepri:node_props(),
     NewPayload :: khepri:node_props()) -> any()).
%% An extended projection function.
%%
%% In some cases, a tree node in the store might correspond to many objects in
%% a projection table. Extended projection functions are allowed to call ETS
%% functions directly in order to build the projection table.
%%
%% `OldPayload' or `NewPayload' are empty maps if there is no tree node. For
%% example, a newly created tree node will have an empty map for `OldPayload'
%% and a {@link khepri:node_props()} map with values for `NewPayload'.
%%
%% This function is compiled like a transaction function except that calls
%% to the {@link ets} module are allowed.
%%
%% The return value of this function is ignored.

-type projection_fun() :: copy |
                          simple_projection_fun() |
                          extended_projection_fun().
%% A function that formats an entry in the tree into a record to be stored in a
%% projection.
%%
%% Projection functions may either be:
%% <ul>
%% <li>"simple" - a function that takes the path within the tree for a tree
%% node and the payload and returns a record. See {@link
%% simple_projection_fun()}.</li>
%% <li>"copy" - a projection that ignores the path and use the tree node's
%% payload as the record directly. This is a special case that avoids the
%% overhead of executing standalone functions for the sake of performance.</li>
%% <li>"extended" - a function that takes the ETS table identifier, path, and
%% old and new tree node properties and executes ETS functions directly.
%% See {@link extended_projection_fun()}.</li>
%% </ul>
%%
%% The projection function is executed directly by the Ra server process. The
%% function should be as simple and fast as possible to avoid slowing down the
%% server.

-type options() :: #{type => ets:table_type(),
                     keypos => pos_integer(),
                     read_concurrency => boolean(),
                     write_concurrency => boolean() | auto,
                     compressed => boolean(),
                     standalone_fun_options => horus:options()}.
%% Options which control the created ETS table.
%%
%% If provided, `standalone_fun_options' are merged with defaults and passed to
%% {@link horus:to_standalone_fun/2}. The remaining options are a subset of the
%% options available to {@link ets:new/2}. Refer to the {@link ets:new/2}
%% documentation for a reference on each type and available values.
%%
%% When a projection is created from a {@link simple_projection_fun()}, the
%% `type' option may only be `set' or `ordered_set': `bag' types are not
%% allowed. {@link extended_projection_fun()}s may use any valid {@link
%% ets:table_type()}.

-export_type([name/0,
              projection/0,
              projection_fun/0,
              options/0]).

-ifdef(TEST).
%% In testing we will cover failure scenarios like an ETS table being deleted
%% so we need the table to be public. In normal operation though, the ETS table
%% is protected and belongs to the Ra server process.
-define(DEFAULT_ETS_OPTS, [public, named_table]).
-else.
-define(DEFAULT_ETS_OPTS, [protected, named_table]).
-endif.

-spec new(Name, ProjectionFun) -> Projection when
      Name :: khepri_projection:name(),
      ProjectionFun :: khepri_projection:projection_fun(),
      Projection :: khepri_projection:projection().
%% Creates a new projection data structure with default options.
%% This is the same as calling `new(Name, ProjectionFun, #{})'.

new(Name, ProjectionFun) ->
    new(Name, ProjectionFun, #{}).

-spec new(Name, ProjectionFun, Options) -> Projection when
      Name :: khepri_projection:name(),
      ProjectionFun :: khepri_projection:projection_fun(),
      Options :: khepri_projection:options(),
      Projection :: khepri_projection:projection().
%% @doc Creates a new projection data structure.
%%
%% This function throws an error in the shape of `{unexpected_option, Key,
%% Value}' when an unknown or invalid {@link option()} is passed. For example,
%% if the passed `ProjectionFun' is a {@link simple_projection_fun()} and the
%% `Options' map sets the `type' to `bag', this function throws
%% `{unexpected_option, type, bag}' since `bag' is not valid for simple
%% projection funs.
%%
%% @param Name the name of the projection. This corresponds to the name of
%%        the ETS table which is created when the projection is registered.
%% @param ProjectionFun the function which turns paths and data into records
%%        to be stored in the projection table.
%% @param Options options which control properties of the projection table.
%%
%% @returns a {@link projection()} resource.

new(Name, copy, Options) when is_map(Options) ->
    EtsOptions = maps:fold(fun to_ets_options/3, ?DEFAULT_ETS_OPTS, Options),
    #khepri_projection{name = Name,
                       projection_fun = copy,
                       ets_options = EtsOptions};
new(Name, ProjectionFun, Options)
  when is_map(Options) andalso
       (is_function(ProjectionFun, 2) orelse
        is_function(ProjectionFun, 4)) ->
    {CustomFunOptions, EtsOptions} =
    case maps:take(standalone_fun_options, Options) of
        error ->
            {#{}, Options};
        {_CustomFunOptions, _EtsOptions} = Value ->
            Value
    end,
    EtsOptions1 = maps:fold(
                    fun to_ets_options/3, ?DEFAULT_ETS_OPTS, EtsOptions),
    ShouldProcessFunction =
    if
        is_function(ProjectionFun, 2) ->
            %% Ensure that the type is set or ordered_set for simple projection
            %% funs.
            case maps:get(type, Options, set) of
                set ->
                    ok;
                ordered_set ->
                    ok;
                Type ->
                    throw({unexpected_option, type, Type})
            end,
            fun khepri_tx_adv:should_process_function/4;
        is_function(ProjectionFun, 4) ->
            fun (ets, _F, _A, _From) ->
                    false;
                (M, F, A, From) ->
                    khepri_tx_adv:should_process_function(M, F, A, From)
            end
    end,
    DefaultFunOptions = #{ensure_instruction_is_permitted =>
                          fun khepri_tx_adv:ensure_instruction_is_permitted/1,
                          should_process_function => ShouldProcessFunction,
                          is_standalone_fun_still_needed =>
                          fun(_Params) -> true end},
    FunOptions = maps:merge(DefaultFunOptions, CustomFunOptions),
    StandaloneFun = horus:to_standalone_fun(ProjectionFun, FunOptions),
    #khepri_projection{name = Name,
                       projection_fun = StandaloneFun,
                       ets_options = EtsOptions1}.

-spec to_ets_options(Key, Value, Acc) -> Acc
    when
      Key :: atom(),
      Value :: atom() | pos_integer() | boolean(),
      Acc :: [tuple() | atom()].
%% @hidden

to_ets_options(type, Type, Acc)
  when Type =:= set orelse Type =:= ordered_set orelse
       Type =:= bag orelse Type =:= duplicate_bag ->
    [Type | Acc];
to_ets_options(keypos, Pos, Acc) when Pos >= 1 ->
    [{keypos, Pos} | Acc];
to_ets_options(read_concurrency, ReadConcurrency, Acc)
  when is_boolean(ReadConcurrency) ->
    [{read_concurrency, ReadConcurrency} | Acc];
to_ets_options(write_concurrency, WriteConcurrency, Acc)
  when is_boolean(WriteConcurrency) orelse WriteConcurrency =:= auto ->
    [{write_concurrency, WriteConcurrency} | Acc];
to_ets_options(compressed, true, Acc) ->
    [compressed | Acc];
to_ets_options(compressed, false, Acc) ->
    Acc;
to_ets_options(Key, Value, _Acc) ->
    throw({unexpected_option, Key, Value}).

-spec name(Projection) -> Name when
      Projection :: projection(),
      Name :: khepri_projection:name().
%% @doc Returns the name of the given `Projection'.

name(#khepri_projection{name = Name}) ->
    Name.

-spec init(Projection) -> Ret when
      Projection :: projection(),
      Ret :: ok | {error, exists}.
%% @hidden
%% Initializes a projection. The current implementation creates an ETS
%% table using the projection's `name/1' and {@link options()}.

init(#khepri_projection{name = Name, ets_options = EtsOptions}) ->
    case ets:info(Name) of
        undefined ->
            _ = ets:new(Name, EtsOptions),
            ok;
        _Info ->
            {error, exists}
    end.

-spec delete(Projection) -> Ret when
      Projection :: projection(),
      Ret :: ok | {error, does_not_exist}.
%% @hidden
%% Deletes the ETS table for the given projection.
%%
%% @returns `ok' if the table is successfully deleted or `{error,
%% does_not_exist}' if the table does not exist.

delete(#khepri_projection{name = Name}) ->
    try
        ets:delete(Name),
        ok
    catch
        error:badarg:_Stacktrace ->
            {error, does_not_exist}
    end.

-spec trigger(Projection, Path, OldProps, NewProps) -> Ret when
      Projection :: projection(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden
%% Applies the projection function against the entry from the tree to return
%% a projected record. This projected record is then applied to the ETS table.

trigger(
  #khepri_projection{name = Name, projection_fun = ProjectionFun},
  Path, OldProps, NewProps) ->
    case ets:whereis(Name) of
        undefined ->
            %% A table might have been deleted by an `unregister_projections'
            %% effect in between when a `trigger_projection' effect is created
            %% and when it is handled in `khepri_machine:handle_aux/5`. In this
            %% case we should no-op the trigger effect.
            ok;
        Table ->
            case ProjectionFun of
                copy ->
                    trigger_copy_projection(Name, Table, OldProps, NewProps);
                StandaloneFun ->
                    case ?HORUS_STANDALONE_FUN_ARITY(StandaloneFun) of
                        2 ->
                            trigger_simple_projection(
                              Table, Name, StandaloneFun, Path,
                              OldProps, NewProps);
                        4 ->
                            trigger_extended_projection(
                              Table, Name, StandaloneFun, Path,
                              OldProps, NewProps)
                    end
            end
    end.

-spec trigger_extended_projection(
        Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    Ret when
      Table :: ets:tid(),
      Name :: khepri_projection:name(),
      StandaloneFun :: horus:horus_fun(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_extended_projection(
  Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    Args = [Table, Path, OldProps, NewProps],
    try
        horus:exec(StandaloneFun, Args)
    catch
        Class:Reason:Stacktrace ->
            Msg = io_lib:format("Failed to trigger extended projection~n"
                                "  Projection: ~s~n"
                                "  Path:~n"
                                "    ~p~n"
                                "  Old props:~n"
                                "    ~p~n"
                                "  New props:~n"
                                "    ~p~n"
                                "  Crash:~n"
                                "    ~ts",
                                [Name, Path, OldProps, NewProps,
                                 khepri_utils:format_exception(
                                   Class, Reason, Stacktrace,
                                   #{column => 4})]),
            ?LOG_ERROR(Msg, [])
    end,
    ok.

-spec trigger_simple_projection(
        Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    Ret when
      Table :: ets:tid(),
      Name :: khepri_projection:name(),
      StandaloneFun :: horus:horus_fun(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_simple_projection(
  Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    TryExec =
    fun(Args) ->
        try
            {ok, horus:exec(StandaloneFun, Args)}
        catch
            Class:Reason:Stacktrace ->
                %% Funs have better formatting:
                Fun = horus:to_fun(StandaloneFun),
                Exception = khepri_utils:format_exception(
                              Class, Reason, Stacktrace, #{column => 4}),
                Msg = io_lib:format("Failed to execute simple projection "
                                    "function: ~p~n"
                                    "  Projection: ~s~n"
                                    "  Path~n"
                                    "    ~p~n"
                                    "  Old props:~n"
                                    "    ~p~n"
                                    "  New props:~n"
                                    "    ~p~n"
                                    "  Args:~n"
                                    "    ~p~n"
                                    "  Crash:~n"
                                    "    ~ts",
                                    [Fun, Name, Path, OldProps, NewProps, Args,
                                     Exception]),
                ?LOG_ERROR(Msg, []),
                error
        end
    end,
    case {OldProps, NewProps} of
        {_, #{data := NewPayload}} ->
            case TryExec([Path, NewPayload]) of
                {ok, Record} ->
                    try_ets_insert(Name, Table, Record);
                error ->
                    ok
            end;
        {#{data := OldPayload}, _} ->
            case TryExec([Path, OldPayload]) of
                {ok, Record} ->
                    try_ets_delete_object(Name, Table, Record);
                error ->
                    ok
            end;
        {_, _} ->
            ok
    end,
    ok.

-spec trigger_copy_projection(Name, Table, OldProps, NewProps) -> Ret when
      Name :: khepri_projection:name(),
      Table :: ets:tid(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_copy_projection(Name, Table, _OldProps, #{data := NewPayload}) ->
    try_ets_insert(Name, Table, NewPayload),
    ok;
trigger_copy_projection(Name, Table, #{data := OldPayload}, _NewProps) ->
    try_ets_delete_object(Name, Table, OldPayload),
    ok;
trigger_copy_projection(_Name, _Table, _OldProps, _NewProps) ->
    ok.

-spec try_ets_insert(Name, Table, Record) -> ok when
      Name :: khepri_projection:name(),
      Table :: ets:tid(),
      Record :: term().
%% @hidden

try_ets_insert(Name, Table, Record) ->
    try
        ets:insert(Table, Record)
    catch
        Class:Reason:Stacktrace ->
            Exception = khepri_utils:format_exception(
                          Class, Reason, Stacktrace, #{column => 4}),
            Msg = io_lib:format("Failed to insert record into ETS table for "
                                "projection: ~p~n"
                                "  Record:~n"
                                "    ~p~n"
                                "  Crash:~n"
                                "    ~ts",
                                [Name, Record, Exception]),
            ?LOG_ERROR(Msg, [])
    end,
    ok.

-spec try_ets_delete_object(Name, Table, Record) -> ok when
      Name :: khepri_projection:name(),
      Table :: ets:tid(),
      Record :: term().
%% @hidden

try_ets_delete_object(Name, Table, Record) ->
    try
        ets:delete_object(Table, Record)
    catch
        Class:Reason:Stacktrace ->
            Exception = khepri_utils:format_exception(
                          Class, Reason, Stacktrace, #{column => 4}),
            Msg = io_lib:format("Failed to delete record from ETS table for "
                                "projection: ~p~n"
                                "  Record:~n"
                                "    ~p~n"
                                "  Crash:~n"
                                "    ~ts",
                                [Name, Record, Exception]),
            ?LOG_ERROR(Msg, [])
    end,
    ok.
