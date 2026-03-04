%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2026 Broadcom. All Rights Reserved. The term "Broadcom"
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
-include_lib("stdlib/include/assert.hrl").

-include_lib("horus/include/horus.hrl").

-include("src/khepri_error.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_projection.hrl").

-export([new/2, new/3, name/1]).

 %% For internal use only
-export([check_compatibility_with_store/3, init/1, trigger/4, delete/1]).

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
fun((Tids :: ets:tid() | #{atom() => ets:tid()},
     Path :: khepri_path:native_path(),
     OldPayload :: khepri:node_props(),
     NewPayload :: khepri:node_props()) -> any()).
%% An extended projection function.
%%
%% In some cases, a tree node in the store might correspond to many objects in
%% one or more projection tables. Extended projection functions are allowed and
%% expected to call ETS functions directly in order to build the projection
%% table.
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

-type overridable_ets_options() :: #{type => ets:table_type(),
                                     keypos => pos_integer(),
                                     read_concurrency => boolean(),
                                     write_concurrency => boolean() | auto,
                                     compressed => boolean()}.
%% Overrideable ETS options.
%%
%% They can be used for single-table and multi-table projections. For the
%% latter, each per-table option takes precedence over the global option if
%% specified twice.
%%
%% When a projection is created from a {@link simple_projection_fun()}, the
%% `type' option may only be `set' or `ordered_set': `bag' types are not
%% allowed. {@link extended_projection_fun()}s may use any valid {@link
%% ets:table_type()}.

-type options() :: #{tables => multi_table_options(),
                     standalone_fun_options => horus:options(),

                     %% Overridable ETS options used for all tables, for both
                     %% single-table and multi-table projections. These are the
                     %% same as `{@link overridable_ets_options()}' and must be
                     %% kept in sync.
                     type => ets:table_type(),
                     keypos => pos_integer(),
                     read_concurrency => boolean(),
                     write_concurrency => boolean() | auto,
                     compressed => boolean()}.
%% Options which control the created ETS table.
%%
%% If provided, `standalone_fun_options' are merged with defaults and passed to
%% {@link horus:to_standalone_fun/2}. The remaining options are a subset of the
%% options available to {@link ets:new/2}. Refer to the {@link ets:new/2}
%% documentation for a reference on each type and available values.
%%
%% See {@link overridable_ets_options()} for more details about overridable ETS
%% options.

-type multi_table_options() :: #{atom() => overridable_ets_options()}.
%% Options which control multiple created ETS tables.
%%
%% When the projection needs multiple ETS options, this map associates a
%% specific ETS table to its options.

-opaque ets_options() :: [atom() | tuple()].
%% List of ETS options, passed to `ets:new/2'.

-export_type([name/0,
              projection/0,
              projection_fun/0,
              overridable_ets_options/0,
              options/0,
              multi_table_options/0,
              ets_options/0]).

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
    case is_single_table_projection(Options) of
        true ->
            EtsOptions = prepare_ets_options(Options),
            #khepri_projection{name = Name,
                               projection_fun = copy,
                               ets_options = EtsOptions};
        false ->
            ?khepri_misuse(
               multi_table_projection_incompatible_with_copy_func,
               #{name => Name,
                 projection_fun => copy,
                 options => Options})
    end;
new(Name, ProjectionFun, Options)
  when is_map(Options) andalso
       (is_function(ProjectionFun, 2) orelse
        is_function(ProjectionFun, 4)) ->
    {CustomFunOptions, OtherOptions} =
      case maps:take(standalone_fun_options, Options) of
          error                                      -> {#{}, Options};
          {_CustomFunOptions, _OtherOptions} = Value -> Value
      end,
    ShouldProcessFunction =
      if
          is_function(ProjectionFun, 2) ->
              %% Ensure the options map is for a single-table projection.
              case is_single_table_projection(Options) of
                  true ->
                      ok;
                  false ->
                      ?khepri_misuse(
                         multi_table_projection_incompatible_with_simple_func,
                         #{name => Name,
                           projection_fun => ProjectionFun,
                           options => Options})
              end,

              %% Ensure that the type is set or ordered_set for simple
              %% projection funs.
              case maps:get(type, Options, set) of
                  set ->
                      ok;
                  ordered_set ->
                      ok;
                  Type ->
                      ?khepri_misuse(
                         unexpected_projection_option,
                         #{name => type,
                           value => Type})
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

    EtsOptions = prepare_ets_options(OtherOptions),
    #khepri_projection{name = Name,
                       projection_fun = StandaloneFun,
                       ets_options = EtsOptions}.

-spec is_single_table_projection(Options) -> IsSingleTableProjection when
      Options :: khepri_projection:options() | khepri_projection:projection(),
      IsSingleTableProjection :: boolean().
%% @doc Returns if the given options map corresponds to a single-table
%% projection.
%%
%% The presence of the `tables' option determines if the projection is a
%% single-table or a multi-table projection.
%%
%% @private

is_single_table_projection(Options) when is_map(Options) ->
    not maps:is_key(tables, Options);
is_single_table_projection(#khepri_projection{ets_options = EtsOptions}) ->
    is_list(EtsOptions).

prepare_ets_options(Options) ->
    case is_single_table_projection(Options) of
        true  -> prepare_single_table_ets_options(Options);
        false -> prepare_multi_table_ets_options(Options)
    end.

prepare_single_table_ets_options(EtsOptions) ->
    ?assert(is_single_table_projection(EtsOptions)),
    maps:fold(fun to_ets_options/3, ?DEFAULT_ETS_OPTS, EtsOptions).

prepare_multi_table_ets_options(Options) ->
    ?assertNot(is_single_table_projection(Options)),
    {TablesMap, GlobalEtsOptions} = maps:take(tables, Options),
    maps:map(
      fun(_Table, PerTableEtsOptions) ->
              EtsOptions = maps:merge(GlobalEtsOptions, PerTableEtsOptions),
              prepare_single_table_ets_options(EtsOptions)
      end, TablesMap).

-spec to_ets_options(Key, Value, Acc) -> Acc
    when
      Key :: atom(),
      Value :: atom() | pos_integer() | boolean(),
      Acc :: khepri_projection:ets_options().
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
    ?khepri_misuse(
       unexpected_projection_option,
       #{name => Key,
         value => Value}).

-spec name(Projection) -> Name when
      Projection :: projection(),
      Name :: khepri_projection:name().
%% @doc Returns the name of the given `Projection'.

name(#khepri_projection{name = Name}) ->
    Name.

-spec check_compatibility_with_store(StoreId, Projection, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      Projection :: khepri_projection:projection(),
      Timeout :: timeout(),
      Ret :: ok | {error, any()}.
%% @doc Checks if a projection is compatible with the given store.
%%
%% @private

check_compatibility_with_store(StoreId, Projection, Timeout) ->
    case is_single_table_projection(Projection) of
        true ->
            ok;
        false ->
            %% Ensure the Khepri cluster runs a new enough version to support
            %% multi-table projections.
            khepri_machine:wait_for_effective_behaviour(
              StoreId, multi_table_projections, Timeout)
    end.

-spec init(Projection) -> Ret when
      Projection :: projection(),
      Ret :: ok.
%% @doc Initializes a projection.
%%
%% If it is a single-table projection, it creates an ETS table using the
%% projection's `name' and {@link options()}.
%%
%% If it is a multi-table projection, it creates many ETS tables with their
%% specific {@link options()}.
%%
%% @private

init(#khepri_projection{name = Name, ets_options = EtsOptions})
  when is_list(EtsOptions) ->
    create_table(Name, EtsOptions);
init(#khepri_projection{ets_options = MultiEtsOptions})
  when is_map(MultiEtsOptions) ->
    maps:foreach(
      fun(Table, EtsOptions) ->
              create_table(Table, EtsOptions)
      end, MultiEtsOptions).

create_table(Name, EtsOptions) ->
    case ets:info(Name) of
        undefined ->
            _Tid = ets:new(Name, EtsOptions),
            ok;
        _Info ->
            ok
    end.

-spec delete(Projection) -> Ret when
      Projection :: projection(),
      Ret :: ok | {error, does_not_exist}.
%% @hidden
%% Deletes the ETS table for the given projection.
%%
%% @returns `ok' if the table is successfully deleted or `{error,
%% does_not_exist}' if the table does not exist.

delete(#khepri_projection{} = Projection) ->
    try
        case is_single_table_projection(Projection) of
            true ->
                #khepri_projection{name = Name} = Projection,
                ets:delete(Name);
            false ->
                #khepri_projection{ets_options = EtsOptions} = Projection,
                maps:foreach(
                  fun(Table, _) ->
                          ets:delete(Table)
                  end, EtsOptions)
        end,
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
  #khepri_projection{
     name = Name,
     projection_fun = ProjectionFun,
     ets_options = EtsOptions},
  Path, OldProps, NewProps) ->
    Tids = case is_list(EtsOptions) of
               true ->
                   ets:whereis(Name);
               false ->
                   maps:fold(
                     fun
                         (Table, _EtsOptions, Acc) when is_map(Acc) ->
                             case ets:whereis(Table) of
                                 undefined -> undefined;
                                 Tid       -> Acc#{Table => Tid}
                             end;
                         (_Table, _EtsOptions, undefined = Acc) ->
                             Acc
                     end, #{}, EtsOptions)
           end,
    AllTablesExist = Tids =/= undefined,
    case AllTablesExist of
        true ->
            case ProjectionFun of
                copy ->
                    ?assert(not is_map(Tids)),
                    trigger_copy_projection(Name, Tids, OldProps, NewProps);
                StandaloneFun ->
                    case ?HORUS_STANDALONE_FUN_ARITY(StandaloneFun) of
                        2 ->
                            ?assert(not is_map(Tids)),
                            trigger_simple_projection(
                              Name, Tids, StandaloneFun, Path,
                              OldProps, NewProps);
                        4 ->
                            trigger_extended_projection(
                              Name, Tids, StandaloneFun, Path,
                              OldProps, NewProps)
                    end
            end;
        false ->
            %% A table might have been deleted by an `unregister_projections'
            %% effect in between when a `trigger_projection' effect is created
            %% and when it is handled in `khepri_machine:handle_aux/5`. In this
            %% case we should no-op the trigger effect.
            ok
    end.

-spec trigger_extended_projection(
        Name, Tids, StandaloneFun, Path, OldProps, NewProps) ->
    Ret when
      Tids :: ets:tid() | #{atom() => ets:tid()},
      Name :: khepri_projection:name(),
      StandaloneFun :: horus:horus_fun(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_extended_projection(
  Name, Tids, StandaloneFun, Path, OldProps, NewProps) ->
    Args = [Tids, Path, OldProps, NewProps],
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
        Name, Tid, StandaloneFun, Path, OldProps, NewProps) ->
    Ret when
      Name :: khepri_projection:name(),
      Tid :: ets:tid(),
      StandaloneFun :: horus:horus_fun(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_simple_projection(
  Name, Tid, StandaloneFun, Path, OldProps, NewProps) ->
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
                    try_ets_insert(Name, Tid, Record);
                error ->
                    ok
            end;
        {#{data := OldPayload}, _} ->
            case TryExec([Path, OldPayload]) of
                {ok, Record} ->
                    try_ets_delete_object(Name, Tid, Record);
                error ->
                    ok
            end;
        {_, _} ->
            ok
    end,
    ok.

-spec trigger_copy_projection(Name, Tid, OldProps, NewProps) -> Ret when
      Name :: khepri_projection:name(),
      Tid :: ets:tid(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_copy_projection(Name, Tid, _OldProps, #{data := NewPayload}) ->
    try_ets_insert(Name, Tid, NewPayload),
    ok;
trigger_copy_projection(Name, Tid, #{data := OldPayload}, _NewProps) ->
    try_ets_delete_object(Name, Tid, OldPayload),
    ok;
trigger_copy_projection(_Name, _Table, _OldProps, _NewProps) ->
    ok.

-spec try_ets_insert(Name, Tid, Record) -> ok when
      Name :: khepri_projection:name(),
      Tid :: ets:tid(),
      Record :: any().
%% @hidden

try_ets_insert(Name, Tid, Record) ->
    try
        ets:insert(Tid, Record)
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

-spec try_ets_delete_object(Name, Tid, Record) -> ok when
      Name :: khepri_projection:name(),
      Tid :: ets:tid(),
      Record :: any().
%% @hidden

try_ets_delete_object(Name, Tid, Record) ->
    try
        ets:delete_object(Tid, Record)
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
