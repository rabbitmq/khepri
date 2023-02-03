%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
%% memory footprint of a projection table grows with the number of tree nodes in
%% the store matching the pattern.
%%
%% Projection ETS tables are owned by the Khepri cluster and are deleted when
%% the cluster stops.
%%
%% Updates to projection tables are immediately consistent for the member of
%% the cluster on which the change to the store is made and the leader member
%% but are eventually consistent for all other followers.

-module(khepri_projection).

-include_lib("kernel/include/logger.hrl").

-include("src/khepri_projection.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_fun.hrl").

-export([new/2, new/3, name/1]).

 %% For internal use only
-export([init/1, trigger/4]).

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

-type projection_fun() :: simple_projection_fun() | extended_projection_fun().
%% A function that formats an entry in the tree into a record to be stored in a
%% projection.
%%
%% Projection functions may either be "simple" or "extended." See {@link
%% simple_projection_fun()} and {@link extended_projection_fun()} for more
%% information.
%%
%% The projection function is executed directly by the Ra server process. The
%% function should be as simple and fast as possible to avoid slowing down the
%% server.

-type options() :: #{type => ets:table_type(),
                     keypos => pos_integer(),
                     read_concurrency => boolean(),
                     write_concurrency => boolean() | auto,
                     compressed => boolean()}.
%% Options which control the created ETS table.
%%
%% These options are a subset of the options available to {@link ets:new/2}.
%% Refer to the {@link ets:new/2} documentation for a reference on each type
%% and available values.
%%
%% When a projection is created from a {@link simple_projection_fun()}, the
%% `type' option may only be `set' or `ordered_set': `bag' types are not
%% allowed. {@link extended_projection_fun()}s may use any valid {@link
%% ets:table_type()}.

-export_type([projection/0,
              projection_fun/0,
              options/0]).

-spec new(Name, ProjectionFun) -> Projection when
      Name :: atom(),
      ProjectionFun :: khepri_projection:projection_fun(),
      Projection :: khepri_projection:projection().
%% Creates a new projection data structure with default options.
%% This is the same as calling `new(Name, ProjectionFun, #{})'.

new(Name, ProjectionFun) ->
    new(Name, ProjectionFun, #{}).

-spec new(Name, ProjectionFun, Options) -> Projection when
      Name :: atom(),
      ProjectionFun :: khepri_projection:projection_fun(),
      Options :: khepri_projection:options(),
      Projection :: khepri_projection:projection().
%% @doc Creates a new projection data structure.
%%
%% This function throws an error in the shape of `{unexpected_option, Key,
%% Value}' when an unknown or invalid option is passed (see {@link options()}).
%% For example, if the passed `ProjectionFun' is a {@link
%% simple_projection_fun()} and the `Options' map sets the `type' to `bag',
%% this function throws `{unexpected_option, type, bag}' since `bag' is not
%% valid for simple projection funs.
%%
%% @param Name the name of the projection. This corresponds to the name of
%%        the ETS table which is created when the projection is registered.
%% @param ProjectionFun the function which turns paths and data into records
%%        to be stored in the projection table.
%% @param Options options which control properties of the projection table.
%%
%% @returns a {@link projection()} resource.


new(Name, ProjectionFun, Options)
  when is_map(Options) andalso
       (is_function(ProjectionFun, 2) orelse
        is_function(ProjectionFun, 4)) ->
    EtsOptions = maps:fold(fun to_ets_options/3, [named_table], Options),
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
    FunOptions = #{ensure_instruction_is_permitted =>
                   fun khepri_tx_adv:ensure_instruction_is_permitted/1,
                   should_process_function => ShouldProcessFunction,
                   is_standalone_fun_still_needed => fun(_Params) -> true end},
    StandaloneFun = khepri_fun:to_standalone_fun(ProjectionFun, FunOptions),
    #khepri_projection{name = Name,
                       projection_fun = StandaloneFun,
                       ets_options = EtsOptions}.

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
      Name :: atom().
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
    Table = ets:whereis(Name),
    case ProjectionFun#standalone_fun.arity of
        2 ->
            trigger_simple_projection(
              Table, Name, ProjectionFun, Path, OldProps, NewProps);
        4 ->
            trigger_extended_projection(
              Table, Name, ProjectionFun, Path, OldProps, NewProps)
    end.

-spec trigger_extended_projection(
        Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    Ret when
      Table :: ets:tid(),
      Name :: atom(),
      StandaloneFun :: khepri_fun:standalone_fun(),
      Path :: khepri_path:native_path(),
      OldProps :: khepri:node_props(),
      NewProps :: khepri:node_props(),
      Ret :: ok.
%% @hidden

trigger_extended_projection(
  Table, Name, StandaloneFun, Path, OldProps, NewProps) ->
    Args = [Table, Path, OldProps, NewProps],
    try
        khepri_fun:exec(StandaloneFun, Args)
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
      Name :: atom(),
      StandaloneFun :: khepri_fun:standalone_fun(),
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
            {ok, khepri_fun:exec(StandaloneFun, Args)}
        catch
            Class:Reason:Stacktrace ->
                %% Funs have better formatting:
                Fun = khepri_fun:to_fun(StandaloneFun),
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
                    ets:insert(Table, Record);
                error ->
                    ok
            end;
        {#{data := OldPayload}, _} ->
            case TryExec([Path, OldPayload]) of
                {ok, Record} ->
                    ets:delete_object(Table, Record);
                error ->
                    ok
            end;
        {_, _} ->
            ok
    end,
    ok.
