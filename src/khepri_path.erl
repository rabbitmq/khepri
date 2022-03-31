%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Khepri path API.
%%
%% A path is the type used by Khepri to reference nodes in the tree structure.
%% A path describes how to reach a node from the root node.
%%
%% A path, or <em>native path</em>, is a list of components. Components can be
%% Erlang atoms and binaries. Example:
%%
%% ```
%% %% Native path.
%% Path = [stock, wood, oak].
%% '''
%%
%% A path may contain conditions to tune how a node is matched or to match
%% multiple nodes at once. This is called a <em>path pattern</em>. A path
%% pattern may contain conditions in addition to regular components (Erlang
%% atoms and binaries). See {@link khepri_condition} to learn more about
%% conditions. Example:
%%
%% ```
%% %% Path pattern with a condition on `wood'.
%% PathPattern = [stock,
%%                #if_all{conditions = [wood,
%%                                      #if_node_exists{exists = true}]},
%%                oak].
%% '''
%%
%% To be user-friendly, Unix-like string-based paths are accepted by most
%% functions. These <em>Unix paths</em> have the `"/path/to/node"' and is the
%% equivalent of the `[path, to, node]' native path.
%%
%% ```
%% %% Unix path, equivalent of the first native path example.
%% UnixPath = "/stock/wood/oak".
%% '''

-module(khepri_path).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").

-export([compile/1,
         from_string/1,
         maybe_from_string/1,
         to_string/1,
         combine_with_conditions/2,
         targets_specific_node/1,
         component_targets_specific_node/1,
         is_valid/1,
         ensure_is_valid/1,
         abspath/2,
         realpath/1,
         pattern_includes_root_node/1]).

-ifdef(TEST).
-export([component_to_string/1]).
-endif.

-type node_id() :: atom() | binary().
%% A node name.

-type component() :: node_id() | ?ROOT_NODE | ?THIS_NODE | ?PARENT_NODE.
%% Component name in a path to a node.

%% TODO: Rename to node_path()
-type path() :: [component()].
%% Path to a node.

-type pattern() :: [pattern_component()].
%% Path pattern which may match zero, one or more nodes.

-type pattern_component() :: component() | khepri_condition:condition().

-export_type([path/0,
              pattern/0,
              component/0,
              pattern_component/0,
              node_id/0]).

-spec compile(pattern()) -> pattern().
%% @private

compile(PathPattern) ->
    lists:map(fun khepri_condition:compile/1, PathPattern).

-spec from_string(MaybeString) -> PathPattern when
      MaybeString :: string() | pattern(),
      PathPattern :: pattern().

from_string("/" ++ MaybeString) ->
    from_string(MaybeString, [?ROOT_NODE]);
from_string(MaybeString) when is_list(MaybeString) ->
    from_string(MaybeString, []);
from_string(NotPath) ->
    throw({invalid_path, #{path => NotPath}}).

from_string([Component | _] = Rest, ReversedPath)
  when ?IS_NODE_ID(Component) orelse
       ?IS_CONDITION(Component) ->
    finalize_path(Rest, ReversedPath);
from_string([Char, Component | _] = Rest, ReversedPath)
  when ?IS_SPECIAL_PATH_COMPONENT(Char) andalso
       (?IS_NODE_ID(Component) orelse
        ?IS_CONDITION(Component)) ->
    finalize_path(Rest, ReversedPath);
from_string([Char] = Rest, [] = ReversedPath)
  when ?IS_SPECIAL_PATH_COMPONENT(Char) ->
    finalize_path(Rest, ReversedPath);

from_string([$/ | Rest], ReversedPath) ->
    from_string(Rest, ReversedPath);

from_string([$<, $< | Rest], ReversedPath) ->
    ?assertNotEqual("", Rest),
    parse_binary_from_string(Rest, ReversedPath);

from_string([Char | _] = Rest, ReversedPath) when is_integer(Char) ->
    parse_atom_from_string(Rest, ReversedPath);

from_string([], ReversedPath) ->
    finalize_path([], ReversedPath);

from_string(Rest, ReversedPath) ->
    NotPath = lists:reverse(ReversedPath) ++ Rest,
    throw({invalid_path, #{path => NotPath,
                           tail => Rest}}).

parse_atom_from_string(Rest, ReversedPath) ->
    parse_atom_from_string(Rest, "", ReversedPath).

parse_atom_from_string([$/ | _] = Rest, Acc, ReversedPath) ->
    Component = finalize_atom_component(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1);
parse_atom_from_string([Char | Rest], Acc, ReversedPath)
  when is_integer(Char) ->
    Acc1 = [Char | Acc],
    parse_atom_from_string(Rest, Acc1, ReversedPath);
parse_atom_from_string([] = Rest, Acc, ReversedPath) ->
    Component = finalize_atom_component(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1).

finalize_atom_component(Acc) ->
    Acc1 = lists:reverse(Acc),
    case Acc1 of
        "." ->
            ?THIS_NODE;
        ".." ->
            ?PARENT_NODE;
        "*" ->
            ?STAR;
        "**" ->
            ?STAR_STAR;
        _ ->
            case re:run(Acc1, "\\*", [{capture, none}]) of
                match ->
                    ReOpts = [global, {return, list}],
                    Regex = re:replace(Acc1, "\\*", ".*", ReOpts),
                    #if_name_matches{regex = "^" ++ Regex ++ "$"};
                nomatch ->
                    erlang:list_to_atom(Acc1)
            end
    end.

parse_binary_from_string(Rest, ReversedPath) ->
    parse_binary_from_string(Rest, "", ReversedPath).

%% If a binary contains ">>" before its, we consider them to be part of the
%% binary's content, not the end marker.
parse_binary_from_string([$>, $> | Rest], Acc, ReversedPath)
  when Rest =:= "" orelse hd(Rest) =:= $/ ->
    Component = finalize_binary_componenent(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1);
parse_binary_from_string([Char | Rest], Acc, ReversedPath)
  when is_integer(Char) ->
    Acc1 = [Char | Acc],
    parse_binary_from_string(Rest, Acc1, ReversedPath);
parse_binary_from_string([], Acc, ReversedPath) ->
    %% This "binary" has no end marker. We consider it an atom then.
    parse_atom_from_string([], Acc ++ "<<", ReversedPath).

finalize_binary_componenent(Acc) ->
    Acc1 = lists:reverse(Acc),
    erlang:list_to_binary(Acc1).

prepend_component(Component, []) when ?IS_NODE_ID(Component) ->
    %% This is a relative path.
    [Component, ?THIS_NODE];
prepend_component(Component, ReversedPath) ->
    [Component | ReversedPath].

finalize_path(Rest, []) ->
    Rest;
finalize_path(Rest, ReversedPath) ->
    case lists:reverse(ReversedPath) ++ Rest of
        [?ROOT_NODE | Path] -> Path;
        Path                -> Path
    end.

-spec maybe_from_string(pattern() | string()) -> pattern().

maybe_from_string([Component | _] = Path)
  when ?IS_NODE_ID(Component) orelse
       ?IS_CONDITION(Component) ->
    Path;
maybe_from_string([?ROOT_NODE]) ->
    [];
maybe_from_string([Component] = Path)
  when ?IS_SPECIAL_PATH_COMPONENT(Component) ->
    Path;
maybe_from_string([] = Path) ->
    Path;
maybe_from_string([Char | _] = Path)
  when is_integer(Char) andalso
       Char >= 0 andalso Char =< 16#10ffff andalso
       not ?IS_SPECIAL_PATH_COMPONENT(Char) ->
    from_string(Path);
maybe_from_string([Char1, Char2 | _] = Path)
  when ?IS_SPECIAL_PATH_COMPONENT(Char1) ->
    if
        ?IS_NODE_ID(Char2) orelse ?IS_CONDITION(Char2) -> Path;
        true                                           -> from_string(Path)
    end.

-spec to_string(path()) -> string().

to_string([?ROOT_NODE | Path]) ->
    "/" ++
    string:join(
      lists:map(fun component_to_string/1, Path),
      "/");
to_string([?THIS_NODE = Component]) ->
    component_to_string(Component);
to_string([?THIS_NODE | Path]) ->
    string:join(
      lists:map(fun component_to_string/1, Path),
      "/");
to_string([?PARENT_NODE | _] = Path) ->
    string:join(
      lists:map(fun component_to_string/1, Path),
      "/");
to_string(Path) ->
    "/" ++
    string:join(
      lists:map(fun component_to_string/1, Path),
      "/").

-spec component_to_string(component()) -> string().

component_to_string(?ROOT_NODE) ->
    "/";
component_to_string(?THIS_NODE) ->
    ".";
component_to_string(?PARENT_NODE) ->
    "..";
component_to_string(Component) when is_atom(Component) ->
    atom_to_list(Component);
component_to_string(Component) when is_binary(Component) ->
    lists:flatten(
      io_lib:format("<<~s>>", [Component])).

-spec combine_with_conditions(pattern(), [khepri_condition:condition()]) ->
    pattern().

combine_with_conditions(Path, []) ->
    Path;
combine_with_conditions(Path, Conditions) ->
    [ChildName | Rest] = lists:reverse(Path),
    Combined = #if_all{conditions = [ChildName | Conditions]},
    lists:reverse([Combined | Rest]).

-spec targets_specific_node(pattern()) ->
    {true, path()} | false.

targets_specific_node(PathPattern) ->
    targets_specific_node(PathPattern, []).

targets_specific_node([Condition | Rest], Path) ->
    case component_targets_specific_node(Condition) of
        {true, Component} -> targets_specific_node(Rest, [Component | Path]);
        false             -> false
    end;
targets_specific_node([], Path) ->
    {true, lists:reverse(Path)}.

-spec component_targets_specific_node(pattern_component()) ->
    {true, component()} | false.
%% @private

component_targets_specific_node(ChildName)
  when ?IS_PATH_COMPONENT(ChildName) ->
    {true, ChildName};
component_targets_specific_node(#if_not{condition = Cond}) ->
    component_targets_specific_node(Cond);
component_targets_specific_node(#if_all{conditions = []}) ->
    false;
component_targets_specific_node(#if_all{conditions = Conds}) ->
    lists:foldl(
      fun
          (Cond, {true, _} = True) ->
              case component_targets_specific_node(Cond) of
                  True      -> True;
                  {true, _} -> false;
                  false     -> True
              end;
          (Cond, false) ->
              case component_targets_specific_node(Cond) of
                  {true, _} = True -> True;
                  false            -> false
              end;
          (Cond, undefined) ->
              component_targets_specific_node(Cond)
      end, undefined, Conds);
component_targets_specific_node(#if_any{conditions = []}) ->
    false;
component_targets_specific_node(#if_any{conditions = Conds}) ->
    lists:foldl(
      fun
          (Cond, {true, _} = True) ->
              case component_targets_specific_node(Cond) of
                  True      -> True;
                  {true, _} -> false;
                  false     -> false
              end;
          (_, false) ->
              false;
          (Cond, undefined) ->
              component_targets_specific_node(Cond)
      end, undefined, Conds);
component_targets_specific_node(_) ->
    false.

-spec is_valid(PathPattern) -> IsValid when
      PathPattern :: pattern(),
      IsValid :: true | {false, pattern_component()}.

is_valid(PathPattern) when is_list(PathPattern) ->
    lists:foldl(
      fun
          (_, {false, _} = False) -> False;
          (Component, _)          -> khepri_condition:is_valid(Component)
      end, true, PathPattern);
is_valid(NotPathPattern) ->
    {false, NotPathPattern}.

-spec ensure_is_valid(PathPattern) -> ok | no_return() when
      PathPattern :: pattern().

ensure_is_valid(PathPattern) ->
    case is_valid(PathPattern) of
        true               -> ok;
        {false, Component} -> throw({invalid_path, #{path => PathPattern,
                                                     component => Component}})
    end.

-spec abspath(pattern(), pattern()) -> pattern().

abspath([FirstComponent | _] = AbsolutePath, _)
  when FirstComponent =/= ?THIS_NODE andalso FirstComponent =/= ?PARENT_NODE ->
    AbsolutePath;
abspath([_ | _] = RelativePath, BasePath) ->
    realpath(BasePath ++ RelativePath, []);
abspath([] = PathToRoot, _) ->
    PathToRoot.

-spec realpath(pattern()) -> pattern().

realpath(Path) ->
    realpath(Path, []).

realpath([?ROOT_NODE | Rest], _Result) ->
    realpath(Rest, []);
realpath([?THIS_NODE | Rest], Result) ->
    realpath(Rest, Result);
realpath([?PARENT_NODE | Rest], [_ | Result]) ->
    realpath(Rest, Result);
realpath([?PARENT_NODE | Rest], [] = Result) ->
    realpath(Rest, Result);
realpath([Component | Rest], Result) ->
    realpath(Rest, [Component | Result]);
realpath([], Result) ->
    lists:reverse(Result).

pattern_includes_root_node(Path) ->
    pattern_includes_root_node1(realpath(Path)).

pattern_includes_root_node1([#if_name_matches{regex = any}]) -> true;
pattern_includes_root_node1([#if_path_matches{regex = any}]) -> true;
pattern_includes_root_node1(_)                               -> false.
