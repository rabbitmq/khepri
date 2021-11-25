%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-include("include/khepri.hrl").

-export([compile/1,
         from_string/1,
         component_from_string/1,
         maybe_from_string/1,
         to_string/1,
         component_to_string/1,
         combine_with_conditions/2,
         targets_specific_node/1,
         component_targets_specific_node/1,
         is_valid/1,
         ensure_is_valid/1,
         abspath/2,
         realpath/1,
         pattern_includes_root_node/1]).

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

-spec from_string(string()) -> pattern().

from_string("") ->
    [];
from_string("/" ++ PathString) ->
    from_string(PathString, []);
from_string("./" ++ PathString) ->
    from_string(PathString, [?THIS_NODE]);
from_string(".") ->
    [?THIS_NODE];
from_string("../" ++ PathString) ->
    from_string(PathString, [?PARENT_NODE]);
from_string("..") ->
    [?PARENT_NODE];
from_string(PathString) ->
    from_string(PathString, [?THIS_NODE]).

from_string(PathString, ParentPath) ->
    ReOpts = [{capture, all_but_first, list}, dotall],
    case re:run(PathString, "^((?U)<<.*>>)(?:/(.*)|$)", ReOpts) of
        {match, [ComponentString, Rest]} ->
            Component = component_from_string(ComponentString),
            from_string(Rest, [Component | ParentPath]);
        {match, [ComponentString]} ->
            Component = component_from_string(ComponentString),
            lists:reverse([Component | ParentPath]);
        nomatch ->
            case re:run(PathString, "^([^/]*)(?:/(.*)|$)", ReOpts) of
                {match, ["", Rest]} ->
                    from_string(Rest, ParentPath);
                {match, [ComponentString, Rest]} ->
                    Component = component_from_string(ComponentString),
                    from_string(Rest, [Component | ParentPath]);
                {match, [""]} ->
                    lists:reverse(ParentPath);
                {match, [ComponentString]} ->
                    Component = component_from_string(ComponentString),
                    lists:reverse([Component | ParentPath])
            end
    end.

-spec component_from_string(string()) -> pattern_component().

component_from_string("/") ->
    ?ROOT_NODE;
component_from_string(".") ->
    ?THIS_NODE;
component_from_string("..") ->
    ?PARENT_NODE;
component_from_string("*") ->
    ?STAR;
component_from_string("**") ->
    ?STAR_STAR;
component_from_string(Component) ->
    ReOpts1 = [{capture, all_but_first, list}, dotall],
    Component1 = case re:run(Component, "^<<(.*)>>$", ReOpts1) of
                     {match, [C]} -> C;
                     nomatch      -> Component
                 end,
    ReOpts2 = [{capture, none}],
    case re:run(Component1, "\\*", ReOpts2) of
        match ->
            ReOpts3 = [global, {return, list}],
            Regex = re:replace(Component1, "\\*", ".*", ReOpts3),
            #if_name_matches{regex = "^" ++ Regex ++ "$"};
        nomatch when Component1 =:= Component ->
            list_to_atom(Component);
        nomatch ->
            list_to_binary(Component1)
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
        true          -> ok;
        {false, Path} -> throw({invalid_path, Path})
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
