%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri tree data structure where each tree node is a pattern component.
%%
%% The tree structure used to store data in the Khepri store (see {@link
%% khepri_tree}) uses path components for branches (see {@link
%% khepri_path:component()}).
%%
%% The tree structure in {@link khepri_tree} can find many matching paths given
%% a pattern while this tree structure can find many matching patterns given
%% a path.
%%
%% @hidden

-module(khepri_pattern_tree).

-include("src/khepri_tree.hrl").

-record(pattern_node, {child_nodes = #{},
                       payload = ?NO_PAYLOAD}).

-opaque tree_node(Payload) ::
        #pattern_node{child_nodes :: #{khepri_path:pattern_component() =>
                                       tree_node(Payload)},
                      payload :: Payload | ?NO_PAYLOAD}.
%% A node in the tree structure.

-opaque tree(Payload) :: tree_node(Payload).

-type payload() :: any().

-type fold_acc() :: term().
-type fold_fun(Payload) :: fun((khepri_path:native_pattern(),
                                Payload,
                                fold_acc()) -> fold_acc()).

-type update_fun(Payload) :: fun((Payload) -> Payload).

-type find_fun(Payload) :: fun((Payload) -> boolean()).

-export_type([tree_node/1,
              tree/1]).

-export([empty/0,
         is_empty/1,
         update/3,
         fold/5,
         foreach/2,
         compile/1,
         filtermap/2,
         any/2]).

-spec empty() -> TreeNode when
      TreeNode :: khepri_pattern_tree:tree(Payload),
      Payload :: ?NO_PAYLOAD.
%% @doc Returns a new empty tree node.
%%
%% @see tree().
%% @see tree_node().

empty() ->
    #pattern_node{}.

-spec is_empty(PatternTree) -> IsEmpty when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      IsEmpty :: boolean(),
      Payload :: payload().
%% @doc Checks whether the given pattern tree is empty.
%%
%% A pattern tree node is empty if it contains no payload and has no children.
%%
%% @see empty/0.

is_empty(#pattern_node{payload = ?NO_PAYLOAD, child_nodes = ChildNodes})
  when ChildNodes =:= #{} ->
    true;
is_empty(_PatternTree) ->
    false.

-spec update(PatternTree, PathPattern, UpdateFun) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      PathPattern :: khepri_path:native_pattern(),
      UpdateFun :: update_fun(Payload),
      Ret :: khepri_pattern_tree:tree(Payload),
      Payload :: payload().
%% @doc Updates the node of the given pattern tree with the given update
%% function.
%%
%% If any pattern components of `PathPattern' do not yet exist in the tree,
%% this function will add any necessary branches to the tree using empty tree
%% nodes.
%%
%% If the tree node for the given `PathPattern' does not yet exist, the
%% `?NO_PAYLOAD' constant will be passed to the `UpdateFun'.
%%
%% @see empty/0.

update(PatternTree, [], UpdateFun) ->
    update_payload(PatternTree, UpdateFun);
update(
  #pattern_node{child_nodes = ChildNodes0} = PatternTree,
  [Component | Rest],
  UpdateFun) ->
    PatternSubtree = case ChildNodes0 of
                         #{Component := PatternNode} ->
                             PatternNode;
                         _ ->
                             empty()
                     end,
    PatternSubtree1 = update(PatternSubtree, Rest, UpdateFun),
    ChildNodes = maps:put(Component, PatternSubtree1, ChildNodes0),
    PatternTree#pattern_node{child_nodes = ChildNodes}.

-spec update_payload(PatternTreeNode, UpdateFun) -> Ret when
      PatternTreeNode :: khepri_pattern_tree:tree_node(Payload),
      UpdateFun :: update_fun(Payload),
      Ret :: khepri_pattern_tree:tree_node(Payload),
      Payload :: payload().
%% @doc Updates the payload of the given node with the given update function.
%%
%% @private

update_payload(#pattern_node{payload = Payload} = PatternTree, UpdateFun) ->
    Payload1 = UpdateFun(Payload),
    PatternTree#pattern_node{payload = Payload1}.

-spec fold(PatternTree, Tree, Path, FoldFun, Acc) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      Tree :: khepri_tree:tree(),
      Path :: khepri_path:native_path(),
      FoldFun :: fold_fun(Payload),
      Acc :: fold_acc(),
      Ret :: fold_acc(),
      Payload :: payload().
%% @doc Folds over the given pattern tree to find all patterns in the tree
%% which match the given path.
%%
%% The `FoldFun' function takes a path pattern, the payload stored in the
%% pattern tree at that tree node, and an accumulator as arguments. Only tree
%% nodes which have payloads are passed to this function.
%%
%% @see fold_fun().
%% @see fold_acc().

fold(PatternTree, Tree, Path, FoldFun, Acc) ->
    Path1 = khepri_path:realpath(Path),
    case Path1 of
        [] ->
            fold_data(PatternTree, [], FoldFun, Acc);
        _ ->
            Root = Tree#tree.root,
            fold1(PatternTree, Root, Path, FoldFun, Acc, [])
    end.

-spec fold1(PatternTree, Node, Path, FoldFun, Acc, ReversedPath) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      Node :: khepri_tree:tree_node(),
      Path :: khepri_path:native_path(),
      FoldFun :: fold_fun(Payload),
      Acc :: fold_acc(),
      ReversedPath :: khepri_path:native_path(),
      Ret :: fold_acc(),
      Payload :: payload().
%% @private

fold1(
  _PatternTree, _Node, [], _FoldFun, Acc, _ReversedPath) ->
    Acc;
fold1(
  PatternTree, Parent, [Component | Rest], FoldFun, Acc, ReversedPath) ->
    case Parent of
        #node{child_nodes = #{Component := Node}} ->
            ReversedPath1 = [Component | ReversedPath],
            maps:fold(
              fun(Condition, PatternSubtree, Acc0) ->
                      CondMet = khepri_condition:is_met(
                                  Condition, Component, Node),
                      AppliesToGrandchildren = (
                        khepri_condition:applies_to_grandchildren(
                          Condition)),
                      case CondMet of
                          true when Rest =:= [] ->
                              %% The pattern node matches the whole path
                              %% (there is no component left after). We can
                              %% apply the `FoldFun' and return.
                              CurrentPath = lists:reverse(ReversedPath1),
                              fold_data(
                                PatternSubtree, CurrentPath,
                                FoldFun, Acc0);
                          true when not AppliesToGrandchildren ->
                              %% The pattern node matches the path so far but
                              %% there are still components left after (we are
                              %% not at the end of the path).
                              %%
                              %% We continue with the next component.
                              fold1(
                                PatternSubtree, Node, Rest,
                                FoldFun, Acc0, ReversedPath1);
                          true when AppliesToGrandchildren ->
                              %% Same as above, but because this condition can
                              %% be used on grand children, we two scenarios:
                              %%   1. The condition may still match grand
                              %%      children, so we keep it and continue down
                              %%      the tree. This is `Acc1' below.
                              %%   2. The condition won't match any grand
                              %%      children (but it matched so far). We
                              %%      evaluate the grand children with the
                              %%      pattern sub tree. This is `Acc2' below.
                              %%
                              %% For scenario 1, we prepare a special pattern
                              %% tree with only that condition because we don't
                              %% want to evaluate siblings on grand children.
                              PatternTree1 = PatternTree#pattern_node{
                                               child_nodes =
                                               #{Condition => PatternSubtree}},
                              Acc1 = fold1(
                                       PatternTree1, Node, Rest,
                                       FoldFun, Acc0, ReversedPath1),
                              Acc2 = fold1(
                                       PatternSubtree, Node, Rest,
                                       FoldFun, Acc1, ReversedPath1),
                              Acc2;
                          {false, _} ->
                              Acc0
                      end
              end, Acc, PatternTree#pattern_node.child_nodes);
        _ChildNotFound ->
            Acc
    end.

-spec fold_data(PatternTreeNode, CurrentPath, FoldFun, Acc) -> Ret when
      PatternTreeNode :: khepri_pattern_tree:tree_node(Payload),
      CurrentPath :: khepri_path:native_path(),
      FoldFun :: fold_fun(Payload),
      Acc :: fold_acc(),
      Ret :: fold_acc(),
      Payload :: payload().
%% @doc Calls the given fold function with the given tree node's payload, if
%% the given tree node has a payload.
%%
%% @private

fold_data(PatternTreeNode, CurrentPath, FoldFun, Acc) ->
    case PatternTreeNode of
        #pattern_node{payload = ?NO_PAYLOAD} ->
            Acc;
        #pattern_node{payload = Payload} ->
            FoldFun(CurrentPath, Payload, Acc)
    end.

-spec foreach(PatternTree, Fun) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      Fun :: fun((khepri_path:native_pattern(), Payload) -> any()),
      Payload :: payload(),
      Ret :: ok.
%% @doc Iterates over the path patterns and associated payloads for any
%% patterns in the given pattern tree with payloads.

foreach(PatternTree, Fun) ->
    foreach(PatternTree, Fun, []).

foreach(
  #pattern_node{child_nodes = ChildNodes, payload = Payload},
  Fun, ReversedPathPattern) ->
    CurrentPathPattern = lists:reverse(ReversedPathPattern),
    case Payload of
        ?NO_PAYLOAD ->
            ok;
        _ ->
            _ = Fun(CurrentPathPattern, Payload),
            ok
    end,
    maps:foreach(
      fun(Condition, Child) ->
          ReversedPathPattern1 = [Condition | ReversedPathPattern],
          foreach(Child, Fun, ReversedPathPattern1)
      end, ChildNodes).

-spec compile(PatternTree) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      Ret :: khepri_pattern_tree:tree(Payload),
      Payload :: payload().
%% @doc Compiles conditions in the given pattern tree.
%%
%% Some conditions must be compiled before being checked against paths and tree
%% nodes such as ETS match specifications in the `#if_data_matches{}' condition
%% or regular expressions in the `#if_name_matches{}' condition.
%%
%% The pattern tree must be compiled before running {@link fold/5} against the
%% tree. Compiled trees should not be (de)serialized though since the compiled
%% information depends on the runtime system.
%%
%% @see khepri_condition:compile/1.

compile(#pattern_node{child_nodes = ChildNodes0} = PatternTree) ->
    ChildNodes = maps:fold(
                   fun(Condition0, Child0, Acc) ->
                       Condition = khepri_condition:compile(Condition0),
                       Child = compile(Child0),
                       Acc#{Condition => Child}
                   end, #{}, ChildNodes0),
    PatternTree#pattern_node{child_nodes = ChildNodes}.

-spec filtermap(ProjectionTree, Fun) -> Ret when
      ProjectionTree :: khepri_pattern_tree:tree(Payload),
      Fun :: fun((PathPattern, Payload) -> boolean() | {true, NewPayload}),
      Ret :: khepri_pattern_tree:tree(NewPayload),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: payload(),
      NewPayload :: payload().

%% @doc Filters and optionally replaces values in the tree.
%%
%% The filter-map `Fun' receives each `PathPattern' and `Payload' pair in the
%% tree. If the function returns `false', the pair is removed from the tree.
%% If the function returns `true' the value is kept without changes. If the
%% function returns `{true, NewPayload}', the `PathPattern' is associated with
%% `NewPayload' in the returned tree.
%%
%% @param ProjectionTree the initial tree to update with the filter-map
%%        function.
%% @param Fun the filter-map function to apply to each path-pattern and payload
%%        pair.
%% @returns A new pattern tree with entries filtered by the filtered and
%%          updated by the filter-map function.

filtermap(ProjectionTree, Fun) ->
    filtermap1(ProjectionTree, Fun, []).

filtermap1(
  #pattern_node{child_nodes = ChildNodes0, payload = Payload0},
  Fun, ReversedPathPattern) ->
    Payload = case Payload0 of
                  ?NO_PAYLOAD ->
                      Payload0;
                  _ ->
                      PathPattern = lists:reverse(ReversedPathPattern),
                      case Fun(PathPattern, Payload0) of
                          {true, NewPayload} ->
                              NewPayload;
                          true ->
                              Payload0;
                          false ->
                              ?NO_PAYLOAD
                      end
              end,
    ChildNodes = maps:filtermap(
                   fun(PatternComponent, ChildNode) ->
                           MappedChild = filtermap1(
                                           ChildNode, Fun,
                                           [PatternComponent |
                                            ReversedPathPattern]),
                           case MappedChild of
                               #pattern_node{payload = ?NO_PAYLOAD,
                                             child_nodes = Grandchildren}
                                 when Grandchildren =:= #{} ->
                                   %% Trim any children with no children
                                   %% and no payload
                                   false;
                               _ ->
                                   {true, MappedChild}
                           end
                   end, ChildNodes0),
    #pattern_node{payload = Payload, child_nodes = ChildNodes}.

-spec any(PatternTree, FindFun) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      FindFun :: find_fun(Payload),
      Ret :: payload() | undefined,
      Payload :: payload().
%% @doc Determines whether the pattern tree contains a tree node with a payload
%% that matches the given predicate.
%%
%% @param PatternTree the tree to search.
%% @param FindFun the predicate with which to evaluate tree nodes.
%% @returns `true' if the predicate evaluates as `true' for any payload,
%%          `false' otherwise.

any(#pattern_node{payload = Payload, child_nodes = Children}, FindFun) ->
    (Payload =/= ?NO_PAYLOAD andalso FindFun(Payload)) orelse
        lists:any(
          fun(Child) -> any(Child, FindFun) end, maps:values(Children)).
