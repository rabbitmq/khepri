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

-export_type([tree_node/1,
              tree/1]).

-export([empty/0,
         is_empty/1,
         update/3,
         fold/5,
         foreach/2,
         compile/1,
         filtermap/2]).

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
    Acc1 = fold_data(PatternTree, [], FoldFun, Acc),
    Root = Tree#tree.root,
    fold1(PatternTree, Root, Path, FoldFun, Acc1, []).

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

fold1(_PatternTree, _Node, [], _FoldFun, Acc, _ReversedPath) ->
    Acc;
fold1(PatternTree, Parent, [Component | Rest], FoldFun, Acc0, ReversedPath) ->
    case Parent of
        #node{child_nodes = #{Component := Node}} ->
            ReversedPath1 = [Component | ReversedPath],
            CurrentPath = lists:reverse(ReversedPath1),
            maps:fold(
              fun(Condition, PatternSubtree, Acc) ->
                      case khepri_condition:is_met(Condition, Component, Node) of
                          true ->
                              Acc1 = fold_data(
                                       PatternSubtree, CurrentPath,
                                       FoldFun, Acc),
                              Acc2 = fold1(
                                       PatternSubtree, Node, Rest,
                                       FoldFun, Acc1, ReversedPath1),
                              AppliesToGrandchildren =
                              khepri_condition:applies_to_grandchildren(
                                Condition),
                              case AppliesToGrandchildren of
                                  true ->
                                      fold1(
                                        PatternTree, Node, Rest,
                                        FoldFun, Acc2, ReversedPath1);
                                  false ->
                                      Acc2
                              end;
                          {false, _} ->
                              Acc
                      end
              end, Acc0, PatternTree#pattern_node.child_nodes);
        _ChildNotFound ->
            Acc0
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
