%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(khepri_prefix_tree).

%% This module defines a tree that associates paths with optional payloads and
%% specializes in lookup of tree nodes by a prefixing path.
%%
%% This tree is similar to {@link khepri_pattern_tree} but the path components
%% in this tree must be {@link khepri_path:node_id()}s rather than pattern
%% components.
%%
%% This tree is also similar to the main tree type {@link khepri_tree} but it
%% is simpler: it does not support keep-while conditions or properties for
%% tree nodes. This type is used within {@link khepri_tree} for the reverse
%% index of keep-while conditions.
%%
%% See https://en.wikipedia.org/wiki/Trie.

-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_payload.hrl").

-type child_nodes(Payload) :: #{khepri_path:node_id() => tree(Payload)}.

-record(prefix_tree, {payload = ?NO_PAYLOAD,
                      child_nodes = #{}}).

-opaque tree(Payload) :: #prefix_tree{payload :: Payload | ?NO_PAYLOAD,
                                      child_nodes :: child_nodes(Payload)}.

-export_type([tree/1]).

-export([empty/0,
         is_prefix_tree/1,
         from_map/1,
         fold_prefixes_of/4,
         find_path/2,
         update/3]).

-spec empty() -> tree(_).
%% @doc Returns a new empty tree.
%%
%% @see tree().

empty() ->
    #prefix_tree{}.

-spec is_prefix_tree(tree(_)) -> true;
                    (term()) -> false.
%% @doc Determines whether the given term is a prefix tree.

is_prefix_tree(#prefix_tree{}) ->
    true;
is_prefix_tree(_) ->
    false.

-spec from_map(Map) -> Tree when
      Map :: #{khepri_path:native_path() => Payload},
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term().
%% @doc Converts a map of paths to payloads into a prefix tree.

from_map(Map) when is_map(Map) ->
    maps:fold(
      fun(Path, Payload, Tree) ->
              update(
                fun(Payload0) ->
                        %% Map keys are unique so this node in the prefix
                        %% tree must have no payload.
                        ?assertEqual(?NO_PAYLOAD, Payload0),
                        Payload
                end, Path, Tree)
      end, empty(), Map).

-spec fold_prefixes_of(Fun, Acc, Path, Tree) -> Ret when
      Fun :: fun((Payload, Acc) -> Acc1),
      Acc :: term(),
      Acc1 :: term(),
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: Acc1.
%% @doc Folds over all nodes in the tree which are prefixed by the given `Path'
%% building an accumulated value with the given fold function and initial
%% accumulator.

fold_prefixes_of(Fun, Acc, Path, Tree) when is_function(Fun, 2) ->
    fold_prefixes_of1(Fun, Acc, Path, Tree).

fold_prefixes_of1(
  Fun, Acc, [], #prefix_tree{payload = Payload, child_nodes = ChildNodes}) ->
    Acc1 = case Payload of
               ?NO_PAYLOAD ->
                   Acc;
               _ ->
                   Fun(Payload, Acc)
           end,
    maps:fold(
      fun(_Component, Subtree, Acc2) ->
              fold_prefixes_of1(Fun, Acc2, [], Subtree)
      end, Acc1, ChildNodes);
fold_prefixes_of1(
  Fun, Acc, [Component | Rest], #prefix_tree{child_nodes = ChildNodes}) ->
    case maps:find(Component, ChildNodes) of
        {ok, Subtree} ->
            fold_prefixes_of1(Fun, Acc, Rest, Subtree);
        error ->
            Acc
    end.

-spec find_path(Path, Tree) -> Ret when
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: {ok, Payload} | error.
%% @doc Returns the payload associated with a path in the tree.
%%
%% @returns `{ok, Payload}' where `Payload' is associated with the given path
%% or `error' if the path is not associated with a payload in the given tree.

find_path(Path, Tree) ->
    find_path1(Path, Tree).

find_path1([], #prefix_tree{payload = Payload}) ->
    case Payload of
        ?NO_PAYLOAD ->
            error;
        _ ->
            {ok, Payload}
    end;
find_path1([Component | Rest], #prefix_tree{child_nodes = ChildNodes}) ->
    case maps:find(Component, ChildNodes) of
        {ok, Subtree} ->
            find_path1(Rest, Subtree);
        error ->
            error
    end.

-spec update(Fun, Path, Tree) -> Ret when
      Fun :: fun((Payload | ?NO_PAYLOAD) -> Payload | ?NO_PAYLOAD),
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: khepri_prefix_tree:tree(Payload).
%% @doc Updates a given path in the tree.
%%
%% This function can be used to create, update or delete tree nodes. If the
%% tree node does not exist for the given path, the update function is passed
%% `?NO_PAYLOAD'. If the update function returns `?NO_PAYLOAD' then the tree
%% node and all of its ancestors which do not have a payload or children are
%% removed.
%%
%% The update function is also be passed `?NO_PAYLOAD' if a tree node exists
%% but does not have a payload: being passed `?NO_PAYLOAD' is not a reliable
%% sign that a tree node did not exist prior to an update.

update(Fun, Path, Tree) ->
    update1(Fun, Path, Tree).

update1(Fun, [], #prefix_tree{payload = Payload} = Tree) ->
    Tree#prefix_tree{payload = Fun(Payload)};
update1(
  Fun, [Component | Rest], #prefix_tree{child_nodes = ChildNodes} = Tree) ->
    Subtree = maps:get(Component, ChildNodes, khepri_prefix_tree:empty()),
    ChildNodes1 = case update1(Fun, Rest, Subtree) of
                      #prefix_tree{payload = ?NO_PAYLOAD, child_nodes = C}
                        when C =:= #{} ->
                          %% Drop unused branches.
                          maps:remove(Component, ChildNodes);
                      Subtree1 ->
                          maps:put(Component, Subtree1, ChildNodes)
                  end,
    Tree#prefix_tree{child_nodes = ChildNodes1}.
