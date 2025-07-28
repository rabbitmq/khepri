%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri tree manipulation API.
%%
%% @hidden

-module(khepri_tree).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_node.hrl").

-export([new/0,
         get_root/1,
         get_keep_while_conds/1,
         assert_equal/2,

         are_keep_while_conditions_met/2,

         collect_node_props_cb/3,
         count_node_cb/3,

         find_matching_nodes/3,
         fold/5,
         delete_matching_nodes/4,
         insert_or_update_node/5,
         does_path_match/3,
         walk_down_the_tree/5,

         convert_tree/3]).

-record(tree, {root = #node{} :: khepri_tree:tree_node(),
               keep_while_conds = #{} :: khepri_tree:keep_while_conds_map(),
               keep_while_conds_revidx = #{}}).

-type tree_node() :: #node{}.
%% A node in the tree structure.

-type tree(KeepWhileCondsRevIdxType) :: #tree{keep_while_conds_revidx ::
                                              KeepWhileCondsRevIdxType}.

-opaque tree_v0() :: tree(khepri_tree:keep_while_conds_revidx_v0()).
-opaque tree_v1() :: tree(khepri_tree:keep_while_conds_revidx_v1()).

-type tree() :: tree_v0() | tree_v1().

-type keep_while_conds_map() :: #{khepri_path:native_path() =>
                                  khepri_condition:native_keep_while()}.
%% Per-node `keep_while' conditions.

-opaque keep_while_conds_revidx_v0() :: #{khepri_path:native_path() =>
                                          #{khepri_path:native_path() => ok}}.

-opaque keep_while_conds_revidx_v1() :: khepri_prefix_tree:tree(
                                          #{khepri_path:native_path() => ok}).

-type keep_while_conds_revidx() :: keep_while_conds_revidx_v0() |
                                   keep_while_conds_revidx_v1().
%% Internal reverse index of the keep_while conditions.
%%
%% If node A depends on a condition on node B, then this reverse index will
%% have a "node B => node A" association. The version 0 of this type used a map
%% and folded over the entries in the map using `lists:prefix/2' to find
%% matching conditions. In version 1 this type was replaced with a prefix tree
%% which improves lookup time when the reverse index contains many entries.

-type applied_changes() :: #{khepri_path:native_path() =>
                             {create, khepri:node_props()} |
                             {update, khepri:node_props()} |
                             delete}.
%% Internal index of the per-node changes which happened during a traversal.
%% This is used when the tree is walked back up to determine the list of tree
%% nodes to remove after some keep_while condition evaluates to false.

-type walk_down_the_tree_fun() ::
    fun((khepri_path:native_path(),
         tree_node() | {interrupted, any(), map()},
         Acc :: any()) ->
        ok(tree_node() | keep | delete, any()) |
        khepri:error()).
%% Function called to handle a node found (or an error) and used in {@link
%% walk_down_the_tree/6}.

-type ok(Type1, Type2) :: {ok, Type1, Type2}.
-type ok(Type1, Type2, Type3) :: {ok, Type1, Type2, Type3}.

-export_type([tree_node/0,
              tree_v0/0,
              tree_v1/0,
              tree/0,
              keep_while_conds_map/0,
              keep_while_conds_revidx_v0/0,
              keep_while_conds_revidx_v1/0,
              keep_while_conds_revidx/0,
              applied_changes/0]).

%% -------------------------------------------------------------------
%% Tree node functions.
%% -------------------------------------------------------------------

-spec new() -> tree().

new() ->
    #tree{}.

-spec get_root(Tree) -> Root when
      Tree :: khepri_tree:tree(),
      Root :: khepri_tree:tree_node().

get_root(#tree{root = Root}) ->
    Root.

-spec get_keep_while_conds(Tree) -> KeepWhileConds when
      Tree :: khepri_tree:tree(),
      KeepWhileConds :: khepri_tree:keep_while_conds_map().

get_keep_while_conds(#tree{keep_while_conds = KeepWhileConds}) ->
    KeepWhileConds.

-spec assert_equal(Tree1, Tree2) -> ok when
      Tree1 :: khepri_tree:tree(),
      Tree2 :: khepri_tree:tree().

assert_equal(#tree{} = Tree1, #tree{} = Tree2) ->
    ?assertEqual(Tree1, Tree2),
    ok.

-spec create_node_record(Payload) -> Node when
      Payload :: khepri_payload:payload(),
      Node :: tree_node().
%% @private

create_node_record(Payload) ->
    #node{props = ?INIT_NODE_PROPS,
          payload = Payload}.

-spec set_node_payload(Node, Payload) -> Node when
      Node :: tree_node(),
      Payload :: khepri_payload:payload().
%% @private

set_node_payload(#node{payload = Payload} = Node, Payload) ->
    Node;
set_node_payload(#node{props = #{payload_version := DVersion} = Stat} = Node,
                 Payload) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{props = Stat1, payload = Payload}.

-spec remove_node_payload(Node) -> Node when
      Node :: tree_node().
%% @private

remove_node_payload(
  #node{payload = ?NO_PAYLOAD} = Node) ->
    Node;
remove_node_payload(
  #node{props = #{payload_version := DVersion} = Stat} = Node) ->
    Stat1 = Stat#{payload_version => DVersion + 1},
    Node#node{props = Stat1, payload = khepri_payload:none()}.

-spec add_node_child(Node, ChildName, Child) -> Node when
      Node :: tree_node(),
      Child :: tree_node(),
      ChildName :: khepri_path:component().

add_node_child(#node{props = #{child_list_version := CVersion} = Stat,
                     child_nodes = Children} = Node,
               ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{props = Stat1, child_nodes = Children1}.

-spec update_node_child(Node, ChildName, Child) -> Node when
      Node :: tree_node(),
      Child :: tree_node(),
      ChildName :: khepri_path:component().

update_node_child(#node{child_nodes = Children} = Node, ChildName, Child) ->
    Children1 = Children#{ChildName => Child},
    Node#node{child_nodes = Children1}.

-spec remove_node_child(Node, ChildName) -> Node when
      Node :: tree_node(),
      ChildName :: khepri_path:component().

remove_node_child(#node{props = #{child_list_version := CVersion} = Stat,
                        child_nodes = Children} = Node,
                  ChildName) ->
    ?assert(maps:is_key(ChildName, Children)),
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Children1 = maps:remove(ChildName, Children),
    Node#node{props = Stat1, child_nodes = Children1}.

-spec remove_node_child_nodes(Node) -> Node when
      Node :: tree_node().

remove_node_child_nodes(
  #node{child_nodes = Children} = Node) when Children =:= #{} ->
    Node;
remove_node_child_nodes(
  #node{props = #{child_list_version := CVersion} = Stat} = Node) ->
    Stat1 = Stat#{child_list_version => CVersion + 1},
    Node#node{props = Stat1, child_nodes = #{}}.

-spec gather_node_props(Node, TreeOptions) -> NodeProps when
      Node :: tree_node(),
      TreeOptions :: khepri:tree_options(),
      NodeProps :: khepri:node_props().

gather_node_props(#node{props = #{payload_version := PVersion,
                                  child_list_version := CVersion},
                        payload = Payload,
                        child_nodes = Children},
                  #{props_to_return := WantedProps}) ->
    lists:foldl(
      fun
          (payload_version, Acc) ->
              Acc#{payload_version => PVersion};
          (child_list_version, Acc) ->
              Acc#{child_list_version => CVersion};
          (child_list_length, Acc) ->
              Acc#{child_list_length => maps:size(Children)};
          (child_names, Acc) ->
              Acc#{child_names => maps:keys(Children)};
          (payload, Acc) ->
              case Payload of
                  #p_data{data = Data}  -> Acc#{data => Data};
                  #p_sproc{sproc = Fun} -> Acc#{sproc => Fun};
                  _                     -> Acc
              end;
          (has_payload, Acc) ->
              case Payload of
                  #p_data{data = _}   -> Acc#{has_data => true};
                  #p_sproc{sproc = _} -> Acc#{is_sproc => true};
                  _                   -> Acc
              end;
          (raw_payload, Acc) ->
              Acc#{raw_payload => Payload};
          (_Unknown, Acc) ->
              %% We ignore props we don't know about. It might be a new one in
              %% a future version of the machine.
              Acc
      end, #{}, WantedProps);
gather_node_props(#node{}, _Options) ->
    #{}.

gather_node_props_for_error(Node) ->
    gather_node_props(Node, #{props_to_return => ?DEFAULT_PROPS_TO_RETURN}).

-spec reset_versions(Node) -> Node when
      Node :: tree_node().
%% @private

reset_versions(#node{props = Stat} = CurrentNode) ->
    Stat1 = Stat#{payload_version => ?INIT_DATA_VERSION,
                  child_list_version => ?INIT_CHILD_LIST_VERSION},
    CurrentNode#node{props = Stat1}.

-spec squash_version_bumps(OldNode, NewNode) -> Node when
      OldNode :: tree_node(),
      NewNode :: tree_node(),
      Node :: tree_node().
%% @private

squash_version_bumps(
  #node{props = #{payload_version := DVersion,
                  child_list_version := CVersion}},
  #node{props = #{payload_version := DVersion,
                  child_list_version := CVersion}} = CurrentNode) ->
    CurrentNode;
squash_version_bumps(
  #node{props = #{payload_version := OldDVersion,
                  child_list_version := OldCVersion}},
  #node{props = #{payload_version := NewDVersion,
                  child_list_version := NewCVersion} = Stat} = CurrentNode) ->
    DVersion = case NewDVersion > OldDVersion of
                   true  -> OldDVersion + 1;
                   false -> OldDVersion
               end,
    CVersion = case NewCVersion > OldCVersion of
                   true  -> OldCVersion + 1;
                   false -> OldCVersion
               end,
    Stat1 = Stat#{payload_version => DVersion,
                  child_list_version => CVersion},
    CurrentNode#node{props = Stat1}.

squash_version_bumps_after_keep_while(
  Path,
  #node{props = #{child_list_version := CVersion} = Props} = Node,
  AppliedChanges) ->
    ChildPathLength = length(Path) + 1,
    WasModified = maps:fold(
                    fun
                        (P, {create, _NP}, false) ->
                            lists:prefix(Path, P) andalso
                            length(P) =:= ChildPathLength;
                        (_P, _TaNP, Acc) ->
                            Acc
                    end, false, AppliedChanges),
    case WasModified of
        false ->
            Node;
        true ->
            CVersion1 = CVersion - 1,
            ?assert(CVersion1 >= 1),
            Props1 = Props#{child_list_version => CVersion1},
            Node#node{props = Props1}
    end.

%% -------------------------------------------------------------------
%% Keep-while functions.
%% -------------------------------------------------------------------

-spec to_absolute_keep_while(BasePath, KeepWhile) -> AbsKeepWhile when
      BasePath :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while(),
      AbsKeepWhile :: khepri_condition:native_keep_while().
%% @private

to_absolute_keep_while(BasePath, KeepWhile) ->
    maps:fold(
      fun(Path, Cond, Acc) ->
              AbsPath = khepri_path:abspath(Path, BasePath),
              Acc#{AbsPath => Cond}
      end, #{}, KeepWhile).

-spec are_keep_while_conditions_met(Tree, KeepWhile) -> Ret when
      Tree :: tree(),
      KeepWhile :: khepri_condition:native_keep_while(),
      Ret :: true | {false, any()}.
%% @private

are_keep_while_conditions_met(_, KeepWhile)
  when KeepWhile =:= #{} ->
    true;
are_keep_while_conditions_met(Tree, KeepWhile) ->
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    maps:fold(
      fun
          (Path, Condition, true) ->
              case find_matching_nodes(Tree, Path, TreeOptions) of
                  {ok, Result} when Result =/= #{} ->
                      are_keep_while_conditions_met1(Result, Condition);
                  {ok, Result}
                    when Result =:= #{} andalso
                         Condition =:= #if_node_exists{exists = false} ->
                      %% The path pattern of the `keep_while' condition
                      %% matched no tree nodes. The condition indicates the
                      %% target node/pattern should not exist, therefore the
                      %% condition is met.
                      true;
                  {ok, _} ->
                      {false, {pattern_matches_no_nodes, Path}};
                  {error, Reason} ->
                      {false, Reason}
              end;
          (_, _, False) ->
              False
      end, true, KeepWhile).

are_keep_while_conditions_met1(Result, Condition) ->
    maps:fold(
      fun
          (Path, NodeProps, true) ->
              khepri_condition:is_met(Condition, Path, NodeProps);
          (_, _, False) ->
              False
      end, true, Result).

is_keep_while_condition_met_on_self(
  #tree{keep_while_conds = KeepWhileConds}, Path, Node) ->
    case KeepWhileConds of
        #{Path := #{Path := Condition}} ->
            khepri_condition:is_met(Condition, Path, Node);
        _ ->
            true
    end.

-spec update_keep_while_conds(Tree, Watcher, KeepWhile) -> NewTree when
      Tree :: khepri_tree:tree(),
      Watcher :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while(),
      NewTree :: khepri_tree:tree().

update_keep_while_conds(Tree, Watcher, KeepWhile) ->
    AbsKeepWhile = to_absolute_keep_while(Watcher, KeepWhile),
    Tree1 = update_keep_while_conds_revidx(Tree, Watcher, AbsKeepWhile),
    KeepWhileConds = get_keep_while_conds(Tree1),
    KeepWhileConds1 = KeepWhileConds#{Watcher => AbsKeepWhile},
    Tree1#tree{keep_while_conds = KeepWhileConds1}.

-spec update_keep_while_conds_revidx(Tree, Watcher, KeepWhile) -> NewTree when
      Tree :: tree(),
      Watcher :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while(),
      NewTree :: tree().

update_keep_while_conds_revidx(
  #tree{keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  Watcher, KeepWhile) ->
    case is_v1_keep_while_conds_revidx(KeepWhileCondsRevIdx) of
        true ->
            update_keep_while_conds_revidx_v1(Tree, Watcher, KeepWhile);
        false ->
            update_keep_while_conds_revidx_v0(Tree, Watcher, KeepWhile)
    end.

is_v1_keep_while_conds_revidx(KeepWhileCondsRevIdx) ->
    khepri_prefix_tree:is_prefix_tree(KeepWhileCondsRevIdx).

update_keep_while_conds_revidx_v0(
  #tree{keep_while_conds = KeepWhileConds,
        keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  Watcher, KeepWhile) ->
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_while.
    OldWatcheds = maps:get(Watcher, KeepWhileConds, #{}),
    KeepWhileCondsRevIdx1 = maps:fold(
                          fun(Watched, _, KWRevIdx) ->
                                  Watchers = maps:get(Watched, KWRevIdx),
                                  Watchers1 = maps:remove(Watcher, Watchers),
                                  case maps:size(Watchers1) of
                                      0 -> maps:remove(Watched, KWRevIdx);
                                      _ -> KWRevIdx#{Watched => Watchers1}
                                  end
                          end, KeepWhileCondsRevIdx, OldWatcheds),
    %% Then, record the watched paths.
    KeepWhileCondsRevIdx2 = maps:fold(
                          fun(Watched, _, KWRevIdx) ->
                                  Watchers = maps:get(Watched, KWRevIdx, #{}),
                                  Watchers1 = Watchers#{Watcher => ok},
                                  KWRevIdx#{Watched => Watchers1}
                          end, KeepWhileCondsRevIdx1, KeepWhile),
    Tree#tree{keep_while_conds_revidx = KeepWhileCondsRevIdx2}.

update_keep_while_conds_revidx_v1(
  #tree{keep_while_conds = KeepWhileConds,
        keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  Watcher, KeepWhile) ->
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_while.
    OldWatcheds = maps:get(Watcher, KeepWhileConds, #{}),
    KeepWhileCondsRevIdx1 = maps:fold(
                          fun(Watched, _, KWRevIdx) ->
                                  khepri_prefix_tree:update(
                                    fun(Watchers) ->
                                            Watchers1 = maps:remove(
                                                          Watcher, Watchers),
                                            case maps:size(Watchers1) of
                                                0 -> ?NO_PAYLOAD;
                                                _ -> Watchers1
                                            end
                                    end, Watched, KWRevIdx)
                          end, KeepWhileCondsRevIdx, OldWatcheds),
    %% Then, record the watched paths.
    KeepWhileCondsRevIdx2 = maps:fold(
                          fun(Watched, _, KWRevIdx) ->
                                  khepri_prefix_tree:update(
                                    fun (?NO_PAYLOAD) ->
                                            #{Watcher => ok};
                                        (Watchers) ->
                                            Watchers#{Watcher => ok}
                                    end, Watched, KWRevIdx)
                          end, KeepWhileCondsRevIdx1, KeepWhile),
    Tree#tree{keep_while_conds_revidx = KeepWhileCondsRevIdx2}.

%% -------------------------------------------------------------------
%% Find matching nodes.
%% -------------------------------------------------------------------

-spec find_matching_nodes(Tree, PathPattern, TreeOptions) ->
    Ret when
      Tree :: tree(),
      PathPattern :: khepri_path:native_pattern(),
      TreeOptions :: khepri:tree_options(),
      Ret :: khepri_machine:write_ret().
%% @private

find_matching_nodes(Tree, PathPattern, TreeOptions) ->
    fold(
      Tree, PathPattern,
      fun collect_node_props_cb/3, #{},
      TreeOptions).

-spec collect_node_props_cb(Path, NodeProps, Map) ->
    Ret when
      Path :: khepri_path:native_path(),
      NodeProps :: khepri:node_props(),
      Map :: khepri:node_props_map(),
      Ret :: Map.
%% @private

collect_node_props_cb(Path, NodeProps, Map) when is_map(Map) ->
    Map#{Path => NodeProps}.

-spec count_node_cb(Path, NodeProps, Count) ->
    Ret when
      Path :: khepri_path:native_path(),
      NodeProps :: khepri:node_props(),
      Count :: non_neg_integer(),
      Ret :: Count.
%% @private

count_node_cb(_Path, _NodeProps, Count) when is_integer(Count) ->
    Count + 1.

-spec fold(Tree, PathPattern, Fun, Acc, TreeOptions) ->
    Ret when
      Tree :: tree(),
      PathPattern :: khepri_path:native_pattern(),
      Fun :: khepri:fold_fun(),
      Acc :: khepri:fold_acc(),
      TreeOptions :: khepri:tree_options(),
      Ret :: khepri:ok(Acc) | khepri:error().
%% @doc Folds the given `Fun' over each tree node which matches the path
%% pattern.
%%
%% @private

fold(Tree, PathPattern, Fun, Acc, TreeOptions) ->
    WalkFun = fun(Path, Node, Acc1) ->
                      find_matching_nodes_cb(
                        Path, Node, Fun, Acc1, TreeOptions)
              end,
    Ret = walk_down_the_tree(
            Tree, PathPattern, TreeOptions, WalkFun, Acc),
    case Ret of
        {ok, NewTree, _AppliedChanges, Acc2} ->
            ?assertEqual(Tree, NewTree),
            {ok, Acc2};
        Error ->
            Error
    end.

find_matching_nodes_cb(Path, #node{} = Node, Fun, Acc, TreeOptions) ->
    NodeProps = gather_node_props(Node, TreeOptions),
    Acc1 = Fun(Path, NodeProps, Acc),
    {ok, keep, Acc1};
find_matching_nodes_cb(
  _,
  {interrupted, node_not_found = Reason, Info},
  _Fun, _Acc,
  #{expect_specific_node := true}) ->
    %% If we are collecting node properties (the result is a map) and the path
    %% targets a specific node which is not found, we return an error.
    %%
    %% If we are counting nodes, that's fine and the next function clause will
    %% run. The walk won't be interrupted.
    Reason1 = ?khepri_error(Reason, Info),
    {error, Reason1};
find_matching_nodes_cb(_, {interrupted, _, _}, _, Acc, _) ->
    {ok, keep, Acc}.

%% -------------------------------------------------------------------
%% Delete matching nodes.
%% -------------------------------------------------------------------

-spec delete_matching_nodes(Tree, PathPattern, AppliedChanges, TreeOptions) ->
    Ret when
      Tree :: khepri_tree:tree(),
      PathPattern :: khepri_path:pattern(),
      AppliedChanges :: applied_changes(),
      TreeOptions :: khepri:tree_options(),
      Ret :: {ok, NewTree, NewAppliedChanges, Result} | khepri:error(),
      NewTree :: khepri_tree:tree(),
      NewAppliedChanges :: applied_changes(),
      Result :: any().

delete_matching_nodes(Tree, PathPattern, AppliedChanges, TreeOptions) ->
    Fun = fun(Path, Node, Result) ->
                  delete_matching_nodes_cb(
                    Path, Node, TreeOptions, explicit, Result)
          end,
    walk_down_the_tree(
      Tree, PathPattern, TreeOptions, AppliedChanges, Fun, #{}).

delete_matching_nodes_cb(
  [] = Path, #node{} = Node, TreeOptions, DeleteReason, Result) ->
    Result1 = add_deleted_node_to_result(
                Path, Node, TreeOptions, DeleteReason, Result),
    Node1 = remove_node_payload(Node),
    Node2 = remove_node_child_nodes(Node1),
    {ok, Node2, Result1};
delete_matching_nodes_cb(
  Path, #node{} = Node, TreeOptions, DeleteReason, Result) ->
    Result1 = add_deleted_node_to_result(
                Path, Node, TreeOptions, DeleteReason, Result),
    {ok, delete, Result1};
delete_matching_nodes_cb(
  _, {interrupted, _, _}, _Options, _DeleteReason, Result) ->
    {ok, keep, Result}.

add_deleted_node_to_result(
  Path, Node, TreeOptions, explicit = DeleteReason, Result) ->
    add_deleted_node_to_result1(
      Path, Node, TreeOptions, DeleteReason, Result);
add_deleted_node_to_result(
  _Path, _Node, #{return_indirect_deletes := false}, _DeleteReason,
  Result) ->
    Result;
add_deleted_node_to_result(
  Path, Node, TreeOptions, DeleteReason, Result) ->
    add_deleted_node_to_result1(
      Path, Node, TreeOptions, DeleteReason, Result).

add_deleted_node_to_result1(Path, Node, TreeOptions, DeleteReason, Result) ->
    NodeProps1 = gather_node_props(Node, TreeOptions),
    NodeProps2 = maybe_add_delete_reason_prop(
                   NodeProps1, TreeOptions, DeleteReason),
    Result#{Path => NodeProps2}.

maybe_add_delete_reason_prop(
  NodeProps, #{props_to_return := WantedProps}, DeleteReason) ->
    case lists:member(delete_reason, WantedProps) of
        true ->
            NodeProps#{delete_reason => DeleteReason};
        false ->
            NodeProps
    end;
maybe_add_delete_reason_prop(NodeProps, _TreeOptions, _DeleteReason) ->
    NodeProps.

%% -------------------------------------------------------------------
%% Insert or update a tree node.
%% -------------------------------------------------------------------

-spec insert_or_update_node(
    Tree, PathPattern, Payload, PutOptions, TreeOptions) -> Ret when
      Tree :: khepri_tree:tree(),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: khepri_payload:payload(),
      PutOptions :: khepri:put_options(),
      TreeOptions :: khepri:tree_options(),
      NodeProps :: khepri:node_props_map(),
      AppliedChanges :: applied_changes(),
      Ret :: ok(Tree, AppliedChanges, NodeProps) | khepri:error().

insert_or_update_node(
  Tree, PathPattern, Payload, #{keep_while := KeepWhile}, TreeOptions) ->
    Fun = fun(Path, Node, {_, _, Result}) ->
                  Ret = insert_or_update_node_cb(
                          Path, Node, Payload, TreeOptions, Result),
                  case Ret of
                      {ok, Node1, Result1} when Result1 =/= #{} ->
                          AbsKeepWhile = to_absolute_keep_while(
                                           Path, KeepWhile),
                          KeepWhileOnOthers = maps:remove(Path, AbsKeepWhile),
                          KWMet = are_keep_while_conditions_met(
                                    Tree, KeepWhileOnOthers),
                          case KWMet of
                              true ->
                                  {ok, Node1, {updated, Path, Result1}};
                              {false, Reason} ->
                                  %% The keep_while condition is not met. We
                                  %% can't insert the node and return an
                                  %% error instead.
                                  NodeName = case Path of
                                                 [] -> ?KHEPRI_ROOT_NODE;
                                                 _  -> lists:last(Path)
                                             end,
                                  Reason1 = ?khepri_error(
                                               keep_while_conditions_not_met,
                                               #{node_name => NodeName,
                                                 node_path => Path,
                                                 keep_while_reason => Reason}),
                                  {error, Reason1}
                          end;
                      {ok, Node1, Result1} ->
                          {ok, Node1, {updated, Path, Result1}};
                      Error ->
                          Error
                  end
          end,
    Ret1 = walk_down_the_tree(
             Tree, PathPattern, TreeOptions, Fun, {undefined, [], #{}}),
    case Ret1 of
        {ok, Tree1, AppliedChanges, {updated, ResolvedPath, Ret2}} ->
            Tree2 = update_keep_while_conds(
                      Tree1, ResolvedPath, KeepWhile),
            {ok, Tree2, AppliedChanges, Ret2};
        Error ->
            ?assertMatch({error, _}, Error),
            Error
    end;
insert_or_update_node(
  Tree, PathPattern, Payload, _PutOptions, TreeOptions) ->
    Fun = fun(Path, Node, Result) ->
                  insert_or_update_node_cb(
                    Path, Node, Payload, TreeOptions, Result)
          end,
    walk_down_the_tree(Tree, PathPattern, TreeOptions, Fun, #{}).

insert_or_update_node_cb(
  Path, #node{} = Node, Payload, TreeOptions, Result) ->
    case maps:is_key(Path, Result) of
        false ->
            %% After a node is modified, we collect properties from the updated
            %% `#node{}', except the payload which is from the old one.
            Node1 = set_node_payload(Node, Payload),
            NodeProps = gather_node_props_from_old_and_new_nodes(
                          Node, Node1, TreeOptions),
            {ok, Node1, Result#{Path => NodeProps}};
        true ->
            {ok, Node, Result}
    end;
insert_or_update_node_cb(
  Path, {interrupted, node_not_found = Reason, Info}, Payload, TreeOptions,
  Result) ->
    %% We store the payload when we reached the target node only, not in the
    %% parent nodes we have to create in between.
    IsTarget = maps:get(node_is_target, Info),
    case can_continue_update_after_node_not_found(Info) of
        true when IsTarget ->
            Node = create_node_record(Payload),
            NodeProps = gather_node_props_from_old_and_new_nodes(
                          undefined, Node, TreeOptions),
            {ok, Node, Result#{Path => NodeProps}};
        true ->
            Node = create_node_record(khepri_payload:none()),
            {ok, Node, Result};
        false ->
            Reason1 = ?khepri_error(Reason, Info),
            {error, Reason1}
    end;
insert_or_update_node_cb(_, {interrupted, Reason, Info}, _, _, _) ->
    Reason1 = ?khepri_error(Reason, Info),
    {error, Reason1}.

gather_node_props_from_old_and_new_nodes(OldNode, NewNode, TreeOptions) ->
    OldNodeProps = case OldNode of
                       undefined ->
                           #{};
                       _ ->
                           gather_node_props(OldNode, TreeOptions)
                   end,
    NewNodeProps0 = gather_node_props(NewNode, TreeOptions),
    NewNodeProps1 = maps:remove(data, NewNodeProps0),
    NewNodeProps2 = maps:remove(sproc, NewNodeProps1),
    maps:merge(OldNodeProps, NewNodeProps2).

can_continue_update_after_node_not_found(#{condition := Condition}) ->
    can_continue_update_after_node_not_found1(Condition);
can_continue_update_after_node_not_found(#{node_name := NodeName}) ->
    can_continue_update_after_node_not_found1(NodeName).

can_continue_update_after_node_not_found1(ChildName)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildName) ->
    true;
can_continue_update_after_node_not_found1(#if_node_exists{exists = false}) ->
    true;
can_continue_update_after_node_not_found1(#if_all{conditions = Conds}) ->
    lists:all(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(#if_any{conditions = Conds}) ->
    lists:any(fun can_continue_update_after_node_not_found1/1, Conds);
can_continue_update_after_node_not_found1(_) ->
    false.

%% -------------------------------------------------------------------
%% Does path match.
%% -------------------------------------------------------------------

does_path_match(Path, PathPattern, Tree) ->
    PathPattern1 = khepri_path:compile(PathPattern),
    does_path_match(Path, PathPattern1, [], Tree).

does_path_match(PathRest, PathRest, _ReversedPath, _Tree) ->
    true;
does_path_match([], _PathPatternRest, _ReversedPath, _Tree) ->
    false;
does_path_match(_PathRest, [], _ReversedPath, _Tree) ->
    false;
does_path_match(
  [Component | Path], [Component | PathPattern], ReversedPath, Tree)
  when ?IS_KHEPRI_PATH_COMPONENT(Component) ->
    does_path_match(Path, PathPattern, [Component | ReversedPath], Tree);
does_path_match(
  [Component | _Path], [Condition | _PathPattern], _ReversedPath, _Tree)
  when ?IS_KHEPRI_PATH_COMPONENT(Component) andalso
       ?IS_KHEPRI_PATH_COMPONENT(Condition) ->
    false;
does_path_match(
  [Component | Path], [Condition | PathPattern], ReversedPath, Tree) ->
    %% Query the tree node, required to evaluate the condition.
    ReversedPath1 = [Component | ReversedPath],
    CurrentPath = lists:reverse(ReversedPath1),
    TreeOptions = #{expect_specific_node => true,
                    props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    {ok, #{CurrentPath := Node}} = find_matching_nodes(
                                     Tree,
                                     lists:reverse([Component | ReversedPath]),
                                     TreeOptions),
    case khepri_condition:is_met(Condition, Component, Node) of
        true ->
            ConditionMatchesGrandchildren =
            case khepri_condition:applies_to_grandchildren(Condition) of
                true ->
                    does_path_match(
                      Path, [Condition | PathPattern], ReversedPath1, Tree);
                false ->
                    false
            end,
            ConditionMatchesGrandchildren orelse
              does_path_match(Path, PathPattern, ReversedPath1, Tree);
        {false, _} ->
            false
    end.

%% -------------------------------------------------------------------
%% Tree traversal functions.
%% -------------------------------------------------------------------

-record(walk,
        {tree :: #tree{},
         node :: #node{} | delete,
         path_pattern :: khepri_path:native_pattern(),
         tree_options :: khepri:tree_options(),
         %% Used to remember the path of the node the walk is currently on.
         reversed_path = [] :: khepri_path:native_path(),
         %% Used to update parents up in the tree in a tail-recursive function.
         reversed_parent_tree = [] :: [#node{} | {#node{}, child_created}],
         'fun' :: walk_down_the_tree_fun(),
         fun_acc :: any(),
         applied_changes :: applied_changes()}).

-spec walk_down_the_tree(
        Tree, PathPattern, TreeOptions, Fun, FunAcc) -> Ret when
      Tree :: tree(),
      PathPattern :: khepri_path:native_pattern(),
      TreeOptions :: khepri:tree_options(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      AppliedChanges :: applied_changes(),
      Ret :: ok(Tree, AppliedChanges, FunAcc) | khepri:error().
%% @private

walk_down_the_tree(Tree, PathPattern, TreeOptions, Fun, FunAcc) ->
    walk_down_the_tree(Tree, PathPattern, TreeOptions, #{}, Fun, FunAcc).

-spec walk_down_the_tree(
        Tree, PathPattern, TreeOptions,
        AppliedChanges, Fun, FunAcc) -> Ret when
      Tree :: tree(),
      PathPattern :: khepri_path:native_pattern(),
      TreeOptions :: khepri:tree_options(),
      AppliedChanges :: applied_changes(),
      Fun :: walk_down_the_tree_fun(),
      FunAcc :: any(),
      Ret :: ok(Tree, AppliedChanges, FunAcc) | khepri:error().
%% @private

walk_down_the_tree(
  Tree, PathPattern, TreeOptions, AppliedChanges, Fun, FunAcc) ->
    CompiledPathPattern = khepri_path:compile(PathPattern),
    TreeOptions1 = case TreeOptions of
                       #{expect_specific_node := true} ->
                           TreeOptions;
                       _ ->
                           TreeOptions#{expect_specific_node => false}
                   end,
    Walk = #walk{tree = Tree,
                 node = Tree#tree.root,
                 path_pattern = CompiledPathPattern,
                 tree_options = TreeOptions1,
                 'fun' = Fun,
                 fun_acc = FunAcc,
                 applied_changes = AppliedChanges},
    case walk_down_the_tree1(Walk) of
        {ok, #walk{tree = Tree1,
                   node = Root1,
                   applied_changes = AppliedChanges1,
                   fun_acc = FunAcc1}} ->
            Tree2 = Tree1#tree{root = Root1},
            {ok, Tree2, AppliedChanges1, FunAcc1};
        Error ->
            Error
    end.

-spec walk_down_the_tree1(Walk) -> Ret when
      Walk :: #walk{},
      Ret :: khepri:ok(Walk) | khepri:error().
%% @private

walk_down_the_tree1(
  #walk{path_pattern = [?KHEPRI_ROOT_NODE | PathPattern],
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree} = Walk) ->
    ?assertEqual([], ReversedPath),
    ?assertEqual([], ReversedParentTree),
    Walk1 = Walk#walk{path_pattern = PathPattern},
    walk_down_the_tree1(Walk1);
walk_down_the_tree1(
  #walk{path_pattern = [?THIS_KHEPRI_NODE | PathPattern]} = Walk) ->
    Walk1 = Walk#walk{path_pattern = PathPattern},
    walk_down_the_tree1(Walk1);
walk_down_the_tree1(
  #walk{path_pattern = [?PARENT_KHEPRI_NODE | PathPattern],
        reversed_path = [_CurrentName | ReversedPath],
        reversed_parent_tree = [ParentNode0 | ReversedParentTree]} = Walk) ->
    ParentNode = case ParentNode0 of
                     {PN, child_created} -> PN;
                     _                   -> ParentNode0
                 end,
    Walk1 = Walk#walk{node = ParentNode,
                      path_pattern = PathPattern,
                      reversed_path = ReversedPath,
                      reversed_parent_tree = ReversedParentTree},
    walk_down_the_tree1(Walk1);
walk_down_the_tree1(
  #walk{path_pattern = [?PARENT_KHEPRI_NODE | PathPattern],
        reversed_path = [],
        reversed_parent_tree = []} = Walk) ->
    %% The path tries to go above the root node, like "cd /..". In this case,
    %% we stay on the root node.
    Walk1 = Walk#walk{path_pattern = PathPattern},
    walk_down_the_tree1(Walk1);
walk_down_the_tree1(
  #walk{node = #node{child_nodes = Children} = CurrentNode,
        path_pattern = [ChildName | PathPattern],
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree} = Walk)
  when ?IS_KHEPRI_NODE_ID(ChildName) ->
    Walk1 = Walk#walk{path_pattern = PathPattern,
                      reversed_path = [ChildName | ReversedPath],
                      reversed_parent_tree =
                      [CurrentNode | ReversedParentTree]},
    case Children of
        #{ChildName := Child} ->
            Walk2 = Walk1#walk{node = Child},
            walk_down_the_tree1(Walk2);
        _ ->
            interrupted_walk_down(
              Walk1, node_not_found,
              #{node_name => ChildName,
                node_path => lists:reverse([ChildName | ReversedPath])})
    end;
walk_down_the_tree1(
  #walk{node = #node{child_nodes = Children} = CurrentNode,
        path_pattern = [Condition | PathPattern],
        tree_options = #{expect_specific_node := true},
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree} = Walk)
  when ?IS_KHEPRI_CONDITION(Condition) ->
    %% We distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?KHEPRI_ROOT_NODE or
    %% ?THIS_KHEPRI_NODE in the condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?KHEPRI_ROOT_NODE orelse
               NodeName =:= ?THIS_KHEPRI_NODE ->
            CurrentName = special_component_to_node_name(
                            NodeName, ReversedPath),
            CondMet = khepri_condition:is_met(
                        Condition, CurrentName, CurrentNode),
            case CondMet of
                true ->
                    Walk1 = Walk#walk{path_pattern = PathPattern},
                    walk_down_the_tree1(Walk1);
                {false, Cond} ->
                    Walk1 = Walk#walk{path_pattern = PathPattern},
                    interrupted_walk_down(
                      Walk1, mismatching_node,
                      #{node_name => CurrentName,
                        node_path => lists:reverse(ReversedPath),
                        node_props => gather_node_props_for_error(CurrentNode),
                        condition => Cond})
            end;
        {true, ChildName} when ChildName =/= ?PARENT_KHEPRI_NODE ->
            Walk1 = Walk#walk{path_pattern = PathPattern,
                              reversed_path = [ChildName | ReversedPath],
                              reversed_parent_tree =
                              [CurrentNode | ReversedParentTree]},
            case Children of
                #{ChildName := Child} ->
                    Walk2 = Walk1#walk{node = Child},
                    CondMet = khepri_condition:is_met(
                                Condition, ChildName, Child),
                    case CondMet of
                        true ->
                            walk_down_the_tree1(Walk2);
                        {false, Cond} ->
                            interrupted_walk_down(
                              Walk2, mismatching_node,
                              #{node_name => ChildName,
                                node_path => lists:reverse(
                                               [ChildName | ReversedPath]),
                                node_props => gather_node_props_for_error(
                                                Child),
                                condition => Cond})
                    end;
                _ ->
                    interrupted_walk_down(
                      Walk1, node_not_found,
                      #{node_name => ChildName,
                        node_path => lists:reverse([ChildName | ReversedPath]),
                        condition => Condition})
            end;
        {true, ?PARENT_KHEPRI_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           condition_targets_parent_node,
                           #{path => BadPathPattern,
                             condition => Condition}),
            {error, Exception};
        false ->
            %% The caller expects that the path matches a single specific node
            %% (no matter if it exists or not), but the condition could match
            %% several nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           possibly_matching_many_nodes_denied,
                           #{path => BadPathPattern}),
            {error, Exception}
    end;
walk_down_the_tree1(
  #walk{node = #node{child_nodes = Children} = CurrentNode,
        path_pattern = [Condition | PathPattern] = WholePathPattern,
        tree_options = #{expect_specific_node := false} = TreeOptions,
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree,
        applied_changes = AppliedChanges} = Walk)
  when ?IS_KHEPRI_CONDITION(Condition) ->
    %% Like with "expect_specific_node =:= true" function clause above, We
    %% distinguish the case where the condition must be verified against the
    %% current node (i.e. the node name is ?KHEPRI_ROOT_NODE or
    %% ?THIS_KHEPRI_NODE in the condition) instead of its child nodes.
    SpecificNode = khepri_path:component_targets_specific_node(Condition),
    case SpecificNode of
        {true, NodeName}
          when NodeName =:= ?KHEPRI_ROOT_NODE orelse
               NodeName =:= ?THIS_KHEPRI_NODE ->
            CurrentName = special_component_to_node_name(
                            NodeName, ReversedPath),
            CondMet = khepri_condition:is_met(
                        Condition, CurrentName, CurrentNode),
            case CondMet of
                true ->
                    Walk1 = Walk#walk{path_pattern = PathPattern},
                    walk_down_the_tree1(Walk1);
                {false, _} ->
                    StartingNode = starting_node_in_rev_parent_tree(
                                     ReversedParentTree, CurrentNode),
                    Walk1 = Walk#walk{node = StartingNode},
                    {ok, Walk1}
            end;
        {true, ?PARENT_KHEPRI_NODE} ->
            %% TODO: Support calling Fun() with parent node based on
            %% conditions on child nodes.
            BadPathPattern =
            lists:reverse(ReversedPath, [Condition | PathPattern]),
            Exception = ?khepri_exception(
                           condition_targets_parent_node,
                           #{path => BadPathPattern,
                             condition => Condition}),
            {error, Exception};
        _ ->
            %% There is a special case if the current node is the root node.
            %% The caller can request that the root node's properties are
            %% included. This is true by default if the path is `[]'. This
            %% allows to get its props and payload atomically in a single
            %% query.
            IsRoot = ReversedPath =:= [],
            IncludeRootProps = maps:get(
                                 include_root_props, TreeOptions, false),
            Ret0 = case IsRoot andalso IncludeRootProps of
                       true ->
                           Walk1 = Walk#walk{path_pattern = [],
                                             reversed_path = [],
                                             reversed_parent_tree = []},
                           walk_down_the_tree1(Walk1);
                       _ ->
                           {ok, Walk}
                   end,

            %% The result of the first part (the special case for the root
            %% node if relevant) is used as a starting point for handling all
            %% child nodes.
            Ret1 = maps:fold(
                     fun
                         (ChildName, Child,
                          {ok, Walk1}) ->
                             Walk2 = Walk1#walk{path_pattern =
                                                WholePathPattern,
                                                reversed_path = ReversedPath},
                             handle_branch(Walk2, ChildName, Child);
                         (_, _, Error) ->
                             Error
                     end, Ret0, Children),
            case Ret1 of
                {ok,
                 #walk{node = CurrentNode,
                       applied_changes = AppliedChanges} = Walk2} ->
                    %% The current node didn't change, no need to update the
                    %% tree and evaluate keep_while conditions.
                    StartingNode = starting_node_in_rev_parent_tree(
                                     ReversedParentTree, CurrentNode),
                    Walk3 = Walk2#walk{node = StartingNode},
                    {ok, Walk3};
                {ok, #walk{node = CurrentNode1} = Walk2} ->
                    CurrentNode2 = case CurrentNode1 of
                                       CurrentNode ->
                                           CurrentNode;
                                       delete ->
                                           CurrentNode1;
                                       _ ->
                                           %% Because of the loop, payload &
                                           %% child list versions may have
                                           %% been increased multiple times.
                                           %% We want them to increase once
                                           %% for the whole (atomic)
                                           %% operation.
                                           squash_version_bumps(
                                             CurrentNode, CurrentNode1)
                                   end,
                    Walk3 = Walk2#walk{node = CurrentNode2,
                                       reversed_path = ReversedPath,
                                       reversed_parent_tree =
                                       ReversedParentTree},
                    walk_back_up_the_tree(Walk3);
                Error ->
                    Error
            end
    end;
walk_down_the_tree1(
  #walk{node = #node{} = CurrentNode,
        path_pattern = [],
        'fun' = Fun,
        fun_acc = FunAcc,
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree} = Walk) ->
    CurrentPath = lists:reverse(ReversedPath),
    case Fun(CurrentPath, CurrentNode, FunAcc) of
        {ok, keep, FunAcc1} ->
            StartingNode = starting_node_in_rev_parent_tree(
                             ReversedParentTree, CurrentNode),
            Walk1 = Walk#walk{node = StartingNode, fun_acc = FunAcc1},
            {ok, Walk1};
        {ok, delete, FunAcc1} ->
            Walk1 = Walk#walk{node = delete, fun_acc = FunAcc1},
            walk_back_up_the_tree(Walk1);
        {ok, #node{} = CurrentNode1, FunAcc1} ->
            Walk1 = Walk#walk{node = CurrentNode1, fun_acc = FunAcc1},
            walk_back_up_the_tree(Walk1);
        Error ->
            Error
    end.

-spec special_component_to_node_name(SpecialComponent, ReversedPath) ->
    NodeName when
      SpecialComponent :: ?KHEPRI_ROOT_NODE | ?THIS_KHEPRI_NODE,
      ReversedPath :: khepri_path:native_path(),
      NodeName :: khepri_path:component().

special_component_to_node_name(?KHEPRI_ROOT_NODE = NodeName, []) ->
    NodeName;
special_component_to_node_name(?THIS_KHEPRI_NODE, [NodeName | _]) ->
    NodeName;
special_component_to_node_name(?THIS_KHEPRI_NODE, []) ->
    ?KHEPRI_ROOT_NODE.

-spec starting_node_in_rev_parent_tree(ReversedParentTree) -> Node when
      Node :: tree_node(),
      ReversedParentTree :: [Node].
%% @private

starting_node_in_rev_parent_tree(ReversedParentTree) ->
    hd(lists:reverse(ReversedParentTree)).

-spec starting_node_in_rev_parent_tree(ReversedParentTree, Node) -> Node when
      Node :: tree_node(),
      ReversedParentTree :: [Node].
%% @private

starting_node_in_rev_parent_tree([], CurrentNode) ->
    CurrentNode;
starting_node_in_rev_parent_tree(ReversedParentTree, _) ->
    starting_node_in_rev_parent_tree(ReversedParentTree).

-spec handle_branch(Walk, ChildName, Child) -> Ret when
      Walk :: #walk{},
      ChildName :: khepri_path:component(),
      Child :: tree_node(),
      Ret :: khepri:ok(Walk) | khepri:error().
%% @private

handle_branch(
  #walk{node = CurrentNode,
        path_pattern = [Condition | PathPattern] = WholePathPattern,
        reversed_path = ReversedPath} = Walk,
  ChildName, Child) ->
    CondMet = khepri_condition:is_met(Condition, ChildName, Child),
    Ret = case CondMet of
              true ->
                  Walk1 = Walk#walk{node = Child,
                                    path_pattern = PathPattern,
                                    reversed_path = [ChildName | ReversedPath],
                                    reversed_parent_tree = [CurrentNode]},
                  walk_down_the_tree1(Walk1);
              {false, _} ->
                  {ok, Walk}
          end,
    case Ret of
        {ok,
         #walk{node = #node{child_nodes = Children} = CurrentNode1} = Walk2}
         when is_map_key(ChildName, Children) ->
            case khepri_condition:applies_to_grandchildren(Condition) of
                false ->
                    Ret;
                true ->
                    Walk3 = Walk2#walk{node = Child,
                                       path_pattern = WholePathPattern,
                                       reversed_path =
                                       [ChildName | ReversedPath],
                                       reversed_parent_tree = [CurrentNode1]},
                    walk_down_the_tree1(Walk3)
            end;
        {ok, _Walk} ->
            %% The child node is gone, no need to test if the condition
            %% applies to it or recurse.
            Ret;
        Error ->
            Error
    end.

-spec interrupted_walk_down(Walk, Reason, Info) -> Ret when
      Walk :: #walk{},
      Reason :: mismatching_node | node_not_found,
      Info :: map(),
      Ret :: khepri:ok(Walk) | khepri:error().
%% @private

interrupted_walk_down(
  #walk{tree = Tree,
        path_pattern = PathPattern,
        reversed_path = ReversedPath,
        reversed_parent_tree = ReversedParentTree,
        'fun' = Fun,
        fun_acc = FunAcc} = Walk,
  Reason, Info) ->
    NodePath = lists:reverse(ReversedPath),
    IsTarget = khepri_path:realpath(PathPattern) =:= [],
    Info1 = Info#{node_is_target => IsTarget},
    ErrorTuple = {interrupted, Reason, Info1},
    case Fun(NodePath, ErrorTuple, FunAcc) of
        {ok, ToDo, FunAcc1}
          when ToDo =:= keep orelse ToDo =:= delete ->
            ?assertNotEqual([], ReversedParentTree),
            StartingNode = starting_node_in_rev_parent_tree(
                             ReversedParentTree),
            Walk1 = Walk#walk{node = StartingNode,
                              fun_acc = FunAcc1},
            {ok, Walk1};
        {ok, #node{} = NewNode, FunAcc1} ->
            ReversedParentTree1 =
            case Reason of
                node_not_found ->
                    %% We record the fact the child is a new node. This is used
                    %% to reset the child's stats if it got new payload or
                    %% child nodes at the same time.
                    [{hd(ReversedParentTree), child_created}
                     | tl(ReversedParentTree)];
                _ ->
                    ReversedParentTree
            end,
            case PathPattern of
                [] ->
                    %% We reached the target node. We could call
                    %% walk_down_the_tree1() again, but it would call Fun() a
                    %% second time.
                    Walk1 = Walk#walk{node = NewNode,
                                      reversed_parent_tree =
                                      ReversedParentTree1,
                                      fun_acc = FunAcc1},
                    walk_back_up_the_tree(Walk1);
                _ ->
                    %% We created a tree node automatically on our way to the
                    %% target. We want to add a `keep_while' condition for it
                    %% so it is automatically reclaimed when it becomes
                    %% useless (i.e., no payload and no child nodes).
                    Cond = #if_any{conditions =
                                   [#if_child_list_length{count = {gt, 0}},
                                    #if_has_payload{has_payload = true}]},
                    KeepWhile = #{NodePath => Cond},
                    Tree1 = update_keep_while_conds(Tree, NodePath, KeepWhile),

                    Walk1 = Walk#walk{tree = Tree1,
                                      node = NewNode,
                                      reversed_parent_tree =
                                      ReversedParentTree1,
                                      fun_acc = FunAcc1},
                    walk_down_the_tree1(Walk1)
            end;
        Error ->
            Error
    end.

-spec walk_back_up_the_tree(Walk) -> Ret when
      Walk :: #walk{},
      Ret :: khepri:ok(Walk).
%% @private

walk_back_up_the_tree(Walk) ->
    walk_back_up_the_tree(Walk, #{}).

-spec walk_back_up_the_tree(Walk, AppliedChangesAcc) -> Ret when
      Walk :: #walk{},
      AppliedChangesAcc :: applied_changes(),
      Ret :: khepri:ok(Walk).
%% @private

walk_back_up_the_tree(
  #walk{node = delete,
        reversed_path = [ChildName | ReversedPath] = WholeReversedPath,
        reversed_parent_tree = [ParentNode | ReversedParentTree],
        applied_changes = AppliedChanges} = Walk,
  AppliedChangesAcc) ->
    %% Evaluate keep_while of nodes which depended on ChildName (it is
    %% removed) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    AppliedChangesAcc1 = AppliedChangesAcc#{Path => delete},

    %% Evaluate keep_while of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = remove_node_child(ParentNode, ChildName),

    %% If we are handling deletes as part of a `keep_while', it is possible
    %% that this parent node's child list version was bumped if a node was
    %% added in the first pass. In this case, we don't want to bump that
    %% version twice (add + delete), but just once.
    ParentNode2 = squash_version_bumps_after_keep_while(
                    lists:reverse(ReversedPath), ParentNode1, AppliedChanges),

    Walk1 = Walk#walk{node = ParentNode2,
                      reversed_path = ReversedPath,
                      reversed_parent_tree = ReversedParentTree},
    handle_keep_while_for_parent_update(Walk1, AppliedChangesAcc1);
walk_back_up_the_tree(
  #walk{node = Child,
        reversed_path = [ChildName | ReversedPath] = WholeReversedPath,
        reversed_parent_tree =
        [{ParentNode, child_created} | ReversedParentTree]} = Walk,
  AppliedChangesAcc) ->
    Child1 = reset_versions(Child),

    %% Evaluate keep_while of nodes which depend on ChildName (it is
    %% created) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    NodeProps = gather_node_props(Child1, TreeOptions),
    AppliedChangesAcc1 = AppliedChangesAcc#{Path => {create, NodeProps}},

    %% Evaluate keep_while of parent node on itself right now (its child_count
    %% has changed).
    ParentNode1 = add_node_child(ParentNode, ChildName, Child1),
    Walk1 = Walk#walk{node = ParentNode1,
                      reversed_path = ReversedPath,
                      reversed_parent_tree = ReversedParentTree},
    handle_keep_while_for_parent_update(Walk1, AppliedChangesAcc1);
walk_back_up_the_tree(
  #walk{node = Child,
        reversed_path = [ChildName | ReversedPath] = WholeReversedPath,
        reversed_parent_tree = [ParentNode | ReversedParentTree]} = Walk,
  AppliedChangesAcc) ->
    %% Evaluate keep_while of nodes which depend on ChildName (it is
    %% modified) at the end of walk_back_up_the_tree().
    Path = lists:reverse(WholeReversedPath),
    TreeOptions = #{props_to_return => [payload,
                                        payload_version,
                                        child_list_version,
                                        child_list_length]},
    InitialNodeProps = gather_node_props(
                         maps:get(ChildName, ParentNode#node.child_nodes),
                         TreeOptions),
    NodeProps = gather_node_props(Child, TreeOptions),
    AppliedChangesAcc1 = case NodeProps of
                             InitialNodeProps ->
                                 AppliedChangesAcc;
                             _ ->
                                 AppliedChangesAcc#{
                                   Path => {update, NodeProps}}
                         end,

    %% No need to evaluate keep_while of ParentNode, its child_count is
    %% unchanged.
    ParentNode1 = update_node_child(ParentNode, ChildName, Child),
    Walk1 = Walk#walk{node = ParentNode1,
                      reversed_path = ReversedPath,
                      reversed_parent_tree = ReversedParentTree},
    walk_back_up_the_tree(Walk1, AppliedChangesAcc1);
walk_back_up_the_tree(
  #walk{reversed_path = [], %% <-- We reached the root (i.e. not a branch,
                            %% see handle_branch())
        reversed_parent_tree = [],
        applied_changes = AppliedChanges} = Walk,
  AppliedChangesAcc) ->
    AppliedChanges1 = merge_applied_changes(AppliedChanges, AppliedChangesAcc),
    Walk1 = Walk#walk{applied_changes = AppliedChanges1},
    handle_applied_changes(Walk1);
walk_back_up_the_tree(
  #walk{reversed_parent_tree = [],
        applied_changes = AppliedChanges} = Walk,
  AppliedChangesAcc) ->
    AppliedChanges1 = merge_applied_changes(AppliedChanges, AppliedChangesAcc),
    Walk1 = Walk#walk{applied_changes = AppliedChanges1},
    {ok, Walk1}.

handle_keep_while_for_parent_update(
  #walk{reversed_parent_tree = [{_GrandParentNode, child_created} | _]} = Walk,
  AppliedChangesAcc) ->
    %% This is a freshly created node, we don't want to get rid of it right
    %% away.
    walk_back_up_the_tree(Walk, AppliedChangesAcc);
handle_keep_while_for_parent_update(
  #walk{tree = Tree,
        node = ParentNode,
        reversed_path = ReversedPath,
        tree_options = TreeOptions,
        fun_acc = Acc} = Walk,
  AppliedChangesAcc) ->
    ParentPath = lists:reverse(ReversedPath),
    IsMet = is_keep_while_condition_met_on_self(
              Tree, ParentPath, ParentNode),
    case IsMet of
        true ->
            %% We continue with the update.
            walk_back_up_the_tree(Walk, AppliedChangesAcc);
        {false, _Reason} ->
            %% This parent node must be removed because it doesn't meet its
            %% own keep_while condition. keep_while conditions for nodes
            %% depending on this one will be evaluated with the recursion.
            {ok, delete, Acc1} = delete_matching_nodes_cb(
                                   ParentPath, ParentNode,
                                   TreeOptions, keep_while, Acc),
            Walk1 = Walk#walk{node = delete,
                              fun_acc = Acc1},
            walk_back_up_the_tree(Walk1, AppliedChangesAcc)
    end.

merge_applied_changes(AppliedChanges1, AppliedChanges2) ->
    maps:fold(
      fun
          (Path, delete, KWA1) ->
              KWA1#{Path => delete};
          (Path, {_, _} = TypeAndNodeProps, KWA1) ->
              case KWA1 of
                  #{Path := delete} -> KWA1;
                  _                 -> KWA1#{Path => TypeAndNodeProps}
              end
      end, AppliedChanges1, AppliedChanges2).

handle_applied_changes(
  #walk{applied_changes = AppliedChanges} = Walk)
  when AppliedChanges =:= #{} ->
    {ok, Walk};
handle_applied_changes(
  #walk{tree = Tree,
        node = Root,
        applied_changes = AppliedChanges} = Walk) ->
    Tree1 = Tree#tree{root = Root},
    ToDelete = eval_keep_while_conditions(Tree1, AppliedChanges),

    Tree2 = maps:fold(
              fun
                  (RemovedPath, delete, T) ->
                      KW1 = maps:remove(
                              RemovedPath, T#tree.keep_while_conds),
                      T1 = update_keep_while_conds_revidx(T, RemovedPath, #{}),
                      T1#tree{keep_while_conds = KW1};
                  (_, {_, _}, T) ->
                      T
              end, Tree1, AppliedChanges),

    ToDelete1 = filter_and_sort_paths_to_delete(ToDelete, AppliedChanges),
    Walk1 = Walk#walk{tree = Tree2},
    remove_expired_nodes(ToDelete1, Walk1).

eval_keep_while_conditions(
  #tree{keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  AppliedChanges) ->
    %% AppliedChanges lists all nodes which were modified or removed. We
    %% want to transform that into a list of nodes to remove.
    %%
    %% Those marked as `delete' in AppliedChanges are already gone. We
    %% need to find the nodes which depended on them, i.e. their keep_while
    %% condition is not met anymore. Note that removed nodes' child nodes are
    %% gone as well and must be handled (they are not specified in
    %% AppliedChanges).
    %%
    %% Those modified in AppliedChanges must be evaluated again to decide
    %% if they should be removed.
    case is_v1_keep_while_conds_revidx(KeepWhileCondsRevIdx) of
        true ->
            eval_keep_while_conditions_v1(Tree, AppliedChanges);
        false ->
            eval_keep_while_conditions_v0(Tree, AppliedChanges)
    end.

eval_keep_while_conditions_v0(
  #tree{keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  AppliedChanges) ->
    maps:fold(
      fun
          (RemovedPath, delete, ToDelete) ->
              maps:fold(
                fun(Path, Watchers, ToDelete1) ->
                        case lists:prefix(RemovedPath, Path) of
                            true ->
                                eval_keep_while_conditions_after_removal(
                                  Tree, Watchers, ToDelete1);
                            false ->
                                ToDelete1
                        end
                end, ToDelete, KeepWhileCondsRevIdx);
          (UpdatedPath, {_Type, NodeProps}, ToDelete) ->
              case KeepWhileCondsRevIdx of
                  #{UpdatedPath := Watchers} ->
                      eval_keep_while_conditions_after_update(
                        Tree, UpdatedPath, NodeProps, Watchers, ToDelete);
                  _ ->
                      ToDelete
              end
      end, #{}, AppliedChanges).

eval_keep_while_conditions_v1(
  #tree{keep_while_conds_revidx = KeepWhileCondsRevIdx} = Tree,
  AppliedChanges) ->
    maps:fold(
      fun
          (RemovedPath, delete, ToDelete) ->
              khepri_prefix_tree:fold_prefixes_of(
                fun(Watchers, ToDelete1) ->
                        eval_keep_while_conditions_after_removal(
                          Tree, Watchers, ToDelete1)
                end, ToDelete, RemovedPath, KeepWhileCondsRevIdx);
          (UpdatedPath, {_Type, NodeProps}, ToDelete) ->
              Result = khepri_prefix_tree:find_path(
                         UpdatedPath, KeepWhileCondsRevIdx),
              case Result of
                  {ok, Watchers} ->
                      eval_keep_while_conditions_after_update(
                        Tree, UpdatedPath, NodeProps, Watchers, ToDelete);
                  error ->
                      ToDelete
              end
      end, #{}, AppliedChanges).

eval_keep_while_conditions_after_update(
  #tree{keep_while_conds = KeepWhileConds} = Tree,
  UpdatedPath, NodeProps, Watchers, ToDelete) ->
    maps:fold(
      fun(Watcher, ok, ToDelete1) ->
              KeepWhile = maps:get(Watcher, KeepWhileConds),
              CondOnUpdated = maps:get(UpdatedPath, KeepWhile),
              IsMet = khepri_condition:is_met(
                        CondOnUpdated, UpdatedPath, NodeProps),
              case IsMet of
                  true ->
                      ToDelete1;
                  {false, _} ->
                      case are_keep_while_conditions_met(Tree, KeepWhile) of
                          true       -> ToDelete1;
                          {false, _} -> ToDelete1#{Watcher => delete}
                      end
              end
      end, ToDelete, Watchers).

eval_keep_while_conditions_after_removal(
  #tree{keep_while_conds = KeepWhileConds} = Tree,
  Watchers, ToDelete) ->
    maps:fold(
      fun(Watcher, ok, ToDelete1) ->
              KeepWhile = maps:get(Watcher, KeepWhileConds),
              case are_keep_while_conditions_met(Tree, KeepWhile) of
                  true       -> ToDelete1;
                  {false, _} -> ToDelete1#{Watcher => delete}
              end
      end, ToDelete, Watchers).

filter_and_sort_paths_to_delete(ToDelete, AppliedChanges) ->
    Paths1 = lists:sort(
               fun
                   (A, B) when length(A) =:= length(B) ->
                       A < B;
                   (A, B) ->
                       length(A) < length(B)
               end,
               maps:keys(ToDelete)),
    Paths2 = lists:foldl(
               fun(Path, Map) ->
                       case AppliedChanges of
                           #{Path := delete} ->
                               Map;
                           _ ->
                               case is_parent_being_removed(Path, Map) of
                                   false -> Map#{Path => delete};
                                   true  -> Map
                               end
                       end
               end, #{}, Paths1),
    maps:keys(Paths2).

is_parent_being_removed([], _) ->
    false;
is_parent_being_removed(Path, Map) ->
    is_parent_being_removed1(lists:reverse(Path), Map).

is_parent_being_removed1([_ | Parent], Map) ->
    case maps:is_key(lists:reverse(Parent), Map) of
        true  -> true;
        false -> is_parent_being_removed1(Parent, Map)
    end;
is_parent_being_removed1([], _) ->
    false.

remove_expired_nodes([], Walk) ->
    {ok, Walk};
remove_expired_nodes(
  [PathToDelete | Rest],
  #walk{tree = Tree,
        applied_changes = AppliedChanges,
        tree_options = TreeOptions,
        fun_acc = Acc} = Walk) ->
    %% See `delete_matching_nodes/4'. This is the same except that the
    %% accumulator is passed through and the `DeleteReason' is provided as
    %% `keep_while'.
    Fun = fun(Path, Node, Result) ->
                  delete_matching_nodes_cb(
                    Path, Node, TreeOptions, keep_while, Result)
          end,
    Result = walk_down_the_tree(
               Tree, PathToDelete, TreeOptions, AppliedChanges, Fun, Acc),
    case Result of
        {ok, Tree1, AppliedChanges1, Acc1} ->
            AppliedChanges2 = merge_applied_changes(
                                AppliedChanges, AppliedChanges1),
            Walk1 = Walk#walk{tree = Tree1,
                              node = Tree1#tree.root,
                              applied_changes = AppliedChanges2,
                              fun_acc = Acc1},
            remove_expired_nodes(Rest, Walk1)
    end.

%% -------------------------------------------------------------------
%% Conversion between tree versions.
%% -------------------------------------------------------------------

convert_tree(Tree, MacVer, MacVer) ->
    Tree;
convert_tree(Tree, 0, 1) ->
    Tree;
convert_tree(Tree, 1, 2) ->
    %% In version 2 the reverse index for keep while conditions was converted
    %% into a prefix tree. See the `keep_while_conds_revidx_v0()' and
    %% `keep_while_conds_revidx_v1()` types.
    #tree{keep_while_conds_revidx = KeepWhileCondsRevIdxV0} = Tree,
    KeepWhileCondsRevIdxV1 = khepri_prefix_tree:from_map(
                               KeepWhileCondsRevIdxV0),
    Tree#tree{keep_while_conds_revidx = KeepWhileCondsRevIdxV1}.
