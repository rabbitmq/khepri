-module(khepri_prefix_tree).

-include("src/khepri_payload.hrl").

-type children() :: #{khepri_path:node_id() => tree()}.

-record(?MODULE, {children = #{},
                  payload = ?NO_PAYLOAD}).

-type tree(Payload) :: #?MODULE{children :: children(),
                                payload :: Payload | ?NO_PAYLOAD}.

-type tree() :: tree(_).

-export_type([tree/1]).

-export([new/0,
         fold_prefixes_of/4,
         find_path/2,
         update/3]).

-spec new() -> tree().

new() -> #?MODULE{}.

-spec fold_prefixes_of(Fun, Acc, Path, Tree) -> Ret when
      Fun :: fun((Payload, Acc) -> Acc1),
      Acc :: term(),
      Acc1 :: term(),
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: Acc1.

fold_prefixes_of(Fun, Acc, Path, Tree) when is_function(Fun, 2) ->
    do_fold_prefixes_of(Fun, Acc, Path, Tree).

do_fold_prefixes_of(
  Fun, Acc, [], #?MODULE{payload = Payload, children = Children}) ->
    Acc1 = case Payload of
               ?NO_PAYLOAD ->
                   Acc;
               _ ->
                   Fun(Payload, Acc)
           end,
    maps:fold(
      fun(_Component, Subtree, Acc2) ->
              do_fold_prefixes_of(Fun, Acc2, [], Subtree)
      end, Acc1, Children);
do_fold_prefixes_of(
  Fun, Acc, [Component | Rest], #?MODULE{children = Children}) ->
    case maps:find(Component, Children) of
        {ok, Subtree} ->
            do_fold_prefixes_of(Fun, Acc, Rest, Subtree);
        error ->
            Acc
    end.

-spec find_path(Path, Tree) -> Ret when
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: {ok, Payload} | error.

find_path(Path, Tree) ->
    do_find_path(Path, Tree).

do_find_path([], #?MODULE{payload = Payload}) ->
    case Payload of
        ?NO_PAYLOAD ->
            error;
        _ ->
            {ok, Payload}
    end;
do_find_path([Component | Rest], #?MODULE{children = Children}) ->
    case maps:find(Component, Children) of
        {ok, Subtree} ->
            do_find_path(Rest, Subtree);
        error ->
            error
    end.

-spec update(Fun, Path, Tree) -> Ret when
      Fun :: fun((Payload | ?NO_PAYLOAD) -> Payload | ?NO_PAYLOAD),
      Path :: khepri_path:native_path(),
      Tree :: khepri_prefix_tree:tree(Payload),
      Payload :: term(),
      Ret :: khepri_prefix_tree:tree().

update(Fun, Path, Tree) ->
    do_update(Fun, Path, Tree).

do_update(Fun, [], #?MODULE{payload = Payload} = Tree) ->
    Tree#?MODULE{payload = Fun(Payload)};
do_update(Fun, [Component | Rest], #?MODULE{children = Children} = Tree) ->
    Subtree = maps:get(Component, Children, khepri_prefix_tree:new()),
    Children1 = case do_update(Fun, Rest, Subtree) of
                    #?MODULE{payload = ?NO_PAYLOAD, children = C}
                      when C =:= #{} ->
                        %% Drop unused branches.
                        maps:remove(Component, Children);
                    Subtree1 ->
                        maps:put(Component, Subtree1, Children)
                end,
    Tree#?MODULE{children = Children1}.
