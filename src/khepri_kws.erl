%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri `keep_while' conditions storage manipulation API.
%%
%% @hidden

-module(khepri_kws).

-include("src/khepri_payload.hrl").

-export([new/0,
         from_conds/2,

         get_conds/1,
         get_reverse_index/1,

         update_conds/3,
         remove_path/2,
         is_v1_keep_while_conds_revidx/1,
         to_absolute_keep_while/2]).

-type keep_while_conds_map() :: #{khepri_path:native_path() =>
                                  khepri_condition:native_keep_while()}.
%% Per-node `keep_while' conditions.

-type keep_while_conds_revidx_v0() :: #{khepri_path:native_path() =>
                                        #{khepri_path:native_path() => ok}}.

-type keep_while_conds_revidx_v1() :: khepri_prefix_tree:tree(
                                        #{khepri_path:native_path() => ok}).

-type keep_while_conds_revidx() :: keep_while_conds_revidx_v0() |
                                   keep_while_conds_revidx_v1().
%% Internal reverse index of the keep_while conditions.
%%
%% If node A depends on a condition on node B, then this reverse index will
%% have a "node B => node A" association. The version 0 of this type used a
%% map and folded over the entries in the map using `lists:prefix/2' to find
%% matching conditions. In version 1 this type was replaced with a prefix tree
%% which improves lookup time when the reverse index contains many entries.

-record(keep_while,
        {conds = #{} :: khepri_kws:keep_while_conds_map(),
         reverse_index = #{} :: khepri_kws:keep_while_conds_revidx()}).

-opaque state() :: #keep_while{}.

-export_type([state/0,
              keep_while_conds_map/0,
              keep_while_conds_revidx/0,
              keep_while_conds_revidx_v0/0,
              keep_while_conds_revidx_v1/0]).

-spec new() -> State when
      State :: khepri_kws:state().

new() ->
    #keep_while{}.

-spec from_conds(Conds, RevIdx) -> State when
      Conds :: khepri_kws:keep_while_conds_map(),
      RevIdx :: khepri_kws:keep_while_conds_revidx(),
      State :: khepri_kws:state().

from_conds(Conds, RevIdx) ->
    #keep_while{conds = Conds,
                reverse_index = RevIdx}.

%% -------------------------------------------------------------------
%% Getters and setters.
%% -------------------------------------------------------------------

-spec get_conds(State) -> Conds when
      State :: khepri_kws:state(),
      Conds :: khepri_kws:keep_while_conds_map().

get_conds(#keep_while{conds = Conds}) ->
    Conds.

-spec set_conds(State, Conds) -> NewState when
      State :: khepri_kws:state(),
      Conds :: khepri_kws:keep_while_conds_map(),
      NewState :: khepri_kws:state().

set_conds(#keep_while{} = State, Conds) ->
    State1 = State#keep_while{conds = Conds},
    State1.

-spec get_reverse_index(State) -> RevIdx when
      State :: khepri_kws:state(),
      RevIdx :: khepri_kws:keep_while_conds_revidx().

get_reverse_index(#keep_while{reverse_index = RevIdx}) ->
    RevIdx.

-spec set_reverse_index(State, RevIdx) -> NewState when
      State :: khepri_kws:state(),
      RevIdx :: khepri_kws:keep_while_conds_revidx(),
      NewState :: khepri_kws:state().

set_reverse_index(#keep_while{} = State, RevIdx) ->
    State1 = State#keep_while{reverse_index = RevIdx},
    State1.

%% -------------------------------------------------------------------
%% Functions to update the conditions map and reverse index.
%% -------------------------------------------------------------------

-spec to_absolute_keep_while(BasePath, KeepWhile) -> KeepWhile when
      BasePath :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while().
%% @private

to_absolute_keep_while(BasePath, KeepWhile) ->
    maps:fold(
      fun(Path, Cond, Acc) ->
              AbsPath = khepri_path:abspath(Path, BasePath),
              Acc#{AbsPath => Cond}
      end, #{}, KeepWhile).

update_conds(State, Watcher, KeepWhile) ->
    AbsKeepWhile = to_absolute_keep_while(Watcher, KeepWhile),
    State1 = update_reverse_index(State, Watcher, AbsKeepWhile),
    Conds = get_conds(State1),
    Conds1 = Conds#{Watcher => AbsKeepWhile},
    State2 = set_conds(State1, Conds1),
    State2.

-spec update_reverse_index(State, Watcher, KeepWhile) ->
    NewState when
      State :: state(),
      Watcher :: khepri_path:native_path(),
      KeepWhile :: khepri_condition:native_keep_while(),
      NewState :: state().

update_reverse_index(State, Watcher, KeepWhile) ->
    RevIdx = get_reverse_index(State),
    case is_v1_keep_while_conds_revidx(RevIdx) of
        true  -> update_keep_while_conds_revidx_v1(State, Watcher, KeepWhile);
        false -> update_keep_while_conds_revidx_v0(State, Watcher, KeepWhile)
    end.

is_v1_keep_while_conds_revidx(RevIdx) ->
    khepri_prefix_tree:is_prefix_tree(RevIdx).

update_keep_while_conds_revidx_v0(State, Watcher, KeepWhile) ->
    Conds = get_conds(State),
    RevIdx = get_reverse_index(State),
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_while.
    OldWatcheds = maps:get(Watcher, Conds, #{}),
    RevIdx1 = maps:fold(
                fun(Watched, _, KWRevIdx) ->
                        Watchers = maps:get(Watched, KWRevIdx),
                        Watchers1 = maps:remove(Watcher, Watchers),
                        case maps:size(Watchers1) of
                            0 -> maps:remove(Watched, KWRevIdx);
                            _ -> KWRevIdx#{Watched => Watchers1}
                        end
                end, RevIdx, OldWatcheds),
    %% Then, record the watched paths.
    RevIdx2 = maps:fold(
                fun(Watched, _, KWRevIdx) ->
                        Watchers = maps:get(Watched, KWRevIdx, #{}),
                        Watchers1 = Watchers#{Watcher => ok},
                        KWRevIdx#{Watched => Watchers1}
                end, RevIdx1, KeepWhile),
    State1 = set_reverse_index(State, RevIdx2),
    State1.

update_keep_while_conds_revidx_v1(State, Watcher, KeepWhile) ->
    Conds = get_conds(State),
    RevIdx = get_reverse_index(State),
    %% First, clean up reversed index where a watched path isn't watched
    %% anymore in the new keep_while.
    OldWatcheds = maps:get(Watcher, Conds, #{}),
    RevIdx1 = maps:fold(
                fun(Watched, _, KWRevIdx) ->
                        khepri_prefix_tree:update(
                          fun(Watchers) ->
                                  Watchers1 = maps:remove(Watcher, Watchers),
                                  case maps:size(Watchers1) of
                                      0 -> ?NO_PAYLOAD;
                                      _ -> Watchers1
                                  end
                          end, Watched, KWRevIdx)
                end, RevIdx, OldWatcheds),
    %% Then, record the watched paths.
    RevIdx2 = maps:fold(
                fun(Watched, _, KWRevIdx) ->
                        khepri_prefix_tree:update(
                          fun (?NO_PAYLOAD) ->
                                  #{Watcher => ok};
                              (Watchers) ->
                                  Watchers#{Watcher => ok}
                          end, Watched, KWRevIdx)
                end, RevIdx1, KeepWhile),
    State1 = set_reverse_index(State, RevIdx2),
    State1.

remove_path(State, Path) ->
    %% We update the reverse index first before the conditions map because
    %% `update_reverse_index/3' needs to read the initial conditions map
    %% before `Path' is removed.
    State1 = update_reverse_index(State, Path, #{}),
    %% We can now remover `Path' from the conditions map too.
    Conds = get_conds(State1),
    Conds1 = maps:remove(Path, Conds),
    State2 = set_conds(State1, Conds1),
    State2.
