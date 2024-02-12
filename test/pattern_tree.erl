%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(pattern_tree).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_tree.hrl").
-include("test/helpers.hrl").

is_empty_test() ->
    Empty = khepri_pattern_tree:empty(),
    ?assert(khepri_pattern_tree:is_empty(Empty)),
    NonEmpty = put_new(Empty, [stock, wood, <<"oak">>], 100),
    ?assertNot(khepri_pattern_tree:is_empty(NonEmpty)),
    ok.

foreach_iterates_over_all_payloads_test() ->
    PathPattern1 = [stock, wood, <<"oak">>],
    Payload1 = 100,
    PathPattern2 = [stock, wood, <<"birch">>],
    Payload2 = 200,
    PathPattern3 = [stock, wood, <<"maple">>],
    Payload3 = 300,
    PatternTree0 = khepri_pattern_tree:empty(),
    PatternTree1 = put_new(PatternTree0, PathPattern1, Payload1),
    PatternTree2 = put_new(PatternTree1, PathPattern2, Payload2),
    PatternTree3 = put_new(PatternTree2, PathPattern3, Payload3),
    ok = khepri_pattern_tree:foreach(
           PatternTree3,
           fun(PathPattern, Payload) ->
               self() ! {entry, PathPattern, Payload}
           end),
    %% NOTE: the ordering in which tree nodes are visited by foreach is not
    %% guaranteed.
    ?assertReceived({entry, PathPattern1, Payload1}),
    ?assertReceived({entry, PathPattern2, Payload2}),
    ?assertReceived({entry, PathPattern3, Payload3}),
    ?assertNotReceived(_),
    ok.

update_passes_current_payload_test() ->
    PathPattern = [stock, wood, <<"oak">>],
    PatternTree0 = khepri_pattern_tree:empty(),
    Payload1 = 100,
    PatternTree1 = khepri_pattern_tree:update(
                     PatternTree0, PathPattern,
                     fun(Payload) ->
                             ?assertEqual(?NO_PAYLOAD, Payload),
                             Payload1
                     end),
    _ = khepri_pattern_tree:update(
          PatternTree1, PathPattern,
          fun(Payload) ->
                  ?assertEqual(Payload1, Payload)
          end),
    ok.

fold_finds_all_patterns_matching_a_path_test() ->
    TreePayload = #p_data{data = 100},
    Tree = lists:foldl(
             fun(Path, Tree0) ->
                 {ok, Tree, _AppliedChanges, _NodeProps} =
                 khepri_tree:insert_or_update_node(
                   Tree0, Path, TreePayload, #{}, #{}),
                 Tree
             end, #tree{}, [[stock, wood, <<"oak">>],
                            [stock, wood, <<"birch">>]]),
    PathPatterns =
    [[stock, wood, <<"oak">>],                               %% 1
     [stock, wood, <<"birch">>],                             %% 2
     [stock, wood, <<"oak">>],                               %% 3
     [stock, wood, #if_has_data{}],                          %% 4
     [stock, wood, #if_child_list_length{count = 0}],        %% 5
     [stock, #if_child_list_length{count = 2}],              %% 6
     [stock, wood, #if_name_matches{regex = "^b"}]],         %% 7
    PatternTree0 = lists:foldl(
                     fun({Index, PathPattern}, Acc) ->
                             khepri_pattern_tree:update(
                               Acc, PathPattern,
                               fun (?NO_PAYLOAD) ->
                                       [Index];
                                   (Indices) ->
                                       [Index | Indices]
                               end)
                     end,
                     khepri_pattern_tree:empty(),
                     lists_enumerate(PathPatterns)),
    PatternTree = khepri_pattern_tree:compile(PatternTree0),
    MatchingIndices = fun(Path) ->
                              khepri_pattern_tree:fold(
                                PatternTree, Tree, Path,
                                fun(_PathPattern, Indices, Acc) ->
                                    Acc ++ Indices
                                end, [])
                      end,
    ?assertListsEqual(
      [1, 3, 4, 5, 6],
      MatchingIndices([stock, wood, <<"oak">>])),
    ?assertListsEqual(
      [2, 4, 5, 6, 7],
      MatchingIndices([stock, wood, <<"birch">>])),
    ?assertListsEqual(
      [6],
      MatchingIndices([stock, wood, <<"maple">>])),
    ok.

%% Helper functions.

-spec put_new(PatternTree, PathPattern, Payload) -> Ret when
      PatternTree :: khepri_pattern_tree:tree(Payload),
      PathPattern :: khepri_path:native_pattern(),
      Payload :: term(),
      Ret :: khepri_pattern_tree:tree(Payload).

put_new(PatternTree, PathPattern, Data) ->
    khepri_pattern_tree:update(
      PatternTree, PathPattern,
      fun(Payload) ->
              ?assertEqual(?NO_PAYLOAD, Payload),
              Data
      end).

%% TODO: When Erlang/OTP 25 is the minimum supported version, remove this
%% function and replace its callers with `lists:enumerate/1'.
lists_enumerate(List) ->
    lists_enumerate(List, 1, []).

lists_enumerate([Head | Rest], Index, Acc) ->
    lists_enumerate(Rest, Index + 1, [{Index, Head} | Acc]);
lists_enumerate([], _Index, Acc) ->
    lists:reverse(Acc).
