%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers
%% to Broadcom Inc. and/or its subsidiaries.
%%

-module(prefix_tree).

-include_lib("eunit/include/eunit.hrl").
-include("src/khepri_payload.hrl").

update_passes_current_payload_test() ->
    Path = [stock, wood, <<"oak">>],
    Payload = 100,
    Tree0 = khepri_prefix_tree:empty(),
    Tree1 = khepri_prefix_tree:update(
              fun(Payload0) ->
                      ?assertEqual(Payload0, ?NO_PAYLOAD),
                      Payload
              end, Path, Tree0),
    _ = khepri_prefix_tree:update(
          fun(Payload1) ->
                  ?assertEqual(Payload, Payload1),
                  ?NO_PAYLOAD
          end, Path, Tree1),
    ok.

deletion_prunes_empty_branches_test() ->
    %% When deleting a node in the tree, the branch leading to that node is
    %% removed while non-empty.
    Path = [stock, wood, <<"oak">>],
    Payload = 100,
    Tree0 = khepri_prefix_tree:from_map(#{Path => Payload}),

    %% This branch has a child (`<<"oak">>') so it will not be pruned.
    Tree1 = khepri_prefix_tree:update(
              fun(Payload0) ->
                      ?assertEqual(Payload0, ?NO_PAYLOAD),
                      ?NO_PAYLOAD
              end, [stock, wood], Tree0),
    ?assertNotEqual(Tree1, khepri_prefix_tree:empty()),

    Tree2 = khepri_prefix_tree:update(
              fun(Payload0) ->
                      ?assertEqual(Payload0, Payload),
                      ?NO_PAYLOAD
              end, Path, Tree1),
    ?assertEqual(Tree2, khepri_prefix_tree:empty()),

    %% If an update doesn't store a payload then the branch is pruned. So this
    %% update acts as a no-op and the tree remains empty:
    Tree3 = khepri_prefix_tree:update(
              fun(Payload0) ->
                      ?assertEqual(Payload0, ?NO_PAYLOAD),
                      ?NO_PAYLOAD
              end, Path, Tree2),
    ?assertEqual(Tree3, khepri_prefix_tree:empty()),

    ok.

find_path_test() ->
    Path1 = [stock, wood, <<"oak">>],
    Path2 = [stock, wood, <<"birch">>],
    Path3 = [stock, metal, <<"iron">>],
    Tree = khepri_prefix_tree:from_map(
             #{Path1 => 100, Path2 => 150, Path3 => 10}),

    ?assertEqual({ok, 100}, khepri_prefix_tree:find_path(Path1, Tree)),
    ?assertEqual({ok, 150}, khepri_prefix_tree:find_path(Path2, Tree)),
    ?assertEqual({ok, 10}, khepri_prefix_tree:find_path(Path3, Tree)),

    ?assertEqual(
      error,
      khepri_prefix_tree:find_path(
        [stock, wood, <<"oak">>, <<"leaves">>], Tree)),
    ?assertEqual(error, khepri_prefix_tree:find_path([stock, wood], Tree)),
    ?assertEqual(error, khepri_prefix_tree:find_path([stock, metal], Tree)),

    ?assertEqual(
      error,
      khepri_prefix_tree:find_path(Path1, khepri_prefix_tree:empty())),

    ok.

fold_prefixes_of_test() ->
    Path1 = [stock, wood, <<"oak">>],
    Path2 = [stock, wood, <<"birch">>],
    Path3 = [stock, metal, <<"iron">>],
    Tree = khepri_prefix_tree:from_map(
             #{Path1 => 100, Path2 => 150, Path3 => 10}),

    ?assertEqual([100], collect_prefixes_of(Path1, Tree)),
    ?assertEqual([100, 150], collect_prefixes_of([stock, wood], Tree)),
    ?assertEqual([10], collect_prefixes_of([stock, metal], Tree)),
    ?assertEqual([10, 100, 150], collect_prefixes_of([stock], Tree)),
    ?assertEqual([10, 100, 150], collect_prefixes_of([], Tree)),

    ?assertEqual(
      [],
      collect_prefixes_of([stock, wood, <<"oak">>, <<"leaves">>], Tree)),

    ok.

collect_prefixes_of(Path, Tree) ->
    Payloads = khepri_prefix_tree:fold_prefixes_of(
                 fun(Payload, Acc) -> [Payload | Acc] end, [], Path, Tree),
    lists:sort(Payloads).
