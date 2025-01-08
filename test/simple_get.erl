%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(simple_get).

-include_lib("eunit/include/eunit.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

get_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                 node_path => [foo],
                                                 node_is_target => true})},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

get_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

get_existing_node_with_sproc_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], fun() -> ok end)),
      ?_assertMatch(
         {ok, StandaloneFun} when ?IS_HORUS_STANDALONE_FUN(StandaloneFun),
         khepri:get(?FUNCTION_NAME, [foo]))]}.

get_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, undefined},
         khepri:get(?FUNCTION_NAME, [foo]))]}.

invalid_get_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:get(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

get_or_default_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, default},
         khepri:get_or(?FUNCTION_NAME, [foo], default))]}.

get_or_default_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, foo_value},
         khepri:get_or(?FUNCTION_NAME, [foo], default))]}.

get_or_default_on_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, default},
         khepri:get_or(?FUNCTION_NAME, [foo], default))]}.

invalid_get_or_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:get_or(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR], default))]}.

get_many_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

get_many_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => undefined,
                [baz] => baz_value}},
         khepri:get_many(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR])),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?KHEPRI_WILDCARD_STAR]}),
         khepri:get_many(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR],
           #{expect_specific_node => true}))]}.

get_many_or_default_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:get_many_or(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR], default))]}.

get_many_or_default_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertEqual(
         {ok, #{[foo] => default,
                [baz] => baz_value}},
         khepri:get_many_or(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR], default)),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?KHEPRI_WILDCARD_STAR]}),
         khepri:get_many_or(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR], default,
           #{expect_specific_node => true}))]}.

check_node_exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assert(khepri:exists(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:exists(?FUNCTION_NAME, [foo, bar])),
      ?_assertNot(khepri:exists(?FUNCTION_NAME, [baz]))]}.

check_invalid_exists_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:exists(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

check_node_has_data_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertNot(khepri:has_data(?FUNCTION_NAME, [foo]))]}.

check_node_has_data_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),
      ?_assertNot(khepri:has_data(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:has_data(?FUNCTION_NAME, [foo, bar])),
      ?_assert(khepri:has_data(?FUNCTION_NAME, [baz]))]}.

check_invalid_has_data_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:has_data(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

check_node_is_sproc_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertNot(khepri:is_sproc(?FUNCTION_NAME, [foo]))]}.

check_node_is_sproc_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], fun() -> bar_value end)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], fun() -> baz_value end)),
      ?_assertNot(khepri:is_sproc(?FUNCTION_NAME, [foo])),
      ?_assert(khepri:is_sproc(?FUNCTION_NAME, [foo, bar])),
      ?_assert(khepri:is_sproc(?FUNCTION_NAME, [baz]))]}.

check_invalid_is_sproc_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         khepri:is_sproc(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR]))]}.

count_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, 0},
         khepri:count(?FUNCTION_NAME, [foo])),
      ?_assertEqual(
         {ok, 0},
         khepri:count(
           ?FUNCTION_NAME, [foo], #{expect_specific_node => true}))]}.

count_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, 1},
         khepri:count(?FUNCTION_NAME, [foo]))]}.

count_many_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, 2},
         khepri:count(
           ?FUNCTION_NAME, [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR])),
      ?_assertEqual(
         {ok, 3},
         khepri:count(?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR]))]}.

fold_non_existing_node_test_() ->
    Fun = fun list_nodes_cb/3,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, []},
         khepri:fold(?FUNCTION_NAME, [foo], Fun, [])),
      ?_assertEqual(
         {ok, []},
         khepri:fold(
           ?FUNCTION_NAME, [foo], Fun, [], #{expect_specific_node => true}))]}.

fold_existing_node_test_() ->
    Fun = fun list_nodes_cb/3,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, [[foo]]},
         khepri:fold(?FUNCTION_NAME, [foo], Fun, []))]}.

fold_many_nodes_test_() ->
    Fun = fun list_nodes_cb/3,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, [[baz], [foo]]},
         khepri:fold(
           ?FUNCTION_NAME, [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR],
           Fun, [])),
      ?_assertEqual(
         {ok, [[baz], [foo], [foo, bar]]},
         khepri:fold(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR],
           Fun, []))]}.

crash_during_fold_test_() ->
    Fun = fun(_Path, _NodeProps, _Acc) -> throw(bug) end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertThrow(
         bug,
         khepri:fold(?FUNCTION_NAME, [foo], Fun, []))]}.

list_nodes_cb(Path, _NodeProps, List) ->
    lists:sort([Path | List]).

foreach_non_existing_node_test_() ->
    Fun = fun(Path, NodeProps) ->
                  foreach_node_cb(?FUNCTION_NAME, Path, NodeProps)
          end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:foreach(?FUNCTION_NAME, [foo], Fun)),
      ?_assertEqual({ok, #{}}, khepri_adv:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(
         ok,
         khepri:foreach(
           ?FUNCTION_NAME, [foo], Fun, #{expect_specific_node => true})),
      ?_assertEqual({ok, #{}}, khepri_adv:get_many(?FUNCTION_NAME, "**"))]}.

foreach_existing_node_test_() ->
    Fun = fun(Path, NodeProps) ->
                  foreach_node_cb(?FUNCTION_NAME, Path, NodeProps)
          end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         ok,
         khepri:foreach(?FUNCTION_NAME, [foo], Fun)),
      ?_assertEqual(
        {ok, #{[foo] => #{data => foreach_value1,
                           payload_version => 2}}},
        begin
            lists:foldl(
              fun
                  (_, {ok, #{[foo] := #{data := foreach_value1}}} = Ret) ->
                      Ret;
                  (_, {ok, _}) ->
                      timer:sleep(200),
                      khepri_adv:get_many(?FUNCTION_NAME, "**")
              end,
              khepri_adv:get_many(?FUNCTION_NAME, "**"),
              lists:seq(1, 120))
        end)]}.

foreach_many_nodes_test_() ->
    Fun = fun(Path, NodeProps) ->
                  foreach_node_cb(?FUNCTION_NAME, Path, NodeProps)
          end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         ok,
         khepri:foreach(
           ?FUNCTION_NAME, [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Fun)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foreach_value1,
                           payload_version => 2},
                [foo, bar] => #{data => bar_value,
                                payload_version => 1},
                [baz] => #{data => foreach_value1,
                           payload_version => 2}}},
         begin
             lists:foldl(
               fun
                   (_, {ok, #{[foo] := #{data := foreach_value1},
                              [baz] := #{data := foreach_value1}}} = Ret) ->
                       Ret;
                   (_, {ok, _}) ->
                       timer:sleep(200),
                       khepri_adv:get_many(?FUNCTION_NAME, "**")
               end,
               khepri_adv:get_many(?FUNCTION_NAME, "**"),
               lists:seq(1, 120))
         end),
      ?_assertEqual(
         ok,
         khepri:foreach(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR], Fun)),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foreach_value2,
                           payload_version => 3},
                [foo, bar] => #{data => foreach_value1,
                                payload_version => 2},
                [baz] => #{data => foreach_value2,
                           payload_version => 3}}},
         begin
             lists:foldl(
               fun
                   (_, {ok, #{[foo] := #{data := foreach_value2},
                              [foo, bar] := #{data := foreach_value1},
                              [baz] := #{data := foreach_value2}}} = Ret) ->
                       Ret;
                   (_, {ok, _}) ->
                       timer:sleep(200),
                       khepri_adv:get_many(?FUNCTION_NAME, "**")
               end,
               khepri_adv:get_many(?FUNCTION_NAME, "**"),
               lists:seq(1, 120))
         end)]}.

foreach_node_cb(StoreId, Path, #{data := foreach_value1}) ->
    ok = khepri:put(
           StoreId, Path, foreach_value2, #{async => true});
foreach_node_cb(StoreId, Path, _NodeProps) ->
    ok = khepri:put(
           StoreId, Path, foreach_value1, #{async => true}).

map_non_existing_node_test_() ->
    Fun = fun map_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:map(?FUNCTION_NAME, [foo], Fun)),
      ?_assertEqual(
         {ok, #{}},
         khepri:map(
           ?FUNCTION_NAME, [foo], Fun, #{expect_specific_node => true}))]}.

map_existing_node_test_() ->
    Fun = fun map_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{[foo] => {data, foo_value}}},
         khepri:map(?FUNCTION_NAME, [foo], Fun))]}.

map_many_nodes_test_() ->
    Fun = fun map_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, #{[foo] => nodata,
                [baz] => {data, baz_value}}},
         khepri:map(
           ?FUNCTION_NAME, [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Fun)),
      ?_assertEqual(
         {ok, #{[foo] => nodata,
                [foo, bar] => {data, bar_value},
                [baz] => {data, baz_value}}},
         khepri:map(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR], Fun))]}.

map_node_cb(_Path, #{data := Data}) ->
    {data, Data};
map_node_cb(_Path, _NodeProps) ->
    nodata.

filter_non_existing_node_test_() ->
    Pred = fun filter_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, #{}},
         khepri:filter(?FUNCTION_NAME, [foo], Pred)),
      ?_assertEqual(
         {ok, #{}},
         khepri:filter(
           ?FUNCTION_NAME, [foo], Pred, #{expect_specific_node => true}))]}.

filter_existing_node_test_() ->
    Pred = fun filter_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, #{}},
         khepri:filter(?FUNCTION_NAME, [foo], Pred))]}.

filter_many_nodes_test_() ->
    Pred = fun filter_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [baz], baz_value)),

      ?_assertEqual(
         {ok, #{[baz] => baz_value}},
         khepri:filter(
           ?FUNCTION_NAME, [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Pred)),
      ?_assertEqual(
         {ok, #{[baz] => baz_value}},
         khepri:filter(
           ?FUNCTION_NAME, [?KHEPRI_WILDCARD_STAR_STAR], Pred))]}.

filter_node_cb([_ | _] = Path, _NodeProps) ->
    lists:last(Path) =:= baz;
filter_node_cb(_Path, _NodeProps) ->
    false.
