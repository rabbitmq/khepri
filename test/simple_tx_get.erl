%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(simple_tx_get).

-include_lib("eunit/include/eunit.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

-dialyzer([{no_return,
            [crash_during_fold_test_/0]}]).

get_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok,
          {error, ?khepri_error(node_not_found, #{node_name => foo,
                                                  node_path => [foo],
                                                  node_is_target => true})}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_with_sproc_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], fun() -> ok end)),
      ?_assertMatch(
         {ok, {ok, StoredFun}} when ?IS_HORUS_STANDALONE_FUN(StoredFun),
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, {ok, undefined}},
         begin
             Fun = fun() ->
                           khepri_tx:get([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_get_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:get([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, default}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_on_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, foo_value}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_or_default_on_existing_node_with_no_payload_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, {ok, default}},
         begin
             Fun = fun() ->
                           khepri_tx:get_or([foo], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

invalid_get_or_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:get_or([?KHEPRI_WILDCARD_STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
         {ok,
          {ok, #{[foo] => undefined,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => undefined,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?KHEPRI_WILDCARD_STAR]}),
         begin
             Fun = fun() ->
                           khepri_tx:get_many(
                             [?KHEPRI_WILDCARD_STAR],
                             #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

get_many_or_default_non_existing_nodes_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or(
                             [?KHEPRI_WILDCARD_STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
         {ok,
          {ok, #{[foo] => default,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or(
                             [?KHEPRI_WILDCARD_STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => default,
                 [baz] => baz_value}}},
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or(
                             [?KHEPRI_WILDCARD_STAR], default)
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := [?KHEPRI_WILDCARD_STAR]}),
         begin
             Fun = fun() ->
                           khepri_tx:get_many_or(
                             [?KHEPRI_WILDCARD_STAR], default,
                             #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_exists_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo, bar], bar_value)),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:exists([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:exists([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_exists_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:exists([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_has_data_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:has_data([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_has_data_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:has_data([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_node_is_sproc_on_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
      ?_assertEqual(
         {ok, false},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([foo, bar])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, true},
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([baz])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

check_invalid_is_sproc_call_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertError(
         ?khepri_exception(
            possibly_matching_many_nodes_denied,
            #{path := _}),
         begin
             Fun = fun() ->
                           khepri_tx:is_sproc([?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

count_non_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, 0}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, {ok, 0}},
         begin
             Fun = fun() ->
                           khepri_tx:count(
                             [foo], #{expect_specific_node => true})
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

count_existing_node_test_() ->
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, 1}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, ro)
         end),
      ?_assertEqual(
         {ok, {ok, 1}},
         begin
             Fun = fun() ->
                           khepri_tx:count([foo])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

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
         {ok, {ok, 2}},
         begin
             Fun = fun() ->
                           khepri_tx:count(
                             [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end),
      ?_assertEqual(
         {ok, {ok, 3}},
         begin
             Fun = fun() ->
                           khepri_tx:count([?KHEPRI_WILDCARD_STAR_STAR])
                   end,
             khepri:transaction(?FUNCTION_NAME, Fun, rw)
         end)]}.

fold_non_existing_node_test_() ->
    Fun = fun list_nodes_cb/3,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, []}},
         begin
             Tx = fun() ->
                          khepri_tx:fold([foo], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, {ok, []}},
         begin
             Tx = fun() ->
                          khepri_tx:fold(
                            [foo], Fun, [], #{expect_specific_node => true})
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

fold_existing_node_test_() ->
    Fun = fun list_nodes_cb/3,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, [[foo]]}},
         begin
             Tx = fun() ->
                          khepri_tx:fold([foo], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, ro)
         end),
      ?_assertEqual(
         {ok, {ok, [[foo]]}},
         begin
             Tx = fun() ->
                          khepri_tx:fold([foo], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

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
         {ok, {ok, [[baz], [foo]]}},
         begin
             Tx = fun() ->
                          khepri_tx:fold(
                            [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR],
                            Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, {ok, [[baz], [foo], [foo, bar]]}},
         begin
             Tx = fun() ->
                          khepri_tx:fold([?KHEPRI_WILDCARD_STAR_STAR], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

abort_during_fold_test_() ->
    Fun = fun(_Path, _NodeProps, _Acc) -> throw(bug) end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertThrow(
         bug,
         begin
             Tx = fun() ->
                          khepri_tx:fold([foo], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

crash_during_fold_test_() ->
    Fun = fun(_Path, _NodeProps, _Acc) -> khepri_tx:abort(abort_tx) end,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {error, abort_tx},
         begin
             Tx = fun() ->
                          khepri_tx:fold([foo], Fun, [])
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

list_nodes_cb(Path, _NodeProps, List) ->
    lists:sort([Path | List]).

foreach_non_existing_node_test_() ->
    Fun = fun foreach_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, ok},
         begin
             Tx = fun() ->
                          khepri_tx:foreach([foo], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual({ok, #{}}, khepri_adv:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(
         {ok, ok},
         begin
             Tx = fun() ->
                          khepri_tx:foreach(
                            [foo], Fun, #{expect_specific_node => true})
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual({ok, #{}}, khepri_adv:get_many(?FUNCTION_NAME, "**"))]}.

foreach_existing_node_test_() ->
    Fun = fun foreach_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertError(
         ?khepri_exception(denied_update_in_readonly_tx, #{}),
         begin
             Tx = fun() ->
                          khepri_tx:foreach([foo], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, ro)
         end),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foo_value,
                           payload_version => 1}}},
         khepri_adv:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(
         {ok, ok},
         begin
             Tx = fun() ->
                          khepri_tx:foreach([foo], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foreach_value1,
                           payload_version => 2}}},
         khepri_adv:get_many(?FUNCTION_NAME, "**"))]}.

foreach_many_nodes_test_() ->
    Fun = fun foreach_node_cb/2,
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
         {ok, ok},
         begin
             Tx = fun() ->
                          khepri_tx:foreach(
                            [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foreach_value1,
                           payload_version => 2},
                [foo, bar] => #{data => bar_value,
                                payload_version => 1},
                [baz] => #{data => foreach_value1,
                           payload_version => 2}}},
         khepri_adv:get_many(?FUNCTION_NAME, "**")),
      ?_assertEqual(
         {ok, ok},
         begin
             Tx = fun() ->
                          khepri_tx:foreach([?KHEPRI_WILDCARD_STAR_STAR], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, #{[foo] => #{data => foreach_value2,
                           payload_version => 3},
                [foo, bar] => #{data => foreach_value1,
                                payload_version => 2},
                [baz] => #{data => foreach_value2,
                           payload_version => 3}}},
         khepri_adv:get_many(?FUNCTION_NAME, "**"))]}.

foreach_node_cb(Path, #{data := foreach_value1}) ->
    ok = khepri_tx:put(Path, foreach_value2);
foreach_node_cb(Path, _NodeProps) ->
    ok = khepri_tx:put(Path, foreach_value1).

map_non_existing_node_test_() ->
    Fun = fun map_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Tx = fun() ->
                          khepri_tx:map([foo], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Tx = fun() ->
                          khepri_tx:map(
                            [foo], Fun, #{expect_specific_node => true})
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

map_existing_node_test_() ->
    Fun = fun map_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, #{[foo] => {data, foo_value}}}},
         begin
             Tx = fun() ->
                          khepri_tx:map([foo], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

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
         {ok,
          {ok, #{[foo] => nodata,
                 [baz] => {data, baz_value}}}},
         begin
             Tx = fun() ->
                          khepri_tx:map(
                            [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok,
          {ok, #{[foo] => nodata,
                 [foo, bar] => {data, bar_value},
                 [baz] => {data, baz_value}}}},
         begin
             Tx = fun() ->
                          khepri_tx:map(
                            [?KHEPRI_WILDCARD_STAR_STAR], Fun)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

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
         {ok, {ok, #{}}},
         begin
             Tx = fun() ->
                          khepri_tx:filter([foo], Pred)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Tx = fun() ->
                          khepri_tx:filter(
                            [foo], Pred, #{expect_specific_node => true})
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

filter_existing_node_test_() ->
    Pred = fun filter_node_cb/2,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [?_assertEqual(
         ok,
         khepri:create(?FUNCTION_NAME, [foo], foo_value)),
      ?_assertEqual(
         {ok, {ok, #{}}},
         begin
             Tx = fun() ->
                          khepri_tx:filter([foo], Pred)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

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
         {ok, {ok, #{[baz] => baz_value}}},
         begin
             Tx = fun() ->
                          khepri_tx:filter(
                            [?THIS_KHEPRI_NODE, ?KHEPRI_WILDCARD_STAR], Pred)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end),
      ?_assertEqual(
         {ok, {ok, #{[baz] => baz_value}}},
         begin
             Tx = fun() ->
                          khepri_tx:filter(
                            [?KHEPRI_WILDCARD_STAR_STAR], Pred)
                  end,
             khepri:transaction(?FUNCTION_NAME, Tx, rw)
         end)]}.

filter_node_cb([_ | _] = Path, _NodeProps) ->
    lists:last(Path) =:= baz;
filter_node_cb(_Path, _NodeProps) ->
    false.
