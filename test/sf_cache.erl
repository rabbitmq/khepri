%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(sf_cache).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").

%% FIXME: compile:forms/2 is incorrectly specified and doesn't accept
%% assembly. This breaks compile/1 and causes a cascade of errors.
%%
%% The following basically disables Dialyzer for this module unfortunately...
%% This can be removed once we start using Erlang 25 to run Dialyzer.
-dialyzer({nowarn_function, [my_fun_module/1,
                             modified_module_causes_cache_miss_test/0]}).

standalone_fun_is_cached_test() ->
    Fun = fun() -> ok end,

    #{module := Module,
      name := Name,
      arity := Arity,
      type := local,
      new_uniq := Checksum} = maps:from_list(erlang:fun_info(Fun)),
    Options = #{},
    Key = khepri_fun:standalone_fun_cache_key(
            Module, Name, Arity, Checksum, Options),

    StandaloneFun1 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry1 = khepri_fun_cache:get(Key, undefined),
    ?assertMatch(#standalone_fun{}, StandaloneFun1),
    ?assertMatch(#{standalone_fun := StandaloneFun1}, CacheEntry1),
    #{counters := Counters} = CacheEntry1,
    ?assertEqual(0, counters:get(Counters, 1)),

    StandaloneFun2 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry2 = khepri_fun_cache:get(Key, undefined),
    ?assertEqual(StandaloneFun1, StandaloneFun2),
    ?assertEqual(CacheEntry1, CacheEntry2),
    ?assertEqual(1, counters:get(Counters, 1)),

    StandaloneFun3 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry3 = khepri_fun_cache:get(Key, undefined),
    ?assertEqual(StandaloneFun1, StandaloneFun3),
    ?assertEqual(CacheEntry1, CacheEntry3),
    ?assertEqual(2, counters:get(Counters, 1)).

kept_fun_is_cached_test() ->
    Fun = fun() -> ok end,

    #{module := Module,
      name := Name,
      arity := Arity,
      type := local,
      new_uniq := Checksum} = maps:from_list(erlang:fun_info(Fun)),
    Options = #{should_process_function =>
                fun(_Module, _Function, _Arity, _FromModule) -> false end},
    Key = khepri_fun:standalone_fun_cache_key(
            Module, Name, Arity, Checksum, Options),

    StandaloneFun1 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry1 = khepri_fun_cache:get(Key, undefined),
    ?assertEqual(Fun, StandaloneFun1),
    ?assertMatch(#{fun_kept := true}, CacheEntry1),
    #{counters := Counters} = CacheEntry1,
    ?assertEqual(0, counters:get(Counters, 1)),

    StandaloneFun2 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry2 = khepri_fun_cache:get(Key, undefined),
    ?assertEqual(StandaloneFun1, StandaloneFun2),
    ?assertEqual(CacheEntry1, CacheEntry2),
    ?assertEqual(1, counters:get(Counters, 1)),

    StandaloneFun3 = khepri_fun:to_standalone_fun(Fun, Options),
    CacheEntry3 = khepri_fun_cache:get(Key, undefined),
    ?assertEqual(StandaloneFun1, StandaloneFun3),
    ?assertEqual(CacheEntry1, CacheEntry3),
    ?assertEqual(2, counters:get(Counters, 1)).

different_options_means_different_cache_entries_test() ->
    Fun = fun() -> ok end,

    Options1 = #{},
    Options2 = #{should_process_function =>
                 fun(_Module, _Function, _Arity, _FromModule) -> false end},

    StandaloneFun1 = khepri_fun:to_standalone_fun(Fun, Options1),
    StandaloneFun2 = khepri_fun:to_standalone_fun(Fun, Options2),
    ?assertMatch(#standalone_fun{}, StandaloneFun1),
    ?assertEqual(Fun, StandaloneFun2).

my_fun_module(Version) ->
    Module = my_fun,
    Asm = {Module, %% Module
           [{module_info,0}, %% Exports
            {module_info,1},
            {version,0}],
           [], %% Attributes
           [
            {function, version, 0, 2,
             [
              {label, 1},
              {func_info, {atom, Module}, {atom, version},0},
              {label, 2},
              {move, {integer, Version}, {x, 0}},
              return
             ]},
            {function, module_info, 0, 4,
             [
              {label, 3},
              {func_info, {atom, Module}, {atom, module_info}, 0},
              {label, 4},
              {move, {atom, Module}, {x, 0}},
              {call_ext_only, 1, {extfunc, erlang, get_module_info, 1}}
             ]},
            {function, module_info, 1, 6,
             [
              {label, 5},
              {func_info, {atom, Module}, {atom, module_info}, 1},
              {label, 6},
              {move, {x, 0}, {x, 1}},
              {move, {atom, Module}, {x, 0}},
              {call_ext_only, 2, {extfunc, erlang, get_module_info, 2}}
             ]}
           ], %% Functions
           7 %% Label
          },
    khepri_fun:compile(Asm).

modified_module_causes_cache_miss_test() ->
    {Module, Beam1} = my_fun_module(1),
    {Module, Beam2} = my_fun_module(2),

    Options = #{},

    khepri_fun:override_object_code(Module, Beam1),
    ?assertEqual({Module, Beam1, ""}, khepri_fun:get_object_code(Module)),
    ?assertEqual({module, Module}, code:load_binary(Module, "", Beam1)),
    ?assert(erlang:function_exported(Module, version, 0)),
    Fun1 = fun Module:version/0,
    #{module := Module,
      name := Name1,
      arity := Arity1,
      type := external} = maps:from_list(erlang:fun_info(Fun1)),
    Checksum1 = Module:module_info(md5),
    Key1 = khepri_fun:standalone_fun_cache_key(
             Module, Name1, Arity1, Checksum1, Options),

    StandaloneFun1 = khepri_fun:to_standalone_fun(Fun1, Options),
    CacheEntry1 = khepri_fun_cache:get(Key1, undefined),
    ?assertMatch(#standalone_fun{}, StandaloneFun1),
    ?assertEqual(1, khepri_fun:exec(StandaloneFun1, [])),
    #{counters := Counters1} = CacheEntry1,
    ?assertEqual(0, counters:get(Counters1, 1)),

    true = code:delete(Module),
    _ = code:purge(Module),

    khepri_fun:override_object_code(Module, Beam2),
    ?assertEqual({Module, Beam2, ""}, khepri_fun:get_object_code(Module)),
    ?assertEqual({module, Module}, code:load_binary(Module, "", Beam2)),
    ?assert(erlang:function_exported(Module, version, 0)),
    Fun2 = fun Module:version/0,
    #{module := Module,
      name := Name2,
      arity := Arity2,
      type := external} = maps:from_list(erlang:fun_info(Fun2)),
    Checksum2 = Module:module_info(md5),
    Key2 = khepri_fun:standalone_fun_cache_key(
             Module, Name2, Arity2, Checksum2, Options),
    ?assertEqual(Name1, Name2),
    ?assertEqual(Arity1, Arity2),
    ?assertNotEqual(Checksum1, Checksum2),

    StandaloneFun2 = khepri_fun:to_standalone_fun(Fun2, Options),
    CacheEntry2 = khepri_fun_cache:get(Key2, undefined),
    ?assertMatch(#standalone_fun{}, StandaloneFun2),
    ?assertEqual(2, khepri_fun:exec(StandaloneFun2, [])),
    #{counters := Counters2} = CacheEntry2,
    ?assertEqual(0, counters:get(Counters2, 1)),

    true = code:delete(Module),
    _ = code:purge(Module).

lru_cache_evicts_least_recently_used_item_test() ->
    %% In test mode, the cache capacity is 3.
    khepri_fun_cache:clear(),
    khepri_fun_cache:put(1, one),
    khepri_fun_cache:put(2, two),
    khepri_fun_cache:put(3, three),
    ?assertEqual(one, khepri_fun_cache:get(1, undefined)),
    ?assertEqual(two, khepri_fun_cache:get(2, undefined)),
    ?assertEqual(three, khepri_fun_cache:get(3, undefined)),
    %% 1 is evicted.
    khepri_fun_cache:put(4, four),
    ?assertEqual(four, khepri_fun_cache:get(4, undefined)),
    ?assertEqual(undefined, khepri_fun_cache:get(1, undefined)),
    ?assertEqual(two, khepri_fun_cache:get(2, undefined)),
    %% Then 3 is evicted because 2 was just accessed.
    khepri_fun_cache:put(5, five),
    ?assertEqual(undefined, khepri_fun_cache:get(3, undefined)),
    ?assertEqual(two, khepri_fun_cache:get(2, undefined)).

lru_cache_updating_element_resets_rank_test() ->
    %% Updating an existing element with a new Value resets its rank.
    khepri_fun_cache:clear(),
    khepri_fun_cache:put(1, one),
    khepri_fun_cache:put(2, two),
    khepri_fun_cache:put(3, three),
    %% 1's rank is reset.
    khepri_fun_cache:put(1, one_and_a_half),
    %% 2 is evicted.
    khepri_fun_cache:put(4, four),
    ?assertEqual(one_and_a_half, khepri_fun_cache:get(1, undefined)),
    ?assertEqual(undefined, khepri_fun_cache:get(2, undefined)),
    ?assertEqual(three, khepri_fun_cache:get(3, undefined)),
    ?assertEqual(four, khepri_fun_cache:get(4, undefined)).

lru_cache_updating_element_with_same_value_does_not_reset_rank_test() ->
    %% `put/2'-ing an element with the same Value does not reset the rank
    khepri_fun_cache:clear(),
    khepri_fun_cache:put(1, one),
    khepri_fun_cache:put(2, two),
    khepri_fun_cache:put(3, three),
    khepri_fun_cache:put(1, one),
    %% 1 is evicted.
    khepri_fun_cache:put(4, four),
    ?assertEqual(undefined, khepri_fun_cache:get(1, undefined)),
    ?assertEqual(two, khepri_fun_cache:get(2, undefined)),
    ?assertEqual(three, khepri_fun_cache:get(3, undefined)),
    ?assertEqual(four, khepri_fun_cache:get(4, undefined)).
