%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Condition support.
%%
%% Conditions can be used in path patterns and `keep_while' conditions. They
%% allow to point to a specific node only if conditions are met, or to match
%% several tree nodes with a single path pattern.
%%
%% A condition is an Erlang record defining a specific property. Some of them
%% have arguments to further define the condition.
%%
%% Path components (atoms and binaries) also act as conditions which check
%% equality with the path of the tested node. This can be useful for
%% conditions which compose other conditions: {@link if_not()},
%% {@link if_all()} and {@link if_any()}.
%%
%% Example:
%%
%% ```
%% %% Matches `[stock, wood, <<"birch">>]' but not `[stock, wood, <<"oak">>]'
%% [stock, wood, #if_not{condition = <<"oak">>}]
%% '''
%%
%% All supported conditions are described in the <a href="#types">Data Types
%% section</a>.

-module(khepri_condition).

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").

-type comparison_op(Type) :: {eq, Type} |
                             {ne, Type} |
                             {lt, Type} |
                             {le, Type} |
                             {gt, Type} |
                             {ge, Type}.
%% Comparison operator in some {@link condition()}.

-type if_name_matches() :: #if_name_matches{}.
%% Condition. Evaluates to true if the name of the tested node matches the
%% condition pattern.
%%
%% Record fields:
%% <ul>
%% <li>`regex': defines the condition pattern. It can be either:
%% <ul>
%% <li>a regular expression</li>
%% <li>the atom `any' to match any node names; the equivalent of the `".*"'
%% regular expression but more efficient</li>
%% </ul></li>
%% </ul>
%%
%% Example:
%% ```
%% #if_name_matches{regex = "^user_"}.
%% #if_name_matches{regex = any}.
%% '''

-type if_path_matches() :: #if_path_matches{}.
%% Condition. Evaluates to true if the name of the tested node matches the
%% condition pattern. If it does not match, child node names are tested
%% recursively.
%%
%% Record fields:
%% <ul>
%% <li>`regex': defines the condition pattern. It can be either:
%% <ul>
%% <li>a regular expression</li>
%% <li>the atom `any' to match any node names; the equivalent of the `".*"'
%% regular expression but more efficient</li>
%% </ul></li>
%% </ul>
%%
%% Example:
%% ```
%% #if_path_matches{regex = "^user_"}.
%% #if_path_matches{regex = any}.
%% '''

-type if_has_payload() :: #if_has_payload{}.
%% Condition. Evaluates to true if the tested node has any kind of payload.
%%
%% Record fields:
%% <ul>
%% <li>`has_payload': boolean set to the expected presence of any kind of
%% payload.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_has_payload{has_payload = true}.
%% '''

-type if_has_data() :: #if_has_data{}.
%% Condition. Evaluates to true if the tested node's data payload presence
%% corresponds to the expected state.
%%
%% Record fields:
%% <ul>
%% <li>`has_data': boolean set to the expected presence of a data
%% payload.</li>
%% </ul>
%%
%% Data absence is either no payload or a non-data type of payload.
%%
%% Example:
%% ```
%% #if_has_data{has_data = false}.
%% '''

-type if_has_sproc() :: #if_has_sproc{}.
%% Condition. Evaluates to true if the tested node's stored procedure payload
%% presence corresponds to the expected state.
%%
%% Record fields:
%% <ul>
%% <li>`has_sproc': boolean set to the expected presence of a stored procedure
%% payload.</li>
%% </ul>
%%
%% Stored procedure absence is either no payload or a non-sproc type of
%% payload.
%%
%% Example:
%% ```
%% #if_has_sproc{has_sproc = false}.
%% '''

-type if_data_matches() :: #if_data_matches{}.
%% Condition. Evaluates to true if the tested node has a data payload and the
%% data payload term matches the given `pattern' and all `conditions' evaluates
%% to true.
%%
%% Record fields:
%% <ul>
%% <li>`pattern': an ETS-like match pattern. The match pattern can define
%% variables to be used in the `conditions' below.</li>
%% <li>`conditions': a list of guard expressions. All guard expressions must
%% evaluate to true to consider a match. The default is an empty list of
%% conditions which means that only the pattern matching is
%% considered.</li>
%% </ul>
%%
%% Examples:
%% ```
%% %% The data must be of the form `{user, _}', so a tuple of arity 2 with the
%% %% first element being the `user' atom. The second element can be anything.
%% #if_data_matches{pattern = {user, '_'}}.
%% '''
%% ```
%% %% The data must be of the form `{age, Age}' and `Age' must be an
%% %% integer greater than or equal to 18.
%% #if_data_matches{pattern = {age, '$1'},
%%                  conditions = [{is_integer, '$1'},
%%                                {'>=', '$1', 18}]}.
%% '''
%%
%% See <a href="https://www.erlang.org/doc/apps/erts/match_spec.html">Match
%% Specifications in Erlang</a> for a detailed documentation of how it works.

-type if_node_exists() :: #if_node_exists{}.
%% Condition. Evaluates to true if the tested node existence corresponds to
%% the expected state.
%%
%% Record fields:
%% <ul>
%% <li>`exists': boolean set to the expected presence of the node.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_node_exists{exists = false}.
%% '''

-type if_payload_version() :: #if_payload_version{}.
%% Condition. Evaluates to true if the tested node's payload version
%% corresponds to the expected value.
%%
%% Record fields:
%% <ul>
%% <li>`version': integer or {@link comparison_op()} to compare to the actual
%% payload version.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_payload_version{version = 1}.
%% #if_payload_version{version = {gt, 10}}.
%% '''

-type if_child_list_version() :: #if_child_list_version{}.
%% Condition. Evaluates to true if the tested node's child list version
%% corresponds to the expected value.
%%
%% Record fields:
%% <ul>
%% <li>`version': integer or {@link comparison_op()} to compare to the actual
%% child list version.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_child_list_version{version = 1}.
%% #if_child_list_version{version = {gt, 10}}.
%% '''

-type if_child_list_length() :: #if_child_list_length{}.
%% Condition. Evaluates to true if the tested node's child list size
%% corresponds to the expected value.
%%
%% Record fields:
%% <ul>
%% <li>`version': integer or {@link comparison_op()} to compare to the actual
%% child list child.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_child_list_length{count = 1}.
%% #if_child_list_length{count = {gt, 10}}.
%% '''

-type if_not() :: #if_not{}.
%% Condition. Evaluates to true if the inner condition evaluates to false.
%%
%% Record fields:
%% <ul>
%% <li>`condition': the inner condition to evaluate.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_not{condition = #if_name_matches{regex = "^a"}}.
%% '''

-type if_all() :: #if_all{}.
%% Condition. Evaluates to true if all inner conditions evaluate to true.
%%
%% Record fields:
%% <ul>
%% <li>`conditions': a list of inner conditions to evaluate.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_all{conditions = [#if_name_matches{regex = "^a"},
%%                       #if_has_data{has_data = true}]}.
%% '''

-type if_any() :: #if_any{}.
%% Condition. Evaluates to true if any of the inner conditions evaluate to
%% true.
%%
%% Record fields:
%% <ul>
%% <li>`conditions': a list of inner conditions to evaluate.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_any{conditions = [#if_name_matches{regex = "^a"},
%%                       #if_has_data{has_data = true}]}.
%% '''

-type condition() :: if_name_matches() |
                     if_path_matches() |
                     if_has_payload() |
                     if_has_data() |
                     if_has_sproc() |
                     if_data_matches() |
                     if_node_exists() |
                     if_payload_version() |
                     if_child_list_version() |
                     if_child_list_length() |
                     if_not() |
                     if_all() |
                     if_any().
%% All supported conditions.

-type condition_using_regex() :: if_name_matches() |
                                 if_path_matches().
-type condition_using_comparison_op() :: if_payload_version() |
                                         if_child_list_version() |
                                         if_child_list_length().

-type keep_while() :: #{khepri_path:path() => condition()}.
%% An association between a path and a condition. As long as the condition
%% evaluates to true, the tree node is kept. Once the condition evaluates to
%% false, the tree node is deleted.
%%
%% If the `keep_while' conditions are false at the time of the insert, the
%% insert fails. The only exception to that is if the `keep_while' condition
%% is on the inserted node itself.
%%
%% Paths in the map can be native paths or Unix-like paths. However, having
%% two entries that resolve to the same node (one native path entry and one
%% Unix-like path entry for instance) is undefined behavior: one of them will
%% overwrite the other.
%%
%% Example:
%% ```
%% khepri:put(
%%   StoreId,
%%   [foo],
%%   Payload,
%%   #{keep_while => #{
%%     %% The node `[foo]' will be removed as soon as `[bar]' is removed
%%     %% because the condition associated with `[bar]' will not be true
%%     %% anymore.
%%     [bar] => #if_node_exists{exists = true}
%%   }}
%% ).
%% '''

-type native_keep_while() :: #{khepri_path:native_path() => condition()}.
%% An association between a native path and a condition.
%%
%% This is the same as {@link keep_while()} but the paths in the map keys were
%% converted to native paths if necessary.

-type re_compile_ret() :: {ok, {re_pattern, term(), term(), term(), term()}} |
                          {error, {string(), non_neg_integer()}}.
%% Return value of {@link re:compile/1}.
%%
%% The opaque compiled regex type, {@link re:mp()}, is unfortunately not
%% exported by {@link re}, neither is the error tuple (at least up to
%% Erlang/OTP 25.1).

-export([ensure_native_keep_while/1,
         compile/1,
         applies_to_grandchildren/1,
         is_met/3,
         is_valid/1]).

-ifdef(TEST).
-export([eval_regex/4,
         compare_numerical_values/2]).
-endif.

-export_type([condition/0,
              comparison_op/1,
              keep_while/0,
              native_keep_while/0,
              re_compile_ret/0]).

-spec ensure_native_keep_while(KeepWhile) -> NativeKeepWhile when
      KeepWhile :: keep_while(),
      NativeKeepWhile :: native_keep_while().

ensure_native_keep_while(KeepWhile) ->
    maps:fold(
      fun(Path, Condition, Acc) ->
              Path1 = khepri_path:from_string(Path),
              %% TODO: Handle situations where the parsed path yields a native
              %% path already present in the resulting map.
              %%
              %% Should we merge conditions in a `#if_all{}' condition? Return
              %% an error?
              Acc#{Path1 => Condition}
      end, #{}, KeepWhile).

-spec compile(Condition) -> Condition when
      Condition :: khepri_path:pattern_component().
%% @doc Preprocess properties inside some conditions to make them more
%% efficient.
%%
%% An example is the regular expression inside an {@link if_name_matches()}
%% condition.
%%
%% Conditions are also optimized if possible. An example is the replacement of
%% an {@link if_all()} condition and all its sub-conditions if one of them is
%% a specific node name. In this case, it is replaced by the node name
%% directly.
%%
%% @param Condition the condition to compile.
%%
%% @returns the same condition with all its properties preprocessed.
%%
%% @private

compile(#if_name_matches{regex = any} = Cond) ->
    Cond;
compile(#if_name_matches{regex = Re, compiled = undefined} = Cond) ->
    Compiled = re:compile(Re),
    Cond#if_name_matches{compiled = Compiled};
compile(
  #if_data_matches{pattern = Pattern,
                   conditions = Conditions,
                   compiled = undefined} = Cond) ->
    Compiled = ets:match_spec_compile([{Pattern, Conditions, [match]}]),
    Cond#if_data_matches{compiled = Compiled};
compile(#if_not{condition = InnerCond} = Cond) ->
    InnerCond1 = compile(InnerCond),
    Cond#if_not{condition = InnerCond1};
compile(#if_all{conditions = InnerConds} = Cond) ->
    InnerConds1 = lists:map(fun compile/1, InnerConds),
    case optimize_if_all_conditions(InnerConds1) of
        [InnerCond] -> InnerCond;
        InnerConds2 -> Cond#if_all{conditions = InnerConds2}
    end;
compile(#if_any{conditions = InnerConds} = Cond) ->
    InnerConds1 = lists:map(fun compile/1, InnerConds),
    case optimize_if_any_conditions(InnerConds1) of
        [InnerCond] -> InnerCond;
        InnerConds2 -> Cond#if_any{conditions = InnerConds2}
    end;
compile(Cond) ->
    Cond.

-spec optimize_if_all_conditions([condition()]) -> [condition()].
%% @private
%% @hidden

optimize_if_all_conditions(Conds) ->
    optimize_if_all_conditions(Conds, []).

%% @private
%% @hidden

optimize_if_all_conditions([ChildName | Rest], Result)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildName) ->
    %% The path component exact match condition will become the first one
    %% tested.
    Result1 = Result ++ [ChildName],
    optimize_if_all_conditions(Rest, Result1);
optimize_if_all_conditions([Cond | Rest], Result) ->
    Result1 = [Cond | Result],
    optimize_if_all_conditions(Rest, Result1);
optimize_if_all_conditions([], Result) ->
    lists:reverse(Result).

-spec optimize_if_any_conditions([condition()]) -> [condition()].
%% @private
%% @hidden

optimize_if_any_conditions(Conds) ->
    Conds.

-spec applies_to_grandchildren(condition()) -> boolean().
%% @doc Returns true if a condition should be evaluated against child nodes in
%% addition to the current node.
%%
%% An example is the {@link if_path_matches()} condition.
%%
%% @private

applies_to_grandchildren(#if_path_matches{}) ->
    true;
applies_to_grandchildren(#if_not{condition = Cond}) ->
    applies_to_grandchildren(Cond);
applies_to_grandchildren(#if_all{conditions = Conds}) ->
    lists:any(fun applies_to_grandchildren/1, Conds);
applies_to_grandchildren(#if_any{conditions = Conds}) ->
    lists:any(fun applies_to_grandchildren/1, Conds);
applies_to_grandchildren(_) ->
    false.

-spec is_met(Condition, PathOrChildName, Child) -> IsMet when
      Condition :: khepri_path:pattern_component(),
      PathOrChildName :: khepri_path:native_path() | khepri_path:component(),
      Child :: khepri_tree:tree_node() | khepri:node_props(),
      IsMet :: true | IsNotMet1 | IsNotMet2,
      IsNotMet1 :: {false, khepri_path:pattern_component()},
      IsNotMet2 :: {false, {condition(), any()}}.
%% @doc Returns true if the given condition is met when evaluated against the
%% given tree node name and properties.
%%
%% @param Condition the condition to evaluate.
%% @param PathOrChildName the path or child name to consider.
%% @param Child the properties or the tree node.
%%
%% @returns true if the condition is met, false otherwise.
%%
%% @private

is_met(Condition, Path, Child) when ?IS_KHEPRI_PATH(Path) ->
    ChildName = case Path of
                    [] -> '';
                    _  -> lists:last(Path)
                end,
    is_met(Condition, ChildName, Child);

is_met(ChildName, ChildName, _Child)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildName) ->
    true;
is_met(?THIS_KHEPRI_NODE, _ChildNameB, _Child) ->
    true;
is_met(ChildNameA, _ChildNameB, _Child)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildNameA) ->
    {false, ChildNameA};
is_met(#if_node_exists{exists = true}, _ChildName, _Child) ->
    true;
is_met(#if_node_exists{exists = false} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(
  #if_name_matches{regex = SourceRegex, compiled = CompiledRegex} = Cond,
  ChildName,
  _Child) ->
    eval_regex(Cond, SourceRegex, CompiledRegex, ChildName);
is_met(
  #if_path_matches{regex = SourceRegex, compiled = CompiledRegex} = Cond,
  ChildName,
  _Child) ->
    eval_regex(Cond, SourceRegex, CompiledRegex, ChildName);
is_met(
  #if_has_payload{has_payload = false},
  _ChildName, #node{payload = ?NO_PAYLOAD}) ->
    true;
is_met(
  #if_has_payload{has_payload = true} = Cond,
  _ChildName, #node{payload = ?NO_PAYLOAD}) ->
    {false, Cond};
is_met(
  #if_has_payload{has_payload = true},
  _ChildName, #node{payload = _}) ->
    true;
is_met(
  #if_has_payload{has_payload = false},
  _ChildName, NodeProps)
  when not (is_map_key(data, NodeProps) orelse
            is_map_key(sproc, NodeProps)) ->
    true;
is_met(
  #if_has_payload{has_payload = true},
  _ChildName, NodeProps)
  when is_map_key(data, NodeProps) orelse
       is_map_key(sproc, NodeProps) ->
    true;
is_met(#if_has_data{has_data = true},
       _ChildName, #node{payload = #p_data{data = _}}) ->
    true;
is_met(#if_has_data{has_data = false} = Cond,
       _ChildName, #node{payload = #p_data{data = _}}) ->
    {false, Cond};
is_met(#if_has_data{has_data = true},
       _ChildName, #{data := _}) ->
    true;
is_met(#if_has_data{has_data = false} = Cond,
       _ChildName, #{data := _}) ->
    {false, Cond};
is_met(#if_has_data{has_data = true} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(#if_has_data{has_data = false}, _ChildName, _Child) ->
    true;
is_met(#if_has_sproc{has_sproc = true},
       _ChildName, #node{payload = #p_sproc{sproc = _}}) ->
    true;
is_met(#if_has_sproc{has_sproc = false} = Cond,
       _ChildName, #node{payload = #p_sproc{sproc = _}}) ->
    {false, Cond};
is_met(#if_has_sproc{has_sproc = true},
       _ChildName, #{sproc := _}) ->
    true;
is_met(#if_has_sproc{has_sproc = false} = Cond,
       _ChildName, #{sproc := _}) ->
    {false, Cond};
is_met(#if_has_sproc{has_sproc = true} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(#if_has_sproc{has_sproc = false}, _ChildName, _Child) ->
    true;
is_met(#if_data_matches{compiled = CompMatchSpec} = Cond,
       _ChildName, #node{payload = #p_data{data = Data}}) ->
    case term_matches(Data, CompMatchSpec) of
        true  -> true;
        false -> {false, Cond}
    end;
is_met(#if_data_matches{compiled = CompMatchSpec} = Cond,
       _ChildName, #{data := Data}) ->
    case term_matches(Data, CompMatchSpec) of
        true  -> true;
        false -> {false, Cond}
    end;
is_met(#if_payload_version{version = DVersionB} = Cond, _ChildName,
       #node{props = #{payload_version := DVersionA}}) ->
    compare_numerical_values(Cond, DVersionA, DVersionB);
is_met(#if_payload_version{version = DVersionB} = Cond, _ChildName,
       #{payload_version := DVersionA}) ->
    compare_numerical_values(Cond, DVersionA, DVersionB);
is_met(#if_child_list_version{version = CVersionB} = Cond, _ChildName,
       #node{props = #{child_list_version := CVersionA}}) ->
    compare_numerical_values(Cond, CVersionA, CVersionB);
is_met(#if_child_list_version{version = CVersionB} = Cond, _ChildName,
       #{child_list_version := CVersionA}) ->
    compare_numerical_values(Cond, CVersionA, CVersionB);
is_met(#if_child_list_length{count = ExpectedCount} = Cond, _ChildName,
       #node{child_nodes = Children}) ->
    Count = maps:size(Children),
    compare_numerical_values(Cond, Count, ExpectedCount);
is_met(#if_child_list_length{count = ExpectedCount} = Cond, _ChildName,
       #{child_list_length := Count}) ->
    compare_numerical_values(Cond, Count, ExpectedCount);
is_met(#if_not{condition = InnerCond} = Cond, ChildName, Child) ->
    case is_met(InnerCond, ChildName, Child) of
        true       -> {false, Cond};
        {false, _} -> true
    end;
is_met(#if_all{conditions = []}, _ChildName, _Child) ->
    true;
is_met(#if_all{conditions = Conds}, ChildName, Child) ->
    lists:foldl(
      fun
          (_, {false, _} = False) -> False;
          (Cond, _)               -> is_met(Cond, ChildName, Child)
      end, true, Conds);
is_met(#if_any{conditions = []} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(#if_any{conditions = Conds} = IfAnyCond, ChildName, Child) ->
    Ret = lists:foldl(
            fun
                (_, true) -> true;
                (Cond, _) -> is_met(Cond, ChildName, Child)
            end, {false, undefined}, Conds),
    case Ret of
        true       -> true;
        {false, _} -> {false, IfAnyCond}
    end;
is_met(Cond, _, _) ->
    {false, Cond}.

-spec term_matches(Term, MatchSpec) -> Matches when
      Term :: khepri:data(),
      MatchSpec :: ets:comp_match_spec(),
      Matches :: boolean().
%% @doc Returns true if the given match spec matches the given match term.
%%
%% @private
%% @hidden

term_matches(Term, MatchSpec) ->
    case ets:match_spec_run([Term], MatchSpec) of
        [match] -> true;
        _       -> false
    end.

-spec eval_regex(Condition, SourceRegex, CompiledRegex, Value) -> Ret when
      Condition :: condition_using_regex(),
      SourceRegex :: any | iodata() | unicode:charlist(),
      CompiledRegex :: khepri_condition:re_compile_ret() |
                       undefined,
      Value :: atom() | iodata() | unicode:charlist(),
      Ret :: true |
             {false, condition_using_regex()} |
             {false, {condition_using_regex(),
                      {error,
                       match_limit |
                       match_limit_recursion |
                       {string(), non_neg_integer()}}}}.
%% @doc Returns true if the given regular expression matches the given string.
%%
%% @private
%% @hidden

eval_regex(_Cond, any, _CompiledRegex, _Value) ->
    true;
eval_regex(Cond, _SourceRegex, {ok, Regex}, Value)
  when not is_atom(Value) ->
    case re:run(Value, Regex, [{capture, none}]) of
        match   -> true;
        nomatch -> {false, Cond};
        Error   -> {false, {Cond, Error}}
    end;
eval_regex(Cond, _SourceRegex, {error, _} = Error, Value)
  when not is_atom(Value) ->
    {false, {Cond, Error}};
eval_regex(Cond, SourceRegex, CompiledRegex, Value) when is_atom(Value) ->
    eval_regex(Cond, SourceRegex, CompiledRegex, atom_to_list(Value));
eval_regex(Cond, SourceRegex, undefined, Value) ->
    Compiled = re:compile(SourceRegex),
    eval_regex(Cond, SourceRegex, Compiled, Value).

-spec compare_numerical_values(Cond, ValueA, ValueB) -> Equal when
      Cond :: condition_using_comparison_op(),
      ValueA :: non_neg_integer(),
      ValueB :: non_neg_integer() | comparison_op(non_neg_integer()),
      Equal :: true | {false, condition_using_comparison_op()}.
%% @private
%% @hidden

compare_numerical_values(Cond, ValueA, ValueB) ->
    case compare_numerical_values(ValueA, ValueB) of
        true  -> true;
        false -> {false, Cond}
    end.

-spec compare_numerical_values(ValueA, ValueB) -> Equal when
      ValueA :: non_neg_integer(),
      ValueB :: non_neg_integer() | comparison_op(non_neg_integer()),
      Equal :: boolean().
%% @private
%% @hidden

compare_numerical_values(Value,  Value)        -> true;
compare_numerical_values(Value,  {eq, Value})  -> true;
compare_numerical_values(ValueA, {ne, ValueB}) -> ValueA =/= ValueB;
compare_numerical_values(ValueA, {lt, ValueB}) -> ValueA < ValueB;
compare_numerical_values(ValueA, {le, ValueB}) -> ValueA =< ValueB;
compare_numerical_values(ValueA, {gt, ValueB}) -> ValueA > ValueB;
compare_numerical_values(ValueA, {ge, ValueB}) -> ValueA >= ValueB;
compare_numerical_values(_, _)                 -> false.

-spec is_valid(Condition) -> IsValid when
      Condition :: khepri_path:pattern_component(),
      IsValid :: true | {false, khepri_path:pattern_component()}.
%% @doc Returns true if the condition's properties are valid.
%%
%% For instance, the function verifies that {@link if_node_exists()} takes a
%% boolean().
%%
%% @param Condition the condition to verify.
%%
%% @returns true if the condition's properties are valid, false otherwise.
%%
%% @private

is_valid(Component) when ?IS_KHEPRI_PATH_COMPONENT(Component) ->
    true;
is_valid(#if_node_exists{exists = Exists}) ->
    is_boolean(Exists);
is_valid(#if_name_matches{}) ->
    true;
is_valid(#if_path_matches{}) ->
    true;
is_valid(#if_has_payload{has_payload = HasPayload}) ->
    is_boolean(HasPayload);
is_valid(#if_has_data{has_data = HasData}) ->
    is_boolean(HasData);
is_valid(#if_has_sproc{has_sproc = HasStoredProc}) ->
    is_boolean(HasStoredProc);
is_valid(#if_data_matches{conditions = Conditions})
  when is_list(Conditions) ->
    true;
is_valid(#if_payload_version{}) ->
    true;
is_valid(#if_child_list_version{}) ->
    true;
is_valid(#if_child_list_length{}) ->
    true;
is_valid(#if_not{condition = InnerCond}) ->
    is_valid(InnerCond);
is_valid(#if_all{conditions = Conds}) when is_list(Conds) ->
    lists:foldl(
      fun
          (_, {false, _} = False) -> False;
          (Cond, _)               -> is_valid(Cond)
      end, true, Conds);
is_valid(#if_any{conditions = Conds}) when is_list(Conds) ->
    lists:foldl(
      fun
          (_, {false, _} = False) -> False;
          (Cond, _)               -> is_valid(Cond)
      end, true, Conds);
is_valid(Cond) ->
    {false, Cond}.
