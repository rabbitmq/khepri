%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Condition support.
%%
%% Conditions can be used in path patterns and `keep_while' conditions.
%%
%% A condition is an Erlang record defining a specific property. Some of them
%% have arguments to further define the condition.
%%
%% All supported conditions are described in the <a href="#types">Data Types
%% section</a>.

-module(khepri_condition).

-include("include/khepri.hrl").
-include("src/internal.hrl").

-type comparison_op(Type) ::
    {eq, Type}
    | {ne, Type}
    | {lt, Type}
    | {le, Type}
    | {gt, Type}
    | {ge, Type}.
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

-type if_data_matches() :: #if_data_matches{}.
%% Condition. Evaluates to true if the tested node has a data payload and the
%% data payload term matches the given pattern.
%%
%% Record fields:
%% <ul>
%% <li>`pattern': an ETS-like match pattern.</li>
%% </ul>
%%
%% Example:
%% ```
%% #if_data_matches{pattern = {user, '_'}}.
%% '''

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
%% Condition. Evaluates to true if the inner condition evalutes to false.
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
%% Condition. Evaluates to true if all inner conditions evalute to true.
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
%% Condition. Evaluates to true if any of the inner conditions evalute to
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

-type condition() ::
    if_name_matches()
    | if_path_matches()
    | if_has_data()
    | if_data_matches()
    | if_node_exists()
    | if_payload_version()
    | if_child_list_version()
    | if_child_list_length()
    | if_not()
    | if_all()
    | if_any().

-type condition_using_regex() ::
    if_name_matches()
    | if_path_matches().
-type condition_using_comparison_op() ::
    if_payload_version()
    | if_child_list_version()
    | if_child_list_length().

-type keep_while() :: #{khepri_path:path() => condition()}.

-export([
    compile/1,
    applies_to_grandchildren/1,
    is_met/3,
    is_valid/1
]).

-ifdef(TEST).
-export([
    eval_regex/4,
    compare_numerical_values/2
]).
-endif.

-export_type([
    condition/0,
    comparison_op/1,
    keep_while/0
]).

-spec compile(Condition) -> Condition when
    Condition :: khepri_path:pattern_component().
%% @private

compile(#if_name_matches{regex = any} = Cond) ->
    Cond;
compile(#if_name_matches{regex = Re, compiled = undefined} = Cond) ->
    Compiled = re:compile(Re),
    Cond#if_name_matches{compiled = Compiled};
compile(#if_data_matches{pattern = Pattern, compiled = undefined} = Cond) ->
    Compiled = ets:match_spec_compile([{Pattern, [], [match]}]),
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

optimize_if_all_conditions(Conds) ->
    optimize_if_all_conditions(Conds, []).

optimize_if_all_conditions([ChildName | Rest], Result) when
    ?IS_PATH_COMPONENT(ChildName)
->
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

optimize_if_any_conditions(Conds) ->
    Conds.

-spec applies_to_grandchildren(condition()) -> boolean().
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
    PathOrChildName :: khepri_path:path() | khepri_path:component(),
    Child :: khepri_machine:tree_node() | khepri_machine:node_props(),
    IsMet :: true | IsNotMet1 | IsNotMet2,
    IsNotMet1 :: {false, khepri_path:pattern_component()},
    IsNotMet2 :: {false, {condition(), any()}}.
%% @private

is_met(Condition, Path, Child) when ?IS_PATH(Path) ->
    ChildName =
        case Path of
            [] -> '';
            _ -> lists:last(Path)
        end,
    is_met(Condition, ChildName, Child);
is_met(ChildName, ChildName, _Child) when
    ?IS_PATH_COMPONENT(ChildName)
->
    true;
is_met(?THIS_NODE, _ChildNameB, _Child) ->
    true;
is_met(ChildNameA, _ChildNameB, _Child) when
    ?IS_PATH_COMPONENT(ChildNameA)
->
    {false, ChildNameA};
is_met(#if_node_exists{exists = true}, _ChildName, _Child) ->
    true;
is_met(#if_node_exists{exists = false} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(
    #if_name_matches{regex = SourceRegex, compiled = CompiledRegex} = Cond,
    ChildName,
    _Child
) ->
    eval_regex(Cond, SourceRegex, CompiledRegex, ChildName);
is_met(
    #if_path_matches{regex = SourceRegex, compiled = CompiledRegex} = Cond,
    ChildName,
    _Child
) ->
    eval_regex(Cond, SourceRegex, CompiledRegex, ChildName);
is_met(
    #if_has_data{has_data = true},
    _ChildName,
    #node{payload = #kpayload_data{data = _}}
) ->
    true;
is_met(
    #if_has_data{has_data = false} = Cond,
    _ChildName,
    #node{payload = #kpayload_data{data = _}}
) ->
    {false, Cond};
is_met(#if_has_data{has_data = true} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(#if_has_data{has_data = false}, _ChildName, _Child) ->
    true;
is_met(
    #if_data_matches{compiled = CompMatchSpec} = Cond,
    _ChildName,
    #node{payload = #kpayload_data{data = Data}}
) ->
    case term_matches(Data, CompMatchSpec) of
        true -> true;
        false -> {false, Cond}
    end;
is_met(
    #if_data_matches{compiled = CompMatchSpec} = Cond,
    _ChildName,
    #{data := Data}
) ->
    case term_matches(Data, CompMatchSpec) of
        true -> true;
        false -> {false, Cond}
    end;
is_met(
    #if_payload_version{version = DVersionB} = Cond,
    _ChildName,
    #node{stat = #{payload_version := DVersionA}}
) ->
    compare_numerical_values(Cond, DVersionA, DVersionB);
is_met(
    #if_payload_version{version = DVersionB} = Cond,
    _ChildName,
    #{payload_version := DVersionA}
) ->
    compare_numerical_values(Cond, DVersionA, DVersionB);
is_met(
    #if_child_list_version{version = CVersionB} = Cond,
    _ChildName,
    #node{stat = #{child_list_version := CVersionA}}
) ->
    compare_numerical_values(Cond, CVersionA, CVersionB);
is_met(
    #if_child_list_version{version = CVersionB} = Cond,
    _ChildName,
    #{child_list_version := CVersionA}
) ->
    compare_numerical_values(Cond, CVersionA, CVersionB);
is_met(
    #if_child_list_length{count = ExpectedCount} = Cond,
    _ChildName,
    #node{child_nodes = Children}
) ->
    Count = maps:size(Children),
    compare_numerical_values(Cond, Count, ExpectedCount);
is_met(
    #if_child_list_length{count = ExpectedCount} = Cond,
    _ChildName,
    #{child_list_length := Count}
) ->
    compare_numerical_values(Cond, Count, ExpectedCount);
is_met(#if_not{condition = InnerCond} = Cond, ChildName, Child) ->
    case is_met(InnerCond, ChildName, Child) of
        true -> {false, Cond};
        {false, _} -> true
    end;
is_met(#if_all{conditions = []}, _ChildName, _Child) ->
    true;
is_met(#if_all{conditions = Conds}, ChildName, Child) ->
    lists:foldl(
        fun
            (_, {false, _} = False) -> False;
            (Cond, _) -> is_met(Cond, ChildName, Child)
        end,
        true,
        Conds
    );
is_met(#if_any{conditions = []} = Cond, _ChildName, _Child) ->
    {false, Cond};
is_met(#if_any{conditions = Conds} = IfAnyCond, ChildName, Child) ->
    Ret = lists:foldl(
        fun
            (_, true) -> true;
            (Cond, _) -> is_met(Cond, ChildName, Child)
        end,
        {false, undefined},
        Conds
    ),
    case Ret of
        true -> true;
        {false, _} -> {false, IfAnyCond}
    end;
is_met(Cond, _, _) ->
    {false, Cond}.

-spec term_matches(khepri_machine:payload(), ets:comp_match_spec()) ->
    boolean().
%% @private

term_matches(Term, MatchSpec) ->
    case ets:match_spec_run([Term], MatchSpec) of
        [match] -> true;
        _ -> false
    end.

-spec eval_regex(
    condition_using_regex(),
    any | iodata() | unicode:charlist(),
    {ok, re_mp()} | {error, {string(), non_neg_integer()}} | undefined,
    atom() | iodata() | unicode:charlist()
) ->
    true
    | {false, condition_using_regex()}
    | {false, {
        condition_using_regex(),
        {error,
            match_limit
            | match_limit_recursion
            | {string(), non_neg_integer()}}
    }}.
%% @private

eval_regex(_Cond, any, _CompiledRegex, _Value) ->
    true;
eval_regex(Cond, _SourceRegex, {ok, Regex}, Value) when
    not is_atom(Value)
->
    case re:run(Value, Regex, [{capture, none}]) of
        match -> true;
        nomatch -> {false, Cond};
        Error -> {false, {Cond, Error}}
    end;
eval_regex(Cond, _SourceRegex, {error, _} = Error, Value) when
    not is_atom(Value)
->
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

compare_numerical_values(Cond, ValueA, ValueB) ->
    case compare_numerical_values(ValueA, ValueB) of
        true -> true;
        false -> {false, Cond}
    end.

-spec compare_numerical_values(ValueA, ValueB) -> Equal when
    ValueA :: non_neg_integer(),
    ValueB :: non_neg_integer() | comparison_op(non_neg_integer()),
    Equal :: boolean().
%% @private

compare_numerical_values(Value, Value) -> true;
compare_numerical_values(Value, {eq, Value}) -> true;
compare_numerical_values(ValueA, {ne, ValueB}) -> ValueA =/= ValueB;
compare_numerical_values(ValueA, {lt, ValueB}) -> ValueA < ValueB;
compare_numerical_values(ValueA, {le, ValueB}) -> ValueA =< ValueB;
compare_numerical_values(ValueA, {gt, ValueB}) -> ValueA > ValueB;
compare_numerical_values(ValueA, {ge, ValueB}) -> ValueA >= ValueB;
compare_numerical_values(_, _) -> false.

-spec is_valid(Condition) -> IsValid when
    Condition :: khepri_path:pattern_component(),
    IsValid :: true | {false, khepri_path:pattern_component()}.

is_valid(Component) when ?IS_PATH_COMPONENT(Component) ->
    true;
is_valid(?THIS_NODE) ->
    true;
is_valid(#if_node_exists{exists = Exists}) ->
    is_boolean(Exists);
is_valid(#if_name_matches{}) ->
    true;
is_valid(#if_path_matches{}) ->
    true;
is_valid(#if_has_data{has_data = HasData}) ->
    is_boolean(HasData);
is_valid(#if_data_matches{}) ->
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
            (Cond, _) -> is_valid(Cond)
        end,
        true,
        Conds
    );
is_valid(#if_any{conditions = Conds}) when is_list(Conds) ->
    lists:foldl(
        fun
            (_, {false, _} = False) -> False;
            (Cond, _) -> is_valid(Cond)
        end,
        true,
        Conds
    );
is_valid(Cond) ->
    {false, Cond}.
