%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc A parse-transform that converts {@link khepri_path:unix_pattern()}s
%% into {@link khepri_path:native_pattern()}s at compilation-time.
%%
%% Add this parse-transform to a module with the `compile' attribute:
%%
%% ```
%% -compile({parse_transform, khepri_path_transform}).
%% '''
%%
%% or with the `erlc' option `` +'{parse_transform, khepri_path_transform}' ''.
%%
%% Calls to {@link khepri_path:from_string/1} with a string with a string
%% {@link khepri_path:unix_pattern()} are replaced at compile-time with
%% equivalent {@link khepri_path:native_pattern()}s.

-module(khepri_path_transform).

-export([parse_transform/2, map_form/2, rewrite_path_call/1]).

-spec parse_transform([Form], Options) -> [Form]
    when Form :: erl_parse:abstract_form(),
         Options :: term().
parse_transform(Forms, _Options) ->
    map_form(Forms, fun rewrite_path_call/1).

-spec map_form(Form | [Form], Fun) -> Form | [Form]
    when Form :: erl_parse:abstract_form(),
         Fun :: fun((erl_parse:abstract_form()) -> erl_parse:abstract_form()).
map_form(Forms, Fun) when is_list(Forms) ->
    [map_form(Form, Fun) || Form <- Forms];
map_form({function, _Location, _Name, _Arity, Clauses} = Function0, Fun) ->
    Function = setelement(5, Function0, map_form(Clauses, Fun)),
    Fun(Function);
map_form({clause, _Location, _Pattern, _Guard, Body} = Clause0, Fun) ->
    Clause = setelement(5, Clause0, map_form(Body, Fun)),
    Fun(Clause);
map_form({'case', _Location, _Expr, Clauses} = Case0, Fun) ->
    Case = setelement(4, Case0, map_form(Clauses, Fun)),
    Fun(Case);
map_form({'if', _Location, Clauses} = If0, Fun) ->
    If = setelement(3, If0, map_form(Clauses, Fun)),
    Fun(If);
map_form({'try', _Location, Body, _Patterns, Catch, After} = Try0, Fun) ->
    Try1 = setelement(3, Try0, map_form(Body, Fun)),
    Try2 = setelement(5, Try1, map_form(Catch, Fun)),
    Try3 = setelement(6, Try2, map_form(After, Fun)),
    Fun(Try3);
map_form({match, _Location, _Pattern, Expression} = Match0, Fun) ->
    Match = setelement(4, Match0, map_form(Expression, Fun)),
    Fun(Match);
map_form(Form, Fun) ->
    Fun(Form).

rewrite_path_call({call,
                   _Location,
                   {remote, _, {atom, _, khepri_path}, {atom, _, from_string}},
                   [{string, Location, Path}]}) ->
    NativePath = khepri_path:from_string(Path),
    erl_parse:abstract(NativePath, Location);
rewrite_path_call(Form) ->
    Form.
