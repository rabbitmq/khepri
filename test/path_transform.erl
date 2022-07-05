%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(path_transform).

-include_lib("eunit/include/eunit.hrl").

-define(assertContainsForm(Data, Listing),
        fun() ->
            Form = erl_parse:abstract(Data),
            Forms = expand_forms(Listing),
            ?assert(contains_form(Forms, Form))
        end()).

-compile({parse_transform, khepri_path_transform}).

%% Tests on the result of the `khepri_path_transform' on this file.

unix_path_is_compiled_test() ->
    NativePath = [stock, wood, <<"oak">>],
    StringPath = khepri_path:from_string("/:stock/:wood/oak"),
    ?assertEqual(NativePath, StringPath),
    BinaryPath = khepri_path:from_binary(<<"/:stock/:wood/oak">>),
    ?assertEqual(NativePath, BinaryPath),
    ok.

%% Tests on form changes directly

simple_replacement_test() ->
    Listing = ""
    "hello_world() ->"
    "    khepri_path:from_string(\"/:stock/:wood/oak\").",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing),
    Listing1 = ""
    "hello_world() ->"
    "    khepri_path:from_binary(<<\"/:stock/:wood/oak\">>).",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing1),
    ok.

replacement_within_binding_test() ->
    Listing = ""
    "hello_world() ->"
    "    Path = khepri_path:from_string(\"/:stock/:wood/oak\"),"
    "    Path.",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing),
    ok.

replacement_within_case_expression_test() ->
    Listing = ""
    "hello_world(X) ->"
    "    case X of"
    "        true -> khepri_path:from_string(\"/:stock/:wood/oak\");"
    "        false -> khepri_path:from_string(\"/:emails/alice\")"
    "    end.",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing),
    ?assertContainsForm([emails, <<"alice">>], Listing),
    ok.

replacement_within_if_expression_test() ->
    Listing = ""
    "hello_world(N) ->"
    "    if"
    "        N =:= node() ->"
    "            khepri_path:from_string(\"/:stock/:wood/oak\");"
    "        true ->"
    "            khepri_path:from_string(\"/:stock/:wood/birch\")"
    "    end.",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing),
    ?assertContainsForm([stock, wood, <<"birch">>], Listing),
    ok.

replacement_within_try_expression_test() ->
    Listing = ""
    "hello_world() ->"
    "    try"
    "        khepri_path:from_string(\"/:stock/:wood/oak\")"
    "    catch"
    "        _:_ ->"
    "            khepri_path:from_string(\"/:stock/:wood/birch\")"
    "    after"
    "        khepri_path:from_string(\"/:stock/:wood/willow\")"
    "    end.",
    ?assertContainsForm([stock, wood, <<"oak">>], Listing),
    ?assertContainsForm([stock, wood, <<"birch">>], Listing),
    ?assertContainsForm([stock, wood, <<"willow">>], Listing),
    ok.

%% Abstract form helper functions

-spec expand_forms(Listing) -> Forms when
    Listing :: string(),
    Forms :: khepri_path_transform:forms().
%% @doc Expands a valid Erlang listing into abstract forms.
%% {@link khepri_path_transform:parse_transform/2} is applied to the forms.

expand_forms(Listing) ->
    {ok, Tokens, _} = erl_scan:string(Listing),
    {ok, Forms} = erl_parse:parse_form(Tokens),
    khepri_path_transform:parse_transform([Forms], []).

-spec contains_form(Haystack, Needle) -> boolean() when
    Haystack :: khepri_path_transform:forms(),
    Needle :: khepri_path_transform:form().
%% @doc Checks if the `Needle' form is an element of any part of the
%% `Haystack' forms tree.
%%
%% This implementation checks against all elements of all forms including
%% {@link erl_anno:location()} elements for the sake of simplicity.
%%
%% @returns `true' if `Haystack' contains `Needle', `false' otherwise.
contains_form(Haystack, Needle) ->
    contains_form1(strip_annos(Haystack), strip_annos(Needle)).

strip_annos(Forms) when is_list(Forms) ->
    lists:map(fun strip_annos/1, Forms);
strip_annos(Forms) ->
    erl_parse:map_anno(fun(_Anno) -> 0 end, Forms).

contains_form1(Needle, Needle) ->
    true;
contains_form1(Haystack, Needle) when is_tuple(Haystack) ->
    contains_form1(tuple_to_list(Haystack), Needle);
contains_form1(Haystack, Needle) when is_list(Haystack) ->
    lists:any(
        fun(SubHaystack) -> contains_form1(SubHaystack, Needle) end,
        Haystack);
contains_form1(_Haystack, _Needle) ->
    false.
