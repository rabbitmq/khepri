%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(mod_used_for_transactions).

-include_lib("stdlib/include/assert.hrl").

-export([exported/0,
         get_lambda/0,
         %% We export this one just to try to prevent inlining.
         hash_term/1,
         crashing_fun/0,
         make_record/1,
         outer_function/2,
         min/2,
         call_inner_function/2]).

exported() -> unexported().
unexported() -> ok.

get_lambda() ->
    fun(Arg) ->
            {ok, Arg, hash_term(Arg)}
    end.

hash_term(Term) ->
    erlang:phash2(Term).

crashing_fun() ->
    throw("Expected crash").

-record(my_record, {function}).

make_record(Function) ->
    #my_record{function = Function}.

outer_function(#my_record{function = Value} = MyRecord, Term) ->
    is_atom(Value) andalso
    inner_function(MyRecord, Term);
outer_function(_, _) ->
    false.

inner_function(#my_record{function = hash_term = Function}, Term) ->
    _ = (?MODULE:Function(Term)),
    true;
inner_function(_, _) ->
    false.

min(A, B) ->
    erlang:min(A, B).

call_inner_function(InnerFun, Options) when is_map(Options) ->
    InnerFun(Options).
