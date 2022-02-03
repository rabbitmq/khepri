%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mod_used_for_transactions).

-export([exported/0,
         get_lambda/0,
         %% We export this one just to try to prevent inlining.
         hash_term/1,
         make_record/1,
         outer_function/2]).

exported() -> unexported().
unexported() -> ok.

get_lambda() ->
    fun(Arg) ->
            {ok, Arg, hash_term(Arg)}
    end.

hash_term(Term) ->
    erlang:phash2(Term).

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
