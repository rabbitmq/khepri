%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mod_used_for_transactions).

-export([exported/0,
         get_lambda/0,
         %% We export this one just to try to prevent inlining.
         hash_term/1]).

exported() -> unexported().
unexported() -> ok.

get_lambda() ->
    fun(Arg) ->
            {ok, Arg, hash_term(Arg)}
    end.

hash_term(Term) ->
    erlang:phash2(Term).
