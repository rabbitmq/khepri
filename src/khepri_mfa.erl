%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(khepri_mfa).

-include("src/khepri_error.hrl").

-export([to_fun/2]).

-spec to_fun(MFA, Arity) -> Fun when
      MFA :: khepri:mod_func_args(),
      Arity :: arity(),
      Fun :: fun().
%% @private

to_fun({Mod, Func, Args} = MFA, Arity) ->
    case {Args, Arity} of
        {[], 0} ->
            fun() ->
                    Mod:Func()
            end;
        {[], 1} ->
            fun(Arg1) ->
                    Mod:Func(Arg1)
            end;
        {[], 2} ->
            fun(Arg1, Arg2) ->
                    Mod:Func(Arg1, Arg2)
            end;
        {[], 3} ->
            fun(Arg1, Arg2, Arg3) ->
                    Mod:Func(Arg1, Arg2, Arg3)
            end;
        {[], 4} ->
            fun(Arg1, Arg2, Arg3, Arg4) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4)
            end;
        {[], 5} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[], 6} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[], 7} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[], 8} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[], 9} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[], 10} ->
            fun(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1], 0} ->
            fun() ->
                    Mod:Func(Arg1)
            end;
        {[Arg1], 1} ->
            fun(Arg2) ->
                    Mod:Func(Arg1, Arg2)
            end;
        {[Arg1], 2} ->
            fun(Arg2, Arg3) ->
                    Mod:Func(Arg1, Arg2, Arg3)
            end;
        {[Arg1], 3} ->
            fun(Arg2, Arg3, Arg4) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4)
            end;
        {[Arg1], 4} ->
            fun(Arg2, Arg3, Arg4, Arg5) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[Arg1], 5} ->
            fun(Arg2, Arg3, Arg4, Arg5, Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1], 6} ->
            fun(Arg2, Arg3, Arg4, Arg5, Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1], 7} ->
            fun(Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1], 8} ->
            fun(Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1], 9} ->
            fun(Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2)
            end;
        {[Arg1, Arg2], 1} ->
            fun(Arg3) ->
                    Mod:Func(Arg1, Arg2, Arg3)
            end;
        {[Arg1, Arg2], 2} ->
            fun(Arg3, Arg4) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4)
            end;
        {[Arg1, Arg2], 3} ->
            fun(Arg3, Arg4, Arg5) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[Arg1, Arg2], 4} ->
            fun(Arg3, Arg4, Arg5, Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1, Arg2], 5} ->
            fun(Arg3, Arg4, Arg5, Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2], 6} ->
            fun(Arg3, Arg4, Arg5, Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2], 7} ->
            fun(Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2], 8} ->
            fun(Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2, Arg3)
            end;
        {[Arg1, Arg2, Arg3], 1} ->
            fun(Arg4) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4)
            end;
        {[Arg1, Arg2, Arg3], 2} ->
            fun(Arg4, Arg5) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[Arg1, Arg2, Arg3], 3} ->
            fun(Arg4, Arg5, Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1, Arg2, Arg3], 4} ->
            fun(Arg4, Arg5, Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2, Arg3], 5} ->
            fun(Arg4, Arg5, Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3], 6} ->
            fun(Arg4, Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3], 7} ->
            fun(Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 1} ->
            fun(Arg5) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 2} ->
            fun(Arg5, Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 3} ->
            fun(Arg5, Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 4} ->
            fun(Arg5, Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 5} ->
            fun(Arg5, Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4], 6} ->
            fun(Arg5, Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5], 1} ->
            fun(Arg6) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5], 2} ->
            fun(Arg6, Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5], 3} ->
            fun(Arg6, Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5], 4} ->
            fun(Arg6, Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5], 5} ->
            fun(Arg6, Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6], 1} ->
            fun(Arg7) ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6], 2} ->
            fun(Arg7, Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6], 3} ->
            fun(Arg7, Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6], 4} ->
            fun(Arg7, Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7], 0} ->
            fun() ->
                    Mod:Func(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7], 1} ->
            fun(Arg8) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7], 2} ->
            fun(Arg8, Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7], 3} ->
            fun(Arg8, Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8], 0} ->
            fun() ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8], 1} ->
            fun(Arg9) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8], 2} ->
            fun(Arg9, Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9], 0} ->
            fun() ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9)
            end;
        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9], 1} ->
            fun(Arg10) ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        {[Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9, Arg10], 0} ->
            fun() ->
                    Mod:Func(
                      Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, Arg7, Arg8, Arg9,
                      Arg10)
            end;

        _ ->
            ?khepri_misuse(
               tx_mfa_with_arity_too_great,
               #{mfa => MFA,
                 arity => Arity})
    end.
