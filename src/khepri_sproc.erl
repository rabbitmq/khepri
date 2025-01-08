%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri support code for stored procedures.
%%
%% @hidden

-module(khepri_sproc).

-include_lib("stdlib/include/assert.hrl").

-include_lib("horus/include/horus.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").
-include("src/khepri_tx.hrl").

%% For internal use only.
-export([to_standalone_fun/1,
         run/2]).

-spec to_standalone_fun(Fun) -> StandaloneFun | no_return() when
      Fun :: fun(),
      StandaloneFun :: horus:horus_fun().

to_standalone_fun(Fun) when is_function(Fun) ->
    Options = #{should_process_function => fun should_process_function/4},
    try
        horus:to_standalone_fun(Fun, Options)
    catch
        throw:Error:Stacktrace ->
            erlang:error(
              ?khepri_exception(
                 failed_to_prepare_sproc_fun,
                 #{'fun' => Fun,
                   error => Error,
                   stacktrace => Stacktrace}))
    end;
to_standalone_fun(Fun) when ?IS_HORUS_STANDALONE_FUN(Fun) ->
    Fun.

-spec run(StandaloneFun, Args) -> Ret when
      StandaloneFun :: horus:horus_fun(),
      Args :: [any()],
      Ret :: any().

run(StandaloneFun, Args) ->
    try
        horus:exec(StandaloneFun, Args)
    catch
        throw:?TX_ABORT(_Reason):Stacktrace ->
            ?khepri_raise_misuse(
               invalid_use_of_khepri_tx_outside_transaction, #{},
               Stacktrace)
    end.

should_process_function(Module, Name, Arity, FromModule) ->
    case khepri_utils:should_process_module(Module) of
        true ->
            case Module of
                FromModule ->
                    true;
                _ ->
                    _ = code:ensure_loaded(Module),
                    case erlang:function_exported(Module, Name, Arity) of
                        true ->
                            true;
                        false ->
                            throw({call_to_unexported_function,
                                   {Module, Name, Arity}})
                    end
            end;
        false ->
            false
    end.
