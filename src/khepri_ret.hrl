%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(common_ret_to_single_result_ret(__Ret),
        case (__Ret) of
            {ok, __NodePropsMap} ->
                [__NodeProps] = maps:values(__NodePropsMap),
                {ok, __NodeProps};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(single_result_ret_to_payload_ret(__Ret),
        case (__Ret) of
            {ok, #{data := __Data}} ->
                {ok, __Data};
            {ok, #{sproc := __StandaloneFun}} ->
                {ok, __StandaloneFun};
            {ok, _} ->
                {ok, undefined};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(many_results_ret_to_payloads_ret(__Ret, __Default),
        case (__Ret) of
            {ok, __NodePropsMap} ->
                __PayloadsMap = maps:map(
                                  fun
                                      (_, #{data := __Data}) ->
                                          __Data;
                                      (_, #{sproc := __StandaloneFun}) ->
                                          __StandaloneFun;
                                      (_, _) ->
                                          (__Default)
                                  end, __NodePropsMap),
                {ok, __PayloadsMap};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(result_ret_to_minimal_ret(__Ret),
        case (__Ret) of
            {ok, _} -> ok;
            __Other -> __Other
        end).

-define(raise_exception_if_any(__Ret),
        case (__Ret) of
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            _ ->
                __Ret
        end).
