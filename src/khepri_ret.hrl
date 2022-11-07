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
