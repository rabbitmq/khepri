%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

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
