%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-define(IS_MFA(Fun), (is_tuple(Fun) andalso size(Fun) =:= 3 andalso
                      is_atom(element(1, Fun)) andalso
                      is_atom(element(2, Fun)) andalso
                      is_list(element(3, Fun)))).

-define(IS_FUN_OR_MFA(Fun), (is_function(Fun) orelse ?IS_MFA(Fun))).
-define(IS_FUN_OR_MFA(Fun, Arity), (is_function(Fun, Arity) orelse
                                    ?IS_MFA(Fun))).
