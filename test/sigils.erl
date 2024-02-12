%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(sigils).

-include_lib("eunit/include/eunit.hrl").

-include("src/khepri_error.hrl").

-dialyzer({nowarn_function, [
                             %% The following functions explicitely break the
                             %% spec contract on purpose to generate an
                             %% exception.
                             sigil_p_error_test/0,
                             sigil_P_error_test/0
                            ]}).

sigil_p_test() ->
    ?assertEqual(
       [foo],
       khepri_path:sigil_p("/:foo", [])).

sigil_p_error_test() ->
    ?assertError(
       ?khepri_exception(invalid_path, #{path := not_a_path}),
       khepri_path:sigil_p(not_a_path, [])).

sigil_P_test() ->
    ?assertEqual(
       [foo],
       khepri_path:sigil_P("/:foo", [])).

sigil_P_error_test() ->
    ?assertError(
       ?khepri_exception(invalid_path, #{path := not_a_path}),
       khepri_path:sigil_P(not_a_path, [])).
