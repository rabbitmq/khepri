%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(app).

-include_lib("eunit/include/eunit.hrl").

app_starts_workers_test() ->
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),
    ?assert(is_process_alive(whereis(khepri_event_handler))),
    ?assertEqual(ok, application:stop(khepri)),
    ?assertEqual(undefined, whereis(khepri_event_handler)).

event_handler_gen_server_callbacks_test() ->
    %% This testcase is mostly to improve code coverage. We don't really care
    %% about the unused mandatory callbacks of this gen_server.
    ?assertMatch({ok, _}, application:ensure_all_started(khepri)),
    ?assert(is_process_alive(whereis(khepri_event_handler))),
    khepri_event_handler ! timeout,
    khepri_event_handler ! any_message,
    ?assertEqual(ok, gen_server:cast(khepri_event_handler, any_cast)),
    ?assertEqual(ok, gen_server:call(khepri_event_handler, any_call)),
    ?assert(is_process_alive(whereis(khepri_event_handler))),
    ?assertEqual(ok, application:stop(khepri)).
