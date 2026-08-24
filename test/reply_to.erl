%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(reply_to).

-include_lib("eunit/include/eunit.hrl").

-include_lib("khepri/include/khepri.hrl").

can_send_reply_to_other_process_test_() ->
    Pid = erlang:alias([reply]),
    ReplyToPriv = youpi,
    {setup,
     local,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{"Wait for `reply_to_option` behaviour",
       ?_assertEqual(
          ok,
          khepri_cluster:wait_for_effective_behaviour(
            ?FUNCTION_NAME, reply_to_option, infinity))},
      {"Put a value with the `reply_to` option",
       ?_assertEqual(
          ok,
          khepri:put(
            ?FUNCTION_NAME, [foo], foo_value,
            #{reply_to => {Pid, ReplyToPriv}})
         )},
      {"Receive the put command result from a message",
       ?_assertEqual(
          #khepri_reply{result = {ok, #{[foo] => #{}}},
                        priv = ReplyToPriv},
          receive
              Msg ->
                  Msg
          end)},
      {"Check the value was actually stored",
       ?_assertEqual(
          {ok, foo_value},
          khepri:get(?FUNCTION_NAME, [foo]))}]}.
