%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(batch).

-include_lib("eunit/include/eunit.hrl").

can_submit_an_empty_batch_test_() ->
    {setup,
     local,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{"Wait for `batching` behaviour",
       ?_assertEqual(
          ok,
          khepri_cluster:wait_for_effective_behaviour(
            ?FUNCTION_NAME, batching, infinity))},
      {"Put a value with the `reply_to` option",
       ?_assertEqual(
          {ok, []},
          begin
              Options = #{machine_version_from_store => ?FUNCTION_NAME},
              Batch = khepri_batch:new(Options),
              khepri_batch:submit(?FUNCTION_NAME, Batch)
          end
         )}
     ]}.
