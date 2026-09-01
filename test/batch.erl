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

can_submit_a_filled_batch_test_() ->
    {setup,
     local,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{"Wait for `batching` behaviour",
       ?_assertEqual(
          ok,
          khepri_cluster:wait_for_effective_behaviour(
            ?FUNCTION_NAME, batching, infinity))},
      {"Create and apply batch 1",
       ?_assertEqual(
          {ok,
           [{ok, #{[foo] => #{payload_version => 1}}},
            ok]},
          begin
              Options = #{machine_version_from_store => ?FUNCTION_NAME},
              Batch1 = khepri_batch:new(Options),
              Batch2 = khepri_adv:create(Batch1, [foo], foo_value),
              Batch3 = khepri_adv:transaction(
                         Batch2,
                         fun() -> khepri_tx:put([bar], bar_value) end),
              khepri_batch:submit(?FUNCTION_NAME, Batch3)
          end
         )},
      {"Check the result of the batch 1",
       ?_assertEqual(
          {ok, #{[foo] => foo_value,
                 [bar] => bar_value}},
          khepri:get_many(?FUNCTION_NAME, "*"))},
      {"Create and apply batch 2",
       ?_assertEqual(
          {ok,
           [{ok, #{[foo] => #{data => foo_value,
                              payload_version => 1,
                              delete_reason => explicit}}},
            {ok, #{[bar] => #{data => bar_value,
                              payload_version => 2}}}]},
          begin
              Options = #{machine_version_from_store => ?FUNCTION_NAME},
              Batch1 = khepri_batch:new(Options),
              Batch2 = khepri_adv:delete(Batch1, [foo]),
              Batch3 = khepri_adv:put(Batch2, [bar], new_bar_value),
              khepri_batch:submit(?FUNCTION_NAME, Batch3)
          end
         )},
      {"Check the result of the batch 2",
       ?_assertEqual(
          {ok, #{[bar] => new_bar_value}},
          khepri:get_many(?FUNCTION_NAME, "*"))}
     ]}.
