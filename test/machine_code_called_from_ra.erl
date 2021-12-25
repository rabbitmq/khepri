%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(machine_code_called_from_ra).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("test/helpers.hrl").

-dialyzer([
    {nowarn_function, [
        use_an_invalid_path_test_/0,
        use_an_invalid_payload_test_/0
    ]}
]).

insert_a_node_test_() ->
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            ?_assertEqual(
                {ok, #{[foo] => #{}}},
                khepri_machine:put(
                    ?FUNCTION_NAME, [foo], #kpayload_data{data = foo_value}
                )
            )
        ]}.

query_a_node_test_() ->
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            ?_assertEqual(
                {ok, #{
                    [foo] => #{
                        data => foo_value,
                        payload_version => 1,
                        child_list_version => 1,
                        child_list_length => 0
                    }
                }},
                begin
                    _ = khepri_machine:put(
                        ?FUNCTION_NAME, [foo], #kpayload_data{data = foo_value}
                    ),
                    khepri_machine:get(?FUNCTION_NAME, [foo])
                end
            )
        ]}.

delete_a_node_test_() ->
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            {inorder, [
                {"Adding and deleting key/value",
                    ?_assertEqual(
                        {ok, #{
                            [foo] => #{
                                data => foo_value,
                                payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 0
                            }
                        }},
                        begin
                            _ = khepri_machine:put(
                                ?FUNCTION_NAME,
                                [foo],
                                #kpayload_data{data = foo_value}
                            ),
                            khepri_machine:delete(?FUNCTION_NAME, [foo])
                        end
                    )},
                {"Checking the deleted key is gone",
                    ?_assertEqual(
                        {ok, #{}},
                        khepri_machine:get(?FUNCTION_NAME, [foo])
                    )}
            ]}
        ]}.

query_keep_while_conds_state_test_() ->
    KeepWhile = #{[?THIS_NODE] => #if_child_list_length{count = {gt, 0}}},
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            ?_assertEqual(
                {ok, #{
                    [foo] =>
                        #{[foo] => #if_child_list_length{count = {gt, 0}}}
                }},
                begin
                    _ = khepri_machine:put(
                        ?FUNCTION_NAME,
                        [foo],
                        #kpayload_data{data = foo_value},
                        #{keep_while => KeepWhile}
                    ),
                    khepri_machine:get_keep_while_conds_state(?FUNCTION_NAME)
                end
            )
        ]}.

use_an_invalid_path_test_() ->
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            ?_assertThrow(
                {invalid_path, not_a_list},
                khepri_machine:put(
                    ?FUNCTION_NAME,
                    not_a_list,
                    none
                )
            ),
            ?_assertThrow(
                {invalid_path, "not_a_component"},
                khepri_machine:put(
                    ?FUNCTION_NAME,
                    ["not_a_component"],
                    none
                )
            )
        ]}.

use_an_invalid_payload_test_() ->
    {setup, fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
        fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end, [
            ?_assertThrow(
                {invalid_payload, [foo], invalid_payload},
                khepri_machine:put(
                    ?FUNCTION_NAME,
                    [foo],
                    invalid_payload
                )
            ),
            ?_assertThrow(
                {invalid_payload, [foo], {invalid_payload, in_a_tuple}},
                khepri_machine:put(
                    ?FUNCTION_NAME,
                    [foo],
                    {invalid_payload, in_a_tuple}
                )
            )
        ]}.
