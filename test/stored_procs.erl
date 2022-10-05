%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(stored_procs).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_fun.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

store_and_get_sproc_test_() ->
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun() -> return_value end))},

        {"Get the stored procedure",
         ?_assertMatch(
            {ok, #standalone_fun{}},
            khepri:get(
              ?FUNCTION_NAME, StoredProcPath))}]
      }]}.

execute_valid_sproc_test_() ->
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun() -> return_value end))},

        {"Execute the stored procedure",
         ?_assertEqual(
            return_value,
            khepri:run_sproc(
              ?FUNCTION_NAME, StoredProcPath, []))}]
      }]}.

execute_nonexisting_sproc_test_() ->
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Execute the stored procedure",
         ?_assertThrow(
            ?khepri_error(
            failed_to_get_sproc,
            #{path := StoredProcPath,
              args := [],
              error := ?khepri_error(
                          node_not_found,
                          #{node_name := sproc,
                            node_path := StoredProcPath,
                            node_is_target := true})}),
            khepri:run_sproc(
              ?FUNCTION_NAME, StoredProcPath, []))}]
      }]}.

try_to_execute_data_test_() ->
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              value))},

        {"Execute the stored procedure",
         ?_assertThrow(
            ?khepri_exception(
            denied_execution_of_non_sproc_node,
            #{path := StoredProcPath,
              args := [],
              node_props := #{data := value,
                              payload_version := 1}}),
            khepri:run_sproc(
              ?FUNCTION_NAME, StoredProcPath, []))}]
      }]}.

execute_sproc_with_wrong_arity_test_() ->
    StoredProcPath = [sproc],
    Args = [a, b],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun() -> return_value end))},

        {"Execute the stored procedure",
         ?_assertExit(
            {badarity, {#standalone_fun{arity = 0}, Args}},
            khepri:run_sproc(
              ?FUNCTION_NAME, StoredProcPath, Args))}]
      }]}.

execute_crashing_sproc_test_() ->
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun() -> throw("Expected crash") end))},

        {"Execute the stored procedure",
         ?_assertThrow(
            "Expected crash",
            khepri:run_sproc(
              ?FUNCTION_NAME, StoredProcPath, []))}]
      }]}.

crashing_sproc_stacktrace_test_() ->
    {ok, Cwd} = file:get_cwd(),
    File1 = filename:join([Cwd, "test/mod_used_for_transactions.erl"]),
    File2 = filename:join([Cwd, "test/stored_procs.erl"]),
    StoredProcPath = [sproc],
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun mod_used_for_transactions:crashing_fun/0))},

        {"Execute the stored procedure",
         ?_assertMatch(
            {throw,
             "Expected crash",
             [{_GeneratedModuleName, run,0, [{file, File1}, {line, _}]},
              {stored_procs, _, _, [{file, File2}, {line, _}]}
              | _]},
            try
                khepri:run_sproc(
                  ?FUNCTION_NAME, StoredProcPath, [])
            catch
                Class:Reason:Stacktrace ->
                    {Class, Reason, Stacktrace}
            end)}]
      }]}.
