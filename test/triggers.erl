%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(triggers).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("test/helpers.hrl").

event_triggers_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(?FUNCTION_NAME, [foo], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))}]
      }]}.

event_using_matching_pattern1_triggers_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo, #if_child_list_length{count = 0}]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))}]
      }]}.

event_using_matching_pattern2_triggers_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo, ?KHEPRI_WILDCARD_STAR_STAR]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a matching grandchild node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the procedure was executed for the grandchild node",
         ?_assertEqual(
          {create, [foo, bar]},
          receive_sproc_msg_with_props(Key))},

        {"Updating a matching great-grandchild node; "
         "should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar, baz], value))},

        {"Checking the procedure was executed for the great-grandchild node",
         ?_assertEqual(
          {create, [foo, bar, baz]},
          receive_sproc_msg_with_props(Key))}]
      }]}.

event_using_non_matching_pattern1_does_not_trigger_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree(
                    [?KHEPRI_WILDCARD_STAR, #if_child_list_length{count = 1}]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

event_using_non_matching_pattern2_does_not_trigger_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

event_using_non_matching_pattern3_does_not_trigger_associated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo, bar]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

event_does_not_trigger_unassociated_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

event_does_not_trigger_non_existing_sproc_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [non_existing | StoredProcPath],
              make_sproc(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

event_does_not_trigger_data_node_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              not_an_stored_proc))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [bar], value))},

        {"Checking the procedure was executed",
         ?_assertEqual(timeout, receive_sproc_msg(Key))}]
      }]}.

filter_on_change_type_test_() ->
    CreatedEventFilter = khepri_evf:tree([foo], #{on_actions => [create]}),
    UpdatedEventFilter = khepri_evf:tree([foo], #{on_actions => [update]}),
    DeletedEventFilter = khepri_evf:tree([foo], #{on_actions => [delete]}),
    StoredProcPath = [sproc],
    CreatedKey = {?FUNCTION_NAME, created},
    UpdatedKey = {?FUNCTION_NAME, updated},
    DeletedKey = {?FUNCTION_NAME, deleted},
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a procedure for `created` change",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [created],
              make_sproc(self(), CreatedKey)))},

        {"Storing a procedure for `updated` change",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [updated],
              make_sproc(self(), UpdatedKey)))},

        {"Storing a procedure for `deleted` change",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [deleted],
              make_sproc(self(), DeletedKey)))},

        {"Registering a `created` trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              created,
              CreatedEventFilter,
              StoredProcPath ++ [created]))},

        {"Registering a `updated` trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              updated,
              UpdatedEventFilter,
              StoredProcPath ++ [updated]))},

        {"Registering a `deleted` trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              deleted,
              DeletedEventFilter,
              StoredProcPath ++ [deleted]))},

        {"Creating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value1))},

        {"Checking the `created` procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(CreatedKey))},
        {"Checking the `updated` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(UpdatedKey))},
        {"Checking the `deleted` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(DeletedKey))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value2))},

        {"Checking the `created` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(CreatedKey))},
        {"Checking the `updated` procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(UpdatedKey))},
        {"Checking the `deleted` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(DeletedKey))},

        {"Deleting a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:delete(
              ?FUNCTION_NAME, [foo]))},

        {"Checking the `created` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(CreatedKey))},
        {"Checking the `updated` procedure was not executed",
         ?_assertEqual(timeout, receive_sproc_msg(UpdatedKey))},
        {"Checking the `deleted` procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(DeletedKey))}]
      }]}.

a_buggy_sproc_does_not_crash_state_machine_test_() ->
    EventFilter = khepri_evf:tree([foo]),
    StoredProcPath = [sproc],
    Key = ?FUNCTION_NAME,
    {setup,
     fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME) end,
     fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
     [{inorder,
       [{"Storing a working procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [good],
              make_sproc(self(), Key)))},

        {"Storing a failing procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [bad],
              fun(_Props) -> throw("Expected crash") end))},

        {"Registering trigger 1",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              good,
              EventFilter,
              StoredProcPath ++ [good]))},

        {"Registering trigger 2",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              bad,
              khepri_evf:set_priority(EventFilter, 10),
              StoredProcPath ++ [bad]))},

        {"Updating a node; should trigger the procedure",
         ?_test(
            begin
                Log = helpers:capture_log(
                        fun() ->
                                ?assertEqual(
                                  ok, khepri:put(?FUNCTION_NAME, [foo], 1))
                        end),
                ?assertMatch(
                   <<"Triggered stored procedure crash", _/binary>>, Log)
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            begin
                timer:sleep(2000),
                khepri:put(?FUNCTION_NAME, [foo], 2)
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            begin
                timer:sleep(2000),
                khepri:put(?FUNCTION_NAME, [foo], 3)
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            begin
                timer:sleep(2000),
                khepri:put(?FUNCTION_NAME, [foo], 4)
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            begin
                timer:sleep(2000),
                khepri:put(?FUNCTION_NAME, [foo], 5)
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            begin
                timer:sleep(2000),
                {Result, Log} = helpers:with_log(
                                  fun() ->
                                          khepri:put(?FUNCTION_NAME, [foo], 6)
                                  end),
                ?assertSubString(
                  <<"Triggered stored procedure crash">>, Log),
                ?assertSubString(
                  <<"(this crash occurred 6 times in the last 10 seconds)">>,
                  Log),
                Result
            end)},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))}]
      }]}.

make_sproc(Pid, Key) ->
    fun(Props) ->
            #{on_action := OnAction, path := Path} = Props,
            Pid ! {sproc, Key, {OnAction, Path}}
    end.

receive_sproc_msg(Key) ->
    receive {sproc, Key, _} -> executed
    after 1000              -> timeout
    end.

receive_sproc_msg_with_props(Key) ->
    receive {sproc, Key, Props} -> Props
    after 1000                  -> timeout
    end.
