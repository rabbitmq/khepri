%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2026 Broadcom. All Rights Reserved. The term "Broadcom"
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

        {"Adding a node; should not trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the procedure was not executed",
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

        {"Adding a child node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value1))},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a child node; should not trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value2))},

        {"Checking the procedure was not executed",
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

        {"Adding a node; should not trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value))},

        {"Checking the procedure was not executed",
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

data_payload_is_passed_to_sproc_test_() ->
    CreatedEventFilter = khepri_evf:tree(
                           [foo], #{on_actions => [create],
                                    props_to_return => [payload]}),
    UpdatedEventFilter = khepri_evf:tree(
                           [foo], #{on_actions => [update],
                                    props_to_return => [payload]}),
    DeletedEventFilter = khepri_evf:tree(
                           [foo], #{on_actions => [delete],
                                    props_to_return => [payload]}),
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
              make_sproc_returning_props(self(), CreatedKey)))},

        {"Storing a procedure for `updated` change",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [updated],
              make_sproc_returning_props(self(), UpdatedKey)))},

        {"Storing a procedure for `deleted` change",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath ++ [deleted],
              make_sproc_returning_props(self(), DeletedKey)))},

        {"Registering a `created` trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              created,
              CreatedEventFilter,
              StoredProcPath ++ [created]))},

        {"Registering an `updated` trigger",
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

        {"Creating a node; the new payload should be passed",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value1))},

        {"Checking the `created` procedure received the new payload",
         ?_assertEqual(
            #{path => [foo], on_action => create, data => value1},
            receive_sproc_msg_with_props(CreatedKey))},

        {"Updating a node; the new payload should be passed",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value2))},

        {"Checking the `updated` procedure received the new payload",
         ?_assertEqual(
            #{path => [foo], on_action => update, data => value2},
            receive_sproc_msg_with_props(UpdatedKey))},

        {"Deleting a node; the payload before deletion should be passed",
         ?_assertMatch(
            ok,
            khepri:delete(
              ?FUNCTION_NAME, [foo]))},

        {"Checking the `deleted` procedure received the old payload",
         ?_assertEqual(
            #{path => [foo], on_action => delete, data => value2},
            receive_sproc_msg_with_props(DeletedKey))}]
      }]}.

no_data_key_when_node_has_no_payload_test_() ->
    %% The trigger requests the payload but matches the intermediate node
    %% `[foo]' which is created without a payload as a byproduct of creating
    %% `[foo, bar]'. The `data' key is therefore absent.
    EventFilter = khepri_evf:tree([foo], #{props_to_return => [payload]}),
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
              make_sproc_returning_props(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Creating a node with a payload-less parent",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo, bar], value))},

        {"Checking the props don't contain a `data` key",
         ?_assertEqual(
            #{path => [foo], on_action => create},
            receive_sproc_msg_with_props(Key))}]
      }]}.

props_to_return_in_event_filter_test_() ->
    %% The event filter requests extra node properties to be passed to the
    %% triggered stored procedure.
    EventFilter = khepri_evf:tree(
                    [foo],
                    #{props_to_return => [payload, payload_version,
                                          child_list_length]}),
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
              make_sproc_returning_props(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Creating a node",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value))},

        {"Checking the props contain the requested node properties",
         ?_assertEqual(
            #{path => [foo], on_action => create, data => value,
              payload_version => 1, child_list_length => 0},
            receive_sproc_msg_with_props(Key))}]
      }]}.

empty_props_to_return_in_event_filter_test_() ->
    %% An empty `props_to_return' yields only `path' and `on_action'.
    EventFilter = khepri_evf:tree([foo], #{props_to_return => []}),
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
              make_sproc_returning_props(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Creating a node",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value))},

        {"Checking the props only contain `path` and `on_action`",
         ?_assertEqual(
            #{path => [foo], on_action => create},
            receive_sproc_msg_with_props(Key))}]
      }]}.

default_passes_only_path_and_on_action_test_() ->
    %% By default (no `props_to_return' in the event filter), no node
    %% properties are included: the feature is opt-in.
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
              make_sproc_returning_props(self(), Key)))},

        {"Registering a trigger",
         ?_assertEqual(
            ok,
            khepri:register_trigger(
              ?FUNCTION_NAME,
              ?FUNCTION_NAME,
              EventFilter,
              StoredProcPath))},

        {"Creating a node with a payload",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, [foo], value))},

        {"Checking the props only contain `path` and `on_action`",
         ?_assertEqual(
            #{path => [foo], on_action => create},
            receive_sproc_msg_with_props(Key))}]
      }]}.


filter_on_pattern_test_() ->
    Path = [foo, bar],
    EventFilter = khepri_evf:tree([foo, #if_has_data{}]),
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

        {"Creating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, Path, value1))},

        {"Checking the procedure was executed on create",
         ?_assertEqual(executed, receive_sproc_msg(?FUNCTION_NAME))},

        {"Updating a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:put(
              ?FUNCTION_NAME, Path, value2))},

        {"Checking the procedure was executed on update",
         ?_assertEqual(executed, receive_sproc_msg(?FUNCTION_NAME))},

        {"Deleting a node; should trigger the procedure",
         ?_assertMatch(
            ok,
            khepri:delete(
              ?FUNCTION_NAME, Path))},

        {"Checking the procedure was executed on delete",
         ?_assertEqual(executed, receive_sproc_msg(?FUNCTION_NAME))}]
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
         {timeout, 10,
          ?_assertMatch(
             ok,
             begin
                 timer:sleep(2000),
                 khepri:put(?FUNCTION_NAME, [foo], 2)
             end)}},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         {timeout, 10,
          ?_assertMatch(
             ok,
             begin
                 timer:sleep(2000),
                 khepri:put(?FUNCTION_NAME, [foo], 3)
             end)}},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         {timeout, 10,
          ?_assertMatch(
             ok,
             begin
                 timer:sleep(2000),
                 khepri:put(?FUNCTION_NAME, [foo], 4)
             end)}},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         {timeout, 10,
          ?_assertMatch(
             ok,
             begin
                 timer:sleep(2000),
                 khepri:put(?FUNCTION_NAME, [foo], 5)
             end)}},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))},

        {"Updating a node; should trigger the procedure",
         {timeout, 10,
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
             end)}},

        {"Checking the procedure was executed",
         ?_assertEqual(executed, receive_sproc_msg(Key))}]
      }]}.

make_sproc(Pid, Key) ->
    fun(Props) ->
            #{on_action := OnAction, path := Path} = Props,
            Pid ! {sproc, Key, {OnAction, Path}}
    end.

make_sproc_returning_props(Pid, Key) ->
    fun(Props) ->
            Pid ! {sproc, Key, Props}
    end.

receive_sproc_msg(Key) ->
    receive {sproc, Key, _} -> executed
    after 1000              -> timeout
    end.

receive_sproc_msg_with_props(Key) ->
    receive {sproc, Key, Props} -> Props
    after 1000                  -> timeout
    end.
