%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers
%% to Broadcom Inc. and/or its subsidiaries.
%%

-module(protect_against_dups_option).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_error.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

multiple_dedup_commands_test() ->
    S00 = khepri_machine:init(?MACH_PARAMS()),
    S0 = khepri_machine:convert_state(S00, 0, 1),

    Command = #put{path = [foo],
                   payload = khepri_payload:data(value),
                   options = #{expect_specific_node => true,
                               props_to_return => [payload,
                                                   payload_version]}},
    CommandRef = make_ref(),
    Expiry = erlang:system_time(millisecond) + 5000,
    DedupCommand = #dedup{ref = CommandRef,
                          expiry = Expiry,
                          command = Command},
    {S1, Ret1, SE1} = khepri_machine:apply(?META, DedupCommand, S0),
    ExpectedRet = {ok, #{[foo] => #{payload_version => 1}}},

    Dedups1 = khepri_machine:get_dedups(S1),
    ?assertEqual(#{CommandRef => {ExpectedRet, Expiry}}, Dedups1),

    Root1 = khepri_machine:get_root(S1),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               props = ?INIT_NODE_PROPS,
               payload = khepri_payload:data(value)}}},
       Root1),
    ?assertEqual(ExpectedRet, Ret1),
    ?assertEqual([], SE1),

    %% The put command is idempotent, so not really ideal to test
    %% deduplication. Instead, we mess up with the state and silently restore
    %% the initial empty tree. If the dedup mechanism works, the returned
    %% state shouldn't have the `foo' node either because it didn't process
    %% the command.
    PatchedS1 = khepri_machine:set_tree(S1, khepri_machine:get_tree(S0)),
    {S2, Ret2, SE2} = khepri_machine:apply(?META, DedupCommand, PatchedS1),

    Dedups2 = khepri_machine:get_dedups(S2),
    ?assertEqual(#{CommandRef => {ExpectedRet, Expiry}}, Dedups2),

    Root0 = khepri_machine:get_root(S0),
    Root2 = khepri_machine:get_root(S2),
    ?assertEqual(Root0, Root2),

    ?assertEqual(ExpectedRet, Ret2),
    ?assertEqual([], SE2).

dedup_and_dedup_ack_test() ->
    S00 = khepri_machine:init(?MACH_PARAMS()),
    S0 = khepri_machine:convert_state(S00, 0, 1),

    Command = #put{path = [foo],
                   payload = khepri_payload:data(value),
                   options = #{expect_specific_node => true,
                               props_to_return => [payload,
                                                   payload_version]}},
    CommandRef = make_ref(),
    Expiry = erlang:system_time(millisecond) + 5000,
    DedupCommand = #dedup{ref = CommandRef,
                          expiry = Expiry,
                          command = Command},
    {S1, Ret1, SE1} = khepri_machine:apply(?META, DedupCommand, S0),
    ExpectedRet = {ok, #{[foo] => #{payload_version => 1}}},

    Dedups1 = khepri_machine:get_dedups(S1),
    ?assertEqual(#{CommandRef => {ExpectedRet, Expiry}}, Dedups1),

    Root1 = khepri_machine:get_root(S1),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               props = ?INIT_NODE_PROPS,
               payload = khepri_payload:data(value)}}},
       Root1),
    ?assertEqual(ExpectedRet, Ret1),
    ?assertEqual([], SE1),

    DedupAck = #dedup_ack{ref = CommandRef},
    {S2, Ret2, SE2} = khepri_machine:apply(?META, DedupAck, S1),

    Dedups2 = khepri_machine:get_dedups(S2),
    ?assertEqual(#{}, Dedups2),

    Root2 = khepri_machine:get_root(S2),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 2},
          child_nodes =
          #{foo =>
            #node{
               props = ?INIT_NODE_PROPS,
               payload = khepri_payload:data(value)}}},
       Root2),
    ?assertEqual(ok, Ret2),
    ?assertEqual([], SE2).

dedup_expiry_test_() ->
   TickTimeout = 200,
   Config = #{tick_timeout => TickTimeout},
   StoredProcPath = [sproc],
   Path = [stock, wood, <<"oak">>],
   Command = #tx{'fun' = StoredProcPath, args = []},
   CommandRef = make_ref(),
   Expiry = erlang:system_time(millisecond),
   DedupCommand = #dedup{ref = CommandRef,
                         command = Command,
                         expiry = Expiry},
   {setup,
    fun() -> test_ra_server_helpers:setup(?FUNCTION_NAME, Config) end,
    fun(Priv) -> test_ra_server_helpers:cleanup(Priv) end,
    [{inorder,
      [{"Store a procedure for later use",
        ?_assertEqual(
            ok,
            khepri:put(
              ?FUNCTION_NAME, StoredProcPath,
              fun() ->
                    {ok, N} = khepri_tx:get(Path),
                    khepri_tx:put(Path, N + 1)
              end))},

       {"Store initial data",
        ?_assertEqual(
            ok,
            khepri:put(?FUNCTION_NAME, Path, 1))},

       {"Trigger the transaction",
        ?_assertEqual(
            ok,
            khepri_machine:do_process_sync_command(
              ?FUNCTION_NAME, DedupCommand, #{}))},

       {"The transaction was applied and the data is incremented",
        ?_assertEqual(
            {ok, 2},
            khepri:get(?FUNCTION_NAME, Path))},

       {"Trigger the transaction again before the dedup can be expired",
        ?_assertEqual(
            ok,
            khepri_machine:do_process_sync_command(
              ?FUNCTION_NAME, DedupCommand, #{}))},

       {"The transaction was deduplicated and the data is unchanged",
        ?_assertEqual(
            {ok, 2},
            khepri:get(?FUNCTION_NAME, Path))},

       {"Sleep and send the same transaction command",
        ?_test(
            begin
                %% Sleep a little extra for the sake of slow CI runners.
                %% During this sleep time the machine will receive Ra's
                %% periodic `tick' aux effect which will trigger expiration of
                %% dedups.
                SleepTime = TickTimeout + erlang:floor(TickTimeout * 3 / 4),
                timer:sleep(SleepTime),
                %% The dedup should be expired so this duplicate command should
                %% be handled and the data should be incremented.
                ?assertEqual(
                   ok,
                   khepri_machine:do_process_sync_command(
                     ?FUNCTION_NAME, DedupCommand, #{}))
            end)},

       {"The transaction was applied again and the data is incremented",
        ?_assertEqual(
            {ok, 3},
            khepri:get(?FUNCTION_NAME, Path))}]}]}.

dedup_ack_after_no_dedup_test() ->
    S00 = khepri_machine:init(?MACH_PARAMS()),
    S0 = khepri_machine:convert_state(S00, 0, 1),

    CommandRef = make_ref(),
    DedupAck = #dedup_ack{ref = CommandRef},
    {S1, Ret1, SE1} = khepri_machine:apply(?META, DedupAck, S0),

    Dedups1 = khepri_machine:get_dedups(S1),
    ?assertEqual(#{}, Dedups1),

    Root1 = khepri_machine:get_root(S1),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 1},
          child_nodes = #{}},
       Root1),
    ?assertEqual(ok, Ret1),
    ?assertEqual([], SE1).

dedup_on_old_machine_test() ->
    S00 = khepri_machine:init(?MACH_PARAMS()),
    S0 = khepri_machine:convert_state(S00, 0, 1),

    Command = #put{path = [foo],
                   payload = khepri_payload:data(value),
                   options = #{expect_specific_node => true,
                               props_to_return => [payload,
                                                   payload_version]}},
    CommandRef = make_ref(),
    Expiry = erlang:system_time(millisecond) + 5000,
    DedupCommand = #dedup{ref = CommandRef,
                          expiry = Expiry,
                          command = Command},
    MacVer = 0,

    Meta0 = ?META,
    Meta = Meta0#{machine_version => MacVer},
    {S1, Ret1, _SE1} = khepri_machine:apply(Meta, DedupCommand, S0),

    Dedups1 = khepri_machine:get_dedups(S1),
    ?assertEqual(#{}, Dedups1),

    Root1 = khepri_machine:get_root(S1),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 1},
          child_nodes = #{}},
       Root1),
    ?assertEqual(
       {error, ?khepri_exception(
                  unknown_khepri_state_machine_command,
                  #{command => DedupCommand,
                    machine_version => MacVer})},
       Ret1).

dedup_ack_on_old_machine_test() ->
    S00 = khepri_machine:init(?MACH_PARAMS()),
    S0 = khepri_machine:convert_state(S00, 0, 1),

    CommandRef = make_ref(),
    DedupAck = #dedup_ack{ref = CommandRef},
    MacVer = 0,

    Meta0 = ?META,
    Meta = Meta0#{machine_version => MacVer},
    {S1, Ret1, _SE1} = khepri_machine:apply(Meta, DedupAck, S0),

    Dedups1 = khepri_machine:get_dedups(S1),
    ?assertEqual(#{}, Dedups1),

    Root1 = khepri_machine:get_root(S1),
    ?assertEqual(
       #node{
          props =
          #{payload_version => 1,
            child_list_version => 1},
          child_nodes = #{}},
       Root1),
    ?assertEqual(
       {error, ?khepri_exception(
                  unknown_khepri_state_machine_command,
                  #{command => DedupAck,
                    machine_version => MacVer})},
       Ret1).
