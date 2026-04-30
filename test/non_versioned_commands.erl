%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(non_versioned_commands).

-include_lib("eunit/include/eunit.hrl").

-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

non_versioned_put_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    Put = #put{path = [foo],
               payload = khepri_payload:data(value),
               options = #{props_to_return => [payload_version]}},
    {_, Ret, _} = khepri_machine:apply(get_meta(), Put, S0),
    ?assertEqual({ok, #{[foo] => #{payload_version => 1}}}, Ret).

non_versioned_delete_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    Delete = #delete{path = [foo],
                     options = #{props_to_return => [payload_version]}},
    {_, Ret, _} = khepri_machine:apply(get_meta(), Delete, S0),
    ?assertEqual({ok, #{}}, Ret).

non_versioned_tx_test() ->
    helpers:init_list_of_modules_to_skip(),
    S0 = khepri_machine:init(?MACH_PARAMS()),

    TxFun = fun() -> done end,
    TxSF = khepri_tx_adv:to_standalone_fun(TxFun, rw),
    Tx = #tx{'fun' = TxSF},
    {_, Ret, _} = khepri_machine:apply(get_meta(), Tx, S0),
    ?assertEqual(done, Ret).

non_versioned_register_trigger_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    RegisterTrigger = #register_trigger{id = ?FUNCTION_NAME,
                                        event_filter = khepri_evf:tree([foo]),
                                        sproc = [sproc]},
    {_, Ret, _} = khepri_machine:apply(get_meta(), RegisterTrigger, S0),
    ?assertEqual(ok, Ret).

non_versioned_ack_triggered_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    AckTriggered = #ack_triggered{triggered = []},
    {_, Ret, _} = khepri_machine:apply(get_meta(), AckTriggered, S0),
    ?assertEqual(ok, Ret).

non_versioned_register_projection_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    ProjectFun = fun(_Path, _Payload) -> ok end,
    Projection = khepri_projection:new(?MODULE, ProjectFun),
    RegisterProjection = #register_projection{pattern = [],
                                              projection = Projection},
    {_, Ret, _} = khepri_machine:apply(get_meta(), RegisterProjection, S0),
    ?assertEqual(ok, Ret).

non_versioned_unregister_projection_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    UnregisterProjection = #unregister_projection{name = ?FUNCTION_NAME},
    {_, Ret, _} = khepri_machine:apply(get_meta(), UnregisterProjection, S0),
    ?assertEqual({ok, #{}}, Ret).

non_versioned_unregister_projections_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    UnregisterProjections = #unregister_projections{names = all},
    {_, Ret, _} = khepri_machine:apply(get_meta(), UnregisterProjections, S0),
    ?assertEqual({ok, #{}}, Ret).

non_versioned_dedup_ack_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    DedupAck = #dedup_ack{ref = make_ref()},
    {_, Ret, _} = khepri_machine:apply(get_meta(), DedupAck, S0),
    ?assertEqual(ok, Ret).

non_versioned_drop_dedups_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),

    DropDedups = #drop_dedups{refs = []},
    {_, Ret, _} = khepri_machine:apply(get_meta(), DropDedups, S0),
    ?assertEqual(ok, Ret).

get_meta() ->
    EffectiveMacVer = maps:get(uniform_commands, ?API_BEHAV_MACVER_MAP) - 1,
    Meta0 = ?META,
    Meta = Meta0#{machine_version => EffectiveMacVer},
    Meta.

convert_to_uniform_command_test() ->
    TxFun = fun() -> done end,
    TxSF = khepri_tx_adv:to_standalone_fun(TxFun, rw),
    ProjectFun = fun(_Path, _Payload) -> ok end,
    Projection = khepri_projection:new(?MODULE, ProjectFun),
    Ref = make_ref(),

    Records = [{#put{path = []},
                #put_v{args = #put_v1{path = []}}},

               {#delete{path = []},
                #delete_v{args = #delete_v1{path = []}}},

               {#tx{'fun' = TxSF},
                #tx_v{args = #tx_v1{'fun' = TxSF}}},

               {#register_trigger{id = ?FUNCTION_NAME,
                                  event_filter = khepri_evf:tree([foo]),
                                  sproc = [sproc]},
                #register_trigger_v{
                   args = #register_trigger_v1{
                             id = ?FUNCTION_NAME,
                             event_filter = khepri_evf:tree([foo]),
                             sproc = [sproc]}}},

               {#ack_triggered{triggered = []},
                #ack_triggered_v{args = #ack_triggered_v1{triggered = []}}},

               {#register_projection{pattern = [], projection = Projection},
                #register_projection_v{
                   args = #register_projection_v1{
                             pattern = [],
                             projection = Projection}}},

               {#unregister_projection{name = ?FUNCTION_NAME},
                #unregister_projections_v{
                   args = #unregister_projections_v1{
                             names = [?FUNCTION_NAME]}}},

               {#unregister_projections{names = all},
                #unregister_projections_v{
                   args = #unregister_projections_v1{
                             names = all}}},

               {#dedup{ref = Ref, expiry = 1, command = #put{path = []}},
                same},

               {#dedup_ack{ref = Ref},
                #dedup_ack_v{args = #dedup_ack_v1{ref = Ref}}},

               {#drop_dedups{refs = []},
                #drop_dedups_v{args = #drop_dedups_v1{refs = []}}},

               {#put_v{args = #put_v1{path = []}},
                same},

               {#delete_v{args = #delete_v1{path = []}},
                same},

               {#tx_v{args = #tx_v1{'fun' = TxSF}},
                same},

               {#register_trigger_v{
                   args = #register_trigger_v1{
                             id = ?FUNCTION_NAME,
                             event_filter = khepri_evf:tree([foo]),
                             sproc = [sproc]}},
                same},

               {#ack_triggered_v{args = #ack_triggered_v1{triggered = []}},
                same},

               {#register_projection_v{
                   args = #register_projection_v1{
                             pattern = [],
                             projection = Projection}},
                same},

               {#unregister_projections_v{
                   args = #unregister_projections_v1{
                             names = [?FUNCTION_NAME]}},
                same},

               {#dedup_ack_v{args = #dedup_ack_v1{ref = Ref}},
                same},

               {#drop_dedups_v{args = #drop_dedups_v1{refs = []}},
                same}],
    lists:foreach(
      fun({OldRecord, ExpectedNewRecord}) ->
              NewRecord = khepri_machine:convert_to_uniform_command(
                            OldRecord),
              case ExpectedNewRecord of
                  same ->
                      ?assertEqual(OldRecord, NewRecord);
                  _ ->
                      ExpectedNewRecord1 = (
                        khepri_machine:compute_command_size(
                          ExpectedNewRecord)),
                      ?assertEqual(ExpectedNewRecord1, NewRecord)
              end
      end, Records).
