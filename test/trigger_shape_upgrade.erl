%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%

-module(trigger_shape_upgrade).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("src/khepri_evf.hrl").
-include("test/helpers.hrl").

-define(META_V(MacVer), meta_at(MacVer, ?LINE)).

%% The machine version that introduced the extended trigger, and the one
%% before it, both taken from the behaviour map.
-define(NEW_MACVER, maps:get(extended_trigger, ?API_BEHAV_MACVER_MAP)).
-define(OLD_MACVER, (?NEW_MACVER - 1)).

%% A machine that does not comply with `extended_trigger' stores a trigger in
%% the old format, `#{sproc, event_filter}'. Its state can be sent as a
%% snapshot to a Ra member that still runs the old state machine, and that
%% machine only knows this format.
a_trigger_is_stored_in_the_old_format_before_extended_trigger_test() ->
    S0 = machine_at(?OLD_MACVER),
    {S1, Ret, _SideEffects} = register_legacy_trigger(S0, ?OLD_MACVER),
    ?assertEqual(ok, Ret),
    ?assertEqual(
       #{my_trigger => #{sproc => [sprocs, my_sproc],
                         event_filter => #evf_tree{path = [foo]}}},
       khepri_machine:get_triggers(S1)).

%% The same holds for a `#register_trigger_v{}' command applied by a machine
%% that does not comply with `extended_trigger' yet, as long as the trigger
%% is one the old format can express.
a_versioned_sproc_trigger_is_stored_in_the_old_format_test() ->
    S0 = machine_at(?OLD_MACVER),
    Command = register_trigger_v({sproc, [sprocs, my_sproc]}, #{}),
    {S1, Ret, _SideEffects} = khepri_machine:apply(
                                ?META_V(?OLD_MACVER), Command, S0),
    ?assertEqual(ok, Ret),
    ?assertEqual(
       #{my_trigger => #{sproc => [sprocs, my_sproc],
                         event_filter => #evf_tree{path = [foo]}}},
       khepri_machine:get_triggers(S1)).

%% A machine that complies with `extended_trigger' stores the new format.
a_trigger_is_stored_in_the_new_format_with_extended_trigger_test() ->
    S0 = machine_at(?NEW_MACVER),
    {S1, Ret, _SideEffects} = register_legacy_trigger(S0, ?NEW_MACVER),
    ?assertEqual(ok, Ret),
    ?assertEqual(
       #{my_trigger => #{action => {sproc, [sprocs, my_sproc]},
                         where => leader,
                         event_filter => #evf_tree{path = [foo]}}},
       khepri_machine:get_triggers(S1)).

%% The old format cannot express another action than a stored procedure,
%% another `where' than the leader, or another event filter than a tree
%% change. A machine that does not comply with `extended_trigger' refuses to
%% store such a trigger instead of writing a format an old member cannot
%% read.
an_mfa_action_is_refused_before_extended_trigger_test() ->
    Command = register_trigger_v({apply, {erlang, self, []}}, #{}),
    ?assertError(
       {assertMatch, _},
       khepri_machine:apply(
         ?META_V(?OLD_MACVER), Command, machine_at(?OLD_MACVER))).

a_where_other_than_leader_is_refused_before_extended_trigger_test() ->
    Command = register_trigger_v(
                {sproc, [sprocs, my_sproc]}, #{where => all_members}),
    ?assertError(
       {assertEqual, _},
       khepri_machine:apply(
         ?META_V(?OLD_MACVER), Command, machine_at(?OLD_MACVER))).

a_process_event_filter_is_refused_before_extended_trigger_test() ->
    Command = #register_trigger_v{
                 args = #register_trigger_v1{
                           id = my_trigger,
                           event_filter = khepri_evf:process(self()),
                           action = {sproc, [sprocs, my_sproc]}}},
    ?assertError(
       {assertMatch, _},
       khepri_machine:apply(
         ?META_V(?OLD_MACVER), Command, machine_at(?OLD_MACVER))).

%% The snapshot path: a trigger stored before `extended_trigger' fires as a
%% `#triggered{}' record, the one an old member knows how to run, and the
%% upgrade to the `extended_trigger' version then converts the stored trigger
%% to the new format.
a_trigger_stored_before_extended_trigger_fires_and_upgrades_test() ->
    helpers:init_list_of_modules_to_skip(),
    S0 = machine_at(?OLD_MACVER),
    StoredProc = khepri_tx_adv:to_standalone_fun(
                   fun(_Props) -> ok end, rw),
    PutSproc = #put{path = [sprocs, my_sproc],
                    payload = khepri_payload:sproc(StoredProc),
                    options = #{}},
    {S1, {ok, _}, _} = khepri_machine:apply(
                         ?META_V(?OLD_MACVER), PutSproc, S0),
    {S2, ok, _} = register_legacy_trigger(S1, ?OLD_MACVER),

    PutFoo = #put{path = [foo],
                  payload = khepri_payload:data(value),
                  options = #{}},
    {S3, {ok, _}, _} = khepri_machine:apply(?META_V(?OLD_MACVER), PutFoo, S2),
    ?assertMatch(
       [#triggered{id = my_trigger,
                   event_filter = #evf_tree{path = [foo]},
                   props = #{path := [foo], on_action := create}}],
       khepri_machine:get_emitted_triggers(S3)),

    S4 = khepri_machine:convert_state(S3, ?OLD_MACVER, ?NEW_MACVER),
    ?assertEqual(
       #{my_trigger => #{action => {sproc, [sprocs, my_sproc]},
                         where => leader,
                         event_filter => #evf_tree{path = [foo]}}},
       khepri_machine:get_triggers(S4)).

%% Releases 0.19.0 to 0.19.3 stored the new format before `extended_trigger'
%% as well. A store written by one of them is upgraded to the
%% `extended_trigger' version without touching those triggers, and without
%% crashing the machine.
a_trigger_already_in_the_new_format_survives_the_upgrade_test() ->
    Trigger = #{action => {sproc, [sprocs, my_sproc]},
                where => leader,
                event_filter => #evf_tree{path = [foo]}},
    S0 = khepri_machine:set_triggers(
           machine_at(?OLD_MACVER), #{my_trigger => Trigger}),
    S1 = khepri_machine:convert_state(S0, ?OLD_MACVER, ?NEW_MACVER),
    ?assertEqual(#{my_trigger => Trigger}, khepri_machine:get_triggers(S1)).

%% @private Initialises a machine state at the given version.
machine_at(MacVer) ->
    khepri_machine:convert_state(
      khepri_machine:init(?MACH_PARAMS()), 0, MacVer).

%% @private Registers a trigger the way a machine that does not comply with
%% `extended_trigger' receives it: as the legacy `#register_trigger{}' record.
register_legacy_trigger(State, MacVer) ->
    Command = #register_trigger{id = my_trigger,
                                event_filter = #evf_tree{path = [foo]},
                                sproc = [sprocs, my_sproc]},
    khepri_machine:apply(?META_V(MacVer), Command, State).

%% @private Builds a `#register_trigger_v{}' command on `[foo]'.
register_trigger_v(Action, Options) ->
    #register_trigger_v{
       args = #register_trigger_v1{id = my_trigger,
                                   event_filter = #evf_tree{path = [foo]},
                                   action = Action,
                                   options = Options}}.

%% @private Builds the command metadata of a machine at the given version.
meta_at(MacVer, Index) ->
    #{term => 1,
      index => Index,
      system_time => erlang:system_time(millisecond),
      machine_version => MacVer}.
