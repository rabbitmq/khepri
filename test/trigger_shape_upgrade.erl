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

%% A store whose triggers are already in the version 4 shape is upgraded to
%% version 4 without touching them, and without crashing the machine.
%%
%% The shape is not a function of the machine version. Since the uniform
%% commands arrived, the legacy #register_trigger{} record is converted
%% before it is applied (convert_to_uniform_command/1), and that stores
%% #{event_filter, action, where} at machine version 2 and 3 as well. A store
%% that has any subscription therefore holds triggers in the version 4 shape,
%% and the 3 -> 4 conversion has to accept that shape.
a_trigger_already_in_the_version_4_shape_survives_the_upgrade_test() ->
    S0 = khepri_machine:init(?MACH_PARAMS()),
    S1 = khepri_machine:convert_state(S0, 0, 3),
    {S2, RegisterRet, _SideEffects} = register_legacy_trigger(S1),
    ?assertEqual(ok, RegisterRet),

    Triggers2 = khepri_machine:get_triggers(S2),
    ?assertMatch(#{my_trigger := #{action := {sproc, _},
                                   where := leader,
                                   event_filter := #evf_tree{}}},
                 Triggers2),

    S3 = khepri_machine:convert_state(S2, 3, 4),
    ?assertEqual(Triggers2, khepri_machine:get_triggers(S3)).

%% The shape written by the releases before the uniform command is still
%% converted.
a_trigger_in_the_old_shape_is_converted_test() ->
    Trigger = #{sproc => [sprocs, my_sproc],
                event_filter => #evf_tree{path = [foo]}},
    S0 = khepri_machine:init(?MACH_PARAMS()),
    S1 = khepri_machine:convert_state(S0, 0, 3),
    S2 = khepri_machine:set_triggers(S1, #{my_trigger => Trigger}),

    S3 = khepri_machine:convert_state(S2, 3, 4),
    ?assertEqual(#{my_trigger => #{action => {sproc, [sprocs, my_sproc]},
                                   where => leader,
                                   event_filter => #evf_tree{path = [foo]}}},
                 khepri_machine:get_triggers(S3)).

%% @private Register a trigger the way a replayed legacy command does: the
%% record is converted to its uniform equivalent first.
register_legacy_trigger(State) ->
    Command = khepri_machine:convert_to_uniform_command(
                #register_trigger{id = my_trigger,
                                  event_filter = #evf_tree{path = [foo]},
                                  sproc = [sprocs, my_sproc]}),
    khepri_machine:apply(?META, Command, State).
