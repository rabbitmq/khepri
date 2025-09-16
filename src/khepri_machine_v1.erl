%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri machine state V1 handling functions.
%%
%% @hidden

-module(khepri_machine_v1).

-include_lib("stdlib/include/assert.hrl").

-include("src/khepri_machine.hrl").

-export([init/1]).

%% Internal functions to access the opaque #khepri_machine{} state.
-export([is_state/1,
         get_config/1,
         get_tree/1, set_tree/2,
         get_triggers/1, set_triggers/2,
         get_emitted_triggers/1, set_emitted_triggers/2,
         get_projections/1, set_projections/2,
         get_metrics/1, set_metrics/2,
         get_dedups/1, set_dedups/2,
         assert_equal/2,
         state_to_list/1]).

-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         tree = khepri_tree:new() :: khepri_tree:tree(),
         triggers = #{} :: khepri_machine:triggers_map() |
                           khepri_machine:triggers_map_v2(),
         emitted_triggers = [] :: [khepri_machine:triggered()],
         projections = khepri_pattern_tree:empty() ::
                       khepri_machine:projection_tree(),
         metrics = #{} :: khepri_machine:metrics(),

         %% Added in machine version 1.
         dedups = #{} :: khepri_machine:dedups_map()}).

-opaque state() :: #khepri_machine{}.
%% State of this Ra state machine, version 1.
%%
%% Note that this type is used also for machine version 2. Machine version 2
%% changes the type of an opaque member of the {@link khepri_tree} record and
%% doesn't need any changes to the `khepri_machine' type. See the moduledoc of
%% this module for more information about version 2.

-export_type([state/0]).

-spec init(Params) -> State when
      Params :: khepri_machine:machine_init_args(),
      State :: khepri_machine_v0:state().
%% @private

init(Params) ->
    State = khepri_machine_v0:init(Params),
    State.

%% -------------------------------------------------------------------
%% State record management functions.
%% -------------------------------------------------------------------

-spec is_state(State) -> IsState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      IsState :: boolean().
%% @doc Tells if the given argument is a valid state.
%%
%% @private

is_state(State) ->
    is_record(State, khepri_machine) orelse khepri_machine_v0:is_state(State).

-spec get_config(State) -> Config when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Config :: khepri_machine:machine_config().
%% @doc Returns the config from the given state.
%%
%% @private

get_config(#khepri_machine{config = Config}) ->
    Config;
get_config(State) ->
    khepri_machine_v0:get_config(State).

-spec get_tree(State) -> Tree when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Tree :: khepri_tree:tree().
%% @doc Returns the tree from the given state.
%%
%% @private

get_tree(#khepri_machine{tree = Tree}) ->
    Tree;
get_tree(State) ->
    khepri_machine_v0:get_tree(State).

-spec set_tree(State, Tree) -> NewState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Tree :: khepri_tree:tree(),
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the tree in the given state.
%%
%% @private

set_tree(#khepri_machine{} = State, Tree) ->
    State#khepri_machine{tree = Tree};
set_tree(State, Tree) ->
    khepri_machine_v0:set_tree(State, Tree).

-spec get_triggers(State) -> Triggers when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Triggers :: khepri_machine:triggers_map().
%% @doc Returns the triggers from the given state.
%%
%% @private

get_triggers(#khepri_machine{triggers = Triggers}) ->
    Triggers;
get_triggers(State) ->
    khepri_machine_v0:get_triggers(State).

-spec set_triggers(State, Triggers) -> NewState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Triggers :: khepri_machine:triggers_map(),
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the triggers in the given state.
%%
%% @private

set_triggers(#khepri_machine{} = State, Triggers) ->
    State#khepri_machine{triggers = Triggers};
set_triggers(State, Triggers) ->
    khepri_machine_v0:set_triggers(State, Triggers).

-spec get_emitted_triggers(State) -> EmittedTriggers when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      EmittedTriggers :: [khepri_machine:triggered()].
%% @doc Returns the emitted_triggers from the given state.
%%
%% @private

get_emitted_triggers(#khepri_machine{emitted_triggers = EmittedTriggers}) ->
    EmittedTriggers;
get_emitted_triggers(State) ->
    khepri_machine_v0:get_emitted_triggers(State).

-spec set_emitted_triggers(State, EmittedTriggers) -> NewState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      EmittedTriggers :: [khepri_machine:triggered()],
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the emitted_triggers in the given state.
%%
%% @private

set_emitted_triggers(#khepri_machine{} = State, EmittedTriggers) ->
    State#khepri_machine{emitted_triggers = EmittedTriggers};
set_emitted_triggers(State, EmittedTriggers) ->
    khepri_machine_v0:set_emitted_triggers(State, EmittedTriggers).

-spec get_projections(State) -> Projections when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Projections :: khepri_machine:projection_tree().
%% @doc Returns the projections from the given state.
%%
%% @private

get_projections(#khepri_machine{projections = Projections}) ->
    Projections;
get_projections(State) ->
    khepri_machine_v0:get_projections(State).

-spec set_projections(State, Projections) -> NewState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Projections :: khepri_machine:projection_tree(),
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the projections in the given state.
%%
%% @private

set_projections(#khepri_machine{} = State, Projections) ->
    State#khepri_machine{projections = Projections};
set_projections(State, Projections) ->
    khepri_machine_v0:set_projections(State, Projections).

-spec get_metrics(State) -> Metrics when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Metrics :: khepri_machine:metrics().
%% @doc Returns the metrics from the given state.
%%
%% @private

get_metrics(#khepri_machine{metrics = Metrics}) ->
    Metrics;
get_metrics(State) ->
    khepri_machine_v0:get_metrics(State).

-spec set_metrics(State, Metrics) -> NewState when
      State :: khepri_machine_v1:state() |
                  khepri_machine_v0:state(),
      Metrics :: khepri_machine:metrics(),
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the metrics in the given state.
%%
%% @private

set_metrics(#khepri_machine{} = State, Metrics) ->
    State#khepri_machine{metrics = Metrics};
set_metrics(State, Metrics) ->
    khepri_machine_v0:set_metrics(State, Metrics).

-spec get_dedups(State) -> Dedups when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Dedups :: khepri_machine:dedups_map().
%% @doc Returns the dedups from the given state.
%%
%% @private

get_dedups(#khepri_machine{dedups = Dedups}) ->
    Dedups;
get_dedups(_State) ->
    #{}.

-spec set_dedups(State, Dedups) -> NewState when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Dedups :: khepri_machine:dedups_map(),
      NewState :: khepri_machine_v1:state() |
                  khepri_machine_v0:state().
%% @doc Sets the dedups in the given state.
%%
%% @private

set_dedups(#khepri_machine{} = State, Dedups) ->
    State#khepri_machine{dedups = Dedups};
set_dedups(State, _Dedups) ->
    State.

-spec state_to_list(State) -> Fields when
      State :: khepri_machine_v1:state() |
               khepri_machine_v0:state(),
      Fields :: [any()].
%% @doc Returns the fields of the state as a list.
%%
%% @private

state_to_list(#khepri_machine{} = State) ->
    tuple_to_list(State);
state_to_list(State) ->
    khepri_machine_v0:state_to_list(State).

-spec assert_equal(State1, State2) -> ok when
      State1 :: khepri_machine_v1:state() |
                khepri_machine_v0:state(),
      State2 :: khepri_machine_v1:state() |
                khepri_machine_v0:state().

assert_equal(#khepri_machine{} = State1, #khepri_machine{} = State2) ->
    ?assertEqual(State1, State2),
    ok;
assert_equal(State1, State2) ->
    khepri_machine_v0:assert_equal(State1, State2),
    ok.
