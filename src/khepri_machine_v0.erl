%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri machine state V0 handling functions.
%%
%% @hidden

-module(khepri_machine_v0).

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
         assert_equal/2,
         state_to_list/1]).

-record(khepri_machine,
        {config = #config{} :: khepri_machine:machine_config(),
         tree = khepri_tree:new() :: khepri_tree:tree_v0(),
         triggers = #{} ::
           #{khepri:trigger_id() =>
             #{sproc := khepri_path:native_path(),
               event_filter := khepri_evf:event_filter()}},
         emitted_triggers = [] :: [khepri_machine:triggered()],
         projections = khepri_pattern_tree:empty() ::
                       khepri_machine:projection_tree(),
         metrics = #{} :: #{applied_command_count => non_neg_integer()}}).

-opaque state() :: #khepri_machine{}.
%% State of this Ra state machine, version 0.

-export_type([state/0]).

-spec init(Params) -> State when
      Params :: khepri_machine:machine_init_args(),
      State :: state().
%% @private

init(#{store_id := StoreId,
       member := Member} = Params) ->
    Config = case Params of
                 #{snapshot_interval := SnapshotInterval} ->
                     #config{store_id = StoreId,
                             member = Member,
                             snapshot_interval = SnapshotInterval};
                 _ ->
                     #config{store_id = StoreId,
                             member = Member}
             end,
    #khepri_machine{config = Config}.

%% -------------------------------------------------------------------
%% State record management functions.
%% -------------------------------------------------------------------

-spec is_state(State) -> IsState when
      State :: khepri_machine_v0:state(),
      IsState :: boolean().
%% @doc Tells if the given argument is a valid state.
%%
%% @private

is_state(State) ->
    is_record(State, khepri_machine).

-spec get_config(State) -> Config when
      State :: khepri_machine_v0:state(),
      Config :: khepri_machine:machine_config().
%% @doc Returns the config from the given state.
%%
%% @private

get_config(#khepri_machine{config = Config}) ->
    Config.

-spec get_tree(State) -> Tree when
      State :: khepri_machine_v0:state(),
      Tree :: khepri_tree:tree_v0().
%% @doc Returns the tree from the given state.
%%
%% @private

get_tree(#khepri_machine{tree = Tree}) ->
    Tree.

-spec set_tree(State, Tree) -> NewState when
      State :: khepri_machine_v0:state(),
      Tree :: khepri_tree:tree(),
      NewState :: khepri_machine_v0:state().
%% @doc Sets the tree in the given state.
%%
%% @private

set_tree(#khepri_machine{} = State, Tree) ->
    State#khepri_machine{tree = Tree}.

-spec get_triggers(State) -> Triggers when
      State :: khepri_machine_v0:state(),
      Triggers :: khepri_machine:triggers_map().
%% @doc Returns the triggers from the given state.
%%
%% @private

get_triggers(#khepri_machine{triggers = Triggers}) ->
    Triggers.

-spec set_triggers(State, Triggers) -> NewState when
      State :: khepri_machine_v0:state(),
      Triggers :: khepri_machine:triggers_map(),
      NewState :: khepri_machine_v0:state().
%% @doc Sets the triggers in the given state.
%%
%% @private

set_triggers(#khepri_machine{} = State, Triggers) ->
    State#khepri_machine{triggers = Triggers}.

-spec get_emitted_triggers(State) -> EmittedTriggers when
      State :: khepri_machine_v0:state(),
      EmittedTriggers :: [khepri_machine:triggered()].
%% @doc Returns the emitted_triggers from the given state.
%%
%% @private

get_emitted_triggers(#khepri_machine{emitted_triggers = EmittedTriggers}) ->
    EmittedTriggers.

-spec set_emitted_triggers(State, EmittedTriggers) -> NewState when
      State :: khepri_machine_v0:state(),
      EmittedTriggers :: [khepri_machine:triggered()],
      NewState :: khepri_machine_v0:state().
%% @doc Sets the emitted_triggers in the given state.
%%
%% @private

set_emitted_triggers(#khepri_machine{} = State, EmittedTriggers) ->
    State#khepri_machine{emitted_triggers = EmittedTriggers}.

-spec get_projections(State) -> Projections when
      State :: khepri_machine_v0:state(),
      Projections :: khepri_machine:projection_tree().
%% @doc Returns the projections from the given state.
%%
%% @private

get_projections(#khepri_machine{projections = Projections}) ->
    Projections.

-spec set_projections(State, Projections) -> NewState when
      State :: khepri_machine_v0:state(),
      Projections :: khepri_machine:projection_tree(),
      NewState :: khepri_machine_v0:state().
%% @doc Sets the projections in the given state.
%%
%% @private

set_projections(#khepri_machine{} = State, Projections) ->
    State#khepri_machine{projections = Projections}.

-spec get_metrics(State) -> Metrics when
      State :: khepri_machine_v0:state(),
      Metrics :: khepri_machine:metrics().
%% @doc Returns the metrics from the given state.
%%
%% @private

get_metrics(#khepri_machine{metrics = Metrics}) ->
    Metrics.

-spec set_metrics(State, Metrics) -> NewState when
      State :: khepri_machine_v0:state(),
      Metrics :: khepri_machine:metrics(),
      NewState :: khepri_machine_v0:state().
%% @doc Sets the metrics in the given state.
%%
%% @private

set_metrics(#khepri_machine{} = State, Metrics) ->
    State#khepri_machine{metrics = Metrics}.


-spec state_to_list(State) -> Fields when
      State :: khepri_machine_v0:state(),
      Fields :: [any()].
%% @doc Returns the fields of the state as a list.
%%
%% @private

state_to_list(#khepri_machine{} = State) ->
    tuple_to_list(State).

-spec assert_equal(State1, State2) -> ok when
      State1 :: khepri_machine:state(),
      State2 :: khepri_machine:state().

assert_equal(#khepri_machine{} = State1, #khepri_machine{} = State2) ->
    ?assertEqual(State1, State2),
    ok.
