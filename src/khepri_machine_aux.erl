%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri machine aux state V1 handling functions.
%%
%% @hidden

-module(khepri_machine_aux).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").

-export([init/1,
         is_state/1,
         get_store_id/1,
         get_delayed_aux_queries/1,
         set_delayed_aux_queries/2,
         get_maybe_down_procs/1,
         set_maybe_down_procs/2,
         get_unreleased_command_footprint/1,
         bump_unreleased_command_footprint/2,
         reset_unreleased_command_footprint/1,
         get_last_snapshot_request_timestamp/1,
         set_last_snapshot_request_timestamp/2,
         convert/2]).

-record(khepri_machine_aux,
        {store_id :: khepri:store_id(),
         delayed_aux_queries = [] :: [khepri_machine:delayed_aux_query()],
         maybe_down_procs = #{} :: #{pid() => any()},

         %% Added in aux state v1.
         unreleased_command_footprint = 0 :: non_neg_integer(),
         last_snapshot_request_timestamp = undefined :: integer() | undefined}
       ).

-opaque state_v1() :: #khepri_machine_aux{}.

-type state() :: state_v1() |
                 khepri_machine_aux_v0:state().
%% Auxiliary state of the Khepri Ra state machine.

-export_type([state/0,
              state_v1/0]).

init(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    khepri_machine_aux_v0:init(StoreId).

%% -------------------------------------------------------------------
%% State record management functions.
%% -------------------------------------------------------------------

-spec is_state(AuxState) -> IsState when
      AuxState :: khepri_machine_aux:state(),
      IsState :: boolean().
%% @doc Tells if the given argument is a valid aux state.
%%
%% @private

is_state(AuxState) ->
    is_record(AuxState, khepri_machine_aux) orelse
    khepri_machine_aux_v0:is_state(AuxState).

-spec get_store_id(AuxState) -> StoreId when
      AuxState :: khepri_machine_aux:state(),
      StoreId :: khepri:store_id().

get_store_id(#khepri_machine_aux{store_id = StoreId}) ->
    StoreId;
get_store_id(AuxState) ->
    khepri_machine_aux_v0:get_store_id(AuxState).

-spec get_delayed_aux_queries(AuxState) -> DelayedAuxQueries when
      AuxState :: khepri_machine_aux:state(),
      DelayedAuxQueries :: [khepri_machine:delayed_aux_query()].

get_delayed_aux_queries(
  #khepri_machine_aux{delayed_aux_queries = DelayedAuxQueries}) ->
    DelayedAuxQueries;
get_delayed_aux_queries(AuxState) ->
    khepri_machine_aux_v0:get_delayed_aux_queries(AuxState).

-spec set_delayed_aux_queries(AuxState, DelayedAuxQueries) -> NewAuxState when
      AuxState :: khepri_machine_aux:state(),
      DelayedAuxQueries :: [khepri_machine:delayed_aux_query()],
      NewAuxState :: khepri_machine_aux:state().

set_delayed_aux_queries(#khepri_machine_aux{} = AuxState, DelayedAuxQueries) ->
    AuxState1 = AuxState#khepri_machine_aux{
                  delayed_aux_queries = DelayedAuxQueries},
    AuxState1;
set_delayed_aux_queries(AuxState, DelayedAuxQueries) ->
    khepri_machine_aux_v0:set_delayed_aux_queries(AuxState, DelayedAuxQueries).

-spec get_maybe_down_procs(AuxState) -> MaybeDownProcs when
      AuxState :: khepri_machine_aux:state(),
      MaybeDownProcs :: #{pid() => any()}.

get_maybe_down_procs(
  #khepri_machine_aux{maybe_down_procs = MaybeDownProcs}) ->
    MaybeDownProcs;
get_maybe_down_procs(AuxState) ->
    khepri_machine_aux_v0:get_maybe_down_procs(AuxState).

-spec set_maybe_down_procs(AuxState, MaybeDownProcs) -> NewAuxState when
      AuxState :: khepri_machine_aux:state(),
      MaybeDownProcs :: #{pid() => any()},
      NewAuxState :: khepri_machine_aux:state().

set_maybe_down_procs(#khepri_machine_aux{} = AuxState, MaybeDownProcs) ->
    AuxState1 = AuxState#khepri_machine_aux{maybe_down_procs = MaybeDownProcs},
    AuxState1;
set_maybe_down_procs(AuxState, MaybeDownProcs) ->
    khepri_machine_aux_v0:set_maybe_down_procs(AuxState, MaybeDownProcs).

-spec get_unreleased_command_footprint(AuxState) -> Footprint when
      AuxState :: khepri_machine_aux:state_v1(),
      Footprint :: non_neg_integer().

get_unreleased_command_footprint(
  #khepri_machine_aux{unreleased_command_footprint = Footprint}) ->
    Footprint.

-spec bump_unreleased_command_footprint(AuxState, CommandSize) ->
    NewAuxState when
      AuxState :: khepri_machine_aux:state_v1(),
      CommandSize :: non_neg_integer(),
      NewAuxState :: khepri_machine_aux:state_v1().

bump_unreleased_command_footprint(
  #khepri_machine_aux{unreleased_command_footprint = Footprint} = AuxState,
  CommandSize) when is_integer(CommandSize) andalso CommandSize >= 0 ->
    Footprint1 = Footprint + CommandSize,
    AuxState1 = AuxState#khepri_machine_aux{
                  unreleased_command_footprint = Footprint1},
    AuxState1.

-spec reset_unreleased_command_footprint(AuxState) ->
    NewAuxState when
      AuxState :: khepri_machine_aux:state_v1(),
      NewAuxState :: khepri_machine_aux:state_v1().

reset_unreleased_command_footprint(#khepri_machine_aux{} = AuxState) ->
    AuxState1 = AuxState#khepri_machine_aux{unreleased_command_footprint = 0},
    AuxState1.

-spec get_last_snapshot_request_timestamp(AuxState) -> Timestamp when
      AuxState :: khepri_machine_aux:state_v1(),
      Timestamp :: non_neg_integer() | undefined.

get_last_snapshot_request_timestamp(
  #khepri_machine_aux{last_snapshot_request_timestamp = Timestamp}) ->
    Timestamp.

-spec set_last_snapshot_request_timestamp(AuxState, Timestamp) ->
    NewAuxState when
      AuxState :: khepri_machine_aux:state_v1(),
      Timestamp :: non_neg_integer(),
      NewAuxState :: khepri_machine_aux:state_v1().

set_last_snapshot_request_timestamp(
  #khepri_machine_aux{} = AuxState, Timestamp) ->
    AuxState1 = AuxState#khepri_machine_aux{
                  last_snapshot_request_timestamp = Timestamp},
    AuxState1.

%% -------------------------------------------------------------------
%% State record upgrade.
%% -------------------------------------------------------------------

-spec convert(OldAuxState, EffectiveMacVer) -> NewAuxState when
      OldAuxState :: khepri_machine_aux:state(),
      EffectiveMacVer :: ra_machine:version(),
      NewAuxState :: khepri_machine_aux:state().

convert(#khepri_machine_aux{} = AuxState, _EffectiveMacVer) ->
    AuxState;
convert(OldAuxState, EffectiveMacVer) ->
    case khepri_machine:does_api_comply_with(aux_state_v1, EffectiveMacVer) of
        true ->
            ?assert(khepri_machine_aux_v0:is_state(OldAuxState)),
            Fields0 = khepri_machine_aux_v0:state_to_list(OldAuxState),
            %% XXX Take footprint from machine state.
            %% XXX Convert monotonic timestamp to system time?
            Fields1 = Fields0 ++ [0, undefined],
            NewAuxState = list_to_tuple(Fields1),
            ?assert(is_state(NewAuxState)),
            NewAuxState;
        false ->
            OldAuxState
    end.
