%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc
%% Khepri machine aux state V0 handling functions.
%%
%% @hidden

-module(khepri_machine_aux_v0).

-include("include/khepri.hrl").

-export([init/1,
         is_state/1,
         get_store_id/1,
         get_delayed_aux_queries/1,
         set_delayed_aux_queries/2,
         get_maybe_down_procs/1,
         set_maybe_down_procs/2,
         state_to_list/1]).

-record(khepri_machine_aux,
        {store_id :: khepri:store_id(),
         delayed_aux_queries = [] :: [khepri_machine:delayed_aux_query()],
         maybe_down_procs = #{} :: #{pid() => any()}}).

-opaque state() :: #khepri_machine_aux{}.
%% Auxiliary state of the Khepri Ra state machine.

-export_type([state/0]).

-spec init(StoreId) -> AuxState when
      StoreId :: khepri:store_id(),
      AuxState :: khepri_machine_aux_v0:state().

init(StoreId) when ?IS_KHEPRI_STORE_ID(StoreId) ->
    AuxState = #khepri_machine_aux{store_id = StoreId},
    AuxState.

%% -------------------------------------------------------------------
%% State record management functions.
%% -------------------------------------------------------------------

-spec is_state(AuxState) -> IsState when
      AuxState :: khepri_machine_aux_v0:state(),
      IsState :: boolean().
%% @doc Tells if the given argument is a valid aux state.
%%
%% @private

is_state(AuxState) ->
    is_record(AuxState, khepri_machine_aux).

-spec get_store_id(AuxState) -> StoreId when
      AuxState :: khepri_machine_aux_v0:state(),
      StoreId :: khepri:store_id().

get_store_id(#khepri_machine_aux{store_id = StoreId}) ->
    StoreId.

-spec get_delayed_aux_queries(AuxState) -> DelayedAuxQueries when
      AuxState :: khepri_machine_aux_v0:state(),
      DelayedAuxQueries :: [khepri_machine:delayed_aux_query()].

get_delayed_aux_queries(
  #khepri_machine_aux{delayed_aux_queries = DelayedAuxQueries}) ->
    DelayedAuxQueries.

-spec set_delayed_aux_queries(AuxState, DelayedAuxQueries) -> NewAuxState when
      AuxState :: khepri_machine_aux_v0:state(),
      DelayedAuxQueries :: [khepri_machine:delayed_aux_query()],
      NewAuxState :: khepri_machine_aux_v0:state().

set_delayed_aux_queries(#khepri_machine_aux{} = AuxState, DelayedAuxQueries) ->
    AuxState1 = AuxState#khepri_machine_aux{
                  delayed_aux_queries = DelayedAuxQueries},
    AuxState1.

-spec get_maybe_down_procs(AuxState) -> MaybeDownProcs when
      AuxState :: khepri_machine_aux_v0:state(),
      MaybeDownProcs :: #{pid() => any()}.

get_maybe_down_procs(
  #khepri_machine_aux{maybe_down_procs = MaybeDownProcs}) ->
    MaybeDownProcs.

-spec set_maybe_down_procs(AuxState, MaybeDownProcs) -> NewAuxState when
      AuxState :: khepri_machine_aux_v0:state(),
      MaybeDownProcs :: #{pid() => any()},
      NewAuxState :: khepri_machine_aux_v0:state().

set_maybe_down_procs(#khepri_machine_aux{} = AuxState, MaybeDownProcs) ->
    AuxState1 = AuxState#khepri_machine_aux{maybe_down_procs = MaybeDownProcs},
    AuxState1.

%% -------------------------------------------------------------------
%% State record upgrade.
%% -------------------------------------------------------------------

-spec state_to_list(AuxState) -> Fields when
      AuxState :: khepri_machine_aux_v0:state(),
      Fields :: [any()].
%% @doc Returns the fields of the aux state as a list.
%%
%% @private

state_to_list(#khepri_machine_aux{} = AuxState) ->
    tuple_to_list(AuxState).
