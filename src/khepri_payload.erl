%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri payloads.
%%
%% Payloads are the structure used to attach something to a tree node in the
%% store. Khepri supports the following payloads:
%% <ul>
%% <li>No payload at all ({@link no_payload()}</li>
%% <li>Data payload used to store any Erlang term ({@link data()})</li>
%% <li>Stored procedure payload used to store functions ({@link sproc()})</li>
%% </ul>
%%
%% Usually, there is no need to explicitly use this module as the type of
%% payload will be autodetected, thanks to the {@link wrap/1} function already
%% called internally.

-module(khepri_payload).

-include_lib("horus/include/horus.hrl").

-include("src/khepri_error.hrl").
-include("src/khepri_payload.hrl").

-export([none/0,
         data/1,
         sproc/1,
         wrap/1,

         prepare/1]).

-type no_payload() :: ?NO_PAYLOAD.
%% Internal value used to mark that a tree node has no payload attached.

-type data() :: #p_data{}.
%% Internal structure to wrap any Erlang term before it can be stored in a
%% tree node.
%%
%% The only constraint is the conversion to an Erlang binary must be supported
%% by this term.

-type sproc() :: #p_sproc{}.
%% Internal structure to wrap an anonymous function before it can be stored in
%% a tree node and later executed.

-type payload() :: no_payload() | data() | sproc().
%% All types of payload stored in the nodes of the tree structure.
%%
%% Beside the absence of payload, the only type of payload supported is data.

-export_type([no_payload/0,
              data/0,
              sproc/0,
              payload/0]).

-spec none() -> no_payload().
%% @doc Returns the internal value used to mark that a tree node has no
%% payload attached.
%%
%% @see no_payload().
none() ->
    ?NO_PAYLOAD.

-spec data(Term) -> Payload when
      Term :: khepri:data(),
      Payload :: data().
%% @doc Returns the same term wrapped into an internal structure ready to be
%% stored in the tree.
%%
%% @see data().

data(Term) ->
    #p_data{data = Term}.

-spec sproc(Fun) -> Payload when
      Fun :: horus:horus_fun() | fun(),
      Payload :: sproc().
%% @doc Returns the same function wrapped into an internal structure ready to
%% be stored in the tree.
%%
%% @see sproc().

sproc(Fun) when is_function(Fun) ->
    #p_sproc{sproc = Fun,
             %% This will be overridden with the correct value when the
             %% function will be extracted.
             is_valid_as_tx_fun = false};
sproc(Fun) when ?IS_HORUS_STANDALONE_FUN(Fun) ->
    #p_sproc{sproc = Fun,
             is_valid_as_tx_fun = false}.

-spec wrap(Payload) -> WrappedPayload when
      Payload :: payload() | khepri:data() | fun(),
      WrappedPayload :: payload().
%% @doc Automatically detects the payload type and ensures it is wrapped in
%% one of the internal types.
%%
%% The internal types make sure we avoid any collision between any
%% user-provided terms and internal structures.
%%
%% @param Payload an already wrapped payload, or any term which needs to be
%%        wrapped.
%%
%% @returns the wrapped payload.

wrap(Payload) when ?IS_KHEPRI_PAYLOAD(Payload) -> Payload;
wrap(Fun) when is_function(Fun)                -> sproc(Fun);
wrap(Data)                                     -> data(Data).

-spec prepare(Payload) -> Payload when
      Payload :: payload().
%% @doc Finishes any needed changes to the payload before it is ready to be
%% stored.
%%
%% This currently only includes the conversion of anonymous functions to
%% standalone functions for stored procedures' payload records.
%%
%% @private

prepare(?NO_PAYLOAD = Payload) ->
    Payload;
prepare(#p_data{} = Payload) ->
    Payload;
prepare(#p_sproc{sproc = Fun} = Payload)
  when is_function(Fun) ->
    try
        case khepri_tx_adv:to_standalone_fun(Fun, auto) of
            StandaloneFun1 when ?IS_HORUS_STANDALONE_FUN(StandaloneFun1) ->
                Payload#p_sproc{sproc = StandaloneFun1,
                                is_valid_as_tx_fun = rw};
            _ ->
                %% TODO: Improve Horus API to avoid a second extraction.
                StandaloneFun1 = khepri_tx_adv:to_standalone_fun(Fun, rw),
                Payload#p_sproc{sproc = StandaloneFun1,
                                is_valid_as_tx_fun = ro}
        end
    catch
        error:?khepri_exception(failed_to_prepare_tx_fun, _) ->
            %% TODO: Prevent the use of `khepri_tx' and `khepri_tx_adv' in
            %% regular stored procedures.
            StandaloneFun2 = khepri_sproc:to_standalone_fun(Fun),
            Payload#p_sproc{sproc = StandaloneFun2,
                            is_valid_as_tx_fun = false}
    end.
