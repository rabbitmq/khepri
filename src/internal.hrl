%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(DEFAULT_RA_SYSTEM_NAME, khepri).
-define(DEFAULT_STORE_ID, khepri).
-define(IS_STORE_ID(StoreId), is_atom(StoreId)).

-define(INIT_DATA_VERSION, 1).
-define(INIT_CHILD_LIST_VERSION, 1).
-define(INIT_NODE_STAT, #{payload_version => ?INIT_DATA_VERSION,
                          child_list_version => ?INIT_CHILD_LIST_VERSION}).

-define(TX_STATE_KEY, khepri_tx_machine_state).
-define(TX_PROPS, khepri_tx_properties).

-define(NO_PAYLOAD, '$__NO_PAYLOAD__').
-record(p_data, {data :: khepri:data()}).
-record(p_sproc, {sproc :: khepri_fun:standalone_fun()}).

-define(IS_KHEPRI_PAYLOAD(Payload), (Payload =:= ?NO_PAYLOAD orelse
                                     is_record(Payload, p_data) orelse
                                     is_record(Payload, p_sproc))).

-define(IS_TIMEOUT(Timeout), (Timeout =:= infinity orelse
                              (is_integer(Timeout) andalso Timeout >= 0))).
-define(HAS_TIME_LEFT(Timeout), (Timeout =:= infinity orelse Timeout > 0)).

%% timer:sleep/1 time used as a retry interval when the local Ra server is
%% unaware of a leader exit.
-define(NOPROC_RETRY_INTERVAL, 200).

-record(evf_tree, {path :: khepri_path:native_pattern(),
                   props = #{} :: khepri_evf:tree_event_filter_props()}).
%-record(evf_process, {pid :: pid(),
%                      props = #{} :: #{on_reason => ets:match_pattern(),
%                                       priority => integer()}}).

-define(IS_KHEPRI_EVENT_FILTER(EventFilter),
        (is_record(EventFilter, evf_tree))).

%% Structure representing each node in the tree, including the root node.
%% TODO: Rename stat to something more correct?
-record(node, {stat = ?INIT_NODE_STAT :: khepri_machine:stat(),
               payload = ?NO_PAYLOAD :: khepri_payload:payload(),
               child_nodes = #{} :: #{khepri_path:component() := #node{}}}).

%% State machine commands.

-record(put, {path :: khepri_path:native_pattern(),
              payload = ?NO_PAYLOAD :: khepri_payload:payload(),
              extra = #{} :: #{keep_while =>
                               khepri_condition:native_keep_while()},
              options = #{} :: khepri:tree_options()}).

-record(delete, {path :: khepri_path:native_pattern(),
                 options  = #{} :: khepri:tree_options()}).

-record(tx, {'fun' :: khepri_fun:standalone_fun()}).

-record(register_trigger, {id :: khepri:trigger_id(),
                           event_filter :: khepri_evf:event_filter(),
                           sproc :: khepri_path:native_path()}).

-record(ack_triggered, {triggered :: [khepri_machine:triggered()]}).

-record(triggered, {id :: khepri:trigger_id(),
                    %% TODO: Do we need a ref to distinguish multiple
                    %% instances of the same trigger?
                    event_filter :: khepri_evf:event_filter(),
                    sproc :: khepri_fun:standalone_fun(),
                    props = #{} :: map()}).

-define(common_ret_to_single_result_ret(__Ret),
        case (__Ret) of
            {ok, __NodePropsMap} ->
                [__NodeProps] = maps:values(__NodePropsMap),
                {ok, __NodeProps};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(single_result_ret_to_payload_ret(__Ret),
        case (__Ret) of
            {ok, #{data := __Data}} ->
                {ok, __Data};
            {ok, #{sproc := __StandaloneFun}} ->
                {ok, __StandaloneFun};
            {ok, _} ->
                {ok, undefined};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(many_results_ret_to_payloads_ret(__Ret, __Default),
        case (__Ret) of
            {ok, __NodePropsMap} ->
                __PayloadsMap = maps:map(
                                  fun
                                      (_, #{data := __Data}) ->
                                          __Data;
                                      (_, #{sproc := __StandaloneFun}) ->
                                          __StandaloneFun;
                                      (_, _) ->
                                          (__Default)
                                  end, __NodePropsMap),
                {ok, __PayloadsMap};
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            __Error ->
                __Error
        end).

-define(result_ret_to_minimal_ret(__Ret),
        case (__Ret) of
            {ok, _} -> ok;
            __Other -> __Other
        end).

-define(raise_exception_if_any(__Ret),
        case (__Ret) of
            {error, ?khepri_exception(_, _) = __Exception} ->
                ?khepri_misuse(__Exception);
            _ ->
                __Ret
        end).
