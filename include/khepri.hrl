%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-ifndef(KHEPRI_HRL).
-define(KHEPRI_HRL, true).

-define(IS_KHEPRI_STORE_ID(StoreId), is_atom(StoreId)).

-define(IS_KHEPRI_NODE_ID(PathComponent),
        (is_atom(PathComponent) orelse is_binary(PathComponent))).

-define(IS_KHEPRI_PATH_COMPONENT(PathComponent),
        (?IS_KHEPRI_NODE_ID(PathComponent) orelse
         ?IS_SPECIAL_KHEPRI_PATH_COMPONENT(PathComponent))).

-define(KHEPRI_ROOT_NODE, $/).
-define(THIS_KHEPRI_NODE, $.).
-define(PARENT_KHEPRI_NODE, $^).

-define(IS_SPECIAL_KHEPRI_PATH_COMPONENT(PathComponent),
        (PathComponent =:= ?KHEPRI_ROOT_NODE orelse
         PathComponent =:= ?THIS_KHEPRI_NODE orelse
         PathComponent =:= ?PARENT_KHEPRI_NODE)).

-define(IS_KHEPRI_PATH(Path),
        (Path =:= [] orelse ?IS_KHEPRI_PATH_COMPONENT(hd(Path)))).

-define(IS_KHEPRI_CONDITION(Condition),
        (is_record(Condition, if_child_list_length) orelse
         is_record(Condition, if_child_list_version) orelse
         is_record(Condition, if_data_matches) orelse
         is_record(Condition, if_has_data) orelse
         is_record(Condition, if_has_payload) orelse
         is_record(Condition, if_has_sproc) orelse
         is_record(Condition, if_name_matches) orelse
         is_record(Condition, if_node_exists) orelse
         is_record(Condition, if_path_matches) orelse
         is_record(Condition, if_payload_version) orelse
         is_record(Condition, if_not) orelse
         is_record(Condition, if_all) orelse
         is_record(Condition, if_any))).

-define(IS_KHEPRI_PATH_CONDITION(PathCondition),
        (?IS_KHEPRI_PATH_COMPONENT(PathCondition) orelse
         ?IS_KHEPRI_CONDITION(PathCondition))).

-define(IS_KHEPRI_PATH_PATTERN(Path),
        (Path =:= [] orelse ?IS_KHEPRI_PATH_CONDITION(hd(Path)))).

%% -------------------------------------------------------------------
%% Path conditions.
%% -------------------------------------------------------------------

-record(if_name_matches,
        {regex = any :: any | iodata() | unicode:charlist(),
         compiled = undefined :: khepri_condition:re_compile_ret() |
                                 undefined}).

-define(KHEPRI_WILDCARD_STAR, #if_name_matches{regex = any}).

-record(if_path_matches,
        {regex = any :: any | iodata() | unicode:charlist(),
         compiled = undefined :: khepri_condition:re_compile_ret() |
                                 undefined}).

-define(KHEPRI_WILDCARD_STAR_STAR, #if_path_matches{regex = any}).

-record(if_has_payload,
        {has_payload = true :: boolean()}).

-record(if_has_data,
        {has_data = true :: boolean()}).

-record(if_has_sproc,
        {has_sproc = true :: boolean()}).

-record(if_data_matches,
        {pattern = '_' :: ets:match_pattern() | term(),
         conditions = [] :: [any()],
         compiled = undefined :: ets:comp_match_spec() | undefined}).

-record(if_node_exists,
        {exists = true :: boolean()}).

-record(if_payload_version,
        {version = 0 :: khepri:payload_version() |
                        khepri_condition:comparison_op(
                          khepri:payload_version())}).

-record(if_child_list_version,
        {version = 0 :: khepri:child_list_version() |
                        khepri_condition:comparison_op(
                          khepri:child_list_version())}).

-record(if_child_list_length,
        {count = 0 :: non_neg_integer() |
                      khepri_condition:comparison_op(non_neg_integer())}).

-record(if_not,
        {condition :: khepri_path:pattern_component()}).

-record(if_all,
        {conditions = [] :: [khepri_path:pattern_component()]}).

-record(if_any,
        {conditions = [] :: [khepri_path:pattern_component()]}).

%% -------------------------------------------------------------------
%% Error tuple format.
%% -------------------------------------------------------------------

-define(
   khepri_error(Name, Props),
   {khepri, Name, Props}).

-define(
   khepri_exception(Name, Props),
   {khepri_ex, Name, Props}).

-endif.
