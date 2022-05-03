%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(IS_NODE_ID(PathComponent),
        (is_atom(PathComponent) orelse is_binary(PathComponent))).

-define(IS_PATH_COMPONENT(PathComponent),
        (?IS_NODE_ID(PathComponent) orelse
         ?IS_SPECIAL_PATH_COMPONENT(PathComponent))).

-define(ROOT_NODE, $/).
-define(THIS_NODE, $.).
-define(PARENT_NODE, $^).

-define(IS_SPECIAL_PATH_COMPONENT(PathComponent),
        (PathComponent =:= ?ROOT_NODE orelse
         PathComponent =:= ?THIS_NODE orelse
         PathComponent =:= ?PARENT_NODE)).

-define(IS_PATH(Path),
        (Path =:= [] orelse ?IS_PATH_COMPONENT(hd(Path)))).

-define(IS_CONDITION(Condition), is_tuple(Condition)).

-define(IS_PATH_CONDITION(PathCondition),
        (?IS_PATH_COMPONENT(PathCondition) orelse
         ?IS_CONDITION(PathCondition))).

-define(IS_PATH_PATTERN(Path),
        (Path =:= [] orelse ?IS_PATH_CONDITION(hd(Path)))).

%% -------------------------------------------------------------------
%% Path conditions.
%% -------------------------------------------------------------------

-type re_mp() :: tuple().

-record(if_name_matches,
        {regex = any :: any | iodata() | unicode:charlist(),
         compiled = undefined :: re_mp() | undefined}).

-define(STAR, #if_name_matches{regex = any}).

-record(if_path_matches,
        {regex = any :: any | iodata() | unicode:charlist(),
         compiled = undefined :: re_mp() | undefined}).

-define(STAR_STAR, #if_path_matches{regex = any}).

-record(if_has_data,
        {has_data = true :: boolean()}).

-record(if_data_matches,
        {pattern = '_' :: ets:match_pattern(),
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
