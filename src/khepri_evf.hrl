%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-record(evf_tree, {path :: khepri_path:native_pattern(),
                   props = #{} :: khepri_evf:tree_event_filter_props()}).

-record(evf_process, {pid :: pid(),
                      props = #{} :: khepri_evf:process_event_filter_props()}).

-define(IS_KHEPRI_EVENT_FILTER(EventFilter),
        (is_record(EventFilter, evf_tree) orelse
         is_record(EventFilter, evf_process))).

-record(ev_tree, {path :: khepri_path:native_path(),
                  change :: create | update | delete}).

-record(ev_process, {pid :: pid(),
                     change :: {'DOWN', any()}}).
