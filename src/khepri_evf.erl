%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri event filters.

-module(khepri_evf).

-include("include/khepri.hrl").
-include("src/khepri_evf.hrl").

-export([tree/1, tree/2,
         process/1, process/2,
         wrap/1,
         get_priority/1,
         set_priority/2]).

-type tree_event_filter() :: #evf_tree{}.
%% A tree event filter.
%%
%% It takes a path pattern to monitor and optionally properties.

-type tree_event_filter_props() :: #{on_actions => [create | update | delete],
                                     priority => khepri_evf:priority()}.
%% Tree event filter properties.
%%
%% The properties are:
%% <ul>
%% <li>`on_actions': a list of actions to filter among `create', `update' and
%% `delete'; the default is to react to all of them.</li>
%% <li>`priority': a {@link priority()}</li>
%% </ul>
%%
%% A Khepri path, whether it is a native path or a Unix-like path, can be used
%% as a tree event filter. It will be automatically converted to a tree event
%% filter with default properties.

-type process_event_filter() :: #evf_process{}.
%% A process event filter.
%%
%% It takes a PID to monitor and optionally properties.

-type process_event_filter_props() :: #{on_reason => ets:match_pattern(),
                                        priority => khepri_evf:priority()}.
%% Tree event filter properties.
%%
%% The properties are:
%% <ul>
%% <li>`on_actions': a list of actions to filter among `create', `update' and
%% `delete'; the default is to react to all of them.</li>
%% <li>`priority': a {@link priority()}</li>
%% </ul>
%%
%% A Khepri path, whether it is a native path or a Unix-like path, can be used
%% as a tree event filter. It will be automatically converted to a tree event
%% filter with default properties.

-type event_filter() :: tree_event_filter() | process_event_filter().
%% An event filter.
%%
%% The following event filters are supported:
%% <ul>
%% <li>Tree event filter ({@link tree_event_filter()}</li>
%% <li>Process event filter ({@link process_event_filter()}</li>
%% </ul>
%%
%% An event filter can be explicitly constructed using the functions provided
%% in this module. However, some common types will be automatically detected
%% and converted to an event filter with default properties. See each event
%% filter type for more details.

-type event_filter_or_compat() :: khepri_evf:event_filter() |
                                  khepri_path:pattern() |
                                  pid().
%% An event filter, or any type that can be automatically converted to an
%% event filter.
%%
%% For instance, a Khepri path or a string can be converted to a {@link
%% tree_event_filter()}.
%%
%% @see wrap/1.

-type priority() :: integer().
%% An event filter priority.
%%
%% This is an integer to prioritize event filters: the greater the priority,
%% the more it is prioritized. Negative integers are allowed.
%%
%% The default priority is 0.

-type tree_event() :: #ev_tree{}.
%% An event record representing a change tree change.
%%
%% The event has the following fields:
%% <ul>
%% <li>`path': the path of the affected tree node</li>
%% <li>`change': the nature of the change (`create', `update' or
%% `delete').</li>
%% </ul>

-type process_event() :: #ev_process{}.
%% An event record representing a monitored process that exited.
%%
%% The event has the following fields:
%% <ul>
%% <li>`pid': the PID of the terminated process</li>
%% <li>`reason': the exit reason</li>
%% </ul>

-type event() :: tree_event() | process_event().
%% An record representing an event.

-export_type([event_filter/0,
              event_filter_or_compat/0,
              tree_event_filter/0,
              tree_event_filter_props/0,
              process_event_filter/0,
              process_event_filter_props/0,
              priority/0,
              tree_event/0,
              process_event/0,
              event/0]).

-spec tree(PathPattern) -> EventFilter when
      PathPattern :: khepri_path:pattern(),
      EventFilter :: khepri_evf:tree_event_filter().
%% @doc Constructs a tree event filter.
%%
%% @see tree/2.

tree(PathPattern) ->
    tree(PathPattern, #{}).

-spec tree(PathPattern, Props) -> EventFilter when
      PathPattern :: khepri_path:pattern(),
      Props :: khepri_evf:tree_event_filter_props(),
      EventFilter :: khepri_evf:tree_event_filter().
%% @doc Constructs a tree event filter.
%%
%% @see tree_event_filter().

tree(PathPattern, Props) when ?IS_KHEPRI_PATH_PATTERN_OR_COMPAT(PathPattern) ->
    PathPattern1 = khepri_path:from_string(PathPattern),
    #evf_tree{path = PathPattern1,
              props = Props}.

-spec process(Pid) -> EventFilter when
      Pid :: pid(),
      EventFilter :: khepri_evf:process_event_filter().
%% @doc Constructs a process event filter.
%%
%% @see process/2.

process(Pid) ->
    process(Pid, #{}).

-spec process(Pid, Props) -> EventFilter when
      Pid :: pid(),
      Props :: khepri_evf:process_event_filter_props(),
      EventFilter :: khepri_evf:process_event_filter().
%% @doc Constructs a process event filter.
%%
%% @see process_event_filter().

process(Pid, Props) ->
    #evf_process{pid = Pid,
                 props = Props}.

-spec wrap(Input) -> EventFilter when
      Input :: khepri_evf:event_filter_or_compat(),
      EventFilter :: khepri_evf:event_filter().
%% @doc Automatically detects the event filter type and ensures it is wrapped
%% in one of the internal types.
%%
%% @param Input an already created event filter, or any term which can be
%%        automatically converted to an event filter.
%%
%% @returns the created event filter.

wrap(EventFilter) when ?IS_KHEPRI_EVENT_FILTER(EventFilter) ->
    EventFilter;
wrap(PathPattern) when ?IS_KHEPRI_PATH_PATTERN_OR_COMPAT(PathPattern) ->
    tree(PathPattern);
wrap(Pid) when is_pid(Pid) ->
    process(Pid).

-spec get_priority(EventFilter) -> Priority when
      EventFilter :: khepri_evf:event_filter(),
      Priority :: khepri_evf:priority().
%% @doc Returns the priority of the event filter.
%%
%% @param EventFilter the event filter to update.
%%
%% @returns the priority.

get_priority(#evf_tree{props = Props}) ->
    get_priority1(Props);
get_priority(#evf_process{props = Props}) ->
    get_priority1(Props).

get_priority1(#{priority := Priority}) -> Priority;
get_priority1(_)                       -> 0.

-spec set_priority(EventFilter, Priority) -> EventFilter when
      EventFilter :: khepri_evf:event_filter(),
      Priority :: khepri_evf:priority().
%% @doc Sets the priority of the event filter.
%%
%% @param EventFilter the event filter to update.
%% @param Priority the new priority.
%%
%% @returns the updated event filter.

set_priority(#evf_tree{props = Props} = EventFilter, Priority) ->
    Props1 = set_priority1(Props, Priority),
    EventFilter#evf_tree{props = Props1};
set_priority(#evf_process{props = Props} = EventFilter, Priority) ->
    Props1 = set_priority1(Props, Priority),
    EventFilter#evf_process{props = Props1}.

set_priority1(Props, Priority) when is_integer(Priority) ->
    Props#{priority => Priority}.
