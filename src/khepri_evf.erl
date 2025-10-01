%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri event filters.
%%
%% Triggers allow a user to associate an event, described by an event filter,
%% to an action. When an event matches the registered event filter, the
%% associated action is evaluated.
%%
%% == Events and event filters ==
%%
%% === Changes to the tree ===
%%
%% When a tree node is created, modified or deleted, a <em>tree change</em> is
%% emitted.
%%
%% To register a trigger that targets tree changes, you can create a
%% tree-change event filter explicitly:
%%
%% ```
%% EventFilter = khepri_evf:tree([stock, wood, <<"oak">>], %% Required
%%                               #{on_actions => [delete], %% Optional
%%                                 priority => 10}),       %% Optional
%%
%% ok = khepri:register_trigger(
%%        StoreId,
%%        TriggerId,
%%        EventFilter,
%%        Action).
%% '''
%%
%% It's possible to pass a path pattern directly as an event filter: this is
%% the same as creating an explicit tree-change event filter with default
%% options.
%%
%% ```
%% EventFilter = "/:stock/:wood/oak",
%%
%% ok = khepri:register_trigger(
%%        StoreId,
%%        TriggerId,
%%        EventFilter,
%%        Action).
%% '''
%%
%% The path pattern can match many paths. The monitored paths don't neew to
%% exist when the trigger is registered.
%%
%% === Termination of a process ===
%%
%% Khepri can monitor a process. When this process exits, a <em>process</em>
%% event is emitted.
%%
%% To register a trigger that targets process termination, you can create a
%% process event filter explicitly:
%%
%% ```
%% EventFilter = khepri_evf:process(self(),             %% Required
%%                                  #{on_reason => '_', %% Optional
%%                                    priority => 10}), %% Optional
%%
%% ok = khepri:register_trigger(
%%        StoreId,
%%        TriggerId,
%%        EventFilter,
%%        Action).
%% '''
%%
%% It's possible to pass a PID directly as an event filter: this is the same
%% as creating an explicit process event filter with default options.
%%
%% ```
%% EventFilter = self(),
%%
%% ok = khepri:register_trigger(
%%        StoreId,
%%        TriggerId,
%%        EventFilter,
%%        Action).
%% '''
%%
%% The Khepri leader monitors the given process. When the process exits or if
%% it's not running in the first place, it will trigger the execution of the
%% trigger action.
%%
%% If the monitored process is on a different Erlang node than the Ra leader,
%% the loss of connection between the two nodes (i.e. the `noconnection' exit
%% reason) is handled in a specific way:
%% <ul>
%% <li>If the remote node is a member of the Khepri cluster, Khepri will start
%% to monitor that node after receiving the `noconnection' exit reaison. Then
%% two things can happen:
%% <ul>
%% <li>The node comes back online. In this case, processes that are supposed
%% to run on it are monitored again. If the node was restarted and the
%% processes were killed, this new monitoring will detect this situation.</li>
%% <li>The node is removed from the cluster. In this case, it is unlikely to
%% come back. Thus, the processes that were running on it are considered as
%% dead.</li>
%% </ul></li>
%% <li>If the remote node is not a member of the Khepri cluster, Khepri will
%% consider that the processes supposed to run on this node are dead, even if
%% this is network partition and the node and its processes could be reachable
%% again in the future.</li>
%% </ul>
%%
%% == Actions ==
%%
%% === Trigger descriptor ===
%%
%% The trigger descriptor ({@link khepri_event_handler:trigger_descriptor()})
%% is a record used to encapsulate the properties of a specific triggered
%% trigger: it contains details about:
%% <ul>
%% <li>the originating store and trigger IDs</li>
%% <li>the originating event</li>
%% <li>the action properties</li>
%% </ul>
%%
%% This trigger descriptor is passed to the action and allows the action to
%% distinguish each instance of a trigger.
%%
%% === Execution of a stored procedure
%%
%% The action takes be a path to a stored procedure.
%% The action takes the form of:
%% <ul>
%% <li>a path to a stored procedure, or</li>
%% <li>a `{sproc, StoredProcPath}' tuple</li>
%% </ul>
%%
%% The stored procedure must take a single argument, the trigger descriptor.
%%
%% The stored procedure does not have to exist when the trigger is registered.
%% If the trigger is triggered while the stored procedure is missing, the
%% event will be ignored.
%%
%% The return value of the function is ignored.
%%
%% Here is an example of a function that can be used as a stored procedure:
%%
%% ```
%% my_stored_procedure(#khepri_trigger{...}) ->
%%     do_something_with_event().
%% '''
%%
%% === Execution of an arbitrary MFA ===
%%
%% The action takes the form of:
%% <ul>
%% <li>a `{Module, Function, ArgsList}' tuple, or</li>
%% <li>a `{apply, {Module, Function, ArgsList}}' tuple</li>
%% </ul>
%%
%% When the action is triggered, the MFA is executed with the given arguments
%% list with the trigger descriptor appended to it.
%%
%% The return value of the function is ignored.
%%
%% Here is an example of a function that can be used with `{my_mod, my_func,
%% [ExtraArg]}':
%%
%% ```
%% -module(my_mod).
%% -export([my_func/2]).
%%
%% my_func(ExtraArg, #khepri_trigger{...}) ->
%%     do_something_with_event(ExtraArg).
%% '''
%%
%% === Send of a message ===
%%
%% The action takes the form of:
%% <ul>
%% <li>a PID, or</li>
%% <li>a `{send, Pid, Priv}' tuple</li>
%% </ul>
%%
%% When the action is triggered, the trigger descriptor is sent as a message
%% to the given PID. The `Priv' term is added to the action properties. If the
%% action is the PID alone, `Priv' defaults to `undefined' and nothing is
%% added to the action properties.
%%
%% The target process is not monitored. If it's not running anymore or it is
%% on a node that is unreachable at the time of the event, the event is
%% triggered.
%%
%% Here is an example of a `receive' block that expects a trigger descriptor:
%%
%% ```
%% receive
%%     #khepri_trigger{...} ->
%%         do_something_with_event()
%% end.
%% '''
%%
%% === Where is the action evaluated? ===
%%
%% By default, the action is evaluated by the Khepri leader.
%%
%% It's possible to indicate where to evaluate the trigger when the trigger is
%% registered, using the `where' option (see {@link
%% khepri:trigger_options()}).
%%
%% Here is an example of a trigger where the action will be evaluated on all
%% members of the Khepri cluster at the time of the event.
%%
%% ```
%% EventFilter = khepri_evf:tree([stock, wood, <<"oak">>], %% Required
%%                               #{on_actions => [delete], %% Optional
%%                                 priority => 10}),       %% Optional
%%
%% ok = khepri:register_trigger(
%%        StoreId,
%%        TriggerId,
%%        EventFilter,
%%        Action,
%%        #{where => all_members}).
%% '''

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
