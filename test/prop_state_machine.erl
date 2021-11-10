%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(prop_state_machine).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-include("include/khepri.hrl").

-dialyzer([{[no_opaque, no_return],
            [prop_commands_with_simple_paths_work_in_any_order/0]}]).

-export([prop_commands_with_simple_paths_work_in_any_order/0]).
-export([initial_state/0,
         command/1,
         precondition/2,
         next_state/3,
         postcondition/3]).
-export([sample/0]).

-record(state, {entries = #{},
                old_entries = #{}}).

-define(STORE_ID, ?MODULE).

sample() ->
    proper_gen:pick(commands(?MODULE)).

prop_commands_with_simple_paths_work_in_any_order() ->
    ?FORALL(Commands, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   Priv = test_ra_server_helpers:setup(?STORE_ID),
                   {History, State, Result} = run_commands(?MODULE, Commands),
                   _ = test_ra_server_helpers:cleanup(Priv),
                   ?WHENFAIL(
                      io:format(
                        "History: ~p~nState: ~p~nResult: ~p~n",
                        [History, State, Result]),
                      aggregate(command_names(Commands), Result =:= ok))
               end)).

initial_state() ->
    #state{}.

command(_State) ->
    elements([{call, khepri_machine, put, [?STORE_ID, path(), payload()]},
              {call, khepri_machine, get, [?STORE_ID, path()]},
              {call, khepri_machine, delete, [?STORE_ID, path()]}]).

precondition(_State, _Command) ->
    true.

next_state(
  #state{} = State,
  _Result,
  {call, khepri_machine, get, [_StoreId, _Path]}) ->
    State;
next_state(
  #state{entries = Entries} = State,
  _Result,
  {call, khepri_machine, put, [_StoreId, Path, Payload]}) ->
    Entries1 = add_entry(Entries, Path, Payload),
    State#state{entries = Entries1,
                old_entries = Entries};
next_state(
  #state{entries = Entries} = State,
  _Result,
  {call, khepri_machine, delete, [_StoreId, Path]}) ->
    Entries1 = delete_entry(Entries, Path),
    State#state{entries = Entries1,
                old_entries = Entries}.

postcondition(
  #state{entries = Entries},
  {call, khepri_machine, get, [_StoreId, Path]},
  Result) ->
    result_is_ok(Result, Entries, Path, {ok, #{}});
postcondition(
  #state{entries = Entries},
  {call, khepri_machine, put, [_StoreId, Path, _Payload]},
  Result) ->
    result_is_ok(Result, Entries, Path, {ok, #{Path => #{}}});
postcondition(
  #state{entries = Entries},
  {call, khepri_machine, delete, [_StoreId, Path]},
  Result) ->
    result_is_ok(Result, Entries, Path, {ok, #{}}).

add_entry(Entries, Path, Payload) ->
    {Entry1, New} = case Entries of
                       #{Path := #{payload_version := PV} = Entry} ->
                            Entry0 = case set_node_payload(Entry, Payload) of
                                         Entry -> Entry;
                                         E     -> E#{payload_version => PV + 1}
                                     end,
                            {Entry0, false};
                        _ ->
                            Entry0 = set_node_payload(
                                       #{payload_version => 1,
                                         child_list_version => 1,
                                         child_list_length => 0},
                                       Payload),
                            {Entry0, true}
                    end,
    Entry2 = case Payload of
                 ?DATA_PAYLOAD(Data) -> Entry1#{data => Data};
                 ?NO_PAYLOAD         -> maps:remove(data, Entry1)
             end,
    Entries1 = Entries#{Path => Entry2},
    add_entry1(Entries1, tl(lists:reverse(Path)), New).

set_node_payload(#{data := Data} = Entry, ?DATA_PAYLOAD(Data)) ->
    Entry;
set_node_payload(Entry, ?NO_PAYLOAD) when not is_map_key(data, Entry) ->
    Entry;
set_node_payload(Entry, ?DATA_PAYLOAD(Data)) ->
    Entry#{data => Data};
set_node_payload(Entry, ?NO_PAYLOAD) ->
    maps:remove(data, Entry).

add_entry1(Entries, ReversedPath, New) ->
    Path = lists:reverse(ReversedPath),
    {Entry1, New1} = case Entries of
                         #{Path := #{child_list_version := CLV,
                                     child_list_length := CLL} = Entry}
                           when New ->
                             {Entry#{child_list_version => CLV + 1,
                                     child_list_length => CLL + 1},
                              false};
                         #{Path := Entry} ->
                             {Entry,
                              false};
                         _ ->
                             {#{payload_version => 1,
                                child_list_version => 1,
                                child_list_length => 1},
                              true}
                     end,
    Entries1 = Entries#{Path => Entry1},
    case ReversedPath of
        [] -> Entries1;
        _  -> add_entry1(Entries1, tl(ReversedPath), New1)
    end.

delete_entry(Entries, Path) when is_map_key(Path, Entries) ->
    Entries1 = maps:fold(
                 fun(Key, Value, Es) ->
                         case lists:prefix(Path, Key) of
                             true  -> Es;
                             false -> Es#{Key => Value}
                         end
                 end, #{}, Entries),

    ParentPath = lists:reverse(tl(lists:reverse(Path))),
    #{ParentPath := #{child_list_version := CLV,
                      child_list_length := CLL} = Entry} = Entries1,
    Entry1 = Entry#{child_list_version => CLV + 1,
                    child_list_length => CLL - 1},
    Entries1#{ParentPath => Entry1};
delete_entry(Entries, _Path) ->
    Entries.

result_is_ok(Result, Entries, Path, Default) ->
    case Entries of
        #{Path := NodeProps} ->
            Expected = {ok, #{Path => NodeProps}},
            Expected =:= Result;
        _ ->
            Default =:= Result
    end.

path() ->
    ?LET(Path,
         elements([
                   [path_component()],
                   [path_component(),
                    path_component()],
                   [path_component(),
                    path_component(),
                    path_component()],
                   [path_component(),
                    path_component(),
                    path_component(),
                    path_component()],
                   [path_component(),
                    path_component(),
                    path_component(),
                    path_component(),
                    path_component()]
                  ]),
         khepri_path:realpath(Path)).

path_component() ->
    elements([name(), binary()]).

name() ->
    elements([alice,
              bob,
              carol,
              david,
              eve,
              franck,
              grace,
              heidi,
              ivan,
              judy,
              michael,
              niaj,
              olivia,
              peggy,
              rupert,
              sybil,
              ted,
              victor,
              wendy]).

payload() ->
    elements([no_payload(),
              data_payload()]).

no_payload() ->
    ?NO_PAYLOAD.

data_payload() ->
    ?LET(Data,
         binary(),
         ?DATA_PAYLOAD(Data)).
