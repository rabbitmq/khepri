%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2024 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(prop_state_machine).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-include("include/khepri.hrl").
-include("src/khepri_payload.hrl").

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
    Options = #{props_to_return => [payload,
                                    payload_version,
                                    child_list_version,
                                    child_list_length]},
    elements([{call, khepri_adv, put_many, [?STORE_ID, path(), payload(),
                                            Options]},
              {call, khepri_adv, get_many, [?STORE_ID, path(), Options]},
              {call, khepri_adv, delete_many, [?STORE_ID, path(), Options]}]).

precondition(_State, _Command) ->
    true.

next_state(
  #state{} = State,
  _Result,
  {call, khepri_adv, get_many, [_StoreId, _Path, _Options]}) ->
    State;
next_state(
  #state{entries = Entries} = State,
  _Result,
  {call, khepri_adv, put_many, [_StoreId, Path, Payload, _Options]}) ->
    Entries1 = add_entry(Entries, Path, Payload),
    State#state{entries = Entries1,
                old_entries = Entries};
next_state(
  #state{entries = Entries} = State,
  _Result,
  {call, khepri_adv, delete_many, [_StoreId, Path, _Options]}) ->
    Entries1 = delete_entry(Entries, Path),
    State#state{entries = Entries1,
                old_entries = Entries}.

postcondition(
  #state{entries = Entries},
  {call, khepri_adv, get_many, [_StoreId, Path, _Options]},
  Result) ->
    result_is_ok(Result, Entries, Path, {ok, #{}});
postcondition(
  #state{entries = Entries},
  {call, khepri_adv, put_many, [_StoreId, Path, Payload, _Options]},
  Result) ->
    result_is_ok_after_put(
      Result, Entries, Path, Payload,
      {ok, #{Path => #{payload_version => 1,
                       child_list_version => 1,
                       child_list_length => 0}}});
postcondition(
  #state{entries = Entries},
  {call, khepri_adv, delete_many, [_StoreId, Path, _Options]},
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
    Entries1 = Entries#{Path => Entry1},
    add_entry1(Entries1, tl(lists:reverse(Path)), New).

set_node_payload(#{data := Data} = Entry, #p_data{data = Data}) ->
    Entry;
set_node_payload(Entry, ?NO_PAYLOAD) when not is_map_key(data, Entry) ->
    Entry;
set_node_payload(Entry, #p_data{data = Data}) ->
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

result_is_ok_after_put(Result, Entries, Path, Payload, Default) ->
    case Entries of
        #{Path := #{data := Payload} = NodeProps} ->
            %% The payload didn't change.
            Expected = {ok, #{Path => NodeProps}},
            Expected =:= Result;
        #{Path := NodeProps}
          when not is_map_key(data, NodeProps) andalso
               Payload =:= ?NO_PAYLOAD ->
            %% The payload didn't change; there is still none.
            Expected = {ok, #{Path => NodeProps}},
            Expected =:= Result;
        #{Path := #{payload_version := PV} = NodeProps0} ->
            NodeProps1 = NodeProps0#{payload_version => PV + 1},
            Expected = {ok, #{Path => NodeProps1}},
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
         #p_data{data = Data}).
