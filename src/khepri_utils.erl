%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @hidden

-module(khepri_utils).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").

-export([start_timeout_window/1,
         end_timeout_window/2,
         sleep/2,
         is_ra_server_alive/1,

         get_single_node_props/1,
         node_props_to_payload/2,

         flat_struct_to_tree/1,
         flat_struct_to_tree/2,
         display_tree/1,
         display_tree/2,
         display_tree/3,
         should_process_module/1,
         init_list_of_modules_to_skip/0,
         clear_list_of_modules_to_skip/0,
         format_exception/4]).

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

-type display_tree() :: #{data => khepri:data(),
                          sproc => horus:horus_fun(),
                          payload_version => khepri:payload_version(),
                          child_list_version => khepri:child_list_version(),
                          child_list_length => khepri:child_list_length(),
                          child_names => [khepri_path:node_id()],
                          child_nodes => #{khepri_path:node_id() =>
                                           display_tree()}}.

-export_type([display_tree/0]).

-spec start_timeout_window(Timeout) -> Timestamp | none when
      Timeout :: timeout(),
      Timestamp :: integer().

start_timeout_window(infinity) ->
    none;
start_timeout_window(_Timeout) ->
    erlang:monotonic_time().

-spec end_timeout_window(Timeout, Timestamp | none) -> Timeout when
      Timeout :: timeout(),
      Timestamp :: integer().

end_timeout_window(infinity = Timeout, none) ->
    Timeout;
end_timeout_window(Timeout, T0) ->
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    Remaining = Timeout - TDiff,
    erlang:max(Remaining, 0).

-spec sleep(Time, Timeout) -> Timeout when
      Time :: non_neg_integer(),
      Timeout :: timeout().

sleep(Time, infinity = Timeout) ->
    timer:sleep(Time),
    Timeout;
sleep(_Time, 0 = Timeout) ->
    Timeout;
sleep(Time, Timeout) when Time =< Timeout ->
    timer:sleep(Time),
    Timeout - Time;
sleep(Time, Timeout) when Time > Timeout ->
    timer:sleep(Timeout),
    0.

-spec is_ra_server_alive(RaServer) -> IsAlive when
      RaServer :: ra:server_id(),
      IsAlive :: boolean().

is_ra_server_alive({RegName, Node}) when Node =:= node() ->
    is_pid(erlang:whereis(RegName)).

get_single_node_props(NodePropsMap) ->
    [NodeProps] = maps:values(NodePropsMap),
    NodeProps.

node_props_to_payload(#{data := Data}, _Default)           -> Data;
node_props_to_payload(#{sproc := StandaloneFun}, _Default) -> StandaloneFun;
node_props_to_payload(_NodeProps, Default)                 -> Default.

-spec flat_struct_to_tree(NodePropsMap) -> DisplayTree when
      NodePropsMap :: khepri:node_props_map(),
      DisplayTree :: display_tree().

flat_struct_to_tree(FlatStruct) ->
    flat_struct_to_tree(FlatStruct, fun(NodeProps) -> NodeProps end).

-spec flat_struct_to_tree(NodePropsMap, MapFun) -> DisplayTree when
      NodePropsMap :: khepri:node_props_map(),
      MapFun :: fun((khepri:node_props()) -> khepri:node_props()),
      DisplayTree :: display_tree().

flat_struct_to_tree(FlatStruct, MapFun) ->
    NodeProps = maps:get([], FlatStruct, #{}),
    Children = maps:fold(
                 fun(Children, Props, Tree) ->
                         flat_struct_to_tree(Children, Props, Tree, MapFun)
                 end,
                 #{},
                 maps:remove([], FlatStruct)),
    NodeProps#{child_nodes => Children}.

flat_struct_to_tree([ChildName | [_ | _] = Path], NodeProps, Tree, MapFun) ->
    Child1 = case Tree of
                 #{ChildName := Child} ->
                     Children = maps:get(child_nodes, Child, #{}),
                     Children1 = flat_struct_to_tree(
                                   Path, NodeProps, Children, MapFun),
                     Child#{child_nodes => Children1};
                 _ ->
                     Children1 = flat_struct_to_tree(
                                   Path, NodeProps, #{}, MapFun),
                     #{child_nodes => Children1}
    end,
    Tree#{ChildName => Child1};
flat_struct_to_tree([ChildName], NodeProps, Tree, MapFun) ->
    case Tree of
        #{ChildName := Child} ->
            ?assertEqual([child_nodes], maps:keys(Child)),
            ?assertNot(maps:is_key(child_nodes, NodeProps)),
            NodeProps1 = maps:merge(NodeProps, Child),
            Tree#{ChildName => MapFun(NodeProps1)};
        _ ->
            Tree#{ChildName => MapFun(NodeProps)}
    end.

-spec display_tree(display_tree()) -> ok.

display_tree(Tree) ->
    display_tree(Tree, "").

display_tree(Tree, Options) when is_map(Options) ->
    display_tree(Tree, "", Options);
display_tree(Tree, Prefix) when is_list(Prefix) ->
    display_tree(Tree, Prefix, #{colors => true,
                                 lines => true}).

display_tree(#{child_nodes := Children}, Prefix, Options) ->
    Keys = lists:sort(
             fun(A, B) ->
                     ABin = ensure_is_binary(A),
                     BBin = ensure_is_binary(B),
                     case ABin == BBin of
                         true  -> A =< B;
                         false -> ABin =< BBin
                     end
             end, maps:keys(Children)),
    display_nodes(Keys, Children, Prefix, Options);
display_tree(_, _, _) ->
    ok.

ensure_is_binary(Key) when is_atom(Key) ->
    list_to_binary(atom_to_list(Key));
ensure_is_binary(Key) when is_binary(Key) ->
    Key.

display_nodes([Key | Rest], Children, Prefix, Options) ->
    IsLast = Rest =:= [],
    display_node_branch(Key, IsLast, Prefix, Options),
    NodeProps = maps:get(Key, Children),
    NewPrefix = Prefix ++ prefix(IsLast, Options),
    DataPrefix = NewPrefix ++ data_prefix(NodeProps, Options),
    case NodeProps of
        #{data := Data} -> display_data(Data, DataPrefix, Options);
        #{sproc := Fun} -> display_data(Fun, DataPrefix, Options);
        _               -> ok
    end,
    display_tree(NodeProps, NewPrefix, Options),
    display_nodes(Rest, Children, Prefix, Options);
display_nodes([], _, _, _) ->
    ok.

display_node_branch(Key, false, Prefix, #{lines := false}) ->
    io:format("~ts+-- ~ts~n", [Prefix, format_key(Key)]);
display_node_branch(Key, true, Prefix, #{lines := false}) ->
    io:format("~ts`-- ~ts~n", [Prefix, format_key(Key)]);
display_node_branch(Key, false, Prefix, _Options) ->
    io:format("~ts├── ~ts~n", [Prefix, format_key(Key)]);
display_node_branch(Key, true, Prefix, _Options) ->
    io:format("~ts╰── ~ts~n", [Prefix, format_key(Key)]).

format_key(Key) ->
    io_lib:format("~0p", [Key]).

display_data(Data, Prefix, Options) ->
    Formatted = format_data(Data, Options),
    Lines = string:split(Formatted, "\n", all),
    case Options of
        #{colors := false} ->
            lists:foreach(
              fun(Line) ->
                      io:format("~ts~ts~n", [Prefix, Line])
              end, Lines);
        _ ->
            lists:foreach(
              fun(Line) ->
                      io:format(
                        "~ts\033[38;5;246m~ts\033[0m~n",
                        [Prefix, Line])
              end, Lines)
    end,
    io:format("~ts~n", [string:trim(Prefix, trailing)]).

prefix(false, #{lines := false}) -> "|   ";
prefix(false, _Options)          -> "│   ";
prefix(true, _Options)           -> "    ".

data_prefix(#{child_nodes := _}, #{lines := false}) -> "| ";
data_prefix(#{child_nodes := _}, _Options)          -> "│ ";
data_prefix(_, #{lines := false})                   -> "  ";
data_prefix(_, _Options)                            -> "  ".

format_data(Data, _Options) ->
    lists:flatten(io_lib:format("Data: ~tp", [Data])).

%% -------------------------------------------------------------------
%% Helpers to standalone function extraction.
%% -------------------------------------------------------------------

-define(PT_MODULES_TO_SKIP, {khepri, skipped_modules_in_code_collection}).

should_process_module(Module) ->
    case horus_utils:should_process_module(Module) of
        true ->
            Modules = persistent_term:get(?PT_MODULES_TO_SKIP),
            not maps:is_key(Module, Modules);
        false ->
            false
    end.

init_list_of_modules_to_skip() ->
    Applications = [mnesia,
                    sasl,
                    ssl,
                    khepri],
    LoadedApps = [App ||
                  {App, _Desc, _Vsn} <- application:loaded_applications()],
    ?assert(lists:member(khepri, LoadedApps)),
    Modules = lists:foldl(
                fun(App, Modules0) ->
                        _ = case lists:member(App, LoadedApps) of
                                true  -> ok;
                                false -> application:load(App)
                            end,
                        Mods = case application:get_key(App, modules) of
                                   {ok, Mods0} -> Mods0;
                                   undefined   -> []
                               end,
                        lists:foldl(
                          fun(Mod, Modules1) ->
                                  Modules1#{Mod => true}
                          end, Modules0, Mods)
                end, #{}, Applications),
    ?assert(maps:is_key(khepri, Modules)),
    persistent_term:put(?PT_MODULES_TO_SKIP, Modules),
    ok.

clear_list_of_modules_to_skip() ->
    _ = persistent_term:erase(?PT_MODULES_TO_SKIP),
    ok.

format_exception(Class, Reason, Stacktrace, Options) ->
    erl_error:format_exception(Class, Reason, Stacktrace, Options).
