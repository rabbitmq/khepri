%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden

-module(khepri_utils).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").

-export([start_timeout_window/1,
         end_timeout_window/2,
         sleep/2,
         is_ra_server_alive/1,
         flat_struct_to_tree/1,
         display_tree/1,
         display_tree/2,
         display_tree/3,
         should_collect_code_for_module/1,
         format_exception/4]).

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

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

-spec flat_struct_to_tree(khepri:node_props_map()) ->
    khepri:node_props().

flat_struct_to_tree(FlatStruct) ->
    NodeProps = maps:get([], FlatStruct, #{}),
    Children = maps:fold(
                 fun flat_struct_to_tree/3,
                 #{},
                 maps:remove([], FlatStruct)),
    NodeProps#{child_nodes => Children}.

flat_struct_to_tree([ChildName | [_ | _] = Path], NodeProps, Tree) ->
    Child1 = case Tree of
                 #{ChildName := Child} ->
                     Children = maps:get(child_nodes, Child, #{}),
                     Children1 = flat_struct_to_tree(
                                   Path, NodeProps, Children),
                     Child#{child_nodes => Children1};
                 _ ->
                     Children1 = flat_struct_to_tree(
                                   Path, NodeProps, #{}),
                     #{child_nodes => Children1}
    end,
    Tree#{ChildName => Child1};
flat_struct_to_tree([ChildName], NodeProps, Tree) ->
    case Tree of
        #{ChildName := Child} ->
            ?assertEqual([child_nodes], maps:keys(Child)),
            ?assertNot(maps:is_key(child_nodes, NodeProps)),
            NodeProps1 = maps:merge(NodeProps, Child),
            Tree#{ChildName => NodeProps1};
        _ ->
            Tree#{ChildName => NodeProps}
    end.

-spec display_tree(khepri:node_props()) -> ok.

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

should_collect_code_for_module(Module) ->
    Modules = get_list_of_module_to_skip(),
    not maps:is_key(Module, Modules).

get_list_of_module_to_skip() ->
    Key = {?MODULE, skipped_modules_in_code_collection},
    case persistent_term:get(Key, undefined) of
        Modules when Modules =/= undefined ->
            Modules;
        undefined ->
            InitialModules = #{erlang => true},
            Applications = [erts,
                            kernel,
                            stdlib,
                            mnesia,
                            sasl,
                            ssl,
                            khepri],
            Modules = lists:foldl(
                        fun(App, Modules0) ->
                                _ = application:load(App),
                                case application:get_key(App, modules) of
                                    {ok, Mods} ->
                                        lists:foldl(
                                          fun(Mod, Modules1) ->
                                                  Modules1#{Mod => true}
                                          end, Modules0, Mods);
                                    undefined ->
                                        Modules0
                                end
                        end, InitialModules, Applications),
            persistent_term:put(Key, Modules),

            %% Applications which were not loaded before this function are not
            %% unloaded now: we have no way to determine if another process
            %% could have loaded them in parallel.

            Modules
    end.

-if(?OTP_RELEASE >= 24).
format_exception(Class, Reason, Stacktrace, Options) ->
    erl_error:format_exception(Class, Reason, Stacktrace, Options).
-else.
format_exception(Class, Reason, Stacktrace, Options) ->
    Column = case Options of
                 #{column := C} when is_integer(C) andalso C > 1 -> C;
                 _                                               -> 1
             end,
    Prefix = string:chars($\s, Column - 1),
    StacktraceStrs = [begin
                          case proplists:get_value(line, Props) of
                              undefined when is_list(ArgListOrArity) ->
                                  io_lib:format(
                                    Prefix ++ " ~ts:~ts/~b~n" ++
                                    Prefix ++ "     args: ~p",
                                    [Mod, Fun, length(ArgListOrArity),
                                     ArgListOrArity]);
                              undefined when is_integer(ArgListOrArity) ->
                                  io_lib:format(
                                    Prefix ++ " ~ts:~ts/~b",
                                    [Mod, Fun, ArgListOrArity]);
                              Line when is_list(ArgListOrArity) ->
                                  io_lib:format(
                                    Prefix ++ " ~ts:~ts/~b, line ~b~n" ++
                                    Prefix ++ "     args: ~p",
                                    [Mod, Fun, length(ArgListOrArity), Line,
                                     ArgListOrArity]);
                              Line when is_integer(ArgListOrArity) ->
                                  io_lib:format(
                                    Prefix ++ " ~ts:~ts/~b, line ~b",
                                    [Mod, Fun, ArgListOrArity, Line])
                          end
                      end
                      || {Mod, Fun, ArgListOrArity, Props} <- Stacktrace],
    io_lib:format(
      Prefix ++ "exception ~s: ~p~n"
      "~ts",
      [Class, Reason, string:join(StacktraceStrs, "\n")]).
-endif.
