%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(khepri_utils).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").

-export([flat_struct_to_tree/1,
         display_tree/1,
         display_tree/2,
         display_tree/3]).

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

-spec flat_struct_to_tree(khepri_machine:node_props_map()) ->
    khepri_machine:node_props().

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
    ?assertNot(maps:is_key(ChildName, Tree)),
    Tree#{ChildName => NodeProps}.

-spec display_tree(khepri_machine:node_props()) -> ok.

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

format_key(Key) when is_atom(Key) ->
    io_lib:format("~ts", [Key]);
format_key(Key) when is_binary(Key) ->
    io_lib:format("<<~ts>>", [Key]).

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
