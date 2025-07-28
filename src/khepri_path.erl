%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Khepri path API.
%%
%% A path is the type used by Khepri to reference nodes in the tree structure.
%% A path describes how to reach a node from the root node.
%%
%% A path, or <em>native path</em>, is a list of components. Components can be
%% Erlang atoms and binaries. Example:
%%
%% ```
%% %% Native path.
%% Path = [stock, wood, <<"oak">>].
%% '''
%%
%% A path may contain conditions to tune how a node is matched or to match
%% multiple nodes at once. This is called a <em>path pattern</em>. A path
%% pattern may contain conditions in addition to regular components (Erlang
%% atoms and binaries). See {@link khepri_condition} to learn more about
%% conditions. Example:
%%
%% ```
%% %% Path pattern with a condition on `wood'.
%% PathPattern = [stock,
%%                #if_all{conditions = [wood,
%%                                      #if_node_exists{exists = true}]},
%%                oak].
%% '''
%%
%% To be user-friendly, string-based and binary-based <em>Unix-like paths</em>
%% are accepted by most functions. The syntax of these <em>Unix paths</em> is
%% described in the {@link unix_path()} type documentation. Example:
%%
%% ```
%% %% Unix path, equivalent of the first native path example.
%% UnixPath = "/:stock/:wood/oak".
%% '''

-module(khepri_path).

-include_lib("stdlib/include/assert.hrl").

-include("include/khepri.hrl").
-include("src/khepri_error.hrl").

-export([compile/1,
         from_string/1,
         from_binary/1,
         to_string/1,
         to_binary/1,
         sigil_p/2,
         sigil_P/2,
         combine_with_conditions/2,
         targets_specific_node/1,
         component_targets_specific_node/1,
         is_valid/1,
         ensure_is_valid/1,
         abspath/2,
         realpath/1,
         pattern_includes_root_node/1]).

-ifdef(TEST).
-export([component_to_string/1]).
-endif.

-type node_id() :: atom() | binary().
%% A node name.

-type component() :: node_id() |
                     ?KHEPRI_ROOT_NODE |
                     ?THIS_KHEPRI_NODE |
                     ?PARENT_KHEPRI_NODE.
%% Component name in a path to a node.

-type native_path() :: [component()].
%% Native path to a node.
%%
%% A native path is a list of atoms, binaries and special components.
%%
%% It is called <em>native</em> because it requires no further processing
%% (unlike {@link unix_path()}) and is the format used internally by the state
%% machine.
%%
%% Special components are:
%% <ol>
%% <li>`?KHEPRI_ROOT_NODE' to explicitly mark the root node. A path is absolute
%% by default. Using `?KHEPRI_ROOT_NODE' is only useful when manipulating the
%% root node itself (querying it or storing something in the root node).</li>
%% <li>`?THIS_KHEPRI_NODE' to make a relative path (the default being an
%% absolute path). This is mostly useful for {@link
%% khepri_condition:keep_while()} to make it easy to put a condition on the
%% node itself.</li>
%% <li>`?PARENT_KHEPRI_NODE' to target the parent of a node, with the same
%% benefits and use cases as `?THIS_KHEPRI_NODE'.</li>
%% </ol>
%%
%% Example:
%%
%% ```
%% %% Native path.
%% Path = [stock, wood, <<"oak">>].
%% '''

-type native_pattern() :: [pattern_component()].
%% Path pattern which may match zero, one or more nodes.
%%
%% A native pattern is a list of atoms, binaries, special components and
%% conditions.
%%
%% It is called <em>native</em> because it requires no further processing
%% (unlike {@link unix_pattern()}) and is the format used internally by the
%% state machine.
%%
%% See {@link native_path()} for a description of special components.
%%
%% Conditions are any condition defined by {@link
%% khepri_condition:condition()}.
%%
%% Example:
%%
%% ```
%% %% Path pattern with a condition on `wood'.
%% PathPattern = [stock,
%%                #if_all{conditions = [wood,
%%                                      #if_node_exists{exists = true}]},
%%                oak].
%% '''

-type unix_path() :: string() | binary().
%% Unix-like path to a node.
%%
%% These <em>Unix paths</em> have the following syntax:
%%
%% <ul>
%% <li>Path components are separated by a forward slash, `/'.</li>
%% <li>Atom-based node IDs are prefixed with a `:' character: `:wood'.</li>
%% <li>Binary-based node IDs are written as-is: `oak'.</li>
%% <li>Atom and binaries can be percent-encoded.</li>
%% <li>An absolute path must start with `/', otherwise it is considered a
%% relative path</li>
%% <li>`.' and `..' represent `?THIS_KHEPRI_NODE' and `?PARENT_KHEPRI_NODE'
%% respectively</li>
%% <li>Simple glob patterns are accepted:
%% <ul>
%% <li>`abc*def' is the same as `#if_name_matches{regex = "^abc.*def$"}'</li>
%% <li>`*' is the same as `?KHEPRI_WILDCARD_STAR' or `#if_name_matches{regex =
%% any}'</li>
%% <li>`**' is the same as `?KHEPRI_WILDCARD_STAR_STAR' or
%% `if_path_matches{regex = any}'</li>
%% </ul></li>
%% </ul>
%%
%% <strong>Warning</strong>: There is no special handling of Unicode in tree
%% node names. To use Unicode, it is recommended to either use a native path or
%% a binary-based Unix-like path. If using a string-based Unix-like path, the
%% behavior is undefined and the call may crash. Matching against node names is
%% also undefined behavior and may crash, regardless of the type of path being
%% used. It will be improved in the future.
%%
%% Example:
%% ```
%% %% Unix path, equivalent of the first native path example.
%% UnixPath = "/:stock/:wood/oak".
%% '''

-type unix_pattern() :: string() | binary().
%% Unix-like path pattern to a node.
%%
%% It accepts the following special characters:
%% <ol>
%% <li>`*' anywhere in a path component behaves like a {@link
%% khepri_condition:if_name_matches()}.</li>
%% <li>`**' as a path component behaves like a {@link
%% khepri_condition:if_path_matches()}.</li>
%% </ol>
%%
%% A Unix-like path pattern can't express all the conditions of a native path
%% pattern currently.
%%
%% Otherwise it works as a {@link unix_path()} and has the same syntax and
%% limitations.
%%
%% Example:
%% ```
%% %% Unix path pattern, matching multiple types of oak.
%% UnixPathPattern = "/:stock/:wood/*oak".
%% '''

-type path() :: native_path() | unix_path().
%% Path to a node.

-type pattern() :: native_pattern() | unix_pattern().
%% Path pattern which may match zero, one or more nodes.

-type pattern_component() :: component() | khepri_condition:condition().
%% Path pattern component which may match zero, one or more nodes.

-export_type([path/0,
              native_path/0,
              unix_path/0,
              pattern/0,
              native_pattern/0,
              unix_pattern/0,
              component/0,
              pattern_component/0,
              node_id/0]).

-define(
   reject_invalid_path(Path),
   ?khepri_misuse(invalid_path, #{path => Path})).

-define(
   reject_invalid_path(Path, Component),
   ?khepri_misuse(invalid_path, #{path => Path,
                                  component => Component})).

-spec compile(PathPattern) -> PathPattern when
      PathPattern :: native_pattern().
%% @private

compile(PathPattern) ->
    lists:map(fun khepri_condition:compile/1, PathPattern).

-spec from_string(String) -> PathPattern when
      String :: pattern(),
      PathPattern :: native_pattern().
%% @doc Converts a Unix-like path to a native path.
%%
%% The Unix-like string can be either an Erlang string or an Erlang binary.
%%
%% For convenience, a native path is also accepted and returned as-is.

from_string([$/, $/ | MaybeString]) ->
    %% The path starts with two forward slashes. Therefore the path starts
    %% with an empty binary.
    from_string([$/ | MaybeString], [<<>>, ?KHEPRI_ROOT_NODE]);
from_string([$/ | MaybeString]) ->
    from_string(MaybeString, [?KHEPRI_ROOT_NODE]);
from_string(MaybeString) when is_list(MaybeString) ->
    from_string(MaybeString, []);
from_string(Binary) when is_binary(Binary) ->
    String = erlang:binary_to_list(Binary),
    from_string(String);
from_string(NotPath) ->
    ?reject_invalid_path(NotPath).

-spec from_binary(String) -> PathPattern when
      String :: pattern(),
      PathPattern :: native_pattern().
%% @doc Converts a Unix-like path to a native path.
%%
%% This is the same as calling `from_string(String)'. Therefore, it accepts
%% Erlang strings or binaries and native paths.
%%
%% @see from_string/1.

from_binary(MaybeString) ->
    from_string(MaybeString).

-spec sigil_p(PathPattern, Options) -> NativePathPattern when
      PathPattern :: pattern(),
      Options :: [char()],
      NativePathPattern :: native_pattern().
%% @doc Elixir sigil to parse Unix-like path using the `~p"/:path/:to/node"'
%% syntax.
%%
%% The lowercase `~p' sigil means that the string will go through
%% interpolation first before this function is called.
%%
%% @see sigil_P/2.
%%
%% @private

sigil_p(PathPattern, _Options) ->
    from_string(PathPattern).

-spec sigil_P(PathPattern, Options) -> NativePathPattern when
      PathPattern :: pattern(),
      Options :: [char()],
      NativePathPattern :: native_pattern().
%% @doc Elixir sigil to parse Unix-like path using the `~P"/:path/:to/node"'
%% syntax.
%%
%% The uppercase `~P' sigil means that the string will NOT go through
%% interpolation first before this function is called.
%%
%% @see sigil_p/2.
%%
%% @private

sigil_P(PathPattern, _Options) ->
    from_string(PathPattern).

from_string([Component | _] = Rest, ReversedPath)
  when ?IS_KHEPRI_NODE_ID(Component) orelse
       ?IS_KHEPRI_CONDITION(Component) ->
    finalize_path(Rest, ReversedPath);
from_string([Char, Component | _] = Rest, ReversedPath)
  when ?IS_SPECIAL_KHEPRI_PATH_COMPONENT(Char) andalso
       (?IS_KHEPRI_NODE_ID(Component) orelse
        ?IS_KHEPRI_CONDITION(Component)) ->
    finalize_path(Rest, ReversedPath);
from_string([?PARENT_KHEPRI_NODE, $/ | _] = Rest, ReversedPath) ->
    %% If the character used to represent the parent node in a regular path
    %% (`^') appears alone in a path component, it's a regular path. Other
    %% special path components may appear alone in both forms though.
    finalize_path(Rest, ReversedPath);
from_string([Char] = Rest, [] = ReversedPath)
  when ?IS_SPECIAL_KHEPRI_PATH_COMPONENT(Char) ->
    finalize_path(Rest, ReversedPath);

from_string([$/, $/ | Rest], ReversedPath) ->
    %% Two consecutive forward slashes mean there is an empty binary
    %% component.
    ReversedPath1 = prepend_component(<<>>, ReversedPath),
    from_string([$/ | Rest], ReversedPath1);

from_string([$/ | Rest], ReversedPath) ->
    from_string(Rest, ReversedPath);

from_string([$: | Rest], ReversedPath) ->
    parse_atom_from_string(Rest, ReversedPath);

from_string([Char | _] = Rest, ReversedPath) when is_integer(Char) ->
    parse_binary_from_string(Rest, ReversedPath);

from_string([], ReversedPath) ->
    finalize_path([], ReversedPath);

from_string(Rest, ReversedPath) ->
    NotPath = lists:reverse(ReversedPath) ++ Rest,
    ?reject_invalid_path(NotPath).

parse_atom_from_string(Rest, ReversedPath) ->
    parse_atom_from_string(Rest, "", ReversedPath).

parse_atom_from_string([$/ | _] = Rest, Acc, ReversedPath) ->
    Component = finalize_atom_component(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1);
parse_atom_from_string([Char | Rest], Acc, ReversedPath)
  when is_integer(Char) ->
    Acc1 = [Char | Acc],
    parse_atom_from_string(Rest, Acc1, ReversedPath);
parse_atom_from_string([] = Rest, Acc, ReversedPath) ->
    Component = finalize_atom_component(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1).

finalize_atom_component(Acc) ->
    Acc1 = lists:reverse(Acc),
    Acc2 = percent_decode_string(Acc1),
    erlang:list_to_atom(Acc2).

parse_binary_from_string(Rest, ReversedPath) ->
    parse_binary_from_string(Rest, "", ReversedPath).

parse_binary_from_string([$/ | _] = Rest, Acc, ReversedPath) ->
    Component = finalize_binary_componenent(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1);
parse_binary_from_string([Char | Rest], Acc, ReversedPath)
  when is_integer(Char) ->
    Acc1 = [Char | Acc],
    parse_binary_from_string(Rest, Acc1, ReversedPath);
parse_binary_from_string([] = Rest, Acc, ReversedPath) ->
    Component = finalize_binary_componenent(Acc),
    ReversedPath1 = prepend_component(Component, ReversedPath),
    from_string(Rest, ReversedPath1).

finalize_binary_componenent(Acc) ->
    Acc1 = lists:reverse(Acc),
    case Acc1 of
        "." ->
            ?THIS_KHEPRI_NODE;
        ".." ->
            ?PARENT_KHEPRI_NODE;
        "*" ->
            ?KHEPRI_WILDCARD_STAR;
        "**" ->
            ?KHEPRI_WILDCARD_STAR_STAR;
        _ ->
            Acc2 = percent_decode_string(Acc1),
            case re:run(Acc2, "\\*", [{capture, none}]) of
                match ->
                    ReOpts = [global, {return, list}],
                    Regex = re:replace(Acc2, "\\*", ".*", ReOpts),
                    #if_name_matches{regex = "^" ++ Regex ++ "$"};
                nomatch ->
                    erlang:list_to_binary(Acc2)
            end
    end.

prepend_component(Component, []) when ?IS_KHEPRI_NODE_ID(Component) ->
    %% This is a relative path.
    [Component, ?THIS_KHEPRI_NODE];
prepend_component(Component, ReversedPath) ->
    [Component | ReversedPath].

finalize_path(Rest, []) ->
    Rest;
finalize_path(Rest, ReversedPath) ->
    case lists:reverse(ReversedPath) ++ Rest of
        [?KHEPRI_ROOT_NODE | Path] -> Path;
        Path                -> Path
    end.

-spec to_string(NativePath) -> UnixPath when
      NativePath :: native_path(),
      UnixPath :: string().
%% @doc Converts a native path to a string.

to_string([] = Path) ->
    to_string(Path, "/", false);
to_string([?KHEPRI_ROOT_NODE | Path]) ->
    to_string(Path, "/", false);
to_string([?THIS_KHEPRI_NODE] = Path) ->
    to_string(Path, "", false);
to_string([?THIS_KHEPRI_NODE, <<>> | _] = Path) ->
    %% Special case: a relative path starting with an empty binary. We need to
    %% keep the leading '.' because we rely on forward slashes to "encode" the
    %% empty binary. If we don't keep the '.', the path will become absolute.
    to_string(Path, "", false);
to_string([?THIS_KHEPRI_NODE | Path]) ->
    to_string(Path, "", false);
to_string([?PARENT_KHEPRI_NODE | _] = Path) ->
    to_string(Path, "", false);
to_string(Path) ->
    to_string(Path, "", true).

to_string([<<>> = Component], Result, NeedSlash) ->
    Component1 = component_to_string(Component),
    Result1 = append_string_component(Component1, Result, NeedSlash) ++ [$/],
    Result1;
to_string([Component | Rest], Result, NeedSlash) ->
    Component1 = component_to_string(Component),
    Result1 = append_string_component(Component1, Result, NeedSlash),
    to_string(Rest, Result1, true);
to_string([], Result, _NeedSlash) ->
    Result.

append_string_component(Component, Result, true) ->
    Result ++ [$/ | Component];
append_string_component(Component, Result, false) ->
    Result ++ Component.

-spec to_binary(NativePath) -> UnixPath when
      NativePath :: native_path(),
      UnixPath :: binary().
%% @doc Converts a native path to a binary.

to_binary(Path) ->
    String = to_string(Path),
    erlang:list_to_binary(String).

-spec component_to_string(component()) -> string().
%% @private

component_to_string(?KHEPRI_ROOT_NODE) ->
    "/";
component_to_string(?THIS_KHEPRI_NODE) ->
    ".";
component_to_string(?PARENT_KHEPRI_NODE) ->
    "..";
component_to_string(Component) when is_atom(Component) ->
    ":" ++ percent_encode_string(erlang:atom_to_list(Component));
component_to_string(Component) when is_binary(Component) ->
    percent_encode_string(erlang:binary_to_list(Component)).

-define(IS_HEX(Digit), (is_integer(Digit) andalso
                        ((Digit >= $0 andalso Digit =< $9) orelse
                         (Digit >= $A andalso Digit =< $F) orelse
                         (Digit >= $a andalso Digit =< $f)))).

percent_decode_string(String) when is_list(String) ->
    percent_decode_string(String, "").

percent_decode_string([$%, Digit1, Digit2 | Rest], PercentDecoded)
  when ?IS_HEX(Digit1) andalso ?IS_HEX(Digit2) ->
    Char = erlang:list_to_integer([Digit1, Digit2], 16),
    PercentDecoded1 = PercentDecoded ++ [Char],
    percent_decode_string(Rest, PercentDecoded1);
percent_decode_string([Char | Rest], PercentDecoded) ->
    PercentDecoded1 = PercentDecoded ++ [Char],
    percent_decode_string(Rest, PercentDecoded1);
percent_decode_string([], PercentDecoded) ->
    PercentDecoded.

percent_encode_string(String) when is_list(String) ->
    percent_encode_string(String, "").

percent_encode_string([Char | Rest], PercentEncoded)
  when is_integer(Char) andalso
       ((Char >= $A andalso Char =< $Z) orelse
        (Char >= $a andalso Char =< $z) orelse
        (Char >= $0 andalso Char =< $9) orelse
        (Char =:= $. andalso PercentEncoded =/= "") orelse
        Char =:= $- orelse Char =:= $_ orelse Char =:= $~) ->
    PercentEncoded1 = PercentEncoded ++ [Char],
    percent_encode_string(Rest, PercentEncoded1);
percent_encode_string([Char | Rest], PercentEncoded) ->
    PEChar = lists:flatten(io_lib:format("%~2.16.0B", [Char])),
    PercentEncoded1 = PercentEncoded ++ PEChar,
    percent_encode_string(Rest, PercentEncoded1);
percent_encode_string([], PercentEncoded) ->
    PercentEncoded.

-spec combine_with_conditions(PathPattern, Conditions) -> PathPattern when
      PathPattern :: native_pattern(),
      Conditions :: [khepri_condition:condition()].

combine_with_conditions(Path, []) ->
    Path;
combine_with_conditions(Path, Conditions) ->
    [ChildName | Rest] = lists:reverse(Path),
    Combined = #if_all{conditions = [ChildName | Conditions]},
    lists:reverse([Combined | Rest]).

-spec targets_specific_node(PathPattern) -> Ret when
      PathPattern :: native_pattern(),
      Ret :: {true, Path} | false,
      Path :: native_path().

targets_specific_node(PathPattern) ->
    targets_specific_node(PathPattern, []).

targets_specific_node([Condition | Rest], Path) ->
    case component_targets_specific_node(Condition) of
        {true, Component} -> targets_specific_node(Rest, [Component | Path]);
        false             -> false
    end;
targets_specific_node([], Path) ->
    {true, lists:reverse(Path)}.

-spec component_targets_specific_node(ComponentPattern) -> Ret when
      ComponentPattern :: pattern_component(),
      Ret :: {true, Component} | false,
      Component :: component().
%% @private

component_targets_specific_node(ChildName)
  when ?IS_KHEPRI_PATH_COMPONENT(ChildName) ->
    {true, ChildName};
component_targets_specific_node(#if_not{condition = Cond}) ->
    component_targets_specific_node(Cond);
component_targets_specific_node(#if_all{conditions = []}) ->
    false;
component_targets_specific_node(#if_all{conditions = Conds}) ->
    lists:foldl(
      fun
          (Cond, {true, _} = True) ->
              case component_targets_specific_node(Cond) of
                  True      -> True;
                  {true, _} -> false;
                  false     -> True
              end;
          (Cond, false) ->
              case component_targets_specific_node(Cond) of
                  {true, _} = True -> True;
                  false            -> false
              end;
          (Cond, undefined) ->
              component_targets_specific_node(Cond)
      end, undefined, Conds);
component_targets_specific_node(#if_any{conditions = []}) ->
    false;
component_targets_specific_node(#if_any{conditions = Conds}) ->
    lists:foldl(
      fun
          (Cond, {true, _} = True) ->
              case component_targets_specific_node(Cond) of
                  True      -> True;
                  {true, _} -> false;
                  false     -> false
              end;
          (_, false) ->
              false;
          (Cond, undefined) ->
              component_targets_specific_node(Cond)
      end, undefined, Conds);
component_targets_specific_node(_) ->
    false.

-spec is_valid(PathPattern) -> IsValid when
      PathPattern :: native_pattern(),
      IsValid :: true | {false, ComponentPattern},
      ComponentPattern :: pattern_component().

is_valid(PathPattern) when is_list(PathPattern) ->
    lists:foldl(
      fun
          (_, {false, _} = False) -> False;
          (Component, _)          -> khepri_condition:is_valid(Component)
      end, true, PathPattern);
is_valid(NotPathPattern) ->
    {false, NotPathPattern}.

-spec ensure_is_valid(PathPattern) -> ok | no_return() when
      PathPattern :: native_pattern().

ensure_is_valid(PathPattern) ->
    case is_valid(PathPattern) of
        true ->
            ok;
        {false, Component} ->
            ?reject_invalid_path(PathPattern, Component)
    end.

-spec abspath(Path, BasePath) -> Path when
      Path :: native_pattern(),
      BasePath :: native_pattern().

abspath([FirstComponent | _] = AbsolutePath, _)
  when FirstComponent =/= ?THIS_KHEPRI_NODE andalso
       FirstComponent =/= ?PARENT_KHEPRI_NODE ->
    AbsolutePath;
abspath([_ | _] = RelativePath, BasePath) ->
    realpath(BasePath ++ RelativePath, []);
abspath([] = PathToRoot, _) ->
    PathToRoot.

-spec realpath(Path) -> NewPath when
      Path :: native_pattern(),
      NewPath :: native_pattern().

realpath(Path) ->
    realpath(Path, []).

realpath([?KHEPRI_ROOT_NODE | Rest], _Result) ->
    realpath(Rest, []);
realpath([?THIS_KHEPRI_NODE | Rest], Result) ->
    realpath(Rest, Result);
realpath([?PARENT_KHEPRI_NODE | Rest], [_ | Result]) ->
    realpath(Rest, Result);
realpath([?PARENT_KHEPRI_NODE | Rest], [] = Result) ->
    realpath(Rest, Result);
realpath([Component | Rest], Result)
  when is_atom(Component) orelse
       is_binary(Component) orelse
       ?IS_KHEPRI_CONDITION(Component) ->
    realpath(Rest, [Component | Result]);
realpath([], Result) ->
    lists:reverse(Result).

pattern_includes_root_node(Path) ->
    [] =:= realpath(Path).
