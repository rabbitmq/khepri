%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(path_operations).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").

%% -------------------------------------------------------------------
%% Path component parsing.
%% -------------------------------------------------------------------

atom_component_from_string_test() ->
    ?assertEqual(foo, khepri_path:component_from_string("foo")),
    ?assertEqual('<foo>', khepri_path:component_from_string("<foo>")),
    ?assertEqual('<<foo', khepri_path:component_from_string("<<foo")).

binary_component_from_string_test() ->
    ?assertEqual(<<"foo">>, khepri_path:component_from_string("<<foo>>")),
    ?assertEqual(<<"<foo>">>, khepri_path:component_from_string("<<<foo>>>")).

root_component_from_string_test() ->
    ?assertEqual(?ROOT_NODE, khepri_path:component_from_string("/")).

dot_component_from_string_test() ->
    ?assertEqual(?THIS_NODE, khepri_path:component_from_string(".")).

dot_dot_component_from_string_test() ->
    ?assertEqual(?PARENT_NODE, khepri_path:component_from_string("..")).

star_component_from_string_test() ->
    ?assertEqual(?STAR, khepri_path:component_from_string("*")).

star_star_component_from_string_test() ->
    ?assertEqual(?STAR_STAR, khepri_path:component_from_string("**")).

glob_pattern_component_from_string_test() ->
    ?assertEqual(
        #if_name_matches{regex = "^foo.*$"},
        khepri_path:component_from_string("foo*")
    ),
    ?assertEqual(
        #if_name_matches{regex = "^.*foo.*$"},
        khepri_path:component_from_string("*foo*")
    ),
    ?assertEqual(
        #if_name_matches{regex = "^.*foo.*bar.*$"},
        khepri_path:component_from_string("*foo*bar*")
    ).

%% -------------------------------------------------------------------
%% Entire path parsing.
%% -------------------------------------------------------------------

root_path_from_string_test() ->
    ?assertEqual([], khepri_path:from_string("")),
    ?assertEqual([], khepri_path:from_string("/")).

path_with_one_component_from_string_test() ->
    ?assertEqual([foo], khepri_path:from_string("/foo")).

path_with_multiple_components_from_string_test() ->
    ?assertEqual(
        [foo, bar, baz],
        khepri_path:from_string("/foo/bar/baz")
    ).

unprefixed_relative_path_from_string_test() ->
    ?assertEqual(
        [?THIS_NODE, foo, bar, baz],
        khepri_path:from_string("foo/bar/baz")
    ).

relative_path_prefixed_with_dot_from_string_test() ->
    ?assertEqual(
        [?THIS_NODE, foo, bar, baz],
        khepri_path:from_string("./foo/bar/baz")
    ),
    ?assertEqual(
        khepri_path:from_string("foo/bar/baz"),
        khepri_path:from_string("./foo/bar/baz")
    ).

relative_path_prefixed_with_dot_dot_from_string_test() ->
    ?assertEqual(
        [?PARENT_NODE, foo, bar, baz],
        khepri_path:from_string("../foo/bar/baz")
    ).

path_with_star_from_string_test() ->
    ?assertEqual(
        [foo, ?STAR],
        khepri_path:from_string("/foo/*")
    ).

path_with_star_star_from_string_test() ->
    ?assertEqual(
        [foo, ?STAR_STAR],
        khepri_path:from_string("/foo/**")
    ).

path_with_binary_from_string_test() ->
    ?assertEqual(
        [<<"binary">>],
        khepri_path:from_string("/<<binary>>")
    ),
    ?assertEqual(
        [<<"bin/ary">>],
        khepri_path:from_string("/<<bin/ary>>")
    ),
    ?assertEqual(
        [<<"bin/ary">>, atom],
        khepri_path:from_string("/<<bin/ary>>/atom")
    ).

path_with_consecutive_slashes_from_string_test() ->
    ?assertEqual(
        [foo, bar],
        khepri_path:from_string("////foo////bar")
    ).

path_with_trailing_slashes_from_string_test() ->
    ?assertEqual(
        [foo, bar],
        khepri_path:from_string("/foo/bar/")
    ),
    ?assertEqual(
        [foo, bar],
        khepri_path:from_string("/foo/bar////")
    ).

path_with_whitespaces_from_string_test() ->
    ?assertEqual(
        ['foo  ', '  bar'],
        khepri_path:from_string("/foo  /  bar")
    ).

maybe_from_string_on_path_test() ->
    ?assertEqual(
        [foo, bar],
        khepri_path:maybe_from_string([foo, bar])
    ),
    ?assertEqual(
        [?THIS_NODE, foo, bar],
        khepri_path:maybe_from_string([?THIS_NODE, foo, bar])
    ).

maybe_from_string_on_string_test() ->
    ?assertEqual(
        [foo, bar],
        khepri_path:maybe_from_string("/foo/bar")
    ).

maybe_from_string_on_relative_string_test() ->
    ?assertEqual(
        [?THIS_NODE, foo, bar],
        khepri_path:maybe_from_string("foo/bar")
    ),
    ?assertEqual(
        [?THIS_NODE, foo, bar],
        khepri_path:maybe_from_string("./foo/bar")
    ).

maybe_from_string_on_empty_string_test() ->
    ?assertEqual(
        [],
        khepri_path:maybe_from_string("")
    ).

maybe_special_chars_from_string_test() ->
    ?assertEqual(
        [],
        khepri_path:maybe_from_string("/")
    ),
    ?assertEqual(
        [?THIS_NODE],
        khepri_path:maybe_from_string(".")
    ),
    ?assertEqual(
        [?PARENT_NODE],
        khepri_path:maybe_from_string("^")
    ),
    ?assertEqual(
        [?PARENT_NODE],
        khepri_path:maybe_from_string("..")
    ).

%% -------------------------------------------------------------------
%% Path component serializing.
%% -------------------------------------------------------------------

atom_component_to_string_test() ->
    ?assertEqual("foo", khepri_path:component_to_string(foo)).

binary_component_to_string_test() ->
    ?assertEqual("<<foo>>", khepri_path:component_to_string(<<"foo">>)).

root_component_to_string_test() ->
    ?assertEqual("/", khepri_path:component_to_string(?ROOT_NODE)).

dot_component_to_string_test() ->
    ?assertEqual(".", khepri_path:component_to_string(?THIS_NODE)).

dot_dot_component_to_string_test() ->
    ?assertEqual("..", khepri_path:component_to_string(?PARENT_NODE)).

%% -------------------------------------------------------------------
%% Entire path serializing.
%% -------------------------------------------------------------------

root_path_to_string_test() ->
    ?assertEqual("/", khepri_path:to_string([])).

path_with_one_component_to_string_test() ->
    ?assertEqual("/foo", khepri_path:to_string([foo])).

path_with_multiple_components_to_string_test() ->
    ?assertEqual(
        "/foo/bar/baz",
        khepri_path:to_string([foo, bar, baz])
    ).

path_with_explicit_root_to_string_test() ->
    ?assertEqual(
        "/foo/bar/baz",
        khepri_path:to_string([?ROOT_NODE, foo, bar, baz])
    ).

unprefixed_relative_path_to_string_test() ->
    ?assertEqual(
        "foo/bar/baz",
        khepri_path:to_string([?THIS_NODE, foo, bar, baz])
    ).

relative_path_prefixed_with_dot_dot_to_string_test() ->
    ?assertEqual(
        "../foo/bar/baz",
        khepri_path:to_string([?PARENT_NODE, foo, bar, baz])
    ).

path_with_binary_to_string_test() ->
    ?assertEqual(
        "/<<binary>>",
        khepri_path:to_string([<<"binary">>])
    ),
    ?assertEqual(
        "/<<bin/ary>>",
        khepri_path:to_string([<<"bin/ary">>])
    ),
    ?assertEqual(
        "/<<bin/ary>>/atom",
        khepri_path:to_string([<<"bin/ary">>, atom])
    ).

%% -------------------------------------------------------------------
%% Combine path with conditions.
%% -------------------------------------------------------------------

combine_path_with_no_conditions_test() ->
    ?assertEqual(
        [foo, bar],
        khepri_path:combine_with_conditions([foo, bar], [])
    ).

combine_path_with_conditions_test() ->
    ?assertEqual(
        [
            foo,
            #if_all{
                conditions = [
                    bar,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        ],
        khepri_path:combine_with_conditions(
            [foo, bar],
            [
                #if_name_matches{regex = "a"},
                #if_child_list_version{version = 3}
            ]
        )
    ).

%% -------------------------------------------------------------------
%% Does the path target a specific tree node?
%% -------------------------------------------------------------------

simple_component_targets_specific_node_test() ->
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(foo)
    ),
    ?assertEqual(
        {true, <<"foo">>},
        khepri_path:component_targets_specific_node(<<"foo">>)
    ),
    ?assertEqual(
        {true, ?ROOT_NODE},
        khepri_path:component_targets_specific_node(?ROOT_NODE)
    ),
    ?assertEqual(
        {true, ?THIS_NODE},
        khepri_path:component_targets_specific_node(?THIS_NODE)
    ),
    ?assertEqual(
        {true, ?PARENT_NODE},
        khepri_path:component_targets_specific_node(?PARENT_NODE)
    ),
    ?assertNot(khepri_path:component_targets_specific_node(?STAR)),
    ?assertNot(khepri_path:component_targets_specific_node(?STAR_STAR)).

pattern_component_targets_specific_node_test() ->
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_name_matches{regex = any}
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_name_matches{regex = "a"}
        )
    ),
    %% The regex matches a specific name but it could match an atom and a
    %% binary. Anyway, the function doesn't parse the regex.
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_name_matches{regex = "^a$"}
        )
    ),

    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_path_matches{regex = any}
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_path_matches{regex = "a"}
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_path_matches{regex = "^a$"}
        )
    ),

    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_has_data{has_data = true}
        )
    ).

if_not_condition_targets_specific_node_test() ->
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_not{condition = foo}
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_not{condition = ?STAR}
        )
    ).

if_all_condition_targets_specific_node_test() ->
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_all{conditions = []}
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{conditions = [foo]}
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    foo,
                    foo
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    foo,
                    bar
                ]
            }
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    foo,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertEqual(
        {true, ?ROOT_NODE},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    ?ROOT_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertEqual(
        {true, ?THIS_NODE},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    ?THIS_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertEqual(
        {true, ?PARENT_NODE},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    ?PARENT_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    #if_name_matches{regex = "a"},
                    foo,
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3},
                    foo
                ]
            }
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    foo,
                    #if_name_matches{regex = "a"},
                    foo,
                    #if_child_list_version{version = 3},
                    foo
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ).

if_any_condition_targets_specific_node_test() ->
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{conditions = []}
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_any{conditions = [foo]}
        )
    ),
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    foo,
                    foo
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    foo,
                    bar
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    foo,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    ?ROOT_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    ?THIS_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    ?PARENT_NODE,
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    #if_name_matches{regex = "a"},
                    foo,
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3},
                    foo
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    foo,
                    #if_name_matches{regex = "a"},
                    foo,
                    #if_child_list_version{version = 3},
                    foo
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3}
                ]
            }
        )
    ).

complex_condition_targets_specific_node_test() ->
    ?assertEqual(
        {true, foo},
        khepri_path:component_targets_specific_node(
            #if_all{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3},
                    #if_any{conditions = [foo]}
                ]
            }
        )
    ),
    ?assertNot(
        khepri_path:component_targets_specific_node(
            #if_any{
                conditions = [
                    #if_name_matches{regex = "a"},
                    #if_child_list_version{version = 3},
                    #if_all{conditions = [foo]}
                ]
            }
        )
    ).

path_targets_specific_node_test() ->
    ?assertEqual(
        {true, [foo, bar]},
        khepri_path:targets_specific_node([foo, bar])
    ),
    ?assertEqual(
        {true, [foo, <<"bar">>]},
        khepri_path:targets_specific_node([foo, <<"bar">>])
    ),
    ?assertEqual(
        {true, [?THIS_NODE, foo, bar]},
        khepri_path:targets_specific_node([?THIS_NODE, foo, bar])
    ),
    ?assertEqual(
        {true, [?PARENT_NODE, foo, bar]},
        khepri_path:targets_specific_node([?PARENT_NODE, foo, bar])
    ).

path_pattern_targets_specific_node_test() ->
    ?assertNot(
        khepri_path:targets_specific_node(
            [
                foo,
                #if_name_matches{regex = "a"}
            ]
        )
    ),
    ?assertEqual(
        {true, [foo, bar]},
        khepri_path:targets_specific_node(
            [
                foo,
                #if_all{
                    conditions = [
                        #if_name_matches{regex = "a"},
                        #if_child_list_version{version = 3},
                        #if_any{conditions = [bar]}
                    ]
                }
            ]
        )
    ),
    ?assertNot(
        khepri_path:targets_specific_node(
            [
                foo,
                #if_name_matches{regex = "a"},
                baz
            ]
        )
    ),
    ?assertEqual(
        {true, [foo, bar, baz]},
        khepri_path:targets_specific_node(
            [
                foo,
                #if_all{
                    conditions = [
                        #if_name_matches{regex = "a"},
                        #if_child_list_version{version = 3},
                        #if_any{conditions = [bar]}
                    ]
                },
                baz
            ]
        )
    ).

%% -------------------------------------------------------------------
%% Path cleanup.
%% -------------------------------------------------------------------

abspath_test() ->
    ?assertEqual([], khepri_path:abspath([], [])),
    ?assertEqual([foo, bar], khepri_path:abspath([foo, bar], [])),
    ?assertEqual([foo, bar], khepri_path:abspath([foo, bar], [parent, node])),
    ?assertEqual(
        [parent, node, foo, bar],
        khepri_path:abspath([?THIS_NODE, foo, bar], [parent, node])
    ).

realpath_test() ->
    ?assertEqual([], khepri_path:realpath([])),
    ?assertEqual([foo], khepri_path:realpath([foo])),
    ?assertEqual([?STAR], khepri_path:realpath([?STAR])),

    ?assertEqual([], khepri_path:realpath([?THIS_NODE])),
    ?assertEqual([], khepri_path:realpath([?THIS_NODE, ?THIS_NODE])),
    ?assertEqual([foo], khepri_path:realpath([?THIS_NODE, foo])),
    ?assertEqual([foo], khepri_path:realpath([foo, ?THIS_NODE])),
    ?assertEqual([foo], khepri_path:realpath([foo, ?THIS_NODE, ?THIS_NODE])),
    ?assertEqual(
        [foo, bar],
        khepri_path:realpath([foo, ?THIS_NODE, ?THIS_NODE, bar])
    ),

    ?assertEqual([], khepri_path:realpath([?PARENT_NODE])),
    ?assertEqual([], khepri_path:realpath([foo, ?PARENT_NODE])),
    ?assertEqual([foo], khepri_path:realpath([?PARENT_NODE, foo])),
    ?assertEqual([foo], khepri_path:realpath([?PARENT_NODE, foo])),
    ?assertEqual(
        [foo, baz],
        khepri_path:realpath([foo, bar, ?PARENT_NODE, baz])
    ),
    ?assertEqual(
        [qux],
        khepri_path:realpath([
            foo,
            bar,
            ?PARENT_NODE,
            ?PARENT_NODE,
            baz,
            ?PARENT_NODE,
            qux
        ])
    ).

%% -------------------------------------------------------------------
%% Path compilation.
%% -------------------------------------------------------------------

path_compilation_test() ->
    ?assertEqual([foo, bar], khepri_path:compile([foo, bar])),
    ?assertEqual([foo, ?STAR], khepri_path:compile([foo, ?STAR])).
