%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(put_command).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/internal.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

initialize_machine_with_genesis_data_test() ->
    Commands = [
        #put{
            path = [foo, bar],
            payload = #kpayload_data{data = foobar_value}
        },
        #put{
            path = [baz],
            payload = #kpayload_data{data = baz_value}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),
    Root = khepri_machine:get_root(S0),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 3
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            payload = #kpayload_data{data = foobar_value}
                                        }
                                }
                        },
                    baz =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = baz_value}
                        }
                }
        },
        Root
    ).

insert_a_node_at_the_root_of_an_empty_db_test() ->
    S0 = khepri_machine:init(#{}),
    Command = #put{
        path = [foo],
        payload = #kpayload_data{data = value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = value}
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret).

insert_a_node_at_the_root_of_an_empty_db_with_conditions_test() ->
    S0 = khepri_machine:init(#{}),
    Command = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_any{
                            conditions =
                                [
                                    #if_node_exists{exists = false},
                                    #if_payload_version{version = 1}
                                ]
                        }
                    ]
            }
        ],
        payload = #kpayload_data{data = value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = value}
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo] => #{}}}, Ret).

overwrite_an_existing_node_data_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [foo],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 2,
                                child_list_version => 1
                            },
                            payload = #kpayload_data{data = value2}
                        }
                }
        },
        Root
    ),
    ?assertEqual(
        {ok, #{
            [foo] => #{
                data => value1,
                payload_version => 1,
                child_list_version => 1,
                child_list_length => 0
            }
        }},
        Ret
    ).

insert_a_node_with_path_containing_dot_and_dot_dot_test() ->
    S0 = khepri_machine:init(#{}),
    Command = #put{
        path = [foo, ?PARENT_NODE, foo, bar, ?THIS_NODE],
        payload = #kpayload_data{data = value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            payload = #kpayload_data{data = value}
                                        }
                                }
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo, bar] => #{}}}, Ret).

insert_a_node_under_an_nonexisting_parents_test() ->
    S0 = khepri_machine:init(#{}),
    Command = #put{
        path = [foo, bar, baz, qux],
        payload = #kpayload_data{data = value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            child_nodes =
                                                #{
                                                    baz =>
                                                        #node{
                                                            stat = ?INIT_NODE_STAT,
                                                            child_nodes =
                                                                #{
                                                                    qux =>
                                                                        #node{
                                                                            stat = ?INIT_NODE_STAT,
                                                                            payload = #kpayload_data{
                                                                                data = value
                                                                            }
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo, bar, baz, qux] => #{}}}, Ret).

insert_a_node_with_condition_true_on_self_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_data_matches{pattern = value1}
                    ]
            }
        ],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 2
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 2,
                                child_list_version => 1
                            },
                            payload = #kpayload_data{data = value2}
                        }
                }
        },
        Root
    ),
    ?assertEqual(
        {ok, #{
            [foo] => #{
                data => value1,
                payload_version => 1,
                child_list_version => 1,
                child_list_length => 0
            }
        }},
        Ret
    ).

insert_a_node_with_condition_false_on_self_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    %% We compile the condition beforehand because we need the compiled
    %% version to make an exact match on the returned error later.
    Compiled = khepri_condition:compile(#if_data_matches{pattern = value2}),
    Command = #put{
        path = [#if_all{conditions = [foo, Compiled]}],
        payload = #kpayload_data{data = value3}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {mismatching_node, #{
                node_name => foo,
                node_path => [foo],
                node_is_target => true,
                node_props => #{
                    data => value1,
                    payload_version => 1,
                    child_list_version => 1,
                    child_list_length => 0
                },
                condition => Compiled
            }}},
        Ret
    ).

insert_a_node_with_condition_true_on_self_using_dot_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [
            foo,
            #if_all{
                conditions =
                    [
                        ?THIS_NODE,
                        #if_data_matches{pattern = value1}
                    ]
            }
        ],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 2
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 2,
                                child_list_version => 1
                            },
                            payload = #kpayload_data{data = value2}
                        }
                }
        },
        Root
    ),
    ?assertEqual(
        {ok, #{
            [foo] => #{
                data => value1,
                payload_version => 1,
                child_list_version => 1,
                child_list_length => 0
            }
        }},
        Ret
    ).

insert_a_node_with_condition_false_on_self_using_dot_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    %% We compile the condition beforehand because we need the compiled
    %% version to make an exact match on the returned error later.
    Compiled = khepri_condition:compile(#if_data_matches{pattern = value2}),
    Command = #put{
        path = [
            foo,
            #if_all{conditions = [?THIS_NODE, Compiled]}
        ],
        payload = #kpayload_data{data = value3}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {mismatching_node, #{
                node_name => foo,
                node_path => [foo],
                node_is_target => true,
                node_props => #{
                    data => value1,
                    payload_version => 1,
                    child_list_version => 1,
                    child_list_length => 0
                },
                condition => Compiled
            }}},
        Ret
    ).

insert_a_node_with_condition_true_on_parent_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_data_matches{pattern = value1}
                    ]
            },
            bar
        ],
        payload = #kpayload_data{data = bar_value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 2
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 1,
                                child_list_version => 2
                            },
                            payload = #kpayload_data{data = value1},
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            payload = #kpayload_data{data = bar_value}
                                        }
                                }
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo, bar] => #{}}}, Ret).

insert_a_node_with_condition_false_on_parent_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    %% We compile the condition beforehand because we need the compiled
    %% version to make an exact match on the returned error later.
    Compiled = khepri_condition:compile(#if_data_matches{pattern = value2}),
    Command = #put{
        path = [
            #if_all{conditions = [foo, Compiled]},
            bar
        ],
        payload = #kpayload_data{data = bar_value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {mismatching_node, #{
                node_name => foo,
                node_path => [foo],
                node_is_target => false,
                node_props => #{
                    data => value1,
                    payload_version => 1,
                    child_list_version => 1,
                    child_list_length => 0
                },
                condition => Compiled
            }}},
        Ret
    ).

%% The #if_node_exists{} is tested explicitely in addition to the testcases
%% above because there is specific code to manage it when the node is not
%% found (the generic condition evaluation code takes a node to work).

insert_a_node_with_if_node_exists_true_on_self_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command1 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_node_exists{exists = true}
                    ]
            }
        ],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret1} = khepri_machine:apply(?META, Command1, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 2
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 2,
                                child_list_version => 1
                            },
                            payload = #kpayload_data{data = value2}
                        }
                }
        },
        Root
    ),
    ?assertEqual(
        {ok, #{
            [foo] => #{
                data => value1,
                payload_version => 1,
                child_list_version => 1,
                child_list_length => 0
            }
        }},
        Ret1
    ),

    Compiled = khepri_condition:compile(
        #if_all{
            conditions =
                [
                    baz,
                    #if_node_exists{exists = true}
                ]
        }
    ),
    Command2 = #put{
        path = [Compiled],
        payload = #kpayload_data{data = value2}
    },
    {S2, Ret2} = khepri_machine:apply(?META, Command2, S0),

    ?assertEqual(S0#khepri_machine.root, S2#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {node_not_found, #{
                node_name => baz,
                node_path => [baz],
                node_is_target => true,
                condition => Compiled
            }}},
        Ret2
    ).

insert_a_node_with_if_node_exists_false_on_self_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command1 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_node_exists{exists = false}
                    ]
            }
        ],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret1} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {mismatching_node, #{
                node_name => foo,
                node_path => [foo],
                node_is_target => true,
                node_props => #{
                    data => value1,
                    payload_version => 1,
                    child_list_version => 1,
                    child_list_length => 0
                },
                condition => #if_node_exists{exists = false}
            }}},
        Ret1
    ),

    Command2 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        baz,
                        #if_node_exists{exists = false}
                    ]
            }
        ],
        payload = #kpayload_data{data = value2}
    },
    {S2, Ret2} = khepri_machine:apply(?META, Command2, S0),
    Root = khepri_machine:get_root(S2),

    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 3
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = value1}
                        },
                    baz =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = value2}
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[baz] => #{}}}, Ret2).

insert_a_node_with_if_node_exists_true_on_parent_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command1 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_node_exists{exists = true}
                    ]
            },
            bar
        ],
        payload = #kpayload_data{data = bar_value}
    },
    {S1, Ret1} = khepri_machine:apply(?META, Command1, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 2
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 1,
                                child_list_version => 2
                            },
                            payload = #kpayload_data{data = value1},
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            payload = #kpayload_data{data = bar_value}
                                        }
                                }
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[foo, bar] => #{}}}, Ret1),

    Compiled = khepri_condition:compile(
        #if_all{
            conditions =
                [
                    baz,
                    #if_node_exists{exists = true}
                ]
        }
    ),
    Command2 = #put{
        path = [
            Compiled,
            bar
        ],
        payload = #kpayload_data{data = bar_value}
    },
    {S2, Ret2} = khepri_machine:apply(?META, Command2, S0),

    ?assertEqual(S0#khepri_machine.root, S2#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {node_not_found, #{
                node_name => baz,
                node_path => [baz],
                node_is_target => false,
                condition => Compiled
            }}},
        Ret2
    ).

insert_a_node_with_if_node_exists_false_on_parent_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value1}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command1 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        foo,
                        #if_node_exists{exists = false}
                    ]
            },
            bar
        ],
        payload = #kpayload_data{data = value2}
    },
    {S1, Ret1} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual(
        {error,
            {mismatching_node, #{
                node_name => foo,
                node_path => [foo],
                node_is_target => false,
                node_props => #{
                    data => value1,
                    payload_version => 1,
                    child_list_version => 1,
                    child_list_length => 0
                },
                condition => #if_node_exists{exists = false}
            }}},
        Ret1
    ),

    Command2 = #put{
        path = [
            #if_all{
                conditions =
                    [
                        baz,
                        #if_node_exists{exists = false}
                    ]
            },
            bar
        ],
        payload = #kpayload_data{data = bar_value}
    },
    {S2, Ret2} = khepri_machine:apply(?META, Command2, S0),
    Root = khepri_machine:get_root(S2),

    ?assertEqual(#{applied_command_count => 1}, S2#khepri_machine.metrics),
    ?assertEqual(
        #node{
            stat = #{
                payload_version => 1,
                child_list_version => 3
            },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            payload = #kpayload_data{data = value1}
                        },
                    baz =>
                        #node{
                            stat = ?INIT_NODE_STAT,
                            child_nodes =
                                #{
                                    bar =>
                                        #node{
                                            stat = ?INIT_NODE_STAT,
                                            payload = #kpayload_data{data = bar_value}
                                        }
                                }
                        }
                }
        },
        Root
    ),
    ?assertEqual({ok, #{[baz, bar] => #{}}}, Ret2).

insert_with_a_path_matching_many_nodes_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = foo_value}
        },
        #put{
            path = [bar],
            payload = #kpayload_data{data = bar_value}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [#if_name_matches{regex = any}],
        payload = #kpayload_data{data = new_value}
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),

    ?assertEqual(S0#khepri_machine.root, S1#khepri_machine.root),
    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),
    ?assertEqual({error, matches_many_nodes}, Ret).

clear_payload_in_an_existing_node_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value}
        }
    ],
    S0 = khepri_machine:init(#{commands => Commands}),

    Command = #put{
        path = [foo],
        payload = none
    },
    {S1, Ret} = khepri_machine:apply(?META, Command, S0),
    Root = khepri_machine:get_root(S1),

    ?assertEqual(
        #node{
            stat =
                #{
                    payload_version => 1,
                    child_list_version => 2
                },
            child_nodes =
                #{
                    foo =>
                        #node{
                            stat = #{
                                payload_version => 2,
                                child_list_version => 1
                            },
                            payload = none
                        }
                }
        },
        Root
    ),
    ?assertEqual(
        {ok, #{
            [foo] => #{
                data => value,
                payload_version => 1,
                child_list_version => 1,
                child_list_length => 0
            }
        }},
        Ret
    ).

put_command_bumps_applied_command_count_test() ->
    Commands = [
        #put{
            path = [foo],
            payload = #kpayload_data{data = value}
        }
    ],
    S0 = khepri_machine:init(#{
        snapshot_interval => 3,
        commands => Commands
    }),

    ?assertEqual(#{}, S0#khepri_machine.metrics),

    Command1 = #put{
        path = [bar],
        payload = none
    },
    {S1, _} = khepri_machine:apply(?META, Command1, S0),

    ?assertEqual(#{applied_command_count => 1}, S1#khepri_machine.metrics),

    Command2 = #put{
        path = [baz],
        payload = none
    },
    {S2, _} = khepri_machine:apply(?META, Command2, S1),

    ?assertEqual(#{applied_command_count => 2}, S2#khepri_machine.metrics),

    Command3 = #put{
        path = [qux],
        payload = none
    },
    Meta = ?META,
    {S3, _, Effects} = khepri_machine:apply(Meta, Command3, S2),

    ?assertEqual(#{}, S3#khepri_machine.metrics),
    ?assertEqual([{release_cursor, maps:get(index, Meta), S3}], Effects).
