%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(display_tree).

-include_lib("eunit/include/eunit.hrl").

-include("include/khepri.hrl").
-include("src/khepri_machine.hrl").
-include("test/helpers.hrl").

%% khepri:get_root/1 is unexported when compiled without `-DTEST'.
-dialyzer(no_missing_calls).

complex_flat_struct_to_tree_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(pouet_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload,
                                               payload_version,
                                               child_list_version,
                                               child_list_length],
                           include_root_props => true}),

    ?assertEqual(
       #{payload_version => 1,
         child_list_version => 3,
         child_list_length => 2,
         child_nodes =>

         #{foo =>
           #{payload_version => 1,
             child_list_version => 2,
             child_list_length => 2,
             child_nodes =>

             #{bar =>
               #{payload_version => 1,
                 child_list_version => 1,
                 child_list_length => 1,
                 child_nodes =>

                 #{baz =>
                   #{payload_version => 1,
                     child_list_version => 1,
                     child_list_length => 1,
                     child_nodes =>

                     #{qux =>
                       #{payload_version => 1,
                         child_list_version => 1,
                         child_list_length => 0,
                         data => qux_value}}}}},

               youpi =>
               #{data => youpi_value,
                 payload_version => 1,
                 child_list_version => 1,
                 child_list_length => 0}}},

           baz =>
           #{data => baz_value,
             payload_version => 1,
             child_list_version => 2,
             child_list_length => 1,
             child_nodes =>

             #{pouet =>
               #{data => pouet_value,
                 payload_version => 1,
                 child_list_version => 1,
                 child_list_length => 0}}}}},
       khepri_utils:flat_struct_to_tree(FlatStruct)).

unordered_flat_struct_to_tree_test() ->
    ?assertEqual(
       #{child_nodes =>
         #{foo =>
           #{child_nodes =>
             #{bar => #{data => bar_data}}}}},
       khepri_utils:flat_struct_to_tree(
         #{[foo, bar] => #{data => bar_data}})).

flat_struct_with_children_before_parents_test() ->
    %% The following map happens to trigger a situation where the key of a
    %% child node appears before the key of its parent in maps:fold/3, at least
    %% with Erlang 24.1.
    %%
    %% In other words, the goal of this testcase is to handle this:
    %% 1. `[foo, bar]' is added to the tree structure.
    %% 2. `[foo]' is added, even though it is already present in the tree.
    %%
    %% It used to hit an assertion because the code didn't handle this case.
    %% Now it should work and merge `[foo]' node props with its child nodes.
    FlatStruct = #{[rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"/">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v7">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v6">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v10">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost] =>
                   #{child_list_length => 14, child_list_version => 14,
                     payload_version => 1},
                   [rabbit_vhost, <<"v5">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v3">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v9">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v12">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v12">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v2">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v1">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v1">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v8">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"/">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal] =>
                   #{child_list_length => 1, child_list_version => 1,
                     payload_version => 1},
                   [rabbit_vhost, <<"v4">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v9">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>] =>
                   #{child_list_length => 1, child_list_version => 2,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v11">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v8">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v13">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_vhost, <<"v11">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions] =>
                   #{child_list_length => 14, child_list_version => 14,
                     payload_version => 1},
                   [rabbit_auth_backend_internal, users] =>
                   #{child_list_length => 1, child_list_version => 1,
                     payload_version => 1},
                   [rabbit_vhost, <<"v2">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v5">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v7">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v13">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v6">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v4">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v10">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1},
                   [rabbit_auth_backend_internal, users, <<"guest">>,
                    user_permissions, <<"v3">>] =>
                   #{child_list_length => 0, child_list_version => 1,
                     data => true, payload_version => 1}},
    Tree = khepri_utils:flat_struct_to_tree(FlatStruct),
    ?assertEqual(
       #{child_nodes =>
         #{rabbit_auth_backend_internal =>
           #{child_list_length => 1, child_list_version => 1,
             payload_version => 1,
             child_nodes =>
             #{users =>
               #{child_list_length => 1, child_list_version => 1,
                 payload_version => 1,
                 child_nodes =>
                 #{<<"guest">> =>
                   #{child_list_length => 1,
                     child_list_version => 2,
                     data => true,
                     payload_version => 1,
                     child_nodes =>
                     #{user_permissions =>
                       #{child_list_length => 14,
                         child_list_version => 14,
                         payload_version => 1,
                         child_nodes =>
                         #{<<"/">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v1">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v10">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v11">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v12">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v13">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v2">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v3">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v4">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v5">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v6">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v7">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v8">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1},
                           <<"v9">> =>
                           #{child_list_length => 0,
                             child_list_version => 1,
                             data => true,
                             payload_version => 1}}}}}}}}},
           rabbit_vhost =>
           #{child_list_length => 14, child_list_version => 14,
             payload_version => 1,
             child_nodes =>
             #{<<"/">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v1">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v10">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v11">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v12">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v13">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v2">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v3">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v4">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v5">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v6">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v7">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v8">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1},
               <<"v9">> =>
               #{child_list_length => 0, child_list_version => 1,
                 data => true, payload_version => 1}}}}}, Tree).

machine_overview_test() ->
    %% `khepri_machine' uses `khepri_utils:flat_struct_to_map/2' to format
    %% the `gen_statem' status in the `ra_machine:overview/1' callback
    %% implementation.
    Fun = fun() -> return_value end,
    helpers:init_list_of_modules_to_skip(),
    StandaloneFun = khepri_tx_adv:to_standalone_fun(Fun, rw),
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [foo, bar],
                     payload = khepri_payload:data(bar_value)},
                #put{path = [baz, fizz],
                     payload = khepri_payload:sproc(Fun)},
                #put{path = [baz, buzz],
                     payload = khepri_payload:sproc(StandaloneFun)}],
    #{store_id := StoreId} = Params = ?MACH_PARAMS(Commands),
    S0 = khepri_machine:init(Params),
    O0 = khepri_machine:overview(S0),

    ?assertEqual(StoreId, maps:get(store_id, O0)),
    ?assertEqual(#{}, maps:get(triggers, O0)),
    ?assertEqual(
      #{[baz] => #{[baz] =>
                   #if_any{conditions =
                           [#if_child_list_length{count = {gt,0}},
                           #if_has_payload{has_payload = true}]}}},
      maps:get(keep_while_conds, O0)),

    #{tree :=
      #{child_list_length := 2,
        child_list_version := 3,
        payload_version := 1,
        child_nodes :=
        #{baz :=
          #{child_list_length := 2,
            child_list_version := 2,
            payload_version := 1,
            child_nodes := #{buzz := #{child_list_length := 0,
                                       child_list_version := 1,
                                       payload_version := 1,
                                       sproc := CapturedStandaloneFun},
                             fizz := #{child_list_length := 0,
                                       child_list_version := 1,
                                       payload_version := 1,
                                       sproc := Fun}}},
          foo :=
          #{child_list_length := 1,
            child_list_version := 2,
            payload_version := 1,
            data := foo_value,
            child_nodes := #{bar := #{child_list_length := 0,
                                      child_list_version := 1,
                                      data := bar_value,
                                      payload_version := 1}}}}}} = O0,

      ?assert(is_function(CapturedStandaloneFun, 0)),
      ?assertEqual(return_value, CapturedStandaloneFun()).


display_simple_tree_test() ->
    Commands = [#put{path = [foo],
                     payload = khepri_payload:data(foo_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree)),
    ?assertEqual(
       "╰── foo\n"
       "      \033[38;5;246mData: foo_value\033[0m\n"
       "\n",
       ?capturedOutput),
    ok.

display_large_tree_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(
                                 [lorem, ipsum, dolor, sit, amet,
                                  consectetur, adipiscing, elit, sed, do,
                                  eiusmod, tempor, incididunt, ut, labore,
                                  et, dolore, magna, aliqua, ut, enim, ad,
                                  minim, veniam, quis, nostrud, exercitation,
                                  ullamco, laboris, nisi, ut, aliquip, ex,
                                  ea, commodo, consequat, duis, aute, irure,
                                  dolor, in, reprehenderit, in, voluptate,
                                  velit, esse, cillum, dolore, eu, fugiat,
                                  nulla, pariatur, excepteur, sint, occaecat,
                                  cupidatat, non, proident, sunt, in, culpa,
                                  qui, officia, deserunt, mollit, anim, id,
                                  est, laborum])}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree)),
    ?assertEqual(
       "├── baz\n"
       "│   │ \033[38;5;246mData: baz_value\033[0m\n"
       "│   │\n"
       "│   ╰── pouet\n"
       "│         \033[38;5;246mData: [lorem,ipsum,dolor,sit,amet,consectetur,adipiscing,elit,sed,do,eiusmod,\033[0m\n"
       "│         \033[38;5;246m       tempor,incididunt,ut,labore,et,dolore,magna,aliqua,ut,enim,ad,minim,\033[0m\n"
       "│         \033[38;5;246m       veniam,quis,nostrud,exercitation,ullamco,laboris,nisi,ut,aliquip,ex,ea,\033[0m\n"
       "│         \033[38;5;246m       commodo,consequat,duis,aute,irure,dolor,in,reprehenderit,in,voluptate,\033[0m\n"
       "│         \033[38;5;246m       velit,esse,cillum,dolore,eu,fugiat,nulla,pariatur,excepteur,sint,\033[0m\n"
       "│         \033[38;5;246m       occaecat,cupidatat,non,proident,sunt,in,culpa,qui,officia,deserunt,\033[0m\n"
       "│         \033[38;5;246m       mollit,anim,id,est,laborum]\033[0m\n"
       "│\n"
       "╰── foo\n"
       "    ├── bar\n"
       "    │   ╰── baz\n"
       "    │       ╰── qux\n"
       "    │             \033[38;5;246mData: qux_value\033[0m\n"
       "    │\n"
       "    ╰── youpi\n"
       "          \033[38;5;246mData: youpi_value\033[0m\n"
       "\n",
       ?capturedOutput),
    ok.

display_tree_with_plaintext_lines_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(
                                 [lorem, ipsum, dolor, sit, amet,
                                  consectetur, adipiscing, elit, sed, do,
                                  eiusmod, tempor, incididunt, ut, labore,
                                  et, dolore, magna, aliqua, ut, enim, ad,
                                  minim, veniam, quis, nostrud, exercitation,
                                  ullamco, laboris, nisi, ut, aliquip, ex,
                                  ea, commodo, consequat, duis, aute, irure,
                                  dolor, in, reprehenderit, in, voluptate,
                                  velit, esse, cillum, dolore, eu, fugiat,
                                  nulla, pariatur, excepteur, sint, occaecat,
                                  cupidatat, non, proident, sunt, in, culpa,
                                  qui, officia, deserunt, mollit, anim, id,
                                  est, laborum])}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree, #{lines => false})),
    ?assertEqual(
       "+-- baz\n"
       "|   | \033[38;5;246mData: baz_value\033[0m\n"
       "|   |\n"
       "|   `-- pouet\n"
       "|         \033[38;5;246mData: [lorem,ipsum,dolor,sit,amet,consectetur,adipiscing,elit,sed,do,eiusmod,\033[0m\n"
       "|         \033[38;5;246m       tempor,incididunt,ut,labore,et,dolore,magna,aliqua,ut,enim,ad,minim,\033[0m\n"
       "|         \033[38;5;246m       veniam,quis,nostrud,exercitation,ullamco,laboris,nisi,ut,aliquip,ex,ea,\033[0m\n"
       "|         \033[38;5;246m       commodo,consequat,duis,aute,irure,dolor,in,reprehenderit,in,voluptate,\033[0m\n"
       "|         \033[38;5;246m       velit,esse,cillum,dolore,eu,fugiat,nulla,pariatur,excepteur,sint,\033[0m\n"
       "|         \033[38;5;246m       occaecat,cupidatat,non,proident,sunt,in,culpa,qui,officia,deserunt,\033[0m\n"
       "|         \033[38;5;246m       mollit,anim,id,est,laborum]\033[0m\n"
       "|\n"
       "`-- foo\n"
       "    +-- bar\n"
       "    |   `-- baz\n"
       "    |       `-- qux\n"
       "    |             \033[38;5;246mData: qux_value\033[0m\n"
       "    |\n"
       "    `-- youpi\n"
       "          \033[38;5;246mData: youpi_value\033[0m\n"
       "\n",
       ?capturedOutput),
    ok.

display_tree_without_colors_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(
                                 [lorem, ipsum, dolor, sit, amet,
                                  consectetur, adipiscing, elit, sed, do,
                                  eiusmod, tempor, incididunt, ut, labore,
                                  et, dolore, magna, aliqua, ut, enim, ad,
                                  minim, veniam, quis, nostrud, exercitation,
                                  ullamco, laboris, nisi, ut, aliquip, ex,
                                  ea, commodo, consequat, duis, aute, irure,
                                  dolor, in, reprehenderit, in, voluptate,
                                  velit, esse, cillum, dolore, eu, fugiat,
                                  nulla, pariatur, excepteur, sint, occaecat,
                                  cupidatat, non, proident, sunt, in, culpa,
                                  qui, officia, deserunt, mollit, anim, id,
                                  est, laborum])}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree, #{colors => false})),
    ?assertEqual(
       "├── baz\n"
       "│   │ Data: baz_value\n"
       "│   │\n"
       "│   ╰── pouet\n"
       "│         Data: [lorem,ipsum,dolor,sit,amet,consectetur,adipiscing,elit,sed,do,eiusmod,\n"
       "│                tempor,incididunt,ut,labore,et,dolore,magna,aliqua,ut,enim,ad,minim,\n"
       "│                veniam,quis,nostrud,exercitation,ullamco,laboris,nisi,ut,aliquip,ex,ea,\n"
       "│                commodo,consequat,duis,aute,irure,dolor,in,reprehenderit,in,voluptate,\n"
       "│                velit,esse,cillum,dolore,eu,fugiat,nulla,pariatur,excepteur,sint,\n"
       "│                occaecat,cupidatat,non,proident,sunt,in,culpa,qui,officia,deserunt,\n"
       "│                mollit,anim,id,est,laborum]\n"
       "│\n"
       "╰── foo\n"
       "    ├── bar\n"
       "    │   ╰── baz\n"
       "    │       ╰── qux\n"
       "    │             Data: qux_value\n"
       "    │\n"
       "    ╰── youpi\n"
       "          Data: youpi_value\n"
       "\n",
       ?capturedOutput),
    ok.

display_tree_with_plaintext_lines_and_without_colors_test() ->
    Commands = [#put{path = [foo, bar, baz, qux],
                     payload = khepri_payload:data(qux_value)},
                #put{path = [foo, youpi],
                     payload = khepri_payload:data(youpi_value)},
                #put{path = [baz],
                     payload = khepri_payload:data(baz_value)},
                #put{path = [baz, pouet],
                     payload = khepri_payload:data(
                                 [lorem, ipsum, dolor, sit, amet,
                                  consectetur, adipiscing, elit, sed, do,
                                  eiusmod, tempor, incididunt, ut, labore,
                                  et, dolore, magna, aliqua, ut, enim, ad,
                                  minim, veniam, quis, nostrud, exercitation,
                                  ullamco, laboris, nisi, ut, aliquip, ex,
                                  ea, commodo, consequat, duis, aute, irure,
                                  dolor, in, reprehenderit, in, voluptate,
                                  velit, esse, cillum, dolore, eu, fugiat,
                                  nulla, pariatur, excepteur, sint, occaecat,
                                  cupidatat, non, proident, sunt, in, culpa,
                                  qui, officia, deserunt, mollit, anim, id,
                                  est, laborum])}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree, #{lines => false,
                                                           colors => false})),
    ?assertEqual(
       "+-- baz\n"
       "|   | Data: baz_value\n"
       "|   |\n"
       "|   `-- pouet\n"
       "|         Data: [lorem,ipsum,dolor,sit,amet,consectetur,adipiscing,elit,sed,do,eiusmod,\n"
       "|                tempor,incididunt,ut,labore,et,dolore,magna,aliqua,ut,enim,ad,minim,\n"
       "|                veniam,quis,nostrud,exercitation,ullamco,laboris,nisi,ut,aliquip,ex,ea,\n"
       "|                commodo,consequat,duis,aute,irure,dolor,in,reprehenderit,in,voluptate,\n"
       "|                velit,esse,cillum,dolore,eu,fugiat,nulla,pariatur,excepteur,sint,\n"
       "|                occaecat,cupidatat,non,proident,sunt,in,culpa,qui,officia,deserunt,\n"
       "|                mollit,anim,id,est,laborum]\n"
       "|\n"
       "`-- foo\n"
       "    +-- bar\n"
       "    |   `-- baz\n"
       "    |       `-- qux\n"
       "    |             Data: qux_value\n"
       "    |\n"
       "    `-- youpi\n"
       "          Data: youpi_value\n"
       "\n",
       ?capturedOutput),
    ok.

display_tree_with_binary_key_test() ->
    Commands = [#put{path = [<<"foo">>],
                     payload = khepri_payload:data(foo_value)},
                #put{path = [bar],
                     payload = khepri_payload:data(bar_value)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree)),
    ?assertEqual(
       "├── bar\n"
       "│     \033[38;5;246mData: bar_value\033[0m\n"
       "│\n"
       "╰── <<\"foo\">>\n"
       "      \033[38;5;246mData: foo_value\033[0m\n"
       "\n",
       ?capturedOutput),
    ok.

display_tree_with_similar_atom_and_binary_keys_test() ->
    Commands = [#put{path = [<<"foo">>],
                     payload = khepri_payload:data(foo_binary)},
                #put{path = [foo],
                     payload = khepri_payload:data(foo_atom)}],
    S0 = khepri_machine:init(?MACH_PARAMS(Commands)),
    Tree = khepri_machine:get_tree(S0),
    {ok, FlatStruct} = khepri_tree:find_matching_nodes(
                         Tree,
                         [#if_path_matches{regex = any}],
                         #{props_to_return => [payload, payload_version]}),
    NodeTree = khepri_utils:flat_struct_to_tree(FlatStruct),

    ?assertEqual(ok, khepri_utils:display_tree(NodeTree)),
    ?assertEqual(
       "├── foo\n"
       "│     \033[38;5;246mData: foo_atom\033[0m\n"
       "│\n"
       "╰── <<\"foo\">>\n"
       "      \033[38;5;246mData: foo_binary\033[0m\n"
       "\n",
       ?capturedOutput),
    ok.
