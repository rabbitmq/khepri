%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(prop_path_operations).

-include_lib("proper/include/proper.hrl").

-include("include/khepri.hrl").

-dialyzer([{no_improper_lists, [path/0]}]).

-export([prop_a_path_can_be_converted_back_and_forth_to_string/0]).

prop_a_path_can_be_converted_back_and_forth_to_string() ->
    ?FORALL(
        InputPath,
        path(),
        begin
            String = khepri_path:to_string(InputPath),
            OutputPath = khepri_path:from_string(String),
            ?WHENFAIL(
                io:format(
                    "InputPath = ~p -- ~w -- ~p~n"
                    "String = ~p~n"
                    "OutputPath = ~p -- ~w -- ~p~n",
                    [
                        InputPath,
                        InputPath,
                        khepri_path:realpath(InputPath),
                        String,
                        OutputPath,
                        OutputPath,
                        khepri_path:realpath(OutputPath)
                    ]
                ),
                khepri_path:realpath(InputPath) =:=
                    khepri_path:realpath(OutputPath)
            )
        end
    ).

path() ->
    elements([
        list(path_component()),
        [?ROOT_NODE | list(path_component())]
    ]).

path_component() ->
    elements([
        atom_component(),
        binary_component(),
        ?THIS_NODE,
        ?PARENT_NODE
    ]).

atom_component() ->
    %% We discard any atom-based component which can't be used in a string
    %% path or will be converted to a pattern.
    ?SUCHTHAT(
        Atom,
        ?LET(
            AtomString,
            non_empty(
                list(
                    elements([
                        integer(0, 41),
                        integer(43, 46),
                        integer(48, 255)
                    ])
                )
            ),
            list_to_atom(AtomString)
        ),
        Atom =/= '.' andalso Atom =/= '..'
    ).

binary_component() ->
    %% We discard any binary-based component which will be converted to a
    %% pattern.
    ?SUCHTHAT(
        Binary,
        binary(),
        string:chr(binary_to_list(Binary), $*) =:= 0
    ).
