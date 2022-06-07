%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(prop_path_operations).

-include_lib("proper/include/proper.hrl").

-include("include/khepri.hrl").

-dialyzer([{no_improper_lists,
            [path/0]}]).

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
                [InputPath, InputPath, khepri_path:realpath(InputPath),
                 String,
                 OutputPath, OutputPath, khepri_path:realpath(OutputPath)]),
              khepri_path:realpath(InputPath) =:=
              khepri_path:realpath(OutputPath))
       end).

path() ->
    elements([list(path_component()),
              [?ROOT_NODE | list(path_component())]]).

path_component() ->
    elements([atom_component(),
              binary_component(),
              ?THIS_NODE, ?PARENT_NODE]).

atom_component() ->
    atom().

binary_component() ->
    %% We discard any binary-based component which will be converted to a
    %% pattern.
    ?SUCHTHAT(Binary,
              non_empty(binary()),
              string:chr(binary_to_list(Binary), $*) =:= 0).
