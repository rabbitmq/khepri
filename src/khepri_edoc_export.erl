%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Internal module to override the behavior of EDoc.
%%
%% This module acts as an `xml_export' callback module for EDoc. It is
%% responsible for patching the generated HTML to:
%%   1. add a class attribute to be able to use the GitHub Markdown stylesheet
%%   2. include Prism.js to enable syntax highlighting
%%
%% This is not used by Khepri outside of its documentation generation.
%%
%% @see khepri_edoc_wrapper
%%
%% @private

-module(khepri_edoc_export).

-include_lib("xmerl/include/xmerl.hrl").

-include("src/edoc.hrl").

-export(['#xml-inheritance#'/0]).

-export(['#root#'/4,
	 '#element#'/5,
	 '#text#'/1]).

'#xml-inheritance#'() -> [].

'#text#'(Text) ->
    xmerl_html:'#text#'(Text).

'#root#'(Data, Attrs, [], E) ->
    xmerl_html:'#root#'(Data, Attrs, [], E).

'#element#'(head = Tag, Data, Attrs, Parents, E) ->
    Data1 = [Data,
             ?SYNTAX_HIGHLIGHTING_CSS,
             ?ADDITIONAL_STYLE],
    xmerl_html:'#element#'(Tag, Data1, Attrs, Parents, E);
'#element#'(body = Tag, Data, _Attrs, Parents, E) ->
    Data1 = [?SYNTAX_HIGHLIGHTING_JS,
             Data],
    Attrs = [#xmlAttribute{name = class,
                           value = ?BODY_CLASSES}],
    xmerl_html:'#element#'(Tag, Data1, Attrs, Parents, E);
'#element#'(pre = Tag, Data, Attrs, Parents, E) ->
    Data1 = re:replace(Data, "^  ", "", [global, multiline]),
    Data2 = ["<code>",
             Data1,
             "</code>"],
    xmerl_html:'#element#'(Tag, Data2, Attrs, Parents, E);
'#element#'(tt, Data, Attrs, Parents, E) ->
    xmerl_html:'#element#'(code, Data, Attrs, Parents, E);
'#element#'(Tag, Data, Attrs, Parents, E) ->
    xmerl_html:'#element#'(Tag, Data, Attrs, Parents, E).
