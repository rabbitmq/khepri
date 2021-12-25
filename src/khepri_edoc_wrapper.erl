%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc
%% Internal module to override the behavior of EDoc.
%%
%% This module acts as both `layout' and `doclet' callback modules for EDoc.
%% It is responsible for patching the generated HTML to:
%%   1. add a class attribute to be able to use the GitHub Markdown stylesheet
%%   2. include Prism.js to enable syntax highlighting
%%
%% Except for modules documentation, EDoc doesn't allow to set an `xml_export'
%% callback modules to other parts of the documentation. Therefore:
%%   * The `overview-summary.html' file is patched before it is written to
%%     disk.
%%   * The `modules-frame.html' file is patched after it is written to disk.
%%
%% This is not used by Khepri outside of its documentation generation.
%%
%% @see khepri_edoc_wrapper
%%
%% @private

-module(khepri_edoc_wrapper).

-include_lib("edoc/include/edoc_doclet.hrl").

-include("src/edoc.hrl").

-export([module/2, overview/2, type/1]).
-export([run/2]).

module(Element, Options) ->
    edoc_layout:module(Element, Options).

overview(Element, Options) ->
    Overview = edoc_layout:overview(Element, Options),
    patch_html(Overview).

type(Element) ->
    edoc_layout:type(Element).

run(Cmd, Ctxt) ->
    ok = edoc_doclet:run(Cmd, Ctxt),
    %% Ctxt is a #context{} record in Erlang 23 and #doclet_context{} in Erlang
    %% 24. The directory is the second field in that record in both cases.
    Dir = element(2, Ctxt),
    File = filename:join(Dir, "modules-frame.html"),
    {ok, Content0} = file:read_file(File),
    Content1 = add_toc(Content0, Dir),
    Content2 = patch_html(Content1),
    case file:write_file(File, Content2) of
        ok -> ok;
        {error, Reason} -> exit({error, Reason})
    end.

patch_html(Html) ->
    Html1 = re:replace(
        Html,
        "</head>",
        ?SYNTAX_HIGHLIGHTING_CSS "\n" ?ADDITIONAL_STYLE "\n" "</head>",
        [{return, list}]
    ),
    Html2 = re:replace(
        Html1,
        "<body +bgcolor=\"[^\"]*\">",
        "<body class=\"" ?BODY_CLASSES "\">\n" ?SYNTAX_HIGHLIGHTING_JS,
        [{return, list}]
    ),
    Html3 = re:replace(
        Html2,
        "<pre>(.*)</pre>",
        "<pre><code>\\1</code></pre>",
        [{return, list}, ungreedy, dotall, global]
    ),
    Html4 = re:replace(
        Html3,
        "<pre><code> +(" ?LANG_REGEX ")(?:\n)(.*)</code></pre>",
        "<pre><code class=\"language-\\1\">\\2</code></pre>",
        [{return, list}, ungreedy, dotall, global]
    ),
    %% <tt>...</tt> is used for authors' email address, we don't want to
    %% convert them to <code>...</code>.
    %Html5 = re:replace(
    %              Html4,
    %              "<tt>(.*)</tt>",
    %              "<code>\\1</code>",
    %              [{return, list}, ungreedy, dotall, global]),
    Html4.

add_toc(Html, Dir) ->
    Toc = generate_toc(Dir),
    re:replace(
        Html,
        "(<h2 class=\"indextitle\">Modules</h2>)",
        Toc ++ "\\1",
        [{return, list}]
    ).

generate_toc(Dir) ->
    OverviewFile = filename:join(Dir, "overview.edoc"),
    {ok, Overview} = file:read_file(OverviewFile),
    Lines = re:split(Overview, "\\n", [{return, list}]),
    Titles = [
        Line
     || Line <- Lines,
        match =:= re:run(Line, "^=+.*=+$", [{capture, none}])
    ],
    generate_toc1(Titles, 0, []).

generate_toc1([Title | Rest], CurrentLevel, Result) ->
    ReOpts = [{capture, all_but_first, list}],
    {match, [Equals, Title1]} = re:run(Title, "^(=+) *(.*)", ReOpts),
    Title2 = re:replace(Title1, " *=+$", "", [{return, list}]),
    Anchor = "#" ++ re:replace(Title2, " ", "_", [{return, list}, global]),
    Link =
        "<a href=\"overview-summary.html" ++ Anchor ++ "\"" ++
            "target=\"overviewFrame\">" ++ Title2 ++ "</a>",
    Level = length(Equals) - 1,
    if
        Level =:= CurrentLevel ->
            Result1 = Result ++ ["</li>\n", "<li>", Link],
            generate_toc1(Rest, CurrentLevel, Result1);
        CurrentLevel =:= 0 andalso
            Level =:= CurrentLevel + 1 ->
            OverviewLink =
                "<a href=\"overview-summary.html\"" ++
                    "target=\"overviewFrame\">Overview</a>",
            Result1 =
                Result ++
                    [
                        "<ul>\n",
                        "<li>",
                        OverviewLink,
                        "</li>\n",
                        "<li>",
                        Link
                    ],
            generate_toc1(Rest, Level, Result1);
        Level =:= CurrentLevel + 1 ->
            Result1 = Result ++ ["\n", "<ul>\n", "<li>", Link],
            generate_toc1(Rest, Level, Result1);
        Level < CurrentLevel ->
            Result1 =
                Result ++
                    [
                        "</li>\n"
                        "</ul>\n"
                     || _ <- lists:seq(Level, CurrentLevel - 1)
                    ],
            Result2 = Result1 ++ ["</li>\n", "<li>", Link],
            generate_toc1(Rest, Level, Result2)
    end;
generate_toc1([], CurrentLevel, Result) ->
    [
        "<h2 class=\"indextitle\">Khepri</h2>\n",
        Result,
        "</li>\n",
        ["</ul>\n" || _ <- lists:seq(0, CurrentLevel - 1)]
    ].
