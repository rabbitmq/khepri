%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-define(BODY_CLASSES, "markdown-body language-erlang").

-define(
   ADDITIONAL_STYLE,
   "<style>
    body {
      box-sizing: border-box;
      min-width: 200px;
      max-width: 980px;
      margin: 0 auto;
      padding: 45px;
    }

    @media (max-width: 767px) {
      body {
        padding: 15px;
      }
    }

    /* Don't apply the table style to the top-level navigation bar. */
    .navbar table {
      display: table;
      width: 100%;
    }
    .navbar table tr,
    .navbar table th,
    .navbar table td {
      border: 0;
    }

    /* Keep the same font side inside code blocks than everywhere else. */
    .markdown-body pre code,
    code[class*=\"language-\"] {
      font-size: 85%;
    }

    /* Force the color for link on code blocks used for @see tags.  */
    .markdown-body a code,
    .markdown-body a code span.token {
      color: var(--color-accent-fg);
    }

    /* Copy the style of code blocks. */
    .markdown-body .spec {
      background: #f5f2f0;
      padding: 1em;
      margin: .5em 0;
      overflow: auto;
      border-radius: 6px;
    }

    /* Improve margins inside function spec blocks so that:
        - empty paragraphs don't add useless margins
        - the final top and bottom margins are equal */
    .markdown-body .spec p,
    .markdown-body .spec ul {
      margin-top: 16px;
      margin-bottom: 0;
    }
    .markdown-body .spec p:first-child,
    .markdown-body .spec p:empty {
      margin-top: 0;
    }

    /* Put the function prototype in bold characters. */
    .markdown-body .spec > p > code:first-child {
      font-weight: bold;
    }

    /* Add some margin below the module short description between the table
       of contents in the Description section. This text isn't in a
       <p></p>. */
    .index + p {
      margin-top: 16px;
    }
    </style>").

-define(
   SYNTAX_HIGHLIGHTING_CSS,
   "<link href=\"prism.css\" rel=\"stylesheet\" />").
-define(
   SYNTAX_HIGHLIGHTING_JS,
   "<script src=\"prism.js\"></script>").

-define(LANG_REGEX, "none|elixir|erlang").
