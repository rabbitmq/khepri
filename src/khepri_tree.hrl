%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% Maximum number of siblings grouped into a single `if_any' pattern by
%% `paths_to_patterns/1'.
-define(MAX_SIBLINGS_PER_PATTERN, 1000).

-define(INTERNAL_LOOKUP_PROPS_TO_RETURN,
        [payload,
         payload_version,
         child_list_version,
         child_list_length]).
