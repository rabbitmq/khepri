%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% -------------------------------------------------------------------
%% "Bang functions", mostly an Elixir convention.
%% -------------------------------------------------------------------

-type unwrapped_minimal_ret() :: khepri:minimal_ret().

-type unwrapped_payload_ret(Default) :: khepri:data() |
                                        khepri_fun:standalone_fun() |
                                        Default.

-type unwrapped_payload_ret() :: unwrapped_payload_ret(undefined).

-type unwrapped_many_payloads_ret(Default) :: #{khepri_path:path() =>
                                                khepri:data() |
                                                khepri_fun:standalone_fun() |
                                                Default}.

-type unwrapped_many_payloads_ret() :: unwrapped_many_payloads_ret(undefined).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec 'get!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link get/1} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see get/1.

'get!'(PathPattern) ->
    Ret = get(PathPattern),
    unwrap_result(Ret).

-spec 'get!'
(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_payload_ret();
(PathPattern, Options) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link get/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see get/2.

'get!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = get(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'get!'(StoreId, PathPattern, Options) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link get/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see get/3.

'get!'(StoreId, PathPattern, Options) ->
    Ret = get(StoreId, PathPattern, Options),
    unwrap_result(Ret).

-spec 'put!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling {@link put/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/2.

'put!'(PathPattern, Data) ->
    Ret = put(PathPattern, Data),
    unwrap_result(Ret).

-spec 'put!'(StoreId, PathPattern, Data) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling {@link put/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/3.

'put!'(StoreId, PathPattern, Data) ->
    Ret = put(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'put!'(StoreId, PathPattern, Data, Extra | Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling {@link put/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/4.

'put!'(StoreId, PathPattern, Data, ExtraOrOptions) ->
    Ret = put(StoreId, PathPattern, Data, ExtraOrOptions),
    unwrap_result(Ret).

-spec 'put!'(StoreId, PathPattern, Data, Extra, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Creates or modifies a specific tree node in the tree structure.
%%
%% Calling this function is the same as calling {@link put/5} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/5.

'put!'(StoreId, PathPattern, Data, Extra, Options) ->
    Ret = put(StoreId, PathPattern, Data, Extra, Options),
    unwrap_result(Ret).

-spec 'create!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% Calling this function is the same as calling {@link create/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/2.

'create!'(PathPattern, Data) ->
    Ret = create(PathPattern, Data),
    unwrap_result(Ret).

-spec 'create!'(StoreId, PathPattern, Data) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% Calling this function is the same as calling {@link create/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/3.

'create!'(StoreId, PathPattern, Data) ->
    Ret = create(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'create!'(StoreId, PathPattern, Data, Extra | Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% Calling this function is the same as calling {@link create/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/4.

'create!'(StoreId, PathPattern, Data, ExtraOrOptions) ->
    Ret = create(StoreId, PathPattern, Data, ExtraOrOptions),
    unwrap_result(Ret).

-spec 'create!'(StoreId, PathPattern, Data, Extra, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Creates a specific tree node in the tree structure only if it does not
%% exist.
%%
%% Calling this function is the same as calling {@link create/5} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/5.

'create!'(StoreId, PathPattern, Data, Extra, Options) ->
    Ret = create(StoreId, PathPattern, Data, Extra, Options),
    unwrap_result(Ret).

-spec 'update!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% Calling this function is the same as calling {@link update/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/2.

'update!'(PathPattern, Data) ->
    Ret = update(PathPattern, Data),
    unwrap_result(Ret).

-spec 'update!'(StoreId, PathPattern, Data) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% Calling this function is the same as calling {@link update/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/3.

'update!'(StoreId, PathPattern, Data) ->
    Ret = update(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'update!'(StoreId, PathPattern, Data, Extra | Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% Calling this function is the same as calling {@link update/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/4.

'update!'(StoreId, PathPattern, Data, ExtraOrOptions) ->
    Ret = update(StoreId, PathPattern, Data, ExtraOrOptions),
    unwrap_result(Ret).

-spec 'update!'(StoreId, PathPattern, Data, Extra, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists.
%%
%% Calling this function is the same as calling {@link update/5} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/5.

'update!'(StoreId, PathPattern, Data, Extra, Options) ->
    Ret = update(StoreId, PathPattern, Data, Extra, Options),
    unwrap_result(Ret).

-spec 'compare_and_swap!'(PathPattern, DataPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% Calling this function is the same as calling {@link compare_and_swap/3} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see compare_and_swap/3.

'compare_and_swap!'(PathPattern, DataPattern, Data) ->
    Ret = compare_and_swap(PathPattern, DataPattern, Data),
    unwrap_result(Ret).

-spec 'compare_and_swap!'(
        StoreId, PathPattern, DataPattern, Data) ->
    Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% Calling this function is the same as calling {@link compare_and_swap/4} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see compare_and_swap/4.

'compare_and_swap!'(StoreId, PathPattern, DataPattern, Data) ->
    Ret = compare_and_swap(StoreId, PathPattern, DataPattern, Data),
    unwrap_result(Ret).

-spec 'compare_and_swap!'(
        StoreId, PathPattern, DataPattern, Data, Extra | Options) ->
    Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% Calling this function is the same as calling {@link compare_and_swap/5} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see compare_and_swap/5.

'compare_and_swap!'(
 StoreId, PathPattern, DataPattern, Data, ExtraOrOptions) ->
    Ret = compare_and_swap(
            StoreId, PathPattern, DataPattern, Data, ExtraOrOptions),
    unwrap_result(Ret).

-spec 'compare_and_swap!'(
        StoreId, PathPattern, DataPattern, Data, Extra, Options) ->
    Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Extra :: #{keep_while => khepri_condition:keep_while()},
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates a specific tree node in the tree structure only if it already
%% exists and its data matches the given `DataPattern'.
%%
%% Calling this function is the same as calling {@link compare_and_swap/6} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see compare_and_swap/6.

'compare_and_swap!'(
 StoreId, PathPattern, DataPattern, Data, Extra, Options) ->
    Ret = compare_and_swap(
            StoreId, PathPattern, DataPattern, Data, Extra, Options),
    unwrap_result(Ret).

-spec 'delete!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link delete/1} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see delete/1.

'delete!'(PathPattern) ->
    Ret = delete(PathPattern),
    unwrap_result(Ret).

-spec 'delete!'
(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link delete/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see delete/2.

'delete!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = delete(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'delete!'(StoreId, PathPattern, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the path pattern.
%%
%% Calling this function is the same as calling {@link delete/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see delete/3.

'delete!'(StoreId, PathPattern, Options) ->
    Ret = delete(StoreId, PathPattern, Options),
    unwrap_result(Ret).

-spec unwrap_result(Ret) -> UnwrappedRet when
      Ret :: khepri:minimal_ret() |
             khepri:payload_ret() |
             %khepri:many_payloads_ret() |
             khepri_machine:async_ret(),
      UnwrappedRet :: khepri:unwrapped_minimal_ret() |
                      khepri:unwrapped_payload_ret() |
                      %khepri:unwrapped_many_payloads_ret() |
                      khepri_machine:async_ret().
%% @private

unwrap_result({ok, Result})    -> Result;
unwrap_result(ok)              -> ok;
unwrap_result({error, Reason}) -> error(Reason).
