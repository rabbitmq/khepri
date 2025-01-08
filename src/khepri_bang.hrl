%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% -------------------------------------------------------------------
%% "Bang functions", mostly an Elixir convention.
%% -------------------------------------------------------------------

-type unwrapped_minimal_ret() :: ok.

-type unwrapped_payload_ret(Default) :: khepri:data() |
                                        horus:horus_fun() |
                                        Default.

-type unwrapped_payload_ret() :: unwrapped_payload_ret(undefined).

-type unwrapped_many_payloads_ret(Default) :: #{khepri_path:path() =>
                                                khepri:data() |
                                                horus:horus_fun() |
                                                Default}.

-type unwrapped_many_payloads_ret() :: unwrapped_many_payloads_ret(undefined).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec 'get!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
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
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
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
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link get/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see get/3.

'get!'(StoreId, PathPattern, Options) ->
    Ret = get(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% get_or().
%% -------------------------------------------------------------------

-spec 'get_or!'(PathPattern, Default) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% Calling this function is the same as calling {@link get_or/2} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_or/2.

'get_or!'(PathPattern, Default) ->
    Ret = get_or(PathPattern, Default),
    unwrap_result(Ret).

-spec 'get_or!'
(StoreId, PathPattern, Default) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Payload :: khepri:unwrapped_payload_ret();
(PathPattern, Default, Options) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% Calling this function is the same as calling {@link get_or/3} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_or/3.

'get_or!'(StoreIdOrPathPattern, PathPatternOrDefault, DefaultOrOptions) ->
    Ret = get_or(StoreIdOrPathPattern, PathPatternOrDefault, DefaultOrOptions),
    unwrap_result(Ret).

-spec 'get_or!'(StoreId, PathPattern, Default, Options) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Returns the payload of the tree node pointed to by the given path
%% pattern, or a default value.
%%
%% Calling this function is the same as calling {@link get_or/4} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_or/4.

'get_or!'(StoreId, PathPattern, Default, Options) ->
    Ret = get_or(StoreId, PathPattern, Default, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec 'get_many!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link get_many/1} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many/1.

'get_many!'(PathPattern) ->
    Ret = get_many(PathPattern),
    unwrap_result(Ret).

-spec 'get_many!'
(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_many_payloads_ret();
(PathPattern, Options) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link get_many/2} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many/2.

'get_many!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = get_many(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'get_many!'(StoreId, PathPattern, Options) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link get_many/3} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many/3.

'get_many!'(StoreId, PathPattern, Options) ->
    Ret = get_many(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% get_many_or().
%% -------------------------------------------------------------------

-spec 'get_many_or!'(PathPattern, Default) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% Calling this function is the same as calling {@link get_many_or/2} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many_or/2.

'get_many_or!'(PathPattern, Default) ->
    Ret = get_many_or(PathPattern, Default),
    unwrap_result(Ret).

-spec 'get_many_or!'
(StoreId, PathPattern, Default) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Payload :: khepri:unwrapped_many_payloads_ret();
(PathPattern, Default, Options) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% Calling this function is the same as calling {@link get_many_or/3} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many_or/3.

'get_many_or!'(StoreIdOrPathPattern, PathPatternOrDefault, DefaultOrOptions) ->
    Ret = get_many_or(
            StoreIdOrPathPattern, PathPatternOrDefault, DefaultOrOptions),
    unwrap_result(Ret).

-spec 'get_many_or!'(StoreId, PathPattern, Default, Options) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Default :: khepri:data(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_many_payloads_ret().
%% @doc Returns payloads of all the tree nodes matching the given path
%% pattern, or a default payload.
%%
%% Calling this function is the same as calling {@link get_many_or/4} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see get_many_or/4.

'get_many_or!'(StoreId, PathPattern, Default, Options) ->
    Ret = get_many_or(StoreId, PathPattern, Default, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec 'exists!'(PathPattern) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% Calling this function is the same as calling {@link exists/1} but the error,
%% if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see exists/1.

'exists!'(PathPattern) ->
    Ret = exists(PathPattern),
    unwrap_result(Ret).

-spec 'exists!'
(StoreId, PathPattern) -> Exists when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Exists :: boolean();
(PathPattern, Options) -> Exists when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Exists :: boolean().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% Calling this function is the same as calling {@link exists/1} but the error,
%% if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see exists/2.

'exists!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = exists(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'exists!'(StoreId, PathPattern, Options) -> Exists when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Exists :: boolean().
%% @doc Indicates if the tree node pointed to by the given path exists or not.
%%
%% Calling this function is the same as calling {@link exists/1} but the error,
%% if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see exists/3.

'exists!'(StoreId, PathPattern, Options) ->
    Ret = exists(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% has_data().
%% -------------------------------------------------------------------

-spec 'has_data!'(PathPattern) -> HasData when
      PathPattern :: khepri_path:pattern(),
      HasData :: boolean().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% Calling this function is the same as calling {@link has_data/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see has_data/1.

'has_data!'(PathPattern) ->
    Ret = has_data(PathPattern),
    unwrap_result(Ret).

-spec 'has_data!'
(StoreId, PathPattern) -> HasData when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      HasData :: boolean();
(PathPattern, Options) -> HasData when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      HasData :: boolean().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% Calling this function is the same as calling {@link has_data/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see has_data/2.

'has_data!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = has_data(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'has_data!'(StoreId, PathPattern, Options) -> HasData when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      HasData :: boolean().
%% @doc Indicates if the tree node pointed to by the given path has data or
%% not.
%%
%% Calling this function is the same as calling {@link has_data/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see has_data/3.

'has_data!'(StoreId, PathPattern, Options) ->
    Ret = has_data(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% is_sproc().
%% -------------------------------------------------------------------

-spec 'is_sproc!'(PathPattern) -> IsSproc when
      PathPattern :: khepri_path:pattern(),
      IsSproc :: boolean().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% Calling this function is the same as calling {@link is_sproc/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see is_sproc/1.

'is_sproc!'(PathPattern) ->
    Ret = is_sproc(PathPattern),
    unwrap_result(Ret).

-spec 'is_sproc!'
(StoreId, PathPattern) -> IsSproc when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      IsSproc :: boolean();
(PathPattern, Options) -> IsSproc when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      IsSproc :: boolean().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% Calling this function is the same as calling {@link is_sproc/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see is_sproc/2.

'is_sproc!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = is_sproc(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'is_sproc!'(StoreId, PathPattern, Options) -> IsSproc when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      IsSproc :: boolean().
%% @doc Indicates if the tree node pointed to by the given path holds a stored
%% procedure or not.
%%
%% Calling this function is the same as calling {@link is_sproc/1} but the
%% error, if any, is thrown using {link @erlang:error/1}. It closer to Elixir
%% conventions in pipelines however.
%%
%% @see is_sproc/3.

'is_sproc!'(StoreId, PathPattern, Options) ->
    Ret = is_sproc(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec 'count!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link count/1} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see count/1.

'count!'(PathPattern) ->
    Ret = count(PathPattern),
    unwrap_result(Ret).

-spec 'count!'
(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_payload_ret();
(PathPattern, Options) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link count/2} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see count/2.

'count!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = count(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'count!'(StoreId, PathPattern, Options) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: query_options() | khepri:tree_options(),
      Payload :: khepri:unwrapped_payload_ret().
%% @doc Counts all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link count/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see count/3.

'count!'(StoreId, PathPattern, Options) ->
    Ret = count(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% put().
%% -------------------------------------------------------------------

-spec 'put!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
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
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link put/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/3.

'put!'(StoreId, PathPattern, Data) ->
    Ret = put(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'put!'(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Sets the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link put/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see put/4.

'put!'(StoreId, PathPattern, Data, Options) ->
    Ret = put(StoreId, PathPattern, Data, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% put_many().
%% -------------------------------------------------------------------

-spec 'put_many!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link put_many/2} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% If there is an error, an exception is thrown.
%%
%% @see put_many/2.

'put_many!'(PathPattern, Data) ->
    Ret = put_many(PathPattern, Data),
    unwrap_result(Ret).

-spec 'put_many!'(StoreId, PathPattern, Data) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link put_many/3} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% If there is an error, an exception is thrown.
%%
%% @see put_many/3.

'put_many!'(StoreId, PathPattern, Data) ->
    Ret = put_many(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'put_many!'(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Sets the payload of all the tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link put_many/4} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% If there is an error, an exception is thrown.
%%
%% @see put_many/4.

'put_many!'(StoreId, PathPattern, Data, Options) ->
    Ret = put_many(StoreId, PathPattern, Data, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec 'create!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Creates a tree node with the given payload.
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
%% @doc Creates a tree node with the given payload.
%%
%% Calling this function is the same as calling {@link create/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/3.

'create!'(StoreId, PathPattern, Data) ->
    Ret = create(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'create!'(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Creates a tree node with the given payload.
%%
%% Calling this function is the same as calling {@link create/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see create/4.

'create!'(StoreId, PathPattern, Data, Options) ->
    Ret = create(StoreId, PathPattern, Data, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec 'update!'(PathPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates an existing tree node with the given payload.
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
%% @doc Updates an existing tree node with the given payload.
%%
%% Calling this function is the same as calling {@link update/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/3.

'update!'(StoreId, PathPattern, Data) ->
    Ret = update(StoreId, PathPattern, Data),
    unwrap_result(Ret).

-spec 'update!'(StoreId, PathPattern, Data, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload.
%%
%% Calling this function is the same as calling {@link update/4} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. If
%% there is an error, an exception is thrown.
%%
%% @see update/4.

'update!'(StoreId, PathPattern, Data, Options) ->
    Ret = update(StoreId, PathPattern, Data, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% compare_and_swap().
%% -------------------------------------------------------------------

-spec 'compare_and_swap!'(PathPattern, DataPattern, Data) -> Payload when
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
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
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
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
        StoreId, PathPattern, DataPattern, Data, Options) ->
    Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      DataPattern :: ets:match_pattern(),
      Data :: khepri_payload:payload() | data() | fun(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Updates an existing tree node with the given payload only if its data
%% matches the given pattern.
%%
%% Calling this function is the same as calling {@link compare_and_swap/5} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see compare_and_swap/5.

'compare_and_swap!'(
 StoreId, PathPattern, DataPattern, Data, Options) ->
    Ret = compare_and_swap(
            StoreId, PathPattern, DataPattern, Data, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec 'delete!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes the tree node pointed to by the given path pattern.
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
%% @doc Deletes the tree node pointed to by the given path pattern.
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
%% @doc Deletes the tree node pointed to by the given path pattern.
%%
%% Calling this function is the same as calling {@link delete/3} but the result
%% is unwrapped (from the `{ok, Result}' tuple) and returned directly. It
%% closer to Elixir conventions in pipelines however.
%%
%% @see delete/3.

'delete!'(StoreId, PathPattern, Options) ->
    Ret = delete(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% delete_many().
%% -------------------------------------------------------------------

-spec 'delete_many!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link delete_many/1} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see delete_many/1.

'delete_many!'(PathPattern) ->
    Ret = delete_many(PathPattern),
    unwrap_result(Ret).

-spec 'delete_many!'
(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret();
(PathPattern, Options) -> Ret when
      PathPattern :: khepri_path:pattern(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link delete_many/2} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see delete_many/2.

'delete_many!'(StoreIdOrPathPattern, PathPatternOrOptions) ->
    Ret = delete_many(StoreIdOrPathPattern, PathPatternOrOptions),
    unwrap_result(Ret).

-spec 'delete_many!'(StoreId, PathPattern, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: command_options() | khepri:tree_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link delete_many/3} but the
%% result is unwrapped (from the `{ok, Result}' tuple) and returned directly.
%% It closer to Elixir conventions in pipelines however.
%%
%% @see delete_many/3.

'delete_many!'(StoreId, PathPattern, Options) ->
    Ret = delete_many(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% clear_payload().
%% -------------------------------------------------------------------

-spec 'clear_payload!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link clear_payload/1} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_payload/1.

'clear_payload!'(PathPattern) ->
    Ret = clear_payload(PathPattern),
    unwrap_result(Ret).

-spec 'clear_payload!'(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link clear_payload/2} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_payload/2.

'clear_payload!'(StoreId, PathPattern) ->
    Ret = clear_payload(StoreId, PathPattern),
    unwrap_result(Ret).

-spec 'clear_payload!'(StoreId, PathPattern, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes the payload of the tree node pointed to by the given path
%% pattern.
%%
%% Calling this function is the same as calling {@link clear_payload/3} but
%% the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_payload/3.

'clear_payload!'(StoreId, PathPattern, Options) ->
    Ret = clear_payload(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% clear_many_payloads().
%% -------------------------------------------------------------------

-spec 'clear_many_payloads!'(PathPattern) -> Payload when
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link clear_many_payloads/1}
%% but the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_many_payloads/1.

'clear_many_payloads!'(PathPattern) ->
    Ret = clear_many_payloads(PathPattern),
    unwrap_result(Ret).

-spec 'clear_many_payloads!'(StoreId, PathPattern) -> Payload when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Payload :: khepri:unwrapped_minimal_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link clear_many_payloads/2}
%% but the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_many_payloads/2.

'clear_many_payloads!'(StoreId, PathPattern) ->
    Ret = clear_many_payloads(StoreId, PathPattern),
    unwrap_result(Ret).

-spec 'clear_many_payloads!'(StoreId, PathPattern, Options) -> Ret when
      StoreId :: store_id(),
      PathPattern :: khepri_path:pattern(),
      Options :: khepri:command_options() |
                 khepri:tree_options() |
                 khepri:put_options(),
      Ret :: khepri:unwrapped_minimal_ret() |
             khepri_machine:async_ret().
%% @doc Deletes the payload of all tree nodes matching the given path pattern.
%%
%% Calling this function is the same as calling {@link clear_many_payloads/3}
%% but the result is unwrapped (from the `{ok, Result}' tuple) and returned
%% directly. If there is an error, an exception is thrown.
%%
%% @see clear_many_payloads/3.

'clear_many_payloads!'(StoreId, PathPattern, Options) ->
    Ret = clear_many_payloads(StoreId, PathPattern, Options),
    unwrap_result(Ret).

%% -------------------------------------------------------------------
%% Bang function helpers.
%% -------------------------------------------------------------------

-spec unwrap_result(Ret) -> UnwrappedRet when
      Ret :: khepri:minimal_ret() |
             khepri:payload_ret() |
             khepri:many_payloads_ret() |
             khepri_machine:async_ret() |
             boolean(),
      UnwrappedRet :: khepri:unwrapped_minimal_ret() |
                      khepri:unwrapped_payload_ret() |
                      khepri:unwrapped_many_payloads_ret() |
                      khepri_machine:async_ret() |
                      boolean().
%% @private

unwrap_result({ok, Result})    -> Result;
unwrap_result(ok)              -> ok;
unwrap_result(true)            -> true;
unwrap_result(false)           -> false;
unwrap_result({error, Reason}) -> error(Reason).
