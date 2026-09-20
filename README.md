<div align="centeralign="center"">

![Khepri logo](/doc/khepri-logo.svg)

# The Khepri database library

[![Hex.pm](https://img.shields.io/hexpm/v/khepri)](https://hex.pm/packages/khepri/)
[![Test](https://github.com/rabbitmq/khepri/actions/workflows/test-and-release.yaml/badge.svg)](https://github.com/rabbitmq/khepri/actions/workflows/test-and-release.yaml)
[![Codecov](https://codecov.io/gh/rabbitmq/khepri/branch/main/graph/badge.svg?token=R0OGKZ2RK2)](https://codecov.io/gh/rabbitmq/khepri)

</div>

Khepri is a tree-like replicated on-disk database library built on top of the
[Raft consensus algorithm](https://raft.github.io/) for Erlang and other
programming languages running on top of the BEAM virtual machine (Elixir,
Gleam, and so on).

## Features

* **Represents data as a tree** (instead of a flat key/value structure).
* Supports simple operations and complex transactions.
* Supports event-based actions.
* Supports projections for very fast queries.
* Supports data import/export.
* Based on [Ra](https://github.com/rabbitmq/ra) for consensus and data safety.

## Project maturity

Khepri is still under active development. That said, it is **safe to be used in
production**.

From an API stand point, the **API will continue to evolve**, sometimes with
breaking changes, as we are not 100% satisfied with it. That is why Khepri
1.0.0 is not released yet. When we have to introduce a breaking change:
1. We try to provide backward compatibility first
2. When it is not possible, we describe how the change impacts a program
   depending on Khepri, with examples showing how to adapt the source code.

## Getting started

The following sections demonstrate the common basis usages of Khepri. To learn
more, please refer to the [full
documentation](https://rabbitmq.github.io/khepri/).

### Add as a dependency

Add Khepri as a dependency of your project:

Using Rebar:

```erlang
%% In rebar.config
{deps, [{khepri, "0.19.1"}]}.
```

Using Erlang.mk:

```make
# In your Makefile
DEPS += khepri
dep_khepri = hex 0.19.1
```

Using Mix:

```elixir
# In mix.exs
defp deps do
  [
    {:khepri, "0.19.1"}
  ]
end
```

### Start default Khepri store

To start the default store, use `khepri:start/0`:

```erlang
khepri:start().
```

The default Khepri store uses the default Ra system. Data is stored in the
configured default Ra system data directory, which is `khepri#$NODENAME` in
the current working directory.

It is fine to get started and play with Khepri. However, it is recommended to
configure your own Ra system and Ra cluster to select the directory where data
is stored and to be able to have multiple Khepri stores running on the same
Erlang node.

### Insert data

Here's how to **insert** a piece of data, say, an email address of Alice:

```erlang
%% Using a native path:
ok = khepri:put([emails, <<"alice">>], "alice@example.org").

%% Using a Unix-like path string:
ok = khepri:put("/:emails/alice", "alice@example.org").
```

### Read data back

To get Alice's email address back, **query** the same path:

```erlang
{ok, "alice@example.org"} = khepri:get("/:emails/alice").
```

### Delete data

To **delete** Alice's email address:

```erlang
ok = khepri:delete("/:emails/alice").
```

The `emails` parent node was automatically created when the `alice` node was
inserted earlier. It has no data attached to it. At the same time the `alice`
node is deleted, the `emails` node is automatically deleted too (if it still
has no data). You can define how the lifetime of a tree node is linked to the
lifetime of another tree node or a process.

### Transactional Operations

It is also possible to perform **transactional queries and updates** using
anonymous functions, similar to Mnesia:

```erlang
%% This transaction checks the quantity of wood left and returns `true` or
%% `false` if we need to process a new order.
khepri:transaction(
    fun() ->
        case khepri_tx:get([stock, wood]) of
            {ok, Quantity} when Quantity >= 100 ->
                %% There is enough wood left.
                false;
            _ ->
                %% There is less than 100 pieces of wood, or there is none
                %% at all (the node does not exist in Khepri). We need to
                %% request a new order.
                ok = khepri_tx:put([order, wood], 1000),
                true
        end
    end).
```

In this example, the transaction returns a boolean indicating if orders are
ready to be processed. It does not send a message to a process or write
something on disk for instance.

Because of the nature of the Raft consensus algorithm, transactions are not
allowed to have side effects or take non-deterministic inputs such as the node
name or the current date & time.

### Triggers

Khepri supports *stored procedures* and *triggers*. They allow to store code in
the database itself and automatically execute it after some event occurs.

1.  Store an anonymous function in the tree:

    ```erlang
    StoredProcPath = [path, to, stored_procedure],

    Fun = fun(#khepri_trigger{event = #{path := Path,
                                        change := update,
                                        old_node_props := OldNodeProps,
                                        new_node_props := NewNodeProps}}) ->
              ...
          end,

    khepri:put(StoreId, StoredProcPath, Fun).
    ```

2.  Register a trigger using an event filter:

    ```erlang
    %% A path is automatically considered as a tree event filter.
    EventFilter = [stock, wood, ?KHEPRI_WILDCARD_STAR],

    ok = khepri:register_trigger(
           StoreId,
           TriggerId,
           EventFilter,
           StoredProcPath).
    ```

In the example above, as soon as a tree node like `[stock, wood, <<"oak">>]` is
created, updated or deleted, the anonymous function will be executed.

By default, the function is executed at least once on the Ra leader's Erlang node. It may
be executed multiple times if the leader changes and thus should be idempotent.

Unlike transaction functions, stored procedures may have whatever side effects
they want.

## Migrating from Mnesia

To help you migrate an existing Mnesia database, you can use [the
`khepri_mnesia_migration`
application](https://github.com/rabbitmq/khepri_mnesia_migration/). It can take
care of:
* synchronizing the cluster membership and
* copying Mnesia tables to a Khepri store.

## How to build

### Build

```
rebar3 compile
```

### Build documentation

```
rebar3 ex_doc
```

### Test

```
rebar3 xref
rebar3 eunit
rebar3 proper
rebar3 ct
rebar3 as test dialyzer
```

## Copyright and License

© 2021-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to
Broadcom Inc. and/or its subsidiaries.

This work is dual-licensed under the Apache License 2.0 and the Mozilla Public
License 2.0. Users can choose any of these licenses according to their needs.

The logo (`doc/khepri-logo.svg`) and the favicon (`doc/khepri-favicon.svg`) are
based on the following two resources:
* https://www.svgrepo.com/svg/55105/rabbit (license: CC0)
* https://www.svgrepo.com/svg/336625/database-point (license: MIT)


> [!NOTE]
>
> <a href="https://ai-free.io"><img align="right" decoding="async" src="https://ai-free.io/AI-free.io-CODE.png" width="80"/></a>
>
> [AI-free — *Human Made*](https://ai-free.io/)
>
> Khepri is developed by humans and no AI agents were involved in the design or
> the writing of this library, including its tests and its documentation.

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
