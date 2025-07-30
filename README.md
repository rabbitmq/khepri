# The Khepri database library

[![Hex.pm](https://img.shields.io/hexpm/v/khepri)](https://hex.pm/packages/khepri/)
[![Test](https://github.com/rabbitmq/khepri/actions/workflows/test-and-release.yaml/badge.svg)](https://github.com/rabbitmq/khepri/actions/workflows/test-and-release.yaml)
[![Codecov](https://codecov.io/gh/rabbitmq/khepri/branch/main/graph/badge.svg?token=R0OGKZ2RK2)](https://codecov.io/gh/rabbitmq/khepri)

Khepri is a tree-like replicated on-disk database library for Erlang and
Elixir.

<img align="right" width="100" src="/doc/khepri-logo.svg">

## The basics

Data are stored in a **tree structure**. Each node in the tree is referenced by
its path from the root node. A path is a list of Erlang atoms and/or binaries.
For ease of use, Unix-like path strings are accepted as well.

For **consistency and replication** and to manage data on disk, Khepri relies
on [Ra](https://github.com/rabbitmq/ra), an Erlang implementation of the [Raft
consensus algorithm](https://raft.github.io/). In Ra parlance, Khepri is a
state machine in a Ra cluster.

## Project maturity

Khepri is still under active development and should be considered *Beta* at
this stage.

## Known limitations

Khepri currently hosts the entire data set **in memory as well as on disk**, so
there is a realistic limit to how large a data set can be stored in it.

For this reason and others, storing **blobs of files** in Khepri is not
recommended. For that, use an external blob store.

## Documentation

* A short tutorial in the [Getting started](#getting-started) section below
* [Documentation and API reference](https://rabbitmq.github.io/khepri/)

## Getting started

### Add as a dependency

Add Khepri as a dependency of your project:

Using Rebar:

```erlang
%% In rebar.config
{deps, [{khepri, "0.17.2"}]}.
```

Using Erlang.mk:

```make
# In your Makefile
DEPS += khepri
dep_khepri = hex 0.17.2
```

Using Mix:

```elixir
# In mix.exs
defp deps do
  [
    {:khepri, "0.17.2"}
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
is stored and to be able to have multiple Khepri database instances running on
the same Erlang node.

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
inserted earlier. It has no data attached to it. However, after the `alice`
node is deleted, the `emails` node will stay around. It is possible to tell
Khepri to automatically remove `emails` as soon as its last child node is
deleted. Khepri supports many more conditions by the way.

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

    Fun = fun(Props) ->
              #{path := Path,
                on_action := Action} = Props
          end,

    khepri:put(StoreId, StoredProcPath, Fun).
    ```

2.  Register a trigger using an event filter:

    ```erlang
    %% A path is automatically considered a tree event filter.
    EventFilter = [stock, wood, <<"oak">>],

    ok = khepri:register_trigger(
           StoreId,
           TriggerId,
           EventFilter,
           StoredProcPath).
    ```

In the example above, as soon as the `[stock, wood, <<"oak">>]` node is
created, updated or deleted, the anonymous function will be executed.

The function is executed at least once on the Ra leader's Erlang node. It may
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
rebar3 edoc
```

### Test

```
rebar3 xref
rebar3 eunit
rebar3 proper
rebar3 ct --sname ct
rebar3 as test dialyzer
```

## Copyright and License

© 2021-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to
Broadcom Inc. and/or its subsidiaries.

This work is dual-licensed under the Apache License 2.0 and the Mozilla Public
License 2.0. Users can choose any of these licenses according to their needs.

The logo (`doc/khepri-logo.svg`) and the favicon (`doc/khepri-favicon.svg`) are
based on the following two resources:
* https://www.svgrepo.com/svg/55105/rabbit (license: CC0)
* https://www.svgrepo.com/svg/336625/database-point (license: MIT)

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
