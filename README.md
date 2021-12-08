# The Khepri database library

[![Hex.pm](https://img.shields.io/hexpm/v/khepri)](https://hex.pm/packages/khepri/)
[![Test](https://github.com/rabbitmq/khepri/actions/workflows/test.yaml/badge.svg)](https://github.com/rabbitmq/khepri/actions/workflows/test.yaml)
[![Coverage status](https://coveralls.io/repos/github/rabbitmq/khepri/badge.svg?branch=main)](https://coveralls.io/github/rabbitmq/khepri?branch=main)

Khepri is a tree-like replicated on-disk database library for Erlang and
Elixir.

## The basics

Data are stored in a **tree structure**. Each node in the tree is referenced by
its path from the root node. A path is a list of Erlang atoms and/or binaries.
For ease of use, Unix-like path strings are accepted as well.

For **consistency and replication** and to manage data on disk, Khepri relies
on [Ra](https://github.com/rabbitmq/ra), an Erlang implementation of the [Raft
consensus algorithm](https://raft.github.io/). In Ra parlance, Khepri is a
state machine in a Ra cluster.

## Project maturity

Khepri is still under active development and should be considered *Alpha* at
this stage.

## Documentation

* A short tutorial in the [Getting started](#getting-started) section below
* [Documentation and API reference](https://rabbitmq.github.io/khepri/)

## Getting started

### Add as a dependency

Add Khepri as a dependency of your project:

Using Rebar:

```erlang
%% In rebar.config
{deps, [{khepri, "0.1.1"}]}.
```

Using Erlang.mk:

```make
# In your Makefile
DEPS += khepri
dep_khepri = hex 0.1.1
```

Using Mix:

```elixir
# In mix.exs
defp deps do
  [
    {:khepri, "0.1.1"}
  ]
end
```

### Start default Khepri store

To start the default store, use `khepri:start/0`:

```erlang
khepri:start().
```

The default Khepri store uses the default Ra system. Data is stored in the
configured default Ra system data directory, which defaults to the current
working directory.

It is fine to get started and play with Khepri. However, it is recommended to
configure your own Ra system and Ra cluster to select the directory where data
is stored and to be able to have multiple Khepri database instances running on
the same Erlang node.

### Insert data

Here's how to **insert** a piece of data, say, an email address of Alice:

```erlang
%% Using a native path:
khepri:insert([emails, alice], "alice@example.org").

%% USing a Unix-like path string:
khepri:insert("/emails/alice", "alice@example.org").
```

The `khepri` module provides the "simple API". It has several functions to
cover the most common uses. For advanced uses, using the `khepri_machine`
module directly is preferred.

### Read data back

To get Alice's email address back, **query** the same path:

```erlang
Ret = khepri:get("/emails/alice"),

%% Here is the value of `Ret':
{ok, #{[emails, alice] =>
       #{child_list_count => 0,
         child_list_version => 1,
         data => "alice@example.org",
         payload_version => 1}}} = Ret.
```

The `khepri:get/0` function and many other ones accept a "path pattern".
Therefore it is possible to get several nodes in a single call. The result is a
map where keys are the path to each node which matched the path pattern, and
the values are a map of properties. The data payload of the node is one of
them.

### Delete data

To **delete** Alice's email address:

```erlang
khepri:delete("/emails/alice").
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
            {ok, #{[stock, wood] := #{data := Quantity}}}
              when Quantity >= 100 ->
                %% There is enough wood left.
                false;
            _ ->
                %% There is less than 100 pieces of wood, or there is none
                %% at all (the node does not exist in Khepri). We need to
                %% request a new order.
                {ok, _} = khepri_tx:put(
                            [order, wood],
                            #kpayload_data{data = 1000}),
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

## How to build

### Build

```
rebar3 compile
```

### Test

```
rebar3 xref
rebar3 eunit
rebar3 proper
rebar3 dialyzer
```
