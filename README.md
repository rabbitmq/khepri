# The Khepri Database

[![Test](https://github.com/rabbitmq/khepri/actions/workflows/test.yaml/badge.svg)](https://github.com/rabbitmq/khepri/actions/workflows/test.yaml)
[![Coverage Status](https://coveralls.io/repos/github/rabbitmq/khepri/badge.svg?branch=wip)](https://coveralls.io/github/rabbitmq/khepri?branch=wip)

Khepri is a tree-like replicated on-disk database library for Erlang and Elixir.

> Khepri is still in development and should be considered *Alpha* at this stage.

Data are stored in a **tree structure**. Each node in the tree is referenced by
its path from the root node. A path is a list of Erlang atoms and/or binaries.
For ease of use, Unix-like path strings are accepted as well.

For **consistency and replication** and to manage data on disk, Khepri relies
on the [Ra library](https://github.com/rabbitmq/ra). The Ra library implements
the [Raft consensus algorithm](https://raft.github.io/). In Ra parlance,
Khepri is a state machine in a Ra cluster.

## Getting started

1.  Add Khepri as a dependency of your project:

    Using Rebar:
    ```erlang
    %% In rebar.config
    {deps, [{ra,
             {git, "https://github.com/rabbitmq/khepri.git",
             {ref, "master"}}}]}.
    ```

    Using Erlang.mk:
    ```make
    # In your Makefile
    DEPS += khepri
    dep_khepri = git https://github.com/rabbitmq/khepri.git master
    ```

    Using mix:
    ```elixir
    # In mix.exs
    defp deps do
      [
        {:khepri,
          git: "https://github.com/rabbitmq/khepri.git",
          branch: "master"}
      ]
    end
    ```

2.  Start the default store:

    ```erlang
    khepri:start().
    ```

    The default Khepri store uses the default "Ra system". Data are written in
    the configured Ra data dir which defaults to the current working directory.

    It is fine to get started and play with Khepri. However, it is recommended
    to configure your own Ra system and Ra cluster to select the directory
    where data is written and to be able to have multiple database instances
    running in parallel.

3.  **Insert** Alice's email address:

    ```erlang
    %% Using a native path:
    khepri:insert([emails, alice], "alice@example.org").

    %% USing a Unix-like path string:
    khepri:insert("/emails/alice", "alice@example.org").
    ```

    The `khepri` module provides the "simple API". It has several functions to
    cover the most common uses. For advanced uses, using the `khepri_mcachine`
    module directly is preferred.

4.  To get Alice's email address back, **query** the same path:

    ```erlang
    Ret = khepri:get("/emails/alice"),

    %% Here is the value of `Ret':
    {ok, #{[emails, alice] =>
           #{child_list_count => 0,
             child_list_version => 1,
             data => "alice@example.org",
             payload_version => 1}}} = Ret.
    ```

    The `khepri:get()` function and many other ones accept a "path pattern".
    Therefore it is possible to get several nodes in a single call. The result
    is a map where keys are the path to each node which matched the path
    pattern, and the values are a map of properties. The data payload of the
    node is one of them.

5.  To **delete** Alice's email address:

    ```erlang
    khepri:delete("/emails/alice").
    ```

    The `emails` parent node was automatically created when the `alice` node
    was inserted earlier. It has no data attached to it. However, after the
    `alice` node is deleted, the `emails` node will stay around. It is possible
    to tell Khepri to automatically remove `emails` as soon as its last child
    node is deleted. Khepri supports many more conditions by the way.

6.  It is also possible to perform **transactional queries and updates** using
    anonymous functions, similar to Mnesia:

    ```erlang
    %% This transaction checks the left quantity of wood and returns `true` or
    %% `false` if we need to process a new order.
    khepri:transaction(
        fun() ->
            case khepri_tx:get([stock, wood]) of
                {ok, #{[stock, wood] := #{data := Quantity}}}
                  when Quantity < 100 ->
                    %% There is enough wood left.
                    false;
                _ ->
                    %% There is less than 100 pieces of wood, or there is none
                    %% at all (the node does not exist in Khepri). We need to
                    %% request a new order.
                    {ok, _} = khepri_tx:put([order, wood], ?DATA_PAYLOAD(1000)),
                    true
            end
        end).
    ```

    In this example, the transaction returns a boolean indicating if orders are
    ready to be processed. It does not send a message to a process or write
    something on disk for instance.

    Because of the nature of the Raft consensus algorithm, transactions are not
    allowed to have side effects or take non-deterministic inputs such as the
    node name or the current date & time.

## How to build

### Build

```
rebar3 compile
```

### Test

```
rebar3 xref
rebar3 eunit
rebar3 dialyzer
```

## How to contribute
