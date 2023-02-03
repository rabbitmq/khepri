Khepri is a tree-like replicated on-disk database library for Erlang and
Elixir.

Data are stored in a **tree structure**. Each node in the tree is referenced by
its path from the root node. A path is a list of Erlang atoms and/or binaries.
For ease of use, Unix-like path strings are accepted as well.

For **consistency and replication** and to manage data on disk, Khepri relies
on [Ra](https://github.com/rabbitmq/ra), an Erlang implementation of the [Raft
consensus algorithm](https://raft.github.io/). In Ra parlance, Khepri is a
state machine in a Ra cluster.

This page **describes all the concepts in Khepri** and points the reader to the
modules' documentation for more details.

# Why Khepri?

This started as an experiment to replace how data (other than message bodies)
are stored in the [RabbitMQ messaging broker](https://www.rabbitmq.com/).
Before Khepri, those data were stored and replicated to cluster members using
Mnesia.

Mnesia is very handy and powerful:

* It comes out-of-the-box with the Erlang runtime and standard library.
* It does all the heavy lifting and RabbitMQ just uses it as a key/value
  store without thinking too much about replication.

However, recovering from any network partitions is quite difficult. This was
the primary reason why the RabbitMQ team started to explore other options.

Because RabbitMQ already uses an implementation of the Raft consensus algorithm
for its quorum queues, it was decided to leverage that library for all
metadata. That's how Khepri was borne.

Thanks to Ra and Raft, it is **clear how Khepri will behave during a network
partition and recover from it**. This makes it more comfortable for the
RabbitMQ team and users, thanks to the absence of unknowns.

> #### Note {: .info}
>
> At the time of this writing, RabbitMQ does not use Khepri in a production
> release yet because this library and its integration into RabbitMQ are still
> a work in progress.

# The tree structure

## Tree nodes

Data in Khepri are organized as _tree nodes_ (`t:khepri_machine:tree_node()`)
in a tree structure. Every tree node has:

<!-- TODO: check anchor links -->

* a [node ID](#node-id)
* a [payload](#payload) (optional)
* [properties](#properties)

```none
o
|
+-- orders
|
`-- stock
    |
    `-- wood
        |-- <<"mapple">> = 12
        `-- <<"oak">> = 41
```

## Node ID

A tree node name is either an Erlang atom or an Erlang binary
(`t:khepri_path:node_id()`).

## Payload

A tree node may or may not have a payload. Khepri supports two types of
payload, the _data payload_ and the _stored procedure payload_. More payload
types may be added in the future.

When passed to `khepri:put/2`, the type of the payload is autodetected.
However if you need to prepare the payload before passing it to Khepri, you can
use the following functions:

* `khepri_payload:none/0`
* `khepri_payload:data/1`
* `khepri_payload:sproc/1`

## Properties

Properties are:

* The version of the payload, tracking the number of times it was modified
  (`t:khepri:payload_version()`).
* The version of the list of child nodes, tracking the number of times child
  nodes were added or removed (`t:khepri:child_list_version()`).
* The number of child nodes (`t:khepri:child_list_length()`).

## Addressing a tree node

The equivalent of a _key_ in a key/value store is a _path_
(`t:khepri_path:path()`) in Khepri.

A path is a list of node IDs, from the root (unnamed) tree node to the target
(`t:khepri_path:path()`). For instance:

```erlang
%% Points to "/:stock/:wood/oak" in the tree shown above:
Path = [stock, wood, <<"oak">>].
```

It is possible to target multiple tree nodes at once by using a _path pattern_
(`t:khepri_path:pattern()`). In addition to node IDs, path patterns have
conditions (`t:khepri_condition:condition()`). Conditions allow things like:

* checking the existence of a tree node
* targeting all child nodes of a tree node
* matching on node IDs using a regex
* matching on the data payload

For instance:

```erlang
%% Matches all varieties of wood in the stock:
PathPattern = [stock, wood, #if_node_matches{regex = any}].

%% Matches the supplier of oak if there is an active order:
PathPattern = [order,
               wood,
               #if_all{conditions = [
                 <<"oak">>,
                 #if_data_matches{pattern = {active, true}}]},
               supplier].
```

Finally, a path can use some special path component names, handy when using
relative paths:

* `?THIS_NODE` to point to self
* `?PARENT_NODE` to point to the parent tree node
* `?ROOT_NODE` to explicitly point to the root unnamed node

Relative paths are useful when putting conditions on
[tree node lifetime](#tree-node-lifetime).

## Tree node lifetime

A tree node's lifetime starts when it is inserted the first time and ends when
it is removed from the tree. However, intermediary tree nodes created on the
way remain in the tree long after the leaf node was removed.

For instance, when `[stock, wood, <<"walnut">>]` was inserted, the intermediary
tree nodes `stock` and `wood` were created if they were missing. After
`<<"walnut">>` is removed, they will stay in the tree with possibly neither
payload nor child nodes.

Khepri has the concept of _`keep_while` conditions_. A `keep_while` condition
is like the conditions which can be used inside path pattern. When a node is
inserted or updated, it is possible to set `keep_while` conditions: when these
conditions evaluate to false, the tree node is removed from the tree.

For instance, it is possible to set the following condition on `[stock, wood]`
to make sure it is removed after its last child node is removed:

```erlang
%% We keep [stock, wood] as long as its child nodes count is strictly greater
%% than zero.
KeepWhileCondition = #{[stock, wood] => #if_child_list_length{count = {gt, 0}}}.
```

`keep_while` conditions on self (like the example above) are not evaluated on
the first insert though.

# Stores

A Khepri store corresponds to one Ra cluster. In fact, the name of the Ra
cluster is the name of the Khepri store. It is possible to have multiple
database instances running on the same Erlang node or cluster by starting
multiple Ra clusters. Note that it is called a "cluster" but it can have a
single member.

You can start a Khepri store using `khepri:start/0` up to `khepri:start/3`.
See those functions to learn more about the configuration settings.

To expand or shrink a cluster, `khepri_cluster:join/1` and
`khepri_cluster:reset/0` allow a Khepri store node to join or leave a cluster.

# Khepri API

The essential part of the public API is provided by the `khepri` module. It
covers most common use cases and should be straightforward to use.

```erlang
ok = khepri:put([stock, wood, <<"lime tree">>], 150),

{ok, 150} = khepri:get([stock, wood, <<"lime tree">>]),

true = khepri:exists([stock, wood, <<"lime tree">>]),

ok = khepri:delete([stock, wood, <<"lime tree">>]).
```

Inside transaction functions, `khepri_tx` must be used instead of `khepri`. The
former provides the same API, except for functions which don't make sense in
the context of a transaction function.

`khepri` and `khepri_tx` both have counterparts for more advanced use cases,
`khepri_adv` and `khepri_tx_adv`. The return values of the `*_adv` modules are
maps giving more details about what was queried or modified.

The public API is built on top of a low-level internal API, provided by the
private `khepri_machine` module.

# Transactions

## Restrictions

On the surface, Khepri transactions look like Mnesia ones: they are anonymous
functions which can do any arbitrary operations on the data and return any
result. If something goes wrong or the anonymous function aborts, nothing is
committed and the database is left untouched as if the transaction code was
never called.

Under the hood, there are several restrictions and caveats that need to be
understood in order to use transactions in Khepri:

* If the anonymous function only **reads data** from the tree, there is no
  specific restrictions on them.
* If however the anonymous function needs to **modify or delete** data from the
  database, then the constraints described in the next section need to be taken
  into account.

The nature of the anonymous function is passed as the `ReadWrite` argument to
`khepri:transaction/3`.

## The constraints imposed by Raft

The Raft algorithm is used to achieve consensus among Khepri members
participating in the database. Khepri is a state machine executed on each Ra
node and all instances of that Khepri state machine start with the same state
and modify it identically. The goal is that, after the same list of Ra
commands, all instances have the same state.

When a new Ra node joins the cluster and therefore participates to the Khepri
database, it starts a new Khepri state machine instance. This instance needs to
apply all Ra commands from an initial state to be on the same page as other
existing instances.

Likewise, if for any reason, one of the Khepri state machine instance looses
the connection to other members and can't apply Ra commands, then when the link
comes back, it has to catch up.

All this means that the code to modify the state of the state machines (i.e.
the tree) needs to run on all instances, possibly not at the same time, and
give the exact same result everywhere.

## The problem with anonymous functions

This is fine for inserts and deletes because the code is part of Khepri and is
deterministic. This poses a problem when transactions are anonymous functions
outside of Khepri's control:

1. Khepri must be able to store the anonymous function as a Ra command in Ra's
   log. This is the basis for replication and is mandatory to add a new cluster
   member or for a lagging member to catch up.
2. The anonymous function must produce exactly the same result in all state
   machine instances, regardless of the time it runs, the availability of other
   Erlang modules, the state of Erlang processes, files on disk or network
   connections, and so on.

To achieve that, `khepri_fun` and `khepri_tx` extract the assembly
code of the anonymous function and create a standalone Erlang module based on
it. This module can be stored in Ra's log and executed anywhere without the
presence of the initial anonymous function's module.

Here is what they do in more details:

1. The assembly code of the module hosting the anonymous function is extracted.
2. The anonymous function code is located inside that assembly code.
3. The code is analyzed to determine:
    * that it does not perform any forbidden operations (sending or receiving
      inter-process messages, use date and time, access files or network
      connections, etc.)
    * what other functions it calls
4. Based on the listed function calls, the same steps are repeated for all of
   them (extract, verify, list calls).
5. Once all the assembly code to have a standalone anonymous function is
   collected, an Erlang module is generated.

## How to handle side effects?

The consequence of the above constraints is that a transaction function can't
depend on anything else than the tree and it can't have any side effects
outside of the changes to the tree nodes.

If the transaction needs to have side effects, there are two options:

* Perform any side effects after the transaction.
* Use `khepri:put/3` with `t:khepri_condition:if_payload_version()` conditions
  in the path and retry if the put fails because the version changed in
  between.

Here is an example of the second option:

```erlang
Path = [stock, wood, <<"lime tree">>],
{ok, #{data := Term,
       payload_version := PayloadVersion}} =
  khepri_adv:get(StoredId, Path),

%% Do anything with `Term` that depend on external factors and could have side
%% effects.
Term1 = do_something_with_side_effects(Term),

PathPattern = [stock,
               wood,
               #if_all{
                 conditions = [
                   <<"lime tree">>,
                   #if_payload_version{version = PayloadVersion}]}],
case khepri:put(StoredId, PathPattern, Term1) of
    ok ->
        ok; %% `Term1` was stored successfully.
    {error, ?khepri_error(mismatching_node, _)} ->
        loop() %% Restart the whole function to read/modify/write again.
end.
```

# Import and export

To backup and restore a Khepri store, you can use `khepri:export/4` and
`khepri:import/3`.

They both rely on a backend module which follows the [Mnesia Backup &amp;
Restore API](https://www.erlang.org/doc/apps/mnesia/mnesia_app_a). Khepri comes
with one backend module called `khepri_export_erlang`. It uses plaintext files
containing opaque Erlang terms formatted as text. It is possible to provide
your own backend module obviously.

To export the content of a running store:

```erlang
ok = khepri:export(StoreId, khepri_export_erlang, "export.erl").
```

This will export the entire store. It is possible to export a part of it by
passing a path patterns to select the tree nodes you want to export.

Later, to import to a running store:

```erlang
ok = khepri:import(StoreId, khepri_export_erlang, "export.erl").
```

Importing a backup does not touch unrelated tree nodes. In other words, the
content of the exported store is imported but whatever exists in the target
store remains (except if the import overwrites some tree nodes of course). In
particular, the target store is not reset.

You can learn more about this import/export feature in the
`khepri_import_export` module documentation.

You can lean more about the provide backend module by reading the documentation
of `khepri_export_erlang`.

# Stored procedures and triggers

## Triggering a function after some event

It is possible to associate events with an anonymous function to trigger its
execution when something happens. This is what is usually called a _trigger_ in
databases and Khepri supports this feature.

Currently, Khepri supports a single type of event, _tree changes_. This event
is emitted whenever a tree node is being created, updated or deleted.

Here is a summary of what happens when such an event is emitted:

1. Khepri looks up any _event filters_ which could match the emitted event.
1. If one or more event filters are found, their corresponding stored
   procedures are executed.

The indicated stored procedure must have been stored in the tree first.

## Storing an anonymous function

This is possible to store an anonymous function as the payload of a tree node:

```erlang
khepri:put(
  StoreId,
  StoredProcPath,
  fun() -> do_something() end).
```

The `StoredProcPath` can be [any path in the tree](#addressing-a-tree-node).

Unlike transaction functions, a stored procedure has no restrictions on what
it is allowed to do. Therefore, a stored procedure can send or receive
messages, read or write from a disk, generate random numbers and so on.

A stored procedure can accept any numbers of arguments too.

It is possible to execute a stored procedure directly without configuring any
triggers. To execute a stored procedure, you can call `khepri:run_sproc/3`.
Here is an example:

```erlang
Ret = khepri:run_sproc(
        StoreId,
        StoredProcPath,
        [] = _Args).
```

This works exactly like `erlang:apply/2`. The list of arguments passed to
`khepri:run_sproc/3` must correspond to the stored procedure arity.

## Configuring a trigger

Khepri uses _event filters_ to associate a type of events with a stored
procedure. Khepri supports tree changes events and thus only supports a single
event filter called `t:khepri_evf:tree_event_filter()`.

An event filter is registered using `khepri:register_trigger/4`:

```erlang
%% An event filter can be explicitly created using the `khepri_evf'
%% module. This is possible to specify properties at the same time.
EventFilter = khepri_evf:tree([stock, wood, <<"oak">>], %% Required
                              #{on_actions => [delete], %% Optional
                                priority => 10}),       %% Optional

%% For ease of use, some terms can be automatically converted to an event
%% filter. Here, a Unix-like path could be used as a tree event filter, though
%% it would have default properties unlike the previous line:
EventFilter = "/:stock/:wood/oak".

ok = khepri:register_trigger(
       StoreId,
       TriggerId,
       EventFilter,
       StoredProcPath).
```

In this example, the `t:khepri_evf:tree_event_filter()` structure only
requires the path to monitor. The path can be any path pattern and thus can
have conditions to monitor several nodes at once.

The `on_actions` property is optional. By default the event filter matches all
tree changes (`create`, `update` or `delete`).

The `priority` property is also optional and defaults to 0. When several event
filters match a given event, they are sorted by priority (a greater integer
means the event filter will be considered first), then by `TriggerId` in
alphabetical order.

Neither the monitored path nor the stored procedure (pointed to by
`StoredProcPath`) need to exist when the event filter is registered. If the
stored procedure doesn't exist when an event occurs, the event filter is
simply ignored. A stored procedure can change after an event filter is
registered as well.

The stored procedure used for a trigger must accept a single argument, a map
containing properties of the emitted event:

```erlang
my_stored_procedure(Props) ->
    #{path := Path,
      on_action := Action} = Props.
```

`Path` is the path to the tree node created, updated or deleted.

`Action` is the nature of the change (`create`, `update` or `delete`).

The return value of this stored procedure is ignored in the context of a
trigger.

## Execution guarantees

The stored procedure associated with a trigger (event filter) is executed on
the Ra leader node.

If the stored procedure throws an exception, it is logged and there is no
retry.

There is an internal ack mechanism to make sure the stored procedure is
executed at least once. Therefore, if the Ra leader changes before the
execution of the stored procedure could be confirmed to the Khepri state
machine, the execution will be retried on the new Ra leader.

This means that the stored procedure could be executed multiple times.
Therefore it is important it is idempotent.

## Differences with triggers in RDBMS

As described earlier, the rationale for triggers in Khepri is that sometimes,
one needs to execute some code with side effects (e.g. sending a message to a
process) after a record was modified in the database. This can't happen in a
transaction because side effects are forbidden. The caller could handle that
after he modifies the record, but the record could be indirectly modified
(deleted) as a consequence of another record being modified or deleted. In
this case, the caller can't do anything.

Because of the freedom they need, **triggers are not allowed to mess with the
database directly**. In other words, they must go through the regular Khepri
API like any caller. Triggers do not have any privileges or blanket approvals
to tamper with the data.

So even though Khepri uses the same naming than many RDBMS, triggers in Khepri
can't have unexpected consequences.

# Projections

Projections are a system within Khepri for maintaining replicated ETS caches
for tree nodes matching a given path pattern.

Projection resources are created with the `khepri_projection:new/3`
function and are registered in a store with `khepri:register_projection/4`:

```erlang
ProjectionName = wood_stocks,
ProjectionFun = fun([stock, wood, Kind], Stock) -> {Kind, Stock} end,
Options = #{type => set, read_concurrency => true},
Projection = khepri_projection:new(ProjectionName, ProjectionFun, Options).

StoreId = stock,
PathPattern = "/:stock/:wood/*",
khepri:register_projection(StoreId, PathPattern, Projection).
```

When a path within the store which matches `PathPattern` changes, the changed
path and data are passed to the given `ProjectionFun` to create ETS objects
which are then stored in the projection's ETS table.

Projection tables may be queried directly with functions from the `ets`
module.

```erlang
khepri:put(StoreId, "/:stock/:wood/oak", 100),
ets:lookup(ProjectionName, <<"oak">>, 2).
%%=> [{<<"oak">>,100}]

khepri:put(StoreId, "/:stock/:wood/oak", 80),
ets:lookup(ProjectionName, <<"oak">>, 2).
%%=> [{<<"oak">>,80}]

khepri:delete(StoreId, "/:stock/:wood/oak"),
ets:member(ProjectionName, <<"oak">>).
%%=> false
```

Use projections to maximize query throughput and/or minimize query latency.

Projections have some costs though. Expect some increased memory consumption
since information in the projection tables is duplicated between the Khepri
store and ETS. Khepri may also take longer to accomplish writes since
projections are updated by Khepri synchronously when writing to the store.
