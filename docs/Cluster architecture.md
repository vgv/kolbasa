# Cluster architecture

This document describes what changes when you run kolbasa across **multiple PostgreSQL nodes** — the shard model, how produce and
consume are routed, where cluster state lives, and how shards migrate between nodes.

It assumes you've read [Architecture.md](Architecture.md) for the single-node model. The single-node concepts — queues, tables,
meta-fields, the message lifecycle, deduplication, DLQ/archive, sweep — are **not** re-explained here; a cluster is many
single-node databases with a thin routing layer on top. This doc covers only that layer. For operating a cluster (the `butcher`
CLI), see [Butcher.md](Butcher.md).

## Table of Contents

1. [From single node to cluster](#from-single-node-to-cluster)
2. [Enabling cluster mode in code](#enabling-cluster-mode-in-code)
3. [Shards](#shards)
4. [Cluster state](#cluster-state)
5. [Message routing](#message-routing)
6. [The cluster roles](#the-cluster-roles)
7. [Shard migration](#shard-migration)
8. [Schema consistency](#schema-consistency)
9. [Failure modes](#failure-modes)
10. [Expansion and shrinking](#expansion-and-shrinking)
11. [References](#references)

---

## From single node to cluster

A single PostgreSQL node gives you the entire kolbasa feature set. Clustering exists for one reason: to scale write/read
throughput beyond what one database can handle, by **partitioning messages across several independent PostgreSQL nodes**.

The design goal is that clustering is *layered cleanly on top of* the single-node model, not woven into it:

- Each node is an ordinary kolbasa database — the same tables, the same schema, the same behavior described in
  [Architecture.md](Architecture.md). A node has no idea it is part of a cluster.
- The cluster layer is a small amount of routing logic — pick a node to write to, read from the nodes that own data — plus one
  shared table recording which node currently owns which partition.
- A standalone database is just a cluster of size one. The message-`id` layout (see [Message IDs.md](Message%20IDs.md)), the
  `shard` column, all of it is present from day one, so **a standalone queue can grow into a cluster without rewriting any rows or
  reshaping any table**. (The only schema change is an instant `ALTER SEQUENCE` that narrows the `id` identity to the node's own
  unique id range; existing rows are untouched and stay valid. It is applied automatically by kolbasa's schema update — no manual
  step.)

```
   Single node                Cluster (3 nodes)
   ───────────                ─────────────────

   ┌───────────────┐          ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐
   │  PostgreSQL   │          │       node A       │   │       node B       │   │       node C       │
   │               │          │      q_orders      │   │      q_orders      │   │      q_orders      │
   │   q_orders    │          │                    │   │                    │   │                    │
   │  (all rows)   │          │   shards 0..340    │   │  shards 341..681   │   │  shards 682..1023  │
   └───────────────┘          └────────────────────┘   └────────────────────┘   └────────────────────┘
                                         └───────────────────────┼───────────────────────┘
                                                  one logical "orders" queue
```

Everything below describes the right-hand side.

There is **no broker and no coordinator process** in front of those nodes. kolbasa is a library embedded in each of your
services, and that library *is* the routing layer. The client side barely changes between the two modes — the same services
embed the same library; only the number of databases they connect to grows.

**Before — standalone.** Every service embeds kolbasa and talks to a single database:

```
   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
   │ orders-service  │   │ billing-service │   │ worker-service  │
   │                 │   │                 │   │                 │
   │   ┌─────────┐   │   │   ┌─────────┐   │   │   ┌─────────┐   │
   │   │ kolbasa │   │   │   │ kolbasa │   │   │   │ kolbasa │   │
   └───┴────┬────┴───┘   └───┴────┬────┴───┘   └───┴────┬────┴───┘
            │                     │                     │
            └──────────┬──────────┴──────────┬──────────┘
                                  │   all services talk to
                                  ▼   the one database
                         ┌─────────────────┐
                         │   PostgreSQL    │
                         │  (single node)  │
                         └─────────────────┘
```

**After — cluster.** The same services, but now each service's kolbasa connects directly to every node and routes each message
itself:

```
   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
   │ orders-service  │   │ billing-service │   │ worker-service  │
   │                 │   │                 │   │                 │
   │   ┌─────────┐   │   │   ┌─────────┐   │   │   ┌─────────┐   │
   │   │ kolbasa │   │   │   │ kolbasa │   │   │   │ kolbasa │   │
   └───┴────┬────┴───┘   └───┴────┬────┴───┘   └───┴────┬────┴───┘
            │                     │                     │
            └──────────┬──────────┴──────────┬──────────┘
                       │  every service's kolbasa
                       │  connects to every database node
            ┌──────────┴──────────┬──────────┴──────────┐
            │                     │                     │
   ┌────────┴────────┐   ┌────────┴────────┐   ┌────────┴────────┐
   │     node A      │   │     node B      │   │     node C      │
   │   PostgreSQL    │   │   PostgreSQL    │   │   PostgreSQL    │
   └─────────────────┘   └─────────────────┘   └─────────────────┘
```

## Enabling cluster mode in code

Going from standalone to cluster is a **construction-time change only**. Each of the four
[operational roles](Architecture.md#operational-roles) is an **interface** with two implementations — a `Database*` one backed by
a single `DataSource`, and a `Cluster*` one backed by a `Cluster`. Your application code holds the *interface* type, so once the
object is built, every call is identical in both modes:

```
                     ┌──────────────┐
   your code  ──►    │   Producer   │   (the interface)
   holds this        └──────┬───────┘
                 ┌──────────┴─────────┐
                 ▼                    ▼
        ┌──────────────────┐ ┌─────────────────┐
        │ DatabaseProducer │ │ ClusterProducer │
        │   (standalone)   │ │    (cluster)    │
        └──────────────────┘ └─────────────────┘
```

`Consumer`, `Mutator`, and `Inspector` follow the identical shape — each is an interface with a `Database…` and a `Cluster…`
implementation (`DatabaseConsumer`/`ClusterConsumer`, `DatabaseMutator`/`ClusterMutator`, `DatabaseInspector`/`ClusterInspector`).

So the only thing that changes between modes is which implementation you construct.

**Standalone** wires a role to a single `DataSource`:

```kotlin
val producer: Producer = DatabaseProducer(dataSource)
val consumer: Consumer = DatabaseConsumer(dataSource)

producer.send(orders, message)
val messages = consumer.receive(orders, limit = 100)
```

**Cluster** introduces one new object — a `Cluster` — built from the **list** of per-node `DataSource`s, and then builds the
roles from that `Cluster` instead of from a raw `DataSource`:

```kotlin
// One DataSource per node — the cluster connects to all of them.
val cluster = Cluster(listOf(nodeA, nodeB, nodeC))

// Start the background poll that keeps each client's view of the
// shard-ownership map fresh (see "Cluster state").
cluster.initAndScheduleStateUpdate()

val producer: Producer = ClusterProducer(cluster)
val consumer: Consumer = ClusterConsumer(cluster)

// Identical from here on — same interfaces, same calls.
producer.send(orders, message)
val messages = consumer.receive(orders, limit = 100)
```

That is the whole migration of application code: build a `Cluster`, start its state updater, and replace
`DatabaseProducer`/`DatabaseConsumer` with `ClusterProducer`/`ClusterConsumer`. The `Mutator` and `Inspector` roles have the same
treatment (`ClusterMutator`, `ClusterInspector`) — see [The cluster roles](#the-cluster-roles). Everything else in this document
explains what that `Cluster` object does on your behalf.

### One `Cluster` object per kolbasa cluster

A `Cluster` object represents exactly one kolbasa cluster — the set of PostgreSQL nodes you pass into it. It is a long-lived
object that holds a connection (`DataSource`) to **every** one of those nodes and runs a background thread monitoring the
[shard-ownership map](#cluster-state), so it is **not** something to build per request, per queue, or per operation:

- **One object covers the whole cluster.** A `Cluster` is not scoped to a queue (queues are passed to each `send`/`receive` call,
  not to the `Cluster`) and not scoped to a node (it holds them all). The same instance serves every queue across the entire node
  set, however many there are.
- **Create it once, at startup, and share it.** Build the `Cluster` and call `initAndScheduleStateUpdate()` during application
  bootstrap — usually registered as a singleton in your dependency-injection container (Spring, Guice, Koin, …) — then inject it
  wherever roles are built. The `ClusterProducer` / `ClusterConsumer` / `ClusterMutator` / `ClusterInspector` wrappers are cheap,
  so creating one is never a problem — but in practice they are best treated the same way: build each once and keep it for the
  life of the application, since a single role instance already handles every queue and every node. You would keep more than one
  of the same role only to pin different defaults — e.g. two `ClusterProducer`s carrying different `ProducerOptions` — since those
  options are fixed when the wrapper is built. The heavyweight, shared thing they all point at is the one `Cluster`.

You only need more than one `Cluster` object if your application genuinely talks to more than one *separate* kolbasa cluster — one
object per cluster. That is uncommon in practice: because a single cluster scales out to 1024 nodes, most applications grow their
one cluster rather than run several side by side.

## Shards

A **shard** is a logical partition of a queue, numbered `0..1023` (10 bits → up to 1024 nodes' worth of partitioning). Every
message carries a `shard` value in its `shard` column. Shards are the unit of distribution and of migration.

> **Shards are a cluster concern, but adopt them from day one.** A shard selects *which node* a message lands on, so on a single
> node there is nothing to route and the selection has no effect yet — every row is stored with shard `0`. That is exactly why
> it's worth writing shard-aware code now anyway: choose a `ShardStrategy`, and pin shards where
> [co-location](#how-a-messages-shard-is-chosen) will matter. It costs nothing on one node, and when you later add nodes that same
> code immediately starts routing — no rewrite, no change to how you send.

A few properties:

- **Shards have no physical existence.** There is no "shard table." "The data of shard N" simply means "rows whose `shard`
  column equals N," spread across whatever queue tables exist. A shard is a label, not a container.
- **There are always exactly 1024 shards**, fixed. You do not create or destroy them; you only change *which node owns* each one.
  Growing the cluster moves shards between nodes; it never changes their number.
- **Each shard is owned by exactly one node** in the steady state — that node both stores new messages for the shard and is read
  from for it. (During [migration](#shard-migration) ownership is split transiently between two nodes; see below.)

### How a message's shard is chosen

The shard follows the same **override hierarchy** as every other producer setting (see
[Architecture.md → The override hierarchy](Architecture.md#the-override-hierarchy)): a broad default, narrowed where you need it,
where the **most specific layer that sets a value wins**.

```
Kolbasa.shardStrategy        process-wide default
  │
  └─ ProducerOptions.shard   this producer
       │
       └─ SendOptions.shard  this send() call (applies to its whole batch)
```

The lower two layers *pin* a shard — you set it explicitly via the options. The top layer, `ShardStrategy`, is the default that
chooses a shard for any message you don't pin:

| Strategy | Behavior |
|---|---|
| `Random` | A fresh random shard per message — spreads load evenly. |
| `Fixed(n)` | Always shard `n`. |
| `ThreadLocal` | A stable random shard per thread. |
| `ThreadLocalWithInterval(d)` | A per-thread shard that rotates every interval `d` (default 15 min). **This is the default.** |

Whichever layer supplies the value, any `Int` is accepted and normalized into the `0..1023` range.

Why would you reach for the overrides at all? Because a shard is the unit of **co-location**, and two single-node guarantees only
hold *within* one node:

- **Deduplication.** Unique indexes are per-table, so messages that must be deduplicated against each other must land on the same
  node — i.e. carry the same shard.
- **Ordering.** Ordered processing of related messages requires them on one node — again, one shard.

In both cases you derive the shard from whatever **business key** groups the related messages — for example `merchant_id`,
`sale_number`, `user_id`, or `tenant_id` — usually `Fixed(key.hashCode())`, so every message for the same entity deterministically
lands on the same shard, and therefore the same node.

These co-location concerns are why the `shard` column and the strategies exist even though they lie dormant on a single node (per
the note above). Writing shard-aware code up front costs nothing and means **no code changes** when you later cluster — only that
the shard you choose starts taking effect.

## Cluster state

We now know there are 1024 shards and that each is owned by one node — but *which* node owns *which* shard is not fixed by any
rule. The assignment is arbitrary (it starts random) and changes over the cluster's life as shards migrate, so it cannot be
computed; it has to be written down somewhere every client can read. That record is the one piece of genuinely shared state in a
cluster.

It is the **shard ownership map**: for each of the 1024 shards, which node produces it and which node consumes it. It lives in a
single internal table, `q__shard`:

```sql
create table q__shard(
    shard              int not null primary key,          -- 0..1023
    producer_node      varchar(100) not null,             -- node new messages are written to
    consumer_node      varchar(100),                      -- node read from for this shard (null while migrating)
    next_consumer_node varchar(100),                      -- migration destination (null while stable)
    check (
        (producer_node = consumer_node      and next_consumer_node is null) or  -- stable
        (producer_node = next_consumer_node and consumer_node      is null)      -- migrating
    )
);
```

The `check` constraint encodes the only two legal states a shard can be in; it is impossible to store an illegal one. We come
back to those two states in [Shard migration](#shard-migration).

**A snapshot, for concreteness.** In a healthy three-node cluster every shard is in the stable state — `producer_node` and
`consumer_node` are equal and `next_consumer_node` is empty — with the 1024 shards scattered across the nodes:

```
 shard | producer_node | consumer_node | next_consumer_node
-------+---------------+---------------+--------------------
     0 | node A        | node A        | (null)
     1 | node C        | node C        | (null)
     2 | node A        | node A        | (null)
   ... | ...           | ...           | ...
  1023 | node B        | node B        | (null)
```

(The `*_node` values shown are friendly labels; the real column holds each node's internal **node id** — an opaque 16-character string
generated when the node is first initialized, not an operator-assigned name. A row mid-migration looks different: `consumer_node`
is null and `next_consumer_node` names the destination — see [Shard migration](#shard-migration).)

Every client process holds a **`Cluster`** object (`kolbasa.cluster.Cluster`) — it opens connections to every node and keeps a
cached snapshot of this table, which all the routing below consults. Three things are worth knowing about how that state is
managed:

- **It lives on one node, not all of them.** The table is authoritative and unreplicated. When a `Cluster` starts, it scans the
  nodes for a fully-initialized `q__shard` (all 1024 rows); if none exists yet, it creates one on the node with the lowest node id and
  seeds all 1024 shards, each assigned to a **random** node in a stable state.
- **Clients discover state by polling.** That cached snapshot is refreshed on a timer — every
  `Kolbasa.clusterStateUpdateConfig.interval` (default **1 minute**). There is no push or live subscription. The practical
  consequence: after an operator changes ownership (a migration step), it takes up to one refresh interval for every client to
  notice. This lag is *designed for* — the migration protocol stays correct across it (see [Shard migration](#shard-migration)).
- **Startup also reconciles identity and schema.** Bringing up a `Cluster` remaps any colliding
  [buckets](Message%20IDs.md) and runs [schema generation](#schema-consistency) on every node, so all nodes share one
  identity scheme and one schema before traffic flows.

> **If the node holding `q__shard` is down.** Because the map is *cached* in every client, routing keeps working off the last
> snapshot — a brief outage of that node does not by itself stall producers or consumers. What stops is the *refresh*: each
> refresh reads from **all** nodes (it reconciles identity and schema too), so if any node is unreachable, the whole refresh fails
> and the cached map is simply kept as-is. A failed refresh also ends the periodic update loop — the schedule for the next tick is
> set only after a successful pass — so the snapshot then stays frozen until the `Cluster` is recreated (typically an application
> restart). The practical guidance: keep the node that hosts `q__shard` available, and restart clients if they have been running
> against an unreachable cluster long enough for the map to drift.

## Message routing

With shards and the ownership map in hand, routing is straightforward. Both directions consult the cached cluster state.

### Produce path

```
producer.send(orders, msg)
        │
        ▼
  pick shard  ── most specific wins:
        │          SendOptions.shard      (this send() call, whole batch)
        │            else ProducerOptions.shard   (this producer)
        │              else Kolbasa.shardStrategy  (process-wide default)
        ▼
  shardMap[shard] → producer_node   (in-memory; the cached q__shard snapshot, not a query)
        │
        ▼
  INSERT the rows on that node
```

That INSERT is the whole produce path. The node assigns the row its `id` locally, with no round-trip or consensus with the other
nodes, yet the value is still globally unique across the cluster — so clustering adds nothing to the cost of an insert (see
[Message IDs.md](Message%20IDs.md) for how a node mints a cluster-unique id on its own).

If the shard's `producer_node` is **not a current member of the cluster** — for example the cached map still points at a node
that has since been removed — the producer does **not** fail. Rather than drop the message, it stores it on another node instead,
preferring one that currently has an active producer (and falling back to any node if somehow none do). The reasoning is "better
to store the message on the wrong node than to lose it": a later migration relocates it to wherever its shard is owned. The cost
is that a message can briefly live on a node that doesn't currently own its shard.

(This fallback is about the target node being *absent from the cluster state*, not about a live connection failing. If the target
node is a known cluster member but its database is momentarily unreachable, the `send` surfaces that error normally — it is not
silently rerouted.)

### Consume path

```
consumer.receive(orders, limit)
        │
        ▼
  pick ONE node at random among the active-consumer nodes   (in-memory; the cached q__shard snapshot, not a query)
        │
        ▼
  receive from that node, restricted to the shards IT owns
        │
        ▼
  return those messages

consumer.delete(orders, ids)            ── acknowledgement
        │
        ▼
  shardMap[id.shard] → owning node, delete there   (in-memory; the cached q__shard snapshot, not a query)
        │
        ▼
  any ids still not deleted?  ── yes ──▶ retry the delete on ALL nodes
        │                                 (safety net for a shard that moved mid-flight)
        ▼
  return total deleted
```

Each `receive()` call talks to a **single** node and reads only the shards that node consumes; spreading calls over time spreads
consumption across the cluster. `delete()` (acknowledgement) is routed by the message's `shard` to its owning node; as a safety
net, any ids not deleted there (e.g. because a shard moved mid-flight) are retried against **all** nodes, so an ack is never lost
to a routing race.

## The cluster roles

The four [operational roles](Architecture.md#operational-roles) each have a cluster-aware implementation that wraps the
single-node roles and applies the routing above:

| Role | Cluster type | How it spans nodes |
|---|---|---|
| **Producer** | `ClusterProducer` | Routes each send to one node — the shard's `producer_node`. |
| **Consumer** | `ClusterConsumer` | Each `receive()` reads one node (its owned shards); `delete()` is shard-routed with an all-node fallback. |
| **Mutator** | `ClusterMutator` | By message id: shard-routed to the owning nodes (like `delete()`). By filter: fans out across all nodes, since a filter can match anywhere. |
| **Inspector** | `ClusterInspector` | Fans out across **all** nodes and merges the per-node results — how depends on the method (see below). |

**How the Inspector merges.** Every `Inspector` method queries all nodes and combines the answers; the combine rule fits the
question:

| Method | Returns | Merge across nodes |
|---|---|---|
| `count` | `Messages` (per-state counts) | **Sum** each state (scheduled, ready, in-flight, retry, dead). |
| `size` | `Long` | **Sum**. |
| `distinctValues` | `Map<value, count>` | **Sum** counts for the same key, then re-sort and re-apply the `limit` to the merged map. |
| `isEmpty` | `Boolean` | **AND** — empty only if **every** node reports empty. |
| `isDeadOrEmpty` | `Boolean` | **AND** — same, for dead-or-empty. |
| `messageAge` | `MessageAge` | Field-wise: **max** `oldest` and `oldestReady` (the longest age wins), **min** `newest` (the most recent wins). |

The mental model — three routing patterns cover every operation:

- **Address by id → the owning node(s).** A `delete()`, or a `Mutator` call that names message ids, routes by each id's shard to
  the node that owns it.
- **Scan → fan out to all nodes, then aggregate.** A filter-based mutate or any `Inspector` query can match anywhere, so it hits
  every node and combines the results.
- **Produce, and each `receive()` → a single node.** A send goes to the shard's `producer_node`; a `receive()` reads one
  randomly-picked consumer node.

Everything each node does locally is exactly the single-node behavior from [Architecture.md](Architecture.md).

## Shard migration

Over a cluster's life you periodically need to change which node owns which shards. Perhaps you are **adding nodes** and want to
hand some shards to them; perhaps you need to **drain a node** — emptying it of all shards so you can reboot, upgrade, or retire
it; or perhaps load has drifted **unevenly** and you simply want to rebalance. All three reduce to the same primitive: moving
shards from one node to another. That primitive is **migration**.

Migration is the heart of cluster operations and the reason the `q__shard` columns are shaped the way they are. The whole protocol
is built on the two legal shard states from the `check` constraint [above](#cluster-state):

```
STABLE                                   MIGRATING (A → B)
──────                                   ─────────────────
producer_node      = A                   producer_node      = B   (already the destination!)
consumer_node      = A                   consumer_node      = null
next_consumer_node = null                next_consumer_node = B
```

Read that carefully, because the migrating state is not the obvious one: **the moment migration begins, new messages already go
to the destination node B, while the shard temporarily has no consumer at all.** Migration is a three-step operator workflow
(driven by [`butcher`](Butcher.md); here we describe only what each step does to the data model).

For example, shard #42 is located on node A. Let's look at the data flows — produce and consume:

```
STABLE — shard #42 owned by node A

            ┌────────────┐
            │   client   │
            │  producer  │
            │  consumer  │
            └──┬─────────┘
        produce│      ▲ consume
               ▼      │
          ┌───────────┴─┐        ┌─────────────┐
          │   node A    │        │             │
          │  produce ✓  │        │   node B    │
          │  consume ✓  │        │             │
          └─────────────┘        └─────────────┘

     Produce writes to node A (client → A); consume reads from node A (A → client).
     Node B is part of the cluster but doesn't handle shard #42.
```

Now suppose we start migrating shard #42 from node A to node B. The data flows change the moment migration begins:

```
MIGRATING — shard #42 moving A → B  (after `prepare`, before `finalize`)

            ┌────────────┐
            │   client   │
            │  producer  │
            │  consumer  │
            └─┬────────┬─┘
      consume │        │ produce
              ✗        └──────────────┐
        (no consumer)                 ▼
          ┌──────────────┐   move  ┌──────────────┐
          │    node A    │ ──────▶ │    node B    │
          │  produce  ✗  │ (drain  │  produce  ✓  │
          │  consume  ✗  │  rows)  │  consume  ✗  │
          │  (old rows)  │         │              │
          └──────────────┘         └──────────────┘

     Produce already writes to node B; consume is dead (no consumer) until `finalize`.
     `move` drains #42's old rows from A to B; A stops handling #42 once finalized.
```

Once `finalize` completes, the shard is stable again — but now owned by node B. The picture mirrors the first one, with B in
node A's old role:

```
STABLE (after migration) — shard #42 now owned by node B

                                   ┌────────────┐
                                   │   client   │
                                   │  producer  │
                                   │  consumer  │
                                   └──┬─────────┘
                               produce│      ▲ consume
                                      ▼      │
          ┌─────────────┐        ┌───────────┴─┐
          │             │        │   node B    │
          │   node A    │        │  produce ✓  │
          │             │        │  consume ✓  │
          └─────────────┘        └─────────────┘

     Produce writes to node B (client → B); consume reads from node B (B → client).
     Node A is part of the cluster but no longer handles shard #42.
```

### Step 1 — prepare

Flip the shard from stable to migrating: set `producer_node = B`, `consumer_node = null`, `next_consumer_node = B`.

Immediately after prepare:
- **New messages for the shard go to node B.** Production has cut over.
- **The shard is consumed by no one.** Because `consumer_node` is `null`, no node lists this shard among the ones it reads.
- **The old rows still sit on node A**, not yet moved.

What does "consumed by no one" feel like to a running consumer? Not an error and not a stall — a `ClusterConsumer.receive()` keeps
working normally; this shard simply contributes no rows, exactly as if it were an empty queue. Messages already on node A for this
shard wait, untouched, until the migration finishes and consumption resumes on B. (This is why you don't leave a shard in the
migrating state indefinitely — see the caution at the end of this section.)

### Step 2 — move

**Move** the shard's existing rows from node A to node B — it genuinely relocates them: each batch is copied to B and then
**deleted from A**, so the rows end up on B alone, not duplicated. This is the long-running step. It is **safe to re-run**: rows
are inserted with `ON CONFLICT DO NOTHING` and deleted from the source only after they are safely on the target, so an interrupted
move can simply be run again — already-moved rows are skipped and nothing is lost. Run it as many times as needed until it reports
nothing left to move.

Move does not change `q__shard`. Producers are still writing to B; this step just drains A's existing backlog of the shard across
to B.

### Step 3 — finalize

Return the shard to a stable state on the destination: set `consumer_node = next_consumer_node` (i.e. B) and
`next_consumer_node = null`. Now `producer_node = consumer_node = B` — the shard is fully owned by B, and consumption of it
resumes there.

### Multi-shard migrations

Each shard migrates independently — its row in `q__shard` is flipped on its own. An operator can therefore have many shards
mid-migration at once (e.g. moving a third of the cluster's shards to a new node). The steps above apply per shard; `move`
naturally copies every shard currently in the migrating state, and `finalize` returns every migrating shard to stable.

> **Caution — finalize is global and does not verify the move.** Finalize promotes *every* shard that is currently in the
> migrating state, and it does not check that `move` actually finished copying. If you finalize before the data has fully moved,
> consumption resumes on the destination while un-moved rows are still on the source — and those rows are now owned by no one, so
> they sit unconsumed until you migrate that shard *back*. The safe sequence is always: prepare → move (re-run until it reports
> zero) → finalize. Treat finalize as the point of commitment.

## Schema consistency

Because a consumer may read from any node and a shard's data may be moved between nodes, **every node must carry the same kolbasa
schema** — the same queue tables, with the same columns and indexes. If node A has a `meta_priority` column and node B doesn't,
a message moved or routed between them can't be stored consistently.

kolbasa keeps schemas aligned for you: when a `Cluster` starts up, it runs the normal
[schema generation](Architecture.md#schema-generation-and-migration) against every node, so the queues you declare are created
everywhere with the same shape. Divergence generally arises only from out-of-band changes (a node restored from an old backup, a
manual `ALTER`, a node that missed a deploy).

Two consistency notions matter operationally, both surfaced by [`butcher`](Butcher.md)'s checks:

- **Cross-node schema divergence.** Every kolbasa table that exists anywhere should exist on every node with the same shape. The
  check compares columns, indexes, and the *presence* of the `id` identity — but **not** the identity's value range, which is
  deliberately different on each node (that's the per-node [bucket](Message%20IDs.md)). A node missing a table, or one
  whose table has different columns/indexes, is reported as a divergence.
- **Orphan tables.** A companion table (`_dlq` or `_arc`) whose **main queue table is missing on the same node**. This usually
  means a queue was dropped or only partially created on that node. Orphans are detected per node.

What happens at runtime if schemas *do* diverge: `move` refuses to copy a table whose shape differs between source and target
(it would risk silent data loss), failing fast instead. Ordinary produce/consume against a node simply use whatever schema that
node has — which is why a missing column on one node manifests as errors or missing data rather than a clean failure, and why the
schema-consistency check is the right place to catch it early.

## Failure modes

| Situation | What happens |
|---|---|
| A node is **unreachable during produce** | Routing is by the cached map, not by live probing, so a *known* node that is simply down is still chosen and the `send` fails (the caller can retry). Redirection happens only when the shard's `producer_node` is **absent from the cluster map** (e.g. a removed node) — then the message is stored elsewhere instead of lost, and relocated later by migration. |
| A node is **unreachable during consume** | Each `receive()` picks one consumer node from the map at random, without checking reachability first; if that node happens to be down, that call fails. A subsequent `receive()` re-rolls and may land on a healthy node. Messages on the down node are not lost — they wait until it returns. |
| A node is **unreachable during `move`** | The move can't drain that node's rows; re-run it once the node is back. Because move is re-runnable, an interruption is harmless. |
| **Stale cluster state** (between refreshes) | A client may briefly route by an old map after an operator changes ownership. The protocol tolerates the common cases: produce falls back when the map names a node that no longer exists, `delete()` broadcasts to all nodes for any ids its shard-routing missed, and a migrating shard reads as empty rather than wrong. |
| **Schema divergence** between nodes | Detected by the schema-consistency check; `move` fails fast rather than copying mismatched tables. Fix by re-running schema generation (or restoring the missing objects) on the lagging node. |
| Shard **left in migrating state** too long | Its messages on the source node are not consumed until finalize. Not data loss, but a growing backlog — finish the migration. |

## Expansion and shrinking

Both cluster-resize operations are just migrations, framed differently. (The operator commands live in [Butcher.md](Butcher.md);
here is the architectural shape.)

**Expansion — add a node.**

1. Stand up a new PostgreSQL database and add it to the cluster's node list. It joins with its own identity and bucket; it owns no
   shards yet.
2. Create the kolbasa schema (all your queue tables) on the new node, so it can hold data once shards migrate in. Any of three
   ways works — pick whichever fits how your application is wired:
   - **Do nothing — let the `Cluster` do it.** The easiest option *if you supplied the node list as a dynamic supplier*
     (`() -> List<DataSource>`) that already returns the new node. The `Cluster` re-reads the supplier on every refresh (every
     minute by default), so within one interval it picks up the new node and runs schema generation against it — the whole schema
     appears on the new node with no action from you. (You can force it immediately with `cluster.updateStateOnce()` instead of
     waiting for the tick.) This only creates queues if the `Cluster` was constructed with the `queues` list; an empty list means
     it manages routing only and leaves schema to you, via one of the next two options.
   - **Generate it directly.** Call `SchemaHelpers.createOrUpdateQueues(newDataSource, queues)` against the new node's
     `DataSource` — the same call kolbasa makes internally.
   - **Just restart.** If your application creates its queues at startup (the common pattern), restarting it against the enlarged
     node list creates the schema on the new node as a side effect of normal bootstrap.
3. Migrate a subset of shards from the existing nodes to it (prepare → move → finalize), rebalancing the 1024 shards across the
   now-larger set of nodes.

```
   before (2 nodes)            after expansion (3 nodes)
   A: shards 0..511            A: shards 0..340
   B: shards 512..1023         B: shards 341..681
                               C: shards 682..1023   ← migrated in
```

**Shrinking — remove a node.**

1. Migrate **all** of the node's shards onto the remaining nodes (prepare → move → finalize for each).
2. Once it owns no shards and holds no data, drop it from the node list and decommission the database.

```
   before (3 nodes)            after shrink (2 nodes)
   A: shards 0..340            A: shards 0..511
   B: shards 341..681          B: shards 512..1023
   C: shards 682..1023   ──►   (C drained, then removed)
```

There is no automatic rebalancer: the operator chooses which shards move where. A balance check can *propose* a
minimum-movement redistribution, but applying it is a deliberate, operator-driven sequence of migrations.

## References

| Type | Source | Role |
|---|---|---|
| `Cluster` | `kolbasa/cluster/Cluster.kt` | Holds node connections + cached state; schedules state refresh. |
| `ShardStrategy` | `kolbasa/cluster/ShardStrategy.kt` | Picks a message's shard at send time. |
| `ClusterProducer` / `ClusterConsumer` / `ClusterMutator` / `ClusterInspector` | `kolbasa/cluster/Cluster*.kt` | Cluster-aware roles. |

---

*See [Butcher.md](Butcher.md) — the operator CLI for running the checks and migrations described here.*
