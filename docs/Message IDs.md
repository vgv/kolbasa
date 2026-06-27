# Message IDs

Every kolbasa message has an `id` — a 64-bit `bigint` assigned automatically when the message is stored. This document
explains how that `id` is structured so that it stays **globally unique across every node**, even though no node coordinates
with any other when minting one. That single property is what lets a row keep its `id` when it is relocated between nodes, and
what lets a standalone database grow into a cluster with no id migration at all.

It applies to both deployment modes. On a single node the structure below is still present — a standalone database is simply a
cluster of one — so the same id layout is in force from day one. For the cluster-level routing that *uses* these ids, see
[Cluster architecture.md](Cluster%20architecture.md).

## Anatomy of an `id`

The `id` column is a 64-bit `bigint` with a deliberate internal structure:

```
 bit 63        bits 62..53                       bits 52..0
┌──────┬───────────────────────────┬────────────────────────────────────────────┐
│ sign │      bucket (10 bits)      │               value (53 bits)               │
│  = 0 │   node-of-origin: 0..1023  │        per-node monotonic counter           │
└──────┴───────────────────────────┴────────────────────────────────────────────┘
   1 bit          10 bits                              53 bits
```

- The **sign bit** is always `0` — ids are non-negative.
- The **bucket** (high 10 bits) identifies the node that minted the id. Each node is assigned a bucket `0..1023`, and its `id`
  identity column is bounded to that bucket's range: `[bucket << 53, (bucket << 53) | (2^53 − 1)]`. Those bounds are the
  `minvalue`/`maxvalue` you see on the `id` column in the generated DDL.
- The **value** (low 53 bits) is that node's own monotonically increasing counter within its bucket.

Because every node draws ids from a **disjoint** range, **each id is globally unique on its own** — no cross-node coordination is
needed to avoid collisions. For example:

| `id` (decimal) | bucket (`id >> 53`) | value (`id & (2^53−1)`) |
|---|---|---|
| `1` | 0 | 1 |
| `9007199254740992` | 1 | 0 |
| `9007199254740993` | 1 | 1 |

The first row is bucket 0 (a standalone node, or node 0 of a cluster); the next two are the first ids minted by bucket 1.

> **Bucket is not shard.** This is the one easy confusion in a cluster. The **bucket** (these high `id` bits) records *which node
> minted the message*; the **shard** (the separate `shard` column — see [Cluster architecture.md → Shards](Cluster%20architecture.md#shards))
> records *which node the message is routed to*. They are numbered over the same `0..1023` range and share nothing else — they are
> chosen independently, at different times, for different reasons. A message minted by node-bucket 5 can carry shard 900. Routing
> is purely a matter of the **shard**; the bucket only governs id uniqueness.
>
> In practice the bucket is an internal detail — neither operators nor application developers need to reason about it. The only
> placement decision that is yours to make is how to spread data across **shards**; the bucket takes care of itself.

## How buckets are assigned

**Buckets are assigned automatically, and collisions self-heal.** A node's bucket lives in its `q__node` table (the same
internal table that holds the node's identity), and you never set it by hand. It is assigned in two layers:

- **Seeding (first init).** The bucket is written once, when a node's `q__node` table is first created. A standalone database
  seeds bucket `0`. In a cluster, each node is instead seeded by its position in the configured node list — `0, 1, 2, …` — so
  freshly initialized cluster nodes normally get **distinct** buckets with no further work. Seeding only applies to a node that
  has no `q__node` table yet; a node that already has one keeps whatever bucket it was first given.
- **Reconciliation (every `Cluster` start).** Seeding is not enough on its own — two nodes can still end up claiming the same
  bucket, most commonly when a database that was previously run **standalone** (already sitting at bucket `0`) joins a cluster
  whose position-0 node also wants bucket `0`. So each time the `Cluster` object starts, it reads every node's bucket and, if a
  duplicate exists, **remaps it to the next free bucket** before any new ids are minted into the wrong range. The first node to
  claim a bucket keeps it; only the later duplicate is moved. This repeats until every node holds a distinct bucket.

The remap itself just updates the node's bucket in `q__node`; schema generation then repoints the `id` identity at the new
bucket's range with an `ALTER SEQUENCE` (`minvalue`/`maxvalue` and a `restart`) — both steps are instant and touch no existing
rows.

**Uniform with standalone mode.** This layout is identical in standalone and cluster deployments — a standalone database simply
occupies **one bucket** (`0` by default, so all its ids share the same high bits). This is what lets a standalone queue become
a cluster member with no id migration: the ids it minted on day one are already globally unique and correctly bucketed, and
remap (if it happens) only changes the sequence for *future* ids — old rows keep their original ids and remain unique.

> **Caution — this is "grow," not "merge."** The self-heal above covers *future* ids only. If you try to **merge two databases
> that were each previously run standalone**, both have historical rows in bucket `0` (each minted ids `1, 2, 3, …` in its own
> isolated counter), so their existing ids collide. Bucket remapping does not relocate old rows, and `move-data`'s
> `INSERT ... ON CONFLICT DO NOTHING` would silently drop one side's colliding rows during migration. The "grow into a cluster
> with no id migration" guarantee applies to taking **one** standalone database and adding fresh nodes around it — not to
> stitching multiple pre-existing standalone deployments together.
