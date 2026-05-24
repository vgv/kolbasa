# Architecture

This document describes how kolbasa works on a **single PostgreSQL node** — the queue model, the tables it generates, message IDs,
meta-fields, deduplication, DLQ and archive queues, and the sweep that cleans everything up.

Everything here applies whether you run one database or a thousand: a single node is the complete, self-sufficient unit. Running
kolbasa as a multi-node cluster is a *deployment option*, not a requirement, and is covered separately in [Cluster
architecture.md](Cluster%20architecture.md). For the message lifecycle in depth, see [Message state
transitions.md](Message%20state%20transitions.md).

## Table of Contents

1. [What kolbasa is](#what-kolbasa-is)
2. [The queue model](#the-queue-model)
3. [Meta-fields](#meta-fields)
4. [Anatomy of a queue table](#anatomy-of-a-queue-table)
5. [`FieldOption` — indexing and uniqueness](#fieldoption--indexing-and-uniqueness)
6. [Querying by meta-field](#querying-by-meta-field)
7. [Indexes](#indexes)
8. [Message lifecycle](#message-lifecycle)
9. [How states are stored](#how-states-are-stored)
10. [Deduplication](#deduplication)
11. [DLQ — dead letter queue](#dlq--dead-letter-queue)
12. [Archive queues](#archive-queues)
13. [Sweep](#sweep)
14. [Operational roles](#operational-roles)
15. [Schema generation and migration](#schema-generation-and-migration)
16. [Configuration reference](#configuration-reference)

---

## What kolbasa is

Kolbasa is a **PostgreSQL-backed queue library**. Messages are rows in ordinary PostgreSQL tables; sending is an `INSERT`,
receiving is an `UPDATE ... RETURNING`, and acknowledging is a `DELETE`. There is no broker process and no separate system to
operate — if you already run PostgreSQL, you already run everything kolbasa needs.

It runs on **vanilla PostgreSQL** — no extensions, no `CREATE EXTENSION`, no superuser. Everything is built from standard SQL
(plain tables, indexes, and `GENERATED AS IDENTITY` columns), so kolbasa works on any stock PostgreSQL 10+, including locked-down
managed offerings like Amazon RDS, Cloud SQL, or Azure Database where you can't install extensions.

Choosing PostgreSQL as the substrate buys four things:

- **Existing infrastructure.** No new server to deploy, monitor, back up, or page someone about at 3am.
- **Transactional semantics.** Because messages live in your database, you can enqueue a message and commit your business data in
  the *same* transaction. Either both happen or neither does — no dual-write problem between your DB and a message broker.
- **Inspectability and hands-on debugging.** A queue is a table, so the tools you already have work on it. You can `SELECT` to see
  what's happening, and when something goes wrong you can intervene directly: fix a stuck message with an `UPDATE`, delete a
  poison message, or copy suspect rows into a scratch table to quarantine and investigate them later. In a pinch you can even
  enqueue a message by hand — a plain `INSERT` of the right columns is a valid message — to unblock a stuck consumer or replay
  something urgently. All of this from `psql`, no special tooling.
- **Standard operations.** Because the backend is just PostgreSQL, every operational practice you already run applies unchanged —
  backups and point-in-time recovery, `VACUUM`, connection pooling, replication, role-based access control, and your existing
  metrics and monitoring. There is nothing queue-specific to learn how to operate.

One PostgreSQL node gives you the *entire* feature set described in this document. Clustering (multiple nodes behind one logical
queue) exists only to scale throughput beyond what a single node can handle, and is layered cleanly on top of the single-node
model — see [Cluster architecture.md](Cluster%20architecture.md).

## The queue model

A **queue** is a named channel for messages, backed by exactly one PostgreSQL table. You create a queue by describing it in code —
its name, the data type of its payload, its options, and its optional meta-fields:

```kotlin
val orders = Queue.of("orders", PredefinedDataTypes.ByteArray)
```

Key properties of the model:

- **Many queues coexist in one schema/database.** Each queue is fully independent — its own table, its own indexes, its own
  configuration. There is no shared "messages" table.
- **A queue name maps directly to a table name.** The queue `orders` is stored in a table named `q_orders` (prefix `q_`). You
  never type the `q_` prefix yourself. This direct mapping is deliberate: you can always find a queue's table by name when
  inspecting the database directly.
- **A queue declares its payload type once.** The payload column's SQL type is fixed when the queue is created (see [data
  types](#payload-data-types) below).

We'll use this `orders` queue — a `bytea` payload, with the meta-fields added in the [next section](#meta-fields) — as the running
example throughout this document.

### Payload data types

A queue stores its payload in a single column whose SQL type you pick at creation time via `DatabaseQueueDataType`. Five types are
available:

| Factory (`PredefinedDataTypes` / `DatabaseQueueDataType`) | SQL column type | Notes |
|---|---|---|
| `ByteArray` / `Binary` | `bytea` | Most format- and serialization-agnostic. The safe default. |
| `String` / `Text` | `varchar` | Plain text payloads. |
| `Json` | `jsonb` | Queryable JSON; useful if you want to inspect/filter payloads in SQL. |
| `Int` | `int` | Numeric payloads. |
| `Long` | `bigint` | Numeric payloads. |

Each type carries a `serializer`/`deserializer` pair, so the payload your code sees can be any type you map to one of these column
types. If you have no special requirement, `PredefinedDataTypes.ByteArray` is the most flexible choice.

## Meta-fields

A **meta-field** is a typed, optionally-indexed attribute attached to every message in a queue — stored in its *own column*,
separate from the payload. Meta-fields exist so you can filter, sort, and deduplicate messages **without deserializing the
payload** (which can be expensive).

You declare meta-fields when defining the queue. This is the full `orders` queue we'll use for the rest of the document:

```kotlin
val MERCHANT_ID = MetaField.long("merchant_id", FieldOption.SEARCH)
val PRIORITY    = MetaField.int("priority", FieldOption.SEARCH)
val DEDUP_KEY   = MetaField.string("dedup_key", FieldOption.UNTOUCHED_UNIQUE)

val orders = Queue.of(
    "orders",
    PredefinedDataTypes.ByteArray,
    Metadata.of(MERCHANT_ID, PRIORITY, DEDUP_KEY)
)
```

### How meta-fields map to columns

Each meta-field becomes a column named `meta_<snake_case_name>` — the field name is lower-cased and camelCase is converted to
snake_case. So `merchant_id` → `meta_merchant_id`, and a field declared as `merchantId` would also become `meta_merchant_id`. The
column's SQL type follows the field type:

| Factory | SQL type |
|---|---|
| `byte`, `short` | `smallint` |
| `int` | `int` |
| `long` | `bigint` |
| `boolean` | `boolean` |
| `float` | `real` |
| `double` | `double precision` |
| `string` | `varchar(8192)` |
| `bigInteger`, `bigDecimal` | `numeric` |

So the `orders` queue gains three columns beyond the standard set: `meta_merchant_id bigint`, `meta_priority int`, and
`meta_dedup_key varchar(8192)`.

## Anatomy of a queue table

When you create the `orders` queue from the [previous section](#meta-fields) — a `bytea` payload plus the `merchant_id`,
`priority`, and `dedup_key` meta-fields — kolbasa generates this table:

```sql
create table if not exists q_orders(
    -- id: minvalue/maxvalue bound this node's id range. See Cluster architecture.md.
    id                 bigint generated by default as identity (minvalue 0 maxvalue 9007199254740991 cache 1000 cycle) primary key,
    uc                 int,
    opentelemetry      varchar(1024)[],
    shard              int          not null default 0,
    created_at         timestamp    not null default statement_timestamp(),
    scheduled_at       timestamp    not null,
    processing_at      timestamp,
    producer           varchar(256),
    consumer           varchar(256),
    remaining_attempts int          not null,
    -- meta-field columns
    meta_merchant_id   bigint,
    meta_priority      int,
    meta_dedup_key     varchar(8192),
    -- payload
    data               bytea        not null
);
```

The first ten columns are the same for *every* kolbasa queue. The three `meta_*` columns are specific to this queue's
meta-fields, and the `data` column's type (`bytea` here) is whatever payload type the queue declared.

Column by column:

| Column | Type | What it stores |
|---|---|---|
| `id` | `bigint` identity | Per-node-unique message identifier. The identity range (`minvalue`/`maxvalue`) is bounded to this node's id range — see [Cluster architecture.md](Cluster%20architecture.md). |
| `uc` | `int` | Internal bookkeeping used to map per-message send results; carries no queue meaning, safe to ignore. |
| `opentelemetry` | `varchar(1024)[]` | OpenTelemetry context for trace propagation, stored as a flat `[key1, val1, key2, val2, …]` array. Populated only when OpenTelemetry is configured. |
| `shard` | `int` | The message's shard (0–1023), used to route messages across nodes in a cluster. In standalone mode every row defaults to shard `0`, but the column always exists — see [Cluster architecture.md](Cluster%20architecture.md). |
| `created_at` | `timestamp` | When the row was inserted (`statement_timestamp()` default). Never changes. |
| `scheduled_at` | `timestamp` | When the message next becomes visible to consumers. Drives delay and visibility-timeout — see [How states are stored](#how-states-are-stored). |
| `processing_at` | `timestamp` | When the message was last taken for processing. `NULL` until first received. |
| `producer` | `varchar(256)` | Optional producer name, for debugging. Write-only — never returned on receive. |
| `consumer` | `varchar(256)` | Optional consumer name, for debugging. Write-only. |
| `remaining_attempts` | `int` | How many processing attempts remain. Decremented on each receive; at `0` the message is dead. |
| `data` | (payload type) | The message payload, in the SQL type chosen for the queue. |

You never write this DDL by hand — kolbasa generates and maintains it from your queue definitions. See [Schema generation and
migration](#schema-generation-and-migration).

## `FieldOption` — indexing and uniqueness

Each meta-field carries a `FieldOption` controlling whether it is indexed and whether it enforces uniqueness:

| `FieldOption` | Index created | Filter / sort | Unique |
|---|---|---|---|
| `NONE` | none | no | no |
| `SEARCH` | plain index | yes | no |
| `ALL_LIVE_UNIQUE` | partial unique index | yes | yes — across all *live* messages |
| `UNTOUCHED_UNIQUE` | partial unique index | yes | yes — across *untouched* messages only |

Choose the *minimal* option that meets your need: every index slows inserts. `NONE` is free; reach for the others only when you
actually filter, sort, or deduplicate on the field. In the `orders` queue, `merchant_id` and `priority` are `SEARCH` (we filter
and sort on them), while `dedup_key` is `UNTOUCHED_UNIQUE` (we use it to reject duplicate orders).

The two unique options differ in *which* messages count, and they map directly to the [derived states](#how-states-are-stored) —
the uniqueness is enforced by a **partial** index whose `WHERE` clause selects exactly the relevant states:

- **`ALL_LIVE_UNIQUE`** — unique across `SCHEDULED + READY + IN_FLIGHT + RETRY`. Index predicate: `where remaining_attempts > 0`.
- **`UNTOUCHED_UNIQUE`** — unique across `SCHEDULED + READY` only (a message becomes exempt once it's first picked up). Index
  predicate: `where remaining_attempts > 0 and processing_at is null`.

A DEAD message (`remaining_attempts = 0`) is excluded from both, so a new message with the same key can always be enqueued once
the old one has died. The full reasoning, with diagrams, is in [Message state transitions.md](Message%20state%20transitions.md).

## Querying by meta-field

Consumers filter and order on meta-fields with a small type-safe DSL — `filter` (`kolbasa.consumer.filter.Filter`) and `order`
(`kolbasa.consumer.order.Order`):

```kotlin
import kolbasa.consumer.filter.Filter.eq
import kolbasa.consumer.filter.Filter.greaterEq
import kolbasa.consumer.filter.Filter.and
import kolbasa.consumer.order.Order.desc

consumer.receive(
    orders,
    limit = 10,
    ReceiveOptions(
        // filter and order are independent — set either, both, or neither
        filter = (MERCHANT_ID eq 42L) and (PRIORITY greaterEq 5),
        order = PRIORITY.desc() // highest-priority orders first
    )
)
```

The available filter conditions are `eq`, `neq`, `greater`, `greaterEq`, `less`, `lessEq`, `between`, `like` (strings), `in`,
`isNull`, `isNotNull`, combined with `and` / `or` / `not`. For an expression the DSL doesn't cover, `nativeSql` takes a raw SQL
pattern referencing your meta-fields (use with care — it is not type-safe).

For ordering, each meta-field exposes `asc()` and `desc()`; pass a list to order by several fields. By default, messages are
returned oldest-visible-first (by `scheduled_at`); an explicit `order` overrides that within the visible set.

Filtering and sorting are only meaningful on fields with an index (`SEARCH` or one of the unique options).

> **Body or meta-field?** A rule of thumb: if you filter, sort, route, or alert on a value, it's a meta-field; if it's consumed
> only by your business logic when processing the message, it belongs in the payload. The `Queue.metadata` KDoc has a longer
> checklist.

## Indexes

To support efficient message selection, filtering, and sorting, kolbasa creates a set of indexes on each queue table. They fall
into two groups.

**Always present**, on every queue regardless of its meta-fields (in addition to the `id` primary key):

- An index on `(shard)`, used to select a node's locally-owned shards in cluster mode.
- An index on `(scheduled_at)` — the workhorse for message selection, since consumers order and filter by `scheduled_at`.

**One per indexed meta-field.** Each meta-field with an index option adds exactly one index; for the `orders` queue that is:

- An index on `(meta_merchant_id)`, from the `SEARCH` field — so consumers can filter and sort on `merchant_id`.
- An index on `(meta_priority)`, from the `SEARCH` field — likewise for `priority`.
- A partial *unique* index on `(meta_dedup_key)`, from the `UNTOUCHED_UNIQUE` field, with predicate `where remaining_attempts > 0
  and processing_at is null` — this is what enforces deduplication over untouched messages (see
  [`FieldOption`](#fieldoption--indexing-and-uniqueness)).

A field declared `FieldOption.NONE` adds its column but no index.

## Message lifecycle

A message moves through a small set of states. This is the 30-second version; the authoritative treatment — including retry
semantics and uniqueness scopes — is in [Message state transitions.md](Message%20state%20transitions.md).

```
                                                                          ┌────────────────────> DEAD
                                                                          │ (no attempts left)
                                                                          │
 ● ───────────> SCHEDULED ─────────────────> READY ─────────────────> IN_FLIGHT ─────────────────> COMPLETED
     send()                (delay expired)            ↑   receive()       │         delete()
                                                      │                   │
                                                      │                   │ (timeout)
                                                      └───── RETRY <──────┘
```

| State | Meaning |
|---|---|
| SCHEDULED | Inserted, but its delay has not yet elapsed; not visible to consumers. |
| READY | Visible and waiting to be received; never attempted. |
| IN_FLIGHT | Currently held by a consumer for processing. |
| RETRY | A processing attempt failed (visibility timeout expired); available again, with one fewer attempt. |
| DEAD | All attempts exhausted; logically gone (but physically still in the database), awaiting [sweep](#sweep) to remove it (or move it to the [DLQ](#dlq--dead-letter-queue)). |
| COMPLETED | Successfully processed and removed (or moved to [archive](#archive-queues)). |

## How states are stored

There is **no status column**. Kolbasa does not store `READY`, `IN_FLIGHT`, or `DEAD` anywhere — those names are a mental model. A
message's state is *derived* at query time from three columns: `scheduled_at`, `remaining_attempts`, and `processing_at`.

This is worth understanding because it explains the whole engine. Each state is a self-contained predicate over the three columns:

| State | Derived from |
|---|---|
| SCHEDULED | `processing_at is null` (untouched) **and** `scheduled_at > now` (not visible) **and** `remaining_attempts > 0` (not exhausted) |
| READY | `processing_at is null` (untouched) **and** `scheduled_at <= now` (visible) **and** `remaining_attempts > 0` (not exhausted) |
| IN_FLIGHT | `processing_at is not null` (touched) **and** `scheduled_at > now` (not visible) |
| RETRY | `processing_at is not null` (touched) **and** `scheduled_at <= now` (visible) **and** `remaining_attempts > 0` (not exhausted) |
| DEAD | `scheduled_at <= now` (visible) **and** `remaining_attempts <= 0` (exhausted) |
| COMPLETED | row deleted (or moved to archive) |

Lined up column by column, the same predicates make the pattern obvious — each column is one concept, and `—` marks a condition
that doesn't apply:

```
State       processing_at        scheduled_at         remaining_attempts
─────────────────────────────────────────────────────────────────────────
SCHEDULED   UNTOUCHED            NOT_VISIBLE          NOT_EXHAUSTED
READY       UNTOUCHED            VISIBLE              NOT_EXHAUSTED
IN_FLIGHT   TOUCHED              NOT_VISIBLE          —
RETRY       TOUCHED              VISIBLE              NOT_EXHAUSTED
DEAD        —                    VISIBLE              EXHAUSTED
COMPLETED   (row deleted, or moved to archive)
```

Two columns do most of the work:

- `processing_at` tells you whether the message has ever been picked up — `null` means never touched (SCHEDULED, READY), set means
  touched at least once (IN_FLIGHT, RETRY).
- `scheduled_at` tells you whether the message is visible right now — in the future means not visible (SCHEDULED, IN_FLIGHT), in
  the past means visible (READY, RETRY, DEAD).

`remaining_attempts` is the third factor: once it reaches zero the message can never be claimed again, but it only counts as DEAD
once it is also visible again (`scheduled_at <= now`). An exhausted message that is still in-flight (its visibility timeout hasn't
lapsed) is IN_FLIGHT, not DEAD — which is why the matrix leaves IN_FLIGHT's attempts column blank: at that point it doesn't
matter.

Receiving a message is a single atomic statement. Kolbasa selects visible rows with `FOR UPDATE SKIP LOCKED` (so concurrent
consumers never block each other or take the same row), and in the same statement marks them as taken:

```sql
-- simplified to the essential parts: select visible rows and claim them atomically
with id_to_update_cte as (
    select id from q_orders
    where scheduled_at <= statement_timestamp()  -- message is visible
      and remaining_attempts > 0                 -- message still has attempts
    order by scheduled_at                         -- default order, oldest first
    limit ?
    for update skip locked                        -- concurrent-safe
)
update q_orders set
    processing_at      = statement_timestamp(),                      -- advance processing_at to mark it claimed
    scheduled_at       = statement_timestamp() + <visibility timeout>, -- hide it until the timeout elapses
    remaining_attempts = remaining_attempts - 1
from id_to_update_cte where q_orders.id = id_to_update_cte.id
returning ...;
```

The two consequences of this single statement *are* the lifecycle:

- **Visibility timeout = pushing `scheduled_at` into the future.** A received message has `scheduled_at = now + timeout`, so it
  disappears from the visible set. If the consumer deletes it (success) before the timeout, it's gone. If not, `scheduled_at`
  lapses back into the past and the message reappears as RETRY — automatically, with no background process.
- **Attempts are a countdown.** Each receive decrements `remaining_attempts`. When it hits `0`, the message no longer satisfies
  `remaining_attempts > 0`, so it is never selected again. It is now DEAD, and [sweep](#sweep) will eventually remove it (or move
  it to the [DLQ](#dlq--dead-letter-queue)).

## Deduplication

Deduplication in kolbasa is **two independent layers**. Keeping them separate is the key to understanding it.

**Layer 1 — what counts as a duplicate** is the meta-field's uniqueness option ([above](#fieldoption--indexing-and-uniqueness)):
`ALL_LIVE_UNIQUE` or `UNTOUCHED_UNIQUE` create the partial unique index that defines collisions. No unique field, no
deduplication.

**Layer 2 — what happens on a collision** is the producer's `DeduplicationMode`, set on `ProducerOptions` (for every send) or
`SendOptions` (per call):

| `DeduplicationMode` | On a duplicate key |
|---|---|
| `FAIL_ON_DUPLICATE` (default) | The send fails. The duplicate — and, because failures are handled per batch, the rest of its batch — is rejected. Use when a duplicate signals a bug worth surfacing. |
| `IGNORE_DUPLICATE` | The duplicate is silently skipped (`INSERT … ON CONFLICT DO NOTHING`); every other message inserts normally. Use for idempotent sends. |

The difference shows up directly in the `SendResult` that `send()` returns. Each input message lands in exactly one bucket,
readable via `onlySuccessful()`, `onlyDuplicated()`, and `onlyFailed()` (with `failedMessages` as the failed count). Sending 100
messages of which one is a duplicate:

| | `onlySuccessful()` | `onlyDuplicated()` | `onlyFailed()` |
|---|---|---|---|
| `FAIL_ON_DUPLICATE` | 0 | 0 | 1 error covering the whole failed batch |
| `IGNORE_DUPLICATE` | 99 | 1 | 0 |

Under `FAIL_ON_DUPLICATE` a duplicate is reported as a *failure*, never a duplicate; under `IGNORE_DUPLICATE` it is reported as a
*duplicate*, never a failure. The two buckets are mutually exclusive per mode.

### Batching and partial inserts

A single `send()` does not insert messages one by one — that would be far too slow. The producer splits them into batches of
`batchSize` (default 500) and inserts each batch as one statement. Failures are therefore handled **at batch boundaries**: if any
message in a batch is invalid (e.g. a duplicate under `FAIL_ON_DUPLICATE`), the *whole batch* is rejected, not just that message.

What happens to the *other* batches is governed by `PartialInsert`:

| `PartialInsert` | On a failing batch | Use when |
|---|---|---|
| `PROHIBITED` | The entire `send()` fails — no message is inserted, even from clean batches. | All-or-nothing sends. |
| `UNTIL_FIRST_FAILURE` | Insert batches up to the first failing one, then stop; later batches are not sent. | You need to preserve causal ordering. |
| `INSERT_AS_MANY_AS_POSSIBLE` | Skip only the failing batch(es); insert every other batch. | Messages are independent and you want to land as many as possible. |

For example, sending 10,000 messages with `batchSize = 1000` where message 6,500 is invalid: `PROHIBITED` inserts 0,
`UNTIL_FIRST_FAILURE` inserts the first 6,000 (batches 1–6), and `INSERT_AS_MANY_AS_POSSIBLE` inserts 9,000 (every batch except
6001–7000). The README's [Partial insert and batching](../README.md#partial-insert-and-batching) section walks through a smaller
example with diagrams and a runnable sample.

### A cross-node note

Unique indexes are per-table, so deduplication works **within one node**. In a cluster, messages that must be deduplicated against
each other have to be routed to the same node — i.e. given the same shard (see [Cluster
architecture.md](Cluster%20architecture.md)). On a single node this is automatic.

## DLQ — dead letter queue

A **dead letter queue** captures messages that fail permanently instead of discarding them. Enable it per queue:

```kotlin
val orders = Queue.builder("orders", PredefinedDataTypes.ByteArray)
    .options(QueueOptions.builder().enableDlq().build())
    .build()
```

When enabled, kolbasa creates a **companion table** `q_orders_dlq`, reachable in code via `queue.deadLetterQueue` and usable with
every role (consumer, inspector, mutator, producer) just like any queue.

**When messages land in DLQ:** when a message exhausts its attempts and becomes [DEAD](#how-states-are-stored), the next
[sweep](#sweep) cycle moves it into the DLQ — atomically (a single `DELETE … INSERT`), rather than deleting it. Because sweep is
probabilistic, the move happens *eventually*, not at the instant of death.

**What the DLQ table looks like:** it mirrors the main table, with two differences:

- **Parent meta-fields are preserved as plain columns** — copied across, but with their indexes stripped (a DLQ doesn't need
  unique constraints or search indexes).
- **Provenance columns are added**, capturing the original message's identity and timing as of the moment it died:
  `meta_original_id_dlq`, `meta_original_created_at_dlq`, `meta_original_processing_at_dlq`, `meta_original_scheduled_at_dlq`
  (timestamps stored as epoch-millis `bigint`). The DLQ row gets its own fresh `id`; these columns let you trace it back to the
  source message.

The companion queue is configured for storage, not processing: zero delay and effectively unlimited attempts, so DLQ messages
don't themselves "expire" or cascade into another DLQ.

**Retention.** DLQ rows are cleaned up by the same sweep cycle, per `DlqOptions`:

| Option | Meaning | Default |
|---|---|---|
| `retention` | Delete rows older than this. | 30 days |
| `maxMessages` | Keep approximately this many rows, deleting the oldest first. The count is *estimated* (from table statistics, to avoid a full `count(*)`), so enforcement is approximate. | `null` (no limit) |

## Archive queues

An **archive queue** is the mirror image of a DLQ: it captures messages that **succeed**, for audit, compliance, or replay. Enable
it per queue:

```kotlin
val orders = Queue.builder("orders", PredefinedDataTypes.ByteArray)
    .options(QueueOptions.builder().enableArchiveQueue().build())
    .build()
```

This creates the companion table `q_orders_arc`, reachable via `queue.archiveQueue`.

**When messages land in archive:** the moment a consumer `delete()`s a message after successful processing, the message is moved
into the archive — atomically, *in the same operation* as the delete. (Contrast with DLQ, which moves DEAD messages later, during
sweep.)

| | DLQ (`_dlq`) | Archive (`_arc`) |
|---|---|---|
| Captures | failed messages (attempts exhausted) | successfully processed messages |
| Triggered by | sweep, after the message is DEAD | `delete()`, at success time |
| Timing | eventually (probabilistic sweep) | immediately, with the delete |

The archive table structure follows the same companion pattern: parent meta-fields preserved (indexes stripped) plus provenance
columns `meta_original_id_arc`, `meta_original_created_at_arc`, `meta_original_remaining_attempts_arc`,
`meta_original_processing_at_arc`. Retention is governed by `ArchiveQueueOptions` with the same `retention` / `maxMessages` knobs
(and defaults) as DLQ, enforced during sweep.

## Sweep

**Sweep** is kolbasa's garbage collection. It removes DEAD messages from a queue — deleting them, or moving them to the
[DLQ](#dlq--dead-letter-queue) if one is configured — and runs the [retention](#archive-queues) cleanup for DLQ and archive
companions.

The crucial property: **sweep has no background thread.** It piggybacks on `receive()`. On each receive, kolbasa rolls the dice;
if they come up, it runs a sweep pass on that queue before returning. This keeps kolbasa free of background machinery — there is
nothing to schedule or supervise — at the cost of sweep being *probabilistic and lazy* rather than real-time.

`SweepConfig` controls it:

| Option | Meaning | Default |
|---|---|---|
| `probability` | Chance per `receive()` of triggering a sweep, and the single on/off switch. `1.0` = every receive (constant `SWEEP_IS_ALWAYS_ON`); `0.0` = never, i.e. automatic sweep off (constant `SWEEP_IS_DISABLED`, also what `builder().disable()` sets). | `0.0001` (1 in 10,000) |
| `maxMessages` | How many rows a pass targets. For automatic sweeps this caps rows removed per pass; a manual `SweepHelper.sweep(…, limit)` removes up to `max(limit, maxMessages)`. | `10,000` |

What a sweep pass does, in order:

1. Remove DEAD messages from the queue — `DELETE`, or `DELETE … INSERT` into the DLQ if configured.
2. If a DLQ exists, run its retention cleanup (by age, and by count if `maxMessages` is set).
3. If an archive exists, run its retention cleanup likewise.

Because all of this is driven by the sweep dice, the timing of DLQ moves and retention is best understood as "eventually," not "on
a schedule."

### Running sweep yourself

The probabilistic model is convenient but not deterministic, and it only fires on queues that are actively being received from — a
queue no one consumes is never swept. When you need cleanup on a guaranteed cadence (a quiet queue, a nightly maintenance job,
predictable load), drive sweep manually with `SweepHelper`:

- **Disable the automatic pass** by setting `probability = 0.0` on `SweepConfig` (or call `builder().disable()`), so sweeps happen
  only when you ask.
- **Run a pass** with `SweepHelper.sweep(connection, queue, limit)`. It does exactly what an automatic pass does — removes DEAD
  messages (to the DLQ if configured) and runs DLQ/archive retention — and returns the number of messages removed. It sweeps up to
  `max(limit, SweepConfig.maxMessages)` rows, so a single call won't run unbounded on a large backlog; loop until it returns `0`
  to drain fully.

Schedule those calls from whatever you already use for periodic work — a cron job, a scheduled executor, your framework's task
scheduler. Sweep is just a method call that runs on a JDBC `Connection` you supply; it needs no background thread of its own.

## Operational roles

Kolbasa exposes four roles. Each comes in two flavours that share the same interface and behaviour, differing only in where they
get their database connection:

- a **`Database*`** implementation backed by a `DataSource` — it opens a connection, runs the operation, and commits, all on its
  own. This is what you use most of the time.
- a **`ConnectionAware*`** implementation that takes a JDBC `Connection` as the first argument of every method, and does *not*
  manage the transaction — it just runs its SQL on the connection you hand it. This is how kolbasa participates in a transaction
  you already control (see [Working inside your transaction](#working-inside-your-transaction)).

All of them operate on the same queue tables.

| Role | `DataSource` entry point | `Connection`-aware entry point | Reads | Writes | Use for |
|---|---|---|---|---|---|
| **Producer** | `DatabaseProducer` | `ConnectionAwareProducer` | — | inserts rows | Sending messages (sync or async, batched). |
| **Consumer** | `DatabaseConsumer` | `ConnectionAwareConsumer` | claims visible rows | updates (claim) / deletes (ack) | Receiving, processing, and acknowledging messages. Triggers [sweep](#sweep). |
| **Mutator** | `DatabaseMutator` | `ConnectionAwareMutator` | matches rows | updates `remaining_attempts` / `scheduled_at` | Out-of-band adjustment of live messages without consuming them. |
| **Inspector** | `DatabaseInspector` | `ConnectionAwareInspector` | read-only | — | Diagnostics: approximate counts, message ages, distinct meta values. |

### Working inside your transaction

The promise from [What kolbasa is](#what-kolbasa-is) — enqueue a message and commit your business data in the *same* transaction
— is delivered by the `ConnectionAware*` variants. Because they run on a `Connection` you supply and leave the commit/rollback to
you, a queue operation and your own SQL can share one transaction: **either both are committed, or both are rolled back.** This
closes the dual-write gap you'd otherwise have between your database and a separate message broker.

**Producing in a transaction.** When an order is paid you want to flip its status *and* enqueue the follow-up work it triggers,
with no chance of one happening without the other. Run both on the same connection:

```kotlin
val producer = ConnectionAwareDatabaseProducer() // no DataSource — uses the connection you pass

val connection = dataSource.connection // plain JDBC
connection.autoCommit = false
try {
    // your business logic here: mark the order as paid
    connection.prepareStatement("update orders set status = 'PAID' where id = ?").use { stmt ->
        stmt.setLong(1, orderId)
        stmt.executeUpdate()
    }

    // enqueue the post-payment work for this order on the SAME connection
    producer.send(connection, ordersQueue, orderId)

    connection.commit() // the status change and the queued job commit together
} catch (e: Exception) {
    connection.rollback() // neither the payment nor the job survives
    throw e
} finally {
    connection.close()
}
```

The same idea works with an ORM that exposes its connection — e.g. Hibernate's `session.doWork { connection -> … }` or a Spring
`@Transactional` method — so the message rides along with whatever transaction your framework is already managing.

**Consuming in a transaction.** The mirror image is just as useful: receive the job, do the post-payment writes it triggers, and
acknowledge it, all atomically. If any write fails and the transaction rolls back, the `delete()` is undone too, so the message
reappears and is retried later — you never lose work to a half-finished handler.

```kotlin
val consumer = ConnectionAwareDatabaseConsumer()

val connection = dataSource.connection
connection.autoCommit = false
try {
    consumer.receive(connection, ordersQueue)?.let { message ->
        val orderId = message.data

        // your business logic here: run the post-payment actions for this order
        connection.prepareStatement("update inventory set reserved = reserved + 1 where order_id = ?").use { stmt ->
            stmt.setLong(1, orderId)
            stmt.executeUpdate()
        }
        connection.prepareStatement("insert into shipments(order_id, status) values (?, 'PENDING')").use { stmt ->
            stmt.setLong(1, orderId)
            stmt.executeUpdate()
        }

        consumer.delete(connection, ordersQueue, message) // ack on the same connection
    }
    connection.commit() // the stock reservation, the shipment, and the ack commit together
} catch (e: Exception) {
    connection.rollback() // all of them are undone — the job reappears and is retried
    throw e
} finally {
    connection.close()
}
```

Mutator and Inspector have `ConnectionAware*` variants too, so any mix of roles can be chained onto one connection to build a
fully transactional pipeline. The `Database*` variants in the table below are simply the convenience case where kolbasa opens and
commits the transaction for you.

### Producer

Constructs and inserts messages. A single `send()` can carry many messages; kolbasa splits them into batches of `batchSize`
(default 500) and inserts each batch as one `INSERT … unnest(…)` statement. Behavior is governed by
[`ProducerOptions`](#configuration-reference) and refined per call by `SendOptions` / per message by `MessageOptions`.
Partial-batch failure handling is controlled by [`PartialInsert`](#deduplication) (see [Batching and partial
inserts](#batching-and-partial-inserts)).

### Consumer

Receives messages with `receive()` — the atomic claim described in [How states are stored](#how-states-are-stored) — optionally
filtered and ordered by meta-fields. After processing, `delete()` acknowledges a message (removing it, or archiving it). Not
deleting before the visibility timeout elapses causes the message to reappear as RETRY.

### Mutator

Adjusts messages *in place*, without receiving them. The supported mutations:

| Mutation | Effect |
|---|---|
| `AddRemainingAttempts(delta)` | `remaining_attempts += delta` |
| `SetRemainingAttempts(n)` | `remaining_attempts = n` |
| `AddScheduledAt(duration)` | `scheduled_at += duration` |
| `SetScheduledAt(duration)` | `scheduled_at = now + duration` |

This is how you revive DEAD messages (give them attempts back), kill a live message early (`SetRemainingAttempts(0)` makes it
DEAD), defer or expedite messages (shift `scheduled_at`), and so on. A single mutation call may not touch the same field twice.

### Inspector

Read-only introspection for monitoring, health checks, and debugging. It never modifies the queue. The interface exposes six
methods:

| Method | Returns | Notes |
|---|---|---|
| `count(queue, options)` | `Messages` — per-state counts (`scheduled`, `ready`, `inFlight`, `retry`, `dead`) | **Approximate.** Designed to be called often (e.g. once a minute for metrics). |
| `distinctValues(queue, metaField, limit, options)` | map of a meta-field's distinct values → counts | **Approximate.** E.g. "which tenants/shards have pending messages." |
| `messageAge(queue)` | `MessageAge` — age of the oldest, newest, and oldest *ready* message | Useful for lag/backlog alerting. |
| `size(queue)` | total table size in bytes (incl. indexes, TOAST) | Fast and independent of row count. |
| `isEmpty(queue)` | `true` if the queue has no messages in any state | Fast — stops at the first row. |
| `isDeadOrEmpty(queue)` | `true` if the queue has no *live* messages (empty, or only DEAD) | Fast in the common case; see the caveat below. |

**Why counts are approximate.** An exact `count(*)` grouped by state would scan the whole table — too slow to call on a monitoring
cadence for a large queue. So `count()` and `distinctValues()` instead read a *fraction* of the table using PostgreSQL
`TABLESAMPLE` and extrapolate. The fraction is tunable via `samplePercent` on `CountOptions` / `DistinctValuesOptions`; left at
the default (`YOU_KNOW_BETTER`), kolbasa picks a sampling level from the table's size. The trade-off is accuracy: rare states or
values may be under-represented or missed in the sample. Both also accept an optional `filter` to restrict which messages are
considered, and `distinctValues` can sort results by count via `order`.

**One performance caveat — `isDeadOrEmpty`.** Like `isEmpty`, it scans for the first *live* message and stops there, so it's
normally just as cheap. But if the queue contains *only* DEAD messages, there is no live row to stop at and the scan must read the
entire table. On a multi-gigabyte queue that is the one expensive case to be aware of.

That said, a large accumulation of DEAD messages is itself a red flag: it means [sweep](#sweep) isn't keeping up. Rather than work
around the slow scan, treat it as a signal to revise your sweep strategy — raise `probability` so automatic sweeps fire more
often, or, if you run sweep manually, increase its cadence or `maxMessages`. In a healthy queue DEAD rows are removed (or moved to
the DLQ) promptly, and this caveat never bites.

## Schema generation and migration

A kolbasa queue is more than one table. Each queue needs its own table and indexes, plus a separate table (also with indexes) for
the DLQ and for the archive. As your queues change, all of these have to change too. That is a lot to keep track of by hand.

You don't have to. Kolbasa takes care of its own schema. When you add a queue, add a meta-field, or turn on a companion table, it
finds what is missing and updates the database to match your code.

You describe queues in code and call:

```kotlin
SchemaHelpers.createOrUpdateQueues(
    dataSource, // where to apply the schema
    // the queues to create or bring up to date
    ordersQueue,
    paymentsQueue,
    emailQueue,
    newAccountQueue
)
```

This brings the database in line with your queue definitions. It is designed to be **idempotent and incremental** — safe to call
on every startup. It handles:

1. First use — the table doesn't exist; create it from scratch.
2. Evolved queue — a meta-field was added; add the missing column/index.
3. Evolved kolbasa — an internal column/index changed between versions; add or drop it.
4. Already current — nothing to do.

This check is fast, so calling it on every startup costs you almost nothing. Even with a few thousand queues it usually takes less
than a second to compare the database with your code and apply any changes. Most of the time (case 4) there is nothing to change,
and that path is even faster.

A few things happen automatically:

- **System table.** A small internal table `q__node` is created and seeded with this node's identity — this is what bounds the
  `id` range (see [Cluster architecture.md](Cluster%20architecture.md)). The `q__` prefix marks internal tables.
- **Companion tables.** Enabling DLQ or archive on a queue expands the work to create those companion tables too; you only pass
  the *main* queues.
- **Concurrent-safe index creation.** Indexes are created with `CREATE INDEX CONCURRENTLY`, so adding a meta-field to an existing
  queue doesn't lock it against writes.

If you want to review the DDL before running it, `generateCreateOrUpdateStatements` returns the statements without executing them.
You generally should not modify kolbasa's tables by hand; let schema generation own their structure.

**Bring your own migrator.** You don't have to let kolbasa apply the DDL. The `Schema` returned by
`generateCreateOrUpdateStatements` exposes the raw SQL — `tableStatements` and `indexStatements`, each a `List<String>` — so
instead of calling `createOrUpdateQueues`, you can feed those statements into whatever schema-migration tool your project already
uses ([Flyway](https://flywaydb.org/), [Liquibase](https://www.liquibase.org/), or [jOOQ](https://www.jooq.org/), for example).
This lets the queue DDL live alongside the rest of your migrations — versioned, reviewed, and rolled out through the same pipeline
— rather than being applied out of band at startup. One caveat: the indexes are emitted as `CREATE INDEX CONCURRENTLY`, which
cannot run inside a transaction, so configure those statements accordingly in your migrator (e.g. Flyway's `executeInTransaction =
false`).

## Configuration reference

`Kolbasa` is a **global singleton** — the settings on it are process-wide, shared by every queue, producer, and consumer in the
JVM. You normally set them once at startup; changing one affects everything. The remaining classes are ordinary per-instance
options you construct and pass to a specific queue, role, or call.

| Class | Configures | Key fields |
|---|---|---|
| `Kolbasa` (object) | Process-wide defaults | `sweepConfig`, `shardStrategy`, `asyncExecutor`, `prometheusConfig`, `openTelemetryConfig` |
| `QueueOptions` | A queue's defaults | `defaultDelay`, `defaultAttempts` (5), `defaultVisibilityTimeout` (60s), `dlqOptions`, `archiveQueueOptions` |
| `DlqOptions` | DLQ retention | `retention` (30d), `maxMessages` |
| `ArchiveQueueOptions` | Archive retention | `retention` (30d), `maxMessages` |
| `ProducerOptions` | A producer's defaults | `delay`, `attempts`, `producer`, `deduplicationMode`, `batchSize` (500), `partialInsert`, `shard`, `asyncExecutor` |
| `SendOptions` | One `send()` call | per-call overrides of the producer options |
| `MessageOptions` | One message | per-message `delay`, `attempts` |
| `ConsumerOptions` | A consumer's defaults | `consumer`, `visibilityTimeout` |
| `ReceiveOptions` | One `receive()` call | `consumer`, `filter`, `order`, `visibilityTimeout`, `readMetadata` |
| `MutatorOptions` | A mutator | `maxMutatedMessagesKeepInMemory` (100), `asyncExecutor` |
| `SweepConfig` | Sweep behavior | `probability` (0.0001), `maxMessages` (10,000) |

### The override hierarchy

Kolbasa's configuration is layered by **scope**. A setting is declared once at the broadest level that makes sense and then
narrowed where you need it — from a process-wide default, down through a queue, a producer or consumer, an individual `send()` or
`receive()`, and finally a single message. Each layer overrides the one above it; the **most specific layer that sets a value
wins**, and anything left unset inherits from the layer above:

```
Kolbasa                process-wide defaults (sweep, shard strategy, async executor, telemetry)
  │
  └─ QueueOptions      one queue's defaults (delay, attempts, visibility timeout, DLQ/archive)
       │
       ├─ producer side
       │    ProducerOptions   ──>   SendOptions    ──>   MessageOptions
       │    (this producer)         (this send())        (this message)
       │
       ├─ consumer side
       │    ConsumerOptions   ──>   ReceiveOptions
       │    (this consumer)         (this receive())
       │
       └─ mutator side
            MutatorOptions
            (this mutator)
```

Each role narrows to the depth its work needs. The producer side goes deepest — a *send* setting like `delay` or `attempts` can be
pinned all the way down to one message — while a *receive* setting like `visibilityTimeout` narrows through the consumer instead.
A few producer knobs (`deduplicationMode`, `partialInsert`, `batchSize`, `shard`) live only on the producer level and can be
overridden per `send()`. The mutator is the shallowest: it has no per-call options, so its settings live on `MutatorOptions`
alone.

The table above lists which setting lives on which class; the takeaway here is the *shape* — broad defaults you set once,
overridable at every narrower scope without re-stating the rest. For example, set `defaultDelay` once on the queue, and any
producer, `send()`, or individual message can still override it for just its own messages.

---

*Next: [Cluster architecture.md](Cluster%20architecture.md) — what changes when you run kolbasa across multiple nodes.*
