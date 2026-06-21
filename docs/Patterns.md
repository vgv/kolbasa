# Common queue patterns

This document is a set of **recipes**. The other docs explain how kolbasa works; this one shows how to *use* it to
build the patterns people reach for most often in a message broker:

1. [Priority queue](#priority-queue) — process the most important messages first.
2. [Time-to-live (TTL)](#time-to-live-ttl) — make a message stop being deliverable after some wall-clock deadline.
3. [At-most-once delivery](#at-most-once-delivery) — accept losing a message rather than ever processing it twice.

All three are genuinely useful and turn up as named, first-class features in plenty of other brokers. kolbasa has no
dedicated knob for any of them — not because they're unsupported, but because they fall out so naturally from primitives
it already has that a special-purpose feature would just be a thin wrapper over a few lines you can write yourself. Each
pattern below is a small **composition** of building blocks that [Architecture.md](Architecture.md) already documents —
meta-fields and ordering, `delay` / `attempts` / `scheduled_at`, the visibility timeout, and where you acknowledge a
message. That is the whole point of this doc: once you see how the primitives combine, you can build these patterns (and
variations of them) yourself.

Throughout, we reuse the running `orders` queue from [Architecture.md](Architecture.md#the-queue-model) and link back to
the mechanics rather than repeating them.

---

## Priority queue

**Problem.** Messages are not equally urgent. A paid-express order should be picked up before a free-shipping one, even
if the express order arrived later. A plain queue hands out messages oldest-first; you want *most-important*-first.

**Mechanism.** kolbasa orders the visible set however you ask, as long as you order by an **indexed meta-field**. So a
priority queue is just:

1. a `priority` meta-field with a `SEARCH` index, and
2. a consumer that passes `order = PRIORITY.desc()`.

The ordering is evaluated on the database side, inside the same atomic claim that `receive()` already does — there is no
client-side sorting and no separate "priority queue" type.

```kotlin
val PRIORITY = MetaField.int("priority", FieldOption.SEARCH)   // indexed so we can order by it

val orders = Queue.of("orders", PredefinedDataTypes.ByteArray, Metadata.of(PRIORITY /*, … */))

// producer: stamp each message with its priority
producer.send(orders, SendMessage(payload, MetaValues.of(PRIORITY.value(10))))

// consumer: highest priority first
consumer.receive(
    orders,
    limit = 10,
    ReceiveOptions(order = PRIORITY.desc())
)
```

This is exactly the ordering shown in the README's [filtering-and-sorting example](../README.md#filtering-and-sorting) —
priority is just the most common thing to sort by.

**Tie-breaking.** Within one priority level you usually still want fairness (oldest-first). With no explicit `order`,
kolbasa already returns messages oldest-visible-first (by `scheduled_at`) — but the moment you set an `order`, that
default is replaced, and an explicit `order` can only reference **meta-fields** (not the system `scheduled_at` column).
So to keep an oldest-first tiebreaker *under* the priority sort, add a meta-field you can sort on — e.g. an `instant`
`enqueued_at` you stamp at send time:

```kotlin
ReceiveOptions(order = PRIORITY.desc() then ENQUEUED_AT.asc())   // priority first, then oldest within a priority
```

(If you order by priority alone, messages of equal priority come back in no guaranteed order.)

### Caveats

- **The index isn't free.** A `SEARCH` field adds an index that every insert must maintain. That's the right trade for a
  field you order by on every receive — but don't make a field `SEARCH` unless you actually filter or sort on it. See
  [`FieldOption`](Architecture.md#fieldoption--indexing-and-uniqueness).
- **Starvation.** A strict priority order means that as long as high-priority messages keep arriving, low-priority ones
  are *never* served. If your high-priority stream can saturate your consumers, low-priority work starves indefinitely.
  Two common mitigations:
  - **Aging.** Periodically bump the priority of old, low-priority messages so they eventually rise to the top. A
    `priority` meta-field is mutable in place with the [Mutator](Architecture.md#mutator) — but note kolbasa's mutations
    target `remaining_attempts` and `scheduled_at`, not arbitrary meta-fields, so true priority-aging means re-enqueuing
    (consume + re-send with a higher priority) rather than an in-place mutation.
  - **A reserved lane.** Run a separate consumer (or a separate queue) that always takes some low-priority work, so it
    can't be fully crowded out.
- **Priority is best-effort, not a hard guarantee.** Because consumers claim with `FOR UPDATE SKIP LOCKED` and messages
  can become visible at different times (delays, retries), you get *approximately* highest-first, not a strict global
  total order. For most workloads that is exactly what you want; if you need strict global ordering, a queue is the wrong
  tool.

---

## Time-to-live (TTL)

**Problem.** A message is only useful for a while. A "send the one-time login code" job is pointless ten minutes later;
a "refresh this cache entry" message is stale the moment a newer one is enqueued. You want a message to *stop being
deliverable* once its deadline passes, instead of being processed late.

**There is no built-in TTL.** kolbasa has no wall-clock expiry knob. (The word "expire" in the API refers to the
`attempts` countdown reaching zero — a *retry budget*, not elapsed time.) A message sits in the queue until it is
consumed or [swept](Architecture.md#sweep) as DEAD; nothing deletes a still-deliverable message just because it got old.

But TTL is straightforward to build from two primitives, one on each side. A TTL has two edges — "not before T" and "not
after T" — and kolbasa gives you one tool for each.

### "Not before" — `delay`

The lower edge is already a first-class feature: `delay` (a.k.a. `scheduled_at` in the future) hides a message until its
delay elapses. This is the [send-delay feature](../README.md#send-delay) — set it on the queue, producer, send, or
message:

```kotlin
producer.send(orders, SendMessage(payload, messageOptions = MessageOptions(delay = Duration.ofMinutes(5))))
```

### "Not after" — an `expires_at` meta-field + a receive filter

The upper edge is the part you build. The consumer-side [filter DSL](Architecture.md#querying-by-meta-field) can only
reference **meta-fields** — it can't see system columns like `created_at` — so to filter on a deadline you must store the
deadline *as a meta-field*. Add an indexed `instant` field and have the producer stamp it:

```kotlin
val EXPIRES_AT = MetaField.instant("expires_at", FieldOption.SEARCH)

val orders = Queue.of("orders", PredefinedDataTypes.ByteArray, Metadata.of(EXPIRES_AT /*, … */))

// producer: this message is good for 10 minutes
producer.send(
    orders,
    SendMessage(payload, MetaValues.of(EXPIRES_AT.value(Instant.now().plus(Duration.ofMinutes(10)))))
)
```

Now make every consumer skip messages whose deadline has passed:

```kotlin
consumer.receive(
    orders,
    limit = 10,
    ReceiveOptions(filter = EXPIRES_AT greater Instant.now())   // only still-live messages
)
```

An expired message no longer matches the filter, so it is never handed to a consumer. That gives you the delivery
behaviour you wanted.

### Reaping expired rows

The filter *hides* expired messages; it does not *remove* them. They stay in the table — still costing storage, still
counted as live, never DEAD — so [sweep](Architecture.md#sweep) won't clean them up on its own (sweep only removes DEAD
messages). To actually clear them, drive them to DEAD with the [Mutator](Architecture.md#mutator), then let sweep collect
them:

```kotlin
// run this on a schedule (cron, scheduled executor, …)
mutator.mutate(orders, listOf(SetRemainingAttempts(0))) {
    EXPIRES_AT lessEq Instant.now()   // every message past its deadline
}
```

`SetRemainingAttempts(0)` makes the matched messages [DEAD](Architecture.md#how-states-are-stored); the next sweep then
deletes them (or moves them to the [DLQ](Architecture.md#dlq--dead-letter-queue) if one is configured — handy if you want
to keep expired messages around for inspection). Pick the reaping cadence to match how fast you accumulate expired rows;
on a low-volume queue you may not need it at all.

### Caveats

- **Two halves, two places.** The deadline must be set by the producer *and* honoured by every consumer's filter. A
  consumer that forgets the filter will happily process expired messages.
- **Index the field.** `expires_at` must be `SEARCH` (or another indexed option) — both the receive filter and the
  reaping mutation order/scan on it.
- **`now` is the client's clock at receive time.** `Instant.now()` is evaluated in your JVM when you build the filter.
  For most TTLs (seconds to days) that's fine; just be aware it's not the database clock.

---

## At-most-once delivery

**Problem.** Some work is worse to do twice than to skip. Sending a "your code is `123456`" SMS twice annoys the user and
costs money; emitting the same metric twice corrupts a counter. For this kind of work you'd rather *occasionally drop* a
message than ever deliver it more than once.

**kolbasa is at-least-once by default.** That is the safe default, and it comes from two things working together: the
[visibility timeout](Architecture.md#how-states-are-stored) and the default attempt budget. When a consumer claims a
message, kolbasa atomically *decrements* `remaining_attempts` and pushes `scheduled_at` into the future to hide it. If
the consumer then deletes the message, it's done; if it crashes (or doesn't `delete()` before the timeout lapses) and the
message still has attempts left, the message reappears as [RETRY](Architecture.md#message-lifecycle) to be tried again.
That redelivery is exactly what you *don't* want for at-most-once work.

**Mechanism — `attempts = 1`.** Set the attempt budget to one and the redelivery path disappears. The key fact is that
the decrement happens **on the claim**, before your handler runs: with a budget of `1`, a message has
`remaining_attempts = 0` the instant it is received. There is no second attempt left to fall back into, so if processing
fails the message becomes [DEAD](Architecture.md#how-states-are-stored) rather than RETRY, and a DEAD message is never
handed to a consumer again. One delivery, guaranteed — and on failure, the message is dropped, not repeated.

```kotlin
val orders = Queue.builder("orders", PredefinedDataTypes.ByteArray)
    .options(QueueOptions.builder().defaultAttempts(1).build())   // one shot: no retries
    .build()

consumer.receive(orders)?.let { message ->
    process(message.data)              // a crash here ⇒ the message is already out of attempts: DEAD, never retried
    consumer.delete(orders, message)   // success ⇒ remove it
}
```

### Caveats

- **You are choosing loss over duplication.** Because the single attempt is spent the moment the message is claimed, *any*
  failed processing — a crash, a thrown exception, a timeout — *drops the message* with no retry. That is the deliberate
  trade: only pick at-most-once for work where a missed message is acceptable and a duplicate is not (fire-and-forget
  notifications, best-effort metrics, cache nudges).
- **Exactly-once isn't on the menu.** No queue gives true exactly-once delivery across a crash; at-least-once and
  at-most-once are the two honest options. What you *can* get is **effectively-once**: stay at-least-once, but make your
  handler idempotent or run the work and the ack in one transaction with the
  [`ConnectionAware*`](Architecture.md#working-inside-your-transaction) roles, so the side effect and the `delete()`
  commit (or roll back) together. For most "must not double-process" requirements, effectively-once is the better answer
  than at-most-once — reach for at-most-once only when dropping a message is genuinely preferable to the small machinery
  of idempotency.

---

## See also

- [Architecture.md](Architecture.md) — the primitives these recipes build on: [meta-fields](Architecture.md#meta-fields)
  and [`FieldOption`](Architecture.md#fieldoption--indexing-and-uniqueness),
  [ordering and filtering](Architecture.md#querying-by-meta-field), the [message
  lifecycle](Architecture.md#message-lifecycle) and [how states are stored](Architecture.md#how-states-are-stored), the
  [Mutator](Architecture.md#mutator), and [sweep](Architecture.md#sweep).
- [README](../README.md) — runnable examples for [send delay](../README.md#send-delay), [filtering and
  sorting](../README.md#filtering-and-sorting), and [deduplication](../README.md#deduplication).
- [Message state transitions.md](Message%20state%20transitions.md) — the lifecycle in depth, including retry and
  uniqueness semantics that the TTL and at-most-once patterns lean on.
