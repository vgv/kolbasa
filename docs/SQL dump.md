# SQL dump

kolbasa builds its SQL itself. SQL dump prints these statements as they run, with the time, the duration and the
number of rows. Use it to see what kolbasa does with a queue, to find a slow statement, or to take a statement to
`EXPLAIN`.

It is a debugging tool. It is off by default. You turn it on for the queues and statements you want to look at.

## Table of Contents

1. [Turning it on](#turning-it-on)
2. [What it prints](#what-it-prints)
3. [Statement kinds](#statement-kinds)
4. [Things to know](#things-to-know)
5. [Other ways to see the SQL](#other-ways-to-see-the-sql)

---

## Turning it on

```kotlin
Kolbasa.sqlDumpConfig = SqlDumpConfig.builder()
    .enable()
    .writer(PrintWriter(System.out))
    .queue(ordersQueue, StatementKind.CONSUMER_SELECT, StatementKind.CONSUMER_DELETE)
    .build()
```

There are three things to set:

- `enable()` turns it on. Without it nothing is printed.
- `writer(...)` says where the text goes. The default writer drops everything.
- `queue(queue, kinds...)` says which queue and which [statements](#statement-kinds). Call it once for each queue. To
  print every statement of a queue, pass no kinds: `queue(ordersQueue)`. A second call for the same queue replaces its
  kinds.

To turn it off, set the default back:

```kotlin
Kolbasa.sqlDumpConfig = SqlDumpConfig()
```

## What it prints

Each statement becomes one record:

```
---------------------------------------------
Date: 2026-11-04 21:25:23.620
Queue: orders
Kind: PRODUCER_INSERT
Duration: 3.934ms
Rows: 1
insert into q_orders(scheduled_at,remaining_attempts,shard,…,data) select … from unnest(?::bigint[],?::int[],?::varchar[]) with ordinality as t(delay,remaining_attempts,data,ord) returning id
---------------------------------------------
Date: 2026-11-04 21:25:23.636
Queue: orders
Kind: CONSUMER_DELETE
Duration: 0.596ms
Rows: 1

            with ids_as_values(id,shard) as (values (0,0))
            delete from q_orders
            where (id, shard) in (table ids_as_values)

```

- `Date`: when the statement started, in the time zone of the JVM.
- `Queue`: the queue whose call ran the statement.
- `Node`: the node the statement ran on. Only in a cluster.
- `Kind`: the [kind](#statement-kinds) of the statement.
- `Duration`: how long it took, in milliseconds. For a receive, this includes reading the rows.
- `Rows`: how many messages the statement handled: inserted, received, deleted or changed. Duplicates that were not
  inserted don't count.
- Then the SQL, as kolbasa built it. The indentation can look odd.

Values passed as parameters are not printed. They show as `?`. This is the case for the insert, the receive, and the
filter of a mutate by filter. The other statements have all their values in the text. You can put them into `EXPLAIN`
as they are. Be careful with `EXPLAIN ANALYZE`: it really runs the statement.

## Statement kinds

| Kind | What the statement does | Who runs it |
|---|---|---|
| `PRODUCER_INSERT` | Inserts messages. One statement per chunk, see `ProducerOptions.chunkSize`. | `send()` |
| `CONSUMER_SELECT` | Picks messages and marks them as in flight. | `receive()` |
| `CONSUMER_DELETE` | Deletes processed messages. | `delete()` |
| `CONSUMER_DELETE_TO_ARCHIVE` | Moves processed messages to the archive queue. | `delete()`, if the queue has an archive |
| `SWEEP` | Deletes DEAD messages. | [sweep](Architecture.md#sweep) |
| `SWEEP_TO_DLQ` | Moves DEAD messages to the DLQ. | sweep, if the queue has a DLQ |
| `RETENTION_CLEANUP` | Deletes old messages from a DLQ or an archive queue. | sweep |
| `MUTATE_BY_ID` | Changes messages by id. | mutator |
| `MUTATE_BY_FILTER` | Changes messages by filter. One statement per iteration. | mutator |

`RETENTION_CLEANUP` is printed for the main queue, as a step of its sweep. The SQL itself works on the table of the DLQ
or the archive.

The DLQ and the archive are queues of their own. When your code reads the DLQ or changes messages in the archive, these
statements are printed for them, not for the main queue. To see them, register the DLQ or the archive too:
`.queue(ordersQueue.deadLetterQueue!!)`.

Some statements are never printed: the inspector's queries (also those behind the
[Prometheus queue gauges](Prometheus%20metrics.md#how-queue-gauges-are-updated)), schema creation and migration, and
the cluster's own bookkeeping.

## Things to know

- There is one setting for the whole JVM, `Kolbasa.sqlDumpConfig`. There is no switch per producer or consumer.
- It is read on every statement, so a change takes effect at once. On busy queues, one assignment can start a flood of
  output. Pick only the queues and kinds you need.
- Queues are matched by `Queue.name`.
- kolbasa doesn't synchronize calls to the writer, and many threads can write at once. `PrintWriter` and the other
  writers from the JDK lock themselves. A writer of your own must be thread-safe.
- kolbasa calls `flush()` after every statement. With a lot of records this can slow your application down. In that
  case, write your own `Writer` that decides when to really flush. For example, it can keep the records in a buffer and
  flush only on every 100th call. It must be thread-safe too.
- The writer must not throw. An exception from it fails the call that ran the statement, for example `send()`.

## Other ways to see the SQL

PostgreSQL can log statements too, with `log_min_duration_statement` or the `auto_explain` module. That log sees every
client and shows the parameter values. It needs access to the server settings.

[p6spy](https://github.com/p6spy/p6spy) works on the JDBC level. It wraps your JDBC driver or `DataSource` and logs
every statement that goes through it, with the parameter values. It sees all the SQL of your application, not only
kolbasa's. It needs a change in how your application connects to the database.

SQL dump works inside kolbasa. It needs no server access and no changes to the connection. You choose the queues and the
kinds of statements, and you can turn it on and off at runtime.
