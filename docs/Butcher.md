# Butcher — the cluster operator CLI

`butcher` is the command-line tool for operating a kolbasa **cluster**: checking its health, and moving
shards between nodes to expand, shrink, or rebalance it. It is not part of the library API you call from
application code — it is a standalone tool an operator runs by hand (or from a runbook).

This guide assumes you understand the cluster model. If a term here is unfamiliar, read these first:

- [Architecture.md](Architecture.md) — the single-node model (queues, tables, meta-fields, DLQ/archive, sweep).
- [Cluster architecture.md](Cluster%20architecture.md) — what changes across multiple nodes (shards, the
  `q__shard` ownership map, routing, and the prepare → move → finalize migration protocol). **Butcher is the
  tool that drives that protocol**, so this guide and that doc are companions: the *why* lives there, the
  *how* lives here.

## Table of Contents

1. [Install](#install)
2. [The cluster config file](#the-cluster-config-file)
3. [The four commands](#the-four-commands)
4. [Command reference](#command-reference)
5. [Worked examples](#worked-examples)
6. [Troubleshooting](#troubleshooting)
7. [Limitations](#limitations)

---

## Install

butcher ships two ways. Both accept **identical** arguments, so every command in this guide works verbatim
whichever you choose.

### Docker (recommended)

```sh
docker pull ghcr.io/vgv/butcher:latest
```

The image's working directory is `/work`, so mount your current directory there and config-file paths behave
exactly as they would for a local jar. The full invocation — mount the current directory, run a command — is:

```sh
docker run --rm -v "$(pwd)":/work -w /work \
  ghcr.io/vgv/butcher:latest check-cluster cluster.conf
```

For **migrations** (anything that writes — prepare, move, finalize), pin a version instead of `:latest`. A
specific tag like `:1.2.3` always resolves to the same image, so it is the stable, reproducible choice. Since
`:latest` can be reassigned to a new build at any moment, pinning is worth it whenever a run actually matters —
it's exactly the "it worked yesterday" failure mode you don't want mid-migration:

```sh
docker run --rm -v "$(pwd)":/work -w /work \
  ghcr.io/vgv/butcher:1.2.3 prepare-migration --target=node-03 --shards=0,1,2 cluster.conf
```

That `docker run …` prefix is a lot to type every time, so wrap it in a small script on your `PATH` that
forwards its arguments to the container:

```sh
#!/bin/sh
# /usr/local/bin/butcher
# Pin the tag (e.g. :1.2.3) for migrations; :latest is fine for read-only checks.
exec docker run --rm -v "$(pwd)":/work -w /work \
  ghcr.io/vgv/butcher:1.2.3 "$@"
```

```sh
chmod +x /usr/local/bin/butcher
```

After that, `butcher check-cluster cluster.conf` and friends just work — exactly the form used everywhere in
this guide.

### Fat jar (no Docker)

Download `butcher.jar` from the [latest release](https://github.com/vgv/kolbasa/releases/latest) and run it
with a JRE 17+:

```sh
java -jar butcher.jar check-cluster cluster.conf
```

Everywhere this guide writes `butcher <command>`, the jar equivalent is `java -jar butcher.jar <command>`.

### Verify the install

```sh
butcher --version        # or: java -jar butcher.jar --version
```

This prints the build version, so you can confirm which butcher you're running before a migration.

> **A note on connectivity.** butcher connects **directly to every PostgreSQL node** listed in your config
> file. Run it from somewhere that can reach all of them. It does not talk to your application; it talks to
> the databases.

## The cluster config file

Every command takes one or more **config files** describing the cluster's nodes. The format is a plain text
file, one node per line:

```
<line-id>  host=<host> port=<port> user=<user> password=<pwd> dbname=<db> [schema=<schema>]
```

A complete two-node example (`cluster.conf`):

```
# orders cluster
line-1  host=db01.internal port=5432 user=app password=secret dbname=orders schema=public
line-2  host=db02.internal port=5432 user=app password=secret dbname=orders schema=public
```

### Keys

| Key | Required | Default | Notes |
|---|---|---|---|
| `host` | **yes** | — | PostgreSQL host. |
| `dbname` | **yes** | — | Database name. |
| `port` | no | `5432` | — |
| `user` | no | driver default | — |
| `password` | no | driver default | — |
| `schema` | no | driver default | butcher-specific (not a libpq key); sets the connection's `currentSchema`. |

Other rules:

- The **line-id** is the first whitespace-separated token on the line and must match `[a-zA-Z0-9_.-]+`. It is
  only the key butcher merges config files on (see below), so it can be any string you like — `a`, `b`, `c`,
  or `1`, `2`, `3`, or anything else.
- Unknown keys are silently ignored (so you can keep extra annotations in the file).
- Values may be single-quoted to include whitespace or special characters: `password='p a s s'`.
- Lines starting with `#` and blank lines are ignored.

### Multiple files merge by line-id

You can pass several config files; butcher **merges them by line-id** (union of keys, and on a key collision
the later file wins). The intended use is splitting **topology** from **secrets**:

`topology.conf` (commit to your repo):

```
line-1  host=db01.internal port=5432 dbname=orders schema=public
line-2  host=db02.internal port=5432 dbname=orders schema=public
```

`secrets.conf` (kept out of version control):

```
line-1  user=app password=s3cr3t-01
line-2  user=app password=s3cr3t-02
```

```sh
butcher check-cluster topology.conf secrets.conf
```

## The four commands

butcher has exactly four commands. The first is read-only; the other three are the three steps of the
migration protocol, run in order.

```
┌────────────────┐   ┌────────────────────┐   ┌────────────┐   ┌─────────────────────┐
│ check-cluster  │──▶│ prepare-migration  │──▶│ move-data  │──▶│ finalize-migration  │
│  (read-only)   │   │  (flip metadata)   │   │ (move rows)│   │  (cut over routing) │
└────────────────┘   └────────────────────┘   └────────────┘   └─────────────────────┘
   run anytime          small write op         long-running        small write op
                                               safe to re-run      point of no return
```

| Command | Writes? | Safe to re-run? | Typical duration | When |
|---|---|---|---|---|
| `check-cluster` | no | yes | seconds | anytime — diagnostics |
| `prepare-migration` | yes (only `q__shard`) | yes — re-run is a no-op if state is already set | seconds | start of a migration |
| `move-data` | yes (source + target rows) | yes — `INSERT ... ON CONFLICT DO NOTHING`, deletes only rows it just copied | minutes to hours | after prepare, before finalize |
| `finalize-migration` | yes (only `q__shard`) | yes | seconds | once the data is fully moved |

Run `butcher <command>` with no further arguments to see that command's built-in help.

> butcher reports outcomes with a clear banner: a green **"Completed successfully."** on success, or a red
> **"Invalid configuration."** / **"Execution error."** on the two common failure classes. The exit code is
> `0` on success and `1` on any failure, so it scripts cleanly. (A few rarer migration-specific errors print
> as a raw stack trace rather than a red banner — see [Troubleshooting](#troubleshooting).)

## Command reference

### `check-cluster`

```
butcher check-cluster [<check-name>] <config-file>...
```

Read-only inspection of the cluster. Safe to run at any time — it never modifies anything. If `<check-name>`
is omitted (or is `all`), every check runs. Before running any check, butcher confirms it can connect to
**every** node in the config (a fast `select 1` to each); if any node is unreachable it stops and tells you
which ones.

The available checks:

| `<check-name>` | What it reports |
|---|---|
| `shard-balance` | How the 1024 shards are spread across nodes, and a **proposed** minimum-movement rebalance if they are uneven. Proposes only — it does not move anything. |
| `schema-consistency` | Whether every kolbasa table exists on every node with the same shape (columns + indexes + identity *presence*). The per-node identity *value range* is meant to differ, so it is not compared. |
| `find-orphans` | Companion `_dlq` / `_arc` tables whose main queue table is missing on the same node. |
| `migration-state` | Which shards are currently mid-migration (between `prepare` and `finalize`), grouped by destination node. |
| `all` | Runs all of the above. This is the default. |

The concepts behind each check — what "balanced" means, why schemas must match, what an orphan is — are
explained in [Cluster architecture.md](Cluster%20architecture.md#schema-consistency). This guide focuses on
reading the output, shown under [Worked examples](#worked-examples).

### `prepare-migration`

```
butcher prepare-migration --target=<node-id> --shards=<0,1,2,...> <config-file>...
```

Marks the listed shards for migration to `--target`. Both flags are required. `--shards` is a comma-separated
list of shard numbers (0–1023).

`--target` is the destination node's **internal kolbasa node id** (the 16-character `q__node` identity), **not**
the config line-id. You normally don't type it by hand — `check-cluster shard-balance` proposes the moves and
prints the exact `target:` node id to copy into `--target`.

This updates **shard metadata only** — no data is copied. After it runs, for each listed shard:

- new messages are produced to the **target** node, and
- the shard is consumed by **no** node until you finalize (it reads as an empty queue in the meantime).

That is the **migrating** state described in
[Cluster architecture.md](Cluster%20architecture.md#shard-migration). Producers and consumers keep working
throughout; nothing blocks. Re-running with the same arguments is a no-op (the state is already set); re-running
with a *different* `--shards` list simply prepares those shards too.

butcher refuses two nonsensical requests up front:

- migrating a shard to the node that **already owns it** (`MoveToTheSameShardException`), and
- a `--target` that **isn't a known node id in the cluster** (`MoveToNonExistingNodeException`).

### `move-data`

```
butcher move-data [--include-tables=<t1,t2,...>] [--exclude-tables=<t1,t2,...>] <config-file>...
```

Moves the rows of every shard currently in the migrating state from their source nodes to the target. This is
the long-running step. It works table by table, in batches of 1000 rows: copy a batch into the target with
`INSERT ... ON CONFLICT DO NOTHING`, then delete those same rows from the source, and repeat until the source
holds no more rows for the migrating shards.

Two properties matter operationally:

- **It is safe to re-run.** The insert is idempotent (already-copied rows are skipped), and the delete only
  removes rows it just successfully copied. If a `move-data` is interrupted — crash, network blip, `Ctrl-C` —
  just run it again. It picks up where it left off and is finished when it reports nothing left to move.
- **It validates schemas first.** Before moving anything, butcher checks that every table to be moved has the
  same shape across all nodes. On a mismatch it stops with `InconsistentSchemaException` rather than risk
  copying rows into a differently-shaped table.

`--include-tables` / `--exclude-tables` restrict which queue tables are moved (by table name). A cluster often
accumulates tables that aren't part of live traffic — abandoned queues, scratch/experimental ones, or tables
being investigated — and there's no point spending a long migration relocating their rows. These flags let you
skip such tables and move only the queues that actually matter:

```sh
# Move only the live queues …
butcher move-data --include-tables=q_orders,q_payments cluster.conf
# … or move everything except a scratch/abandoned table.
butcher move-data --exclude-tables=q_experiment_tmp cluster.conf
```

`move-data` does not touch `q__shard`. Routing is unchanged by it — production is already going to the target
(that happened at `prepare`); this step only drains the source's backlog across.

### `finalize-migration`

```
butcher finalize-migration <config-file>...
```

Returns every migrating shard to a stable state owned by its destination: it sets `consumer_node` to the
pending `next_consumer_node` and clears the latter. After this, the destination both produces and consumes
each migrated shard — the migration is complete and consumption resumes there.

Two things to internalize about finalize:

- **It is global.** It promotes *every* shard currently in the migrating state, not a selected subset. There
  is no `--shards` flag.
- **It does not verify the move.** It does not check that `move-data` actually finished. If you finalize while
  rows are still on a source node, those rows become owned by no one and sit unconsumed until you migrate that
  shard back. So finalize is the **point of commitment** — run it only after `move-data` reports zero rows
  remaining.

The safe sequence is always: **prepare → move (re-run until it reports zero) → finalize.**

## Worked examples

The cluster in these examples is the `orders` cluster from
[Architecture.md](Architecture.md) — a single `orders` queue (table `q_orders`), now spread across PostgreSQL
nodes. Output is shown lightly trimmed (the `====` rule lines and ANSI colors are omitted for readability).

### Health check on a running cluster

Run all checks against a healthy two-node cluster:

```sh
butcher check-cluster cluster.conf
```

```
Completed successfully.
Shard balance:
  Current distribution (1024 shards across 2 nodes):
    node-01  512 shards [0, 1, 2, ..., 1022]
    node-02  512 shards [3, 4, 7, ..., 1023]
  Already balanced, no moves needed.
=================================================
Schema consistency: all tables consistent across cluster
=================================================
Orphan tables: no orphan queue tables
=================================================
Migration state: no shards in migration
```

A single check by name:

```sh
butcher check-cluster schema-consistency cluster.conf
```

### What trouble looks like

The checks above all came back clean. Here is what each one reports when something is actually wrong.

**An unbalanced cluster** — e.g. just after adding `node-03`, which owns no shards yet. `shard-balance` shows
the skew and proposes a minimum-movement rebalance:

```
Shard balance:
  Current distribution (1024 shards across 3 nodes):
    node-01  512 shards [0, 1, 2, ..., 1022]
    node-02  512 shards [3, 4, 7, ..., 1023]
    node-03  0 shards []
  Proposed moves (341):
    ⟶ node-03 (341 shards):
      shards: 0,1,2,3, ... ,340
      target: node-03
```

The shard-balance proposal is **advice**, not an action — you carry it out with the three migration commands
below.

**A schema that has drifted** — here `node-03` received a deploy that added a meta-field while the others
lagged, so its `q_orders` carries an extra column and index the reference nodes don't have:

```
Schema consistency: 1 table(s) inconsistent across cluster
  q_orders (reference: shape on [node-01, node-02]):
    node-03: +column meta_priority INT NOT NULL, +index q_orders_priority_j
```

The `+`/`-` are read relative to the **reference** shape (the one most nodes agree on): `+column` means that
node has a column the reference lacks, `-column` means it's missing one the reference has. A node missing the
table entirely shows simply `missing`.

**An orphan** — a `_dlq` companion left behind on a node whose main `q_orders` table is gone (a half-dropped
or half-created queue):

```
Orphan tables: 1 orphan queue(s) across 1 node(s)
  node-02:
    q_orders_dlq  (main queue q_orders missing)
```

### Cluster expansion — add a node

Goal: add a third node (it reports as `node-03` internally) to a two-node cluster and move a third of the
shards onto it.

1. Add the new node to the config file (a new `line-3` row) and bring its database up. (A fresh node joins
   owning no shards; its schema is created automatically when the application's `Cluster` next starts. Run
   `check-cluster schema-consistency` to confirm it matches before migrating onto it.)

2. Ask for a balanced plan and pick the shards to move:

   ```sh
   butcher check-cluster shard-balance cluster.conf
   # → proposes moving shards 0..340 to node-03
   ```

3. Prepare those shards for `node-03`:

   ```sh
   butcher prepare-migration --target=node-03 --shards=0,1,2, ... ,340 cluster.conf
   ```

   ```
   Completed successfully.
   Prepare
   Shards: [0, 1, 2, ..., 340]
   Target node: node-03
   Shards prepared to move (341):
       Shard #0 [producerNode=node-01, consumerNode=node-01, nextConsumerNode=null]=>[producerNode=node-03, consumerNode=null, nextConsumerNode=node-03]
       ...
   ```

   `Target node:` and the `producerNode=…` / `consumerNode=…` values are all **internal node ids** — the same
   ids you passed to `--target` and saw in the `shard-balance` proposal, not the config line-ids.

4. Move the data (re-run until it reports zero moved):

   ```sh
   butcher move-data cluster.conf
   ```

   ```
   Completed successfully.
   Move table 'q_orders' from DataSource(line-1 (db01.internal:5432/orders)) to DataSource(line-3 (db03.internal:5432/orders)) finished, migrated rows: 84213
   Move table 'q_orders' from DataSource(line-2 (db02.internal:5432/orders)) to DataSource(line-3 (db03.internal:5432/orders)) finished, migrated rows: 81902
   ```

   (`move-data` labels each source/target `DataSource` by its config **line-id** — `line-1`, `line-3` — since
   that is the handle that maps back to a row in your config file.)

5. Finalize:

   ```sh
   butcher finalize-migration cluster.conf
   ```

   ```
   Completed successfully.
   Finalize
   Shards moved to stable state (341):
       Shard #0 [producerNode=node-03, consumerNode=null, nextConsumerNode=node-03]=>[producerNode=node-03, consumerNode=node-03, nextConsumerNode=null]
       ...
   ```

A `check-cluster shard-balance` now shows roughly 341 / 341 / 342.

### Cluster shrinking — remove a node

Goal: drain `node-03` and decommission it. Shrinking is just a migration whose target is one of the
**remaining** nodes, repeated until `node-03` owns nothing.

1. Find which shards `node-03` currently owns — `shard-balance`'s current distribution lists the exact shard
   numbers next to each node's count. (`migration-state` separately confirms none are already in flight.)

2. Migrate all of `node-03`'s shards onto the nodes that will stay. You can move them all to one node and let
   a later rebalance even things out, or split them — here, all to `node-01`:

   ```sh
   butcher prepare-migration --target=node-01 --shards=<node-03's shards> cluster.conf
   butcher move-data cluster.conf            # re-run until zero
   butcher finalize-migration cluster.conf
   ```

3. Confirm `node-03` is empty:

   ```sh
   butcher check-cluster shard-balance cluster.conf   # node-03 → 0 shards
   ```

4. Remove that node's `line-3` row from butcher's config file and decommission its database.

5. **Before shutting the database down, remove the node from your application's `Cluster` configuration too** —
   that is the separate node list your services pass to `Cluster` (not this butcher config file). If a service
   still has the drained node in its list when the database goes away, it will keep trying to reach a node that
   is no longer there. Drop it from the application config and redeploy first; only then power the database off.

### Single-shard migration — a minimal test

The smallest possible migration — useful for rehearsing the workflow in staging. Move just shard `5` from its
current owner to `node-02`:

```sh
butcher prepare-migration --target=node-02 --shards=5 cluster.conf
butcher move-data cluster.conf
butcher finalize-migration cluster.conf
```

Between prepare and finalize, `butcher check-cluster migration-state cluster.conf` shows shard 5 in flight:

```
Migration state: 1 shard(s) in migration
  ⟶ node-02 (1 shards):
    shards: 5
    target: node-02
```

## Troubleshooting

butcher classifies failures and, for the two common classes, prints a red banner and exits `1`:

| Message you see | Class | What it means / how to fix |
|---|---|---|
| **Invalid configuration.** | bad input | Malformed command line or config file: a missing required flag (`--target` / `--shards`), a flag not in `--key=value` form, an unknown flag, no config file given, an unreadable file, a node missing `host`/`dbname`, a bad port, or a duplicate line-id in one file. The message names the specific problem; fix it and re-run. |
| **Execution error.** | runtime | Something went wrong reaching or operating the cluster. The two you'll actually hit: **a node is unreachable** (the message lists which nodes and the connection error), or **this isn't an initialized cluster** — `Initialized shard table q__shard not found. Is it a Kolbasa cluster?`, meaning no node has a fully-seeded 1024-row `q__shard` (the cluster has never been started by an application, or you pointed butcher at the wrong databases). |

A few migration-specific errors currently surface as a **raw stack trace** (still exit `1`) rather than a red
banner. If you see one of these, the exception name tells you the cause:

| Exception | Cause | Fix |
|---|---|---|
| `MoveToTheSameShardException` | `prepare-migration --target=X` for a shard node X already owns. | Pick a different target, or drop that shard from `--shards`. |
| `MoveToNonExistingNodeException` | `--target` isn't a known node id in the cluster (remember it's the internal `q__node` id, not the config line-id). | Re-run `check-cluster shard-balance` and copy the exact node id from its `target:` line. |
| `InconsistentSchemaException` | `move-data` found a table whose shape differs between nodes. | Resolve the drift first — run `check-cluster schema-consistency` to see exactly what differs, then re-run schema generation (start a `Cluster` against the lagging node) or restore the missing objects. Then re-run `move-data`. |

Two questions that come up often:

- **"`move-data` got interrupted — did I corrupt anything?"** No. It is safe to re-run; copies are idempotent
  and it only deletes rows from the source after they're safely on the target. Just run it again until it
  reports zero rows moved.
- **"I ran `prepare-migration` with the wrong shards."** Prepare only flips metadata; no data has moved yet.
  Re-run `prepare-migration` with the correct shards (you can prepare additional shards), and to undo a shard
  you flipped by mistake, prepare it back to its original owner. As long as you haven't run `move-data`, the
  rows never left their node.

## Limitations

- **butcher does not auto-rebalance.** `check-cluster shard-balance` *proposes* a minimum-movement plan, but
  carrying it out is a deliberate, operator-driven sequence of `prepare → move → finalize`. You choose which
  shards move where.
- **butcher does not edit cluster membership.** Adding or removing a node means editing the config file (and
  standing up or tearing down the database) yourself, around the migration commands — see the expansion and
  shrinking examples above.
- **All nodes must be reachable.** `check-cluster` refuses to run if any node is down, and `move-data` cannot
  drain a source it can't connect to. Bring offline nodes back before operating.
- **`finalize-migration` is global and unchecked.** It promotes every in-flight shard and does not verify the
  move completed. Always finalize last, only after `move-data` reports zero — see
  [Cluster architecture.md](Cluster%20architecture.md#shard-migration) for the full reasoning.

---

*Back to: [Cluster architecture.md](Cluster%20architecture.md) · [Architecture.md](Architecture.md)*
