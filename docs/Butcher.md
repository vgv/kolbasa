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
3. [The commands](#the-commands)
4. [Command reference](#command-reference)
5. [Worked examples](#worked-examples)
6. [Troubleshooting](#troubleshooting)
7. [The same thing from code](#the-same-thing-from-code)
8. [Limitations](#limitations)

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
<node-id>  host=<host> port=<port> user=<user> password=<pwd> dbname=<db> [schema=<schema>]
```

A complete two-node example (`cluster.conf`):

```
# orders cluster
node-01  host=db01.internal port=5432 user=app password=secret dbname=orders schema=public
node-02  host=db02.internal port=5432 user=app password=secret dbname=orders schema=public
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

- The **node id** is the first whitespace-separated token on the line. It must match `[a-zA-Z0-9_.-]` and be
  at most 100 characters long. It is the node's own id, see [Node ids](#node-ids) below.
- Unknown keys are silently ignored (so you can keep extra annotations in the file).
- Values may be single-quoted to include whitespace or special characters: `password='p a s s'`.
- Lines starting with `#` and blank lines are ignored.

### Node ids

Every node has an id of its own. kolbasa keeps it in the node's `q__node` table, and it never changes. The id
on a line must be that id.

Every command checks this before it does anything else. If a line names a node wrongly, the command stops and
changes nothing. The error shows the node's real id next to each wrong line.

A node with no id yet gets the one from its line. That is how a new node joins: add its line, then run
`check-cluster`. It prints `Initialized node <id>`.

Do this **before** the application's `Cluster` sees the new node. `Cluster` gives a node with no id a random
one, and then the line no longer matches.

A cluster that already runs has its ids already. To find them, run `select id from q__node` on each node, or
run `check-cluster` with any ids and read the error.

### Multiple files merge by node id

You can pass several config files; butcher **merges them by node id** (union of keys, and on a key collision
the later file wins). The intended use is splitting **topology** from **secrets**:

`topology.conf` (commit to your repo):

```
node-01  host=db01.internal port=5432 dbname=orders schema=public
node-02  host=db02.internal port=5432 dbname=orders schema=public
```

`secrets.conf` (kept out of version control):

```
node-01  user=app password=s3cr3t-01
node-02  user=app password=s3cr3t-02
```

```sh
butcher check-cluster topology.conf secrets.conf
```

## The commands

butcher has six commands. One reads the cluster and reports; it changes nothing but a new node's id. The
other five sit on two levels. Three are the single steps of the migration protocol, run in order. Two are
**high-level**: those same three steps in a loop, over a plan butcher works out itself.

```
                        ┌────────────────┐
                        │ check-cluster  │   reports, run anytime
                        └───────┬────────┘
                                │ both of these work out that same plan themselves
                ┌───────────────┴───────────────┐
                ▼                               ▼
      ┌───────────────────┐            ┌────────────────┐
      │ rebalance-cluster │            │  drain-nodes   │   high level: a loop of rounds
      └─────────┬─────────┘            └────────┬───────┘
                └───────────────┬───────────────┘
                                │ one round is exactly these three low-level commands, in order
                                ▼
    ┌────────────────────┐   ┌────────────┐   ┌─────────────────────┐
    │ prepare-migration  │──▶│ move-data  │──▶│ finalize-migration  │
    │  (flip metadata)   │   │ (move rows)│   │  (cut over routing) │
    └────────────────────┘   └────────────┘   └─────────────────────┘
        small write op         long-running        small write op
                               safe to re-run      point of no return
```

| Command | Writes? | Safe to re-run? | Typical duration | When |
|---|---|---|---|---|
| `check-cluster` | only a new node's id | yes | seconds | anytime — diagnostics |
| `rebalance-cluster` | yes (whole rounds) | yes — it reads the plan again and does what is left | minutes to hours | to even out a cluster |
| `drain-nodes` | yes (whole rounds) | yes — same | minutes to hours | to take nodes out of a cluster |
| `prepare-migration` | yes (only `q__shard`) | yes — re-run is a no-op if state is already set | seconds | start of a migration driven by hand |
| `move-data` | yes (source + target rows) | yes — `INSERT ... ON CONFLICT DO NOTHING`, deletes only rows it just copied | minutes to hours | after prepare, before finalize |
| `finalize-migration` | yes (only `q__shard`) | yes | seconds | once the data is fully moved |

**A high-level command can do nothing the low-level ones cannot.** It works out the same plan that
`check-cluster --checks=shard-balance` reports. Then it takes a batch of shards off the plan and runs 
prepare → move → finalize on them. Then it works the plan out again and takes the next batch, until there is nothing 
left to move. That is all of it: no new operation on the data, no state of its own, nothing to clean up afterwards.

Two things make it worth using anyway. The plan for a big cluster is hundreds of shards long, which is a
long evening of copying flags by hand. And every round reads the plan afresh, so the loop never works from
a stale picture of the cluster.

Use the low-level commands for what the high-level ones deliberately do not do: choosing the shards
yourself, restricting which tables move (`move-data --include-tables`), stopping between rounds for hours,
or finishing a migration somebody else started.

Run `butcher <command> --help` to see that command's built-in help, or `butcher --help` for the list of
commands, grouped the same three ways. `butcher` with no arguments prints that list and exits `0`: it is a
question, not a failure.

> **Confirmation.** Every command that writes anything asks before it does, describing what is about to
> happen. `--yes` (or `-y`) skips the question. With no terminal to read an answer from — a pipe, a CI job —
> a command without `--yes` **fails** rather than assuming consent, so an unattended run has to say out loud
> that it means it.
>
> A high-level command asks **once**, before the first round, and prints the plan above the question. So
> answering no is also how you preview a plan: nothing has been touched yet.
>
> **Outcome and exit code.** Success ends with a green **"Completed successfully."** and exit `0`. Any
> failure prints `Error: <what went wrong>` (a usage error also prints the command's usage) and exits `1`.
> That holds for every failure, including the migration-specific ones — nothing reports itself as a stack
> trace.

## Command reference

### `check-cluster`

```
butcher check-cluster [--checks=<check,check,...>] <config-file>...
```

Inspection of the cluster. Safe to run at any time. It changes nothing, except that a node with no id yet gets
the one from its line (see [Node ids](#node-ids)). It is the one command that has no `--yes` to forget. If
`--checks` is omitted, every check runs; name several to run just those.

Like every command, it starts by confirming it can connect to **every** node in the config: one `select 1`
per node, before anything else happens. If any node is unreachable it stops and tells you which ones, and
what the driver said about each.

Then, again like every command, it checks that every node is the node its line names.

The available checks:

| `--checks` value | What it reports |
|---|---|
| `shard-balance` | How the 1024 shards are spread across nodes, and a **proposed** minimum-movement rebalance if they are uneven. "Uneven" means by shard **count**: butcher does not look at how much data or traffic a shard carries. Proposes only — it does not move anything. |
| `schema-consistency` | Whether every kolbasa table exists on every node with the same shape (columns + indexes + identity *presence*). The per-node identity *value range* is meant to differ, so it is not compared. |
| `find-orphans` | Companion `_dlq` / `_arc` tables whose main queue table is missing on the same node. |
| `migration-state` | Which shards are currently mid-migration (between `prepare` and `finalize`), grouped by destination node. |

Omitting `--checks` runs all of them, which is the default.

The concepts behind each check — what "balanced" means, why schemas must match, what an orphan is — are
explained in [Cluster architecture.md](Cluster%20architecture.md#schema-consistency). This guide focuses on
reading the output, shown under [Worked examples](#worked-examples).

### `rebalance-cluster`

```
butcher rebalance-cluster [--shards-per-round=<n>] [--yes] <config-file>...
```

Carries out the `shard-balance` plan. While the plan has anything in it: prepare a batch of at most
`--shards-per-round` shards, move their rows, finalize them, read the plan again. It ends when there is
nothing left to move, and prints the distribution the cluster ended up with.

`--shards-per-round` is the one real decision here. Between prepare and finalize the
shards of a round are consumed by **no** node, so their messages wait for the rows to arrive. A round of
ten leaves 1% of the queue unread at a time; a round of a thousand leaves all of it. Small rounds also fail
smaller: if something looks wrong you have moved ten shards, not three hundred. On the other hand, every
round costs its own prepare and finalize, so the whole rebalance takes longer. The default is 1: the
smallest pause and the slowest rebalance.

What it will not do:

- **It will not start while any shard is already migrating.** `move-data` and `finalize-migration` act on
  every migrating shard, not only on the ones the loop prepared. A rebalance would finalize somebody
  else's migration as a side effect, so it refuses instead. Finish that migration first (`move-data`, then
  `finalize-migration`) and run the rebalance again.
- **It has no `--include-tables` / `--exclude-tables`.** Skipping a table leaves its rows on the old node,
  and the finalize of the same round would hand the shard over without them. Those rows would never be read
  again. A filtered move is safe only when you decide yourself when to finalize, which means driving the
  low-level commands.

**Interrupting it is safe.** The rounds that finished are done, and the round that was running is
mid-migration. `check-cluster --checks=migration-state` shows which shards those are. `move-data` and then
`finalize-migration` complete that round, after which `rebalance-cluster` starts again and carries on.

The plan is the one `shard-balance` prints, so everything said about it there applies (see
[What trouble looks like](#what-trouble-looks-like)): it counts shards and nothing else. When the shards
carry very different amounts of data, look at the real numbers first and choose `--shards` yourself.

### `drain-nodes`

```
butcher drain-nodes --nodes=<node-id,node-id,...> [--shards-per-round=<n>] [--yes] <config-file>...
```

Moves everything the listed nodes own onto the other nodes, in rounds, until they own nothing. That is the
state a node has to be in before it can leave the cluster. `--nodes` is required. It is a comma-separated
list, and a single node is fine. `--shards-per-round` means the same thing it means for `rebalance-cluster`,
with the same trade-off and the same default.

`--nodes` takes node ids, the same ids as in the config file.

**Only the shards of those nodes move.** The nodes that stay are left as they are, even if they are uneven
between themselves. A command asked to free some nodes has no business moving shards that have nothing to do
with them. So a drain can leave a lopsided cluster, with fewer nodes to spread over; `rebalance-cluster`
evens it out afterwards.

To remove several nodes, drain them in one run. Then every shard moves once. Draining them one after
another can move a shard twice: off the first node onto the second, then off the second.

It refuses a node that is not a member of the cluster, and refuses to drain every node (those shards would
have nowhere to go). Like `rebalance-cluster`, it will not start on top of a migration already in
flight, it has no table filters, and interrupting it is safe. See just above for all three.

To see the plan and do nothing, run it and answer `n`: the plan is printed above the question.

Draining is not removing. Afterwards the nodes are still in both config files, still reachable, still carrying
their now-unused tables. [Cluster shrinking](#cluster-shrinking--remove-a-node) has the rest of the
procedure, which is yours rather than butcher's.

### `prepare-migration`

```
butcher prepare-migration --target=<node-id> --shards=<0,1,2,...> [--yes] <config-file>...
```

Marks the listed shards for migration to `--target`. Both flags are required. `--shards` is a comma-separated
list of shard numbers (0–1023).

`--target` is the destination node's id, as in the config file. You normally don't type it by hand —
`check-cluster --checks=shard-balance` proposes the moves and prints the exact `--target=…` to copy.

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
butcher move-data [--include-tables=<t1,t2,...>] [--exclude-tables=<t1,t2,...>] [--yes] <config-file>...
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
butcher finalize-migration [--yes] <config-file>...
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
nodes. Output is shown as butcher prints it, minus the ANSI colours.

### Health check on a running cluster

Run all checks against a healthy two-node cluster:

```sh
butcher check-cluster cluster.conf
```

```
Completed successfully.
Shard balance: 1024 shard(s) evenly spread across 2 node(s)
┌─────────┬────────┐
│ Node    │ Shards │
├─────────┼────────┤
│ node-01 │    512 │
│ node-02 │    512 │
└─────────┴────────┘
────────────────────────────────────────────────────────────────────────────────
Schema consistency: all tables consistent across cluster
────────────────────────────────────────────────────────────────────────────────
Orphan tables: no orphan queue tables
────────────────────────────────────────────────────────────────────────────────
Migration state: no shards in migration
```

Each section's first line is green when there is nothing to do and yellow when there is, so a clean cluster
is green all the way down.

One check, or a few:

```sh
butcher check-cluster --checks=schema-consistency cluster.conf
butcher check-cluster --checks=shard-balance,migration-state cluster.conf
```

### What trouble looks like

The checks above all came back clean. Here is what each one reports when something is actually wrong.

**An unbalanced cluster** — e.g. just after adding `node-03`, which owns no shards yet. `shard-balance` shows
the skew and proposes a minimum-movement rebalance:

```
Shard balance: 1024 shard(s) across 3 node(s), 341 should move
┌─────────┬────────┬────────┐
│ Node    │ Shards │ Change │
├─────────┼────────┼────────┤
│ node-01 │    512 │   -171 │
│ node-02 │    512 │   -170 │
│ node-03 │      0 │   +341 │
└─────────┴────────┴────────┘
Proposed moves (341):
  node-03 receives 341 shard(s):
    --target=node-03 --shards=0,1,2,3, ... ,340
```

`Change` is what the plan would do to each node: how many shards it takes over (`+`) or hands away (`-`).
The three columns say the same thing three ways — where the shards are now, how the plan moves them, and
(by addition) where they end up.

The shard-balance proposal is **advice**, not an action: the check changes nothing. There are two ways to
act on it. Hand it to `rebalance-cluster`, which walks the whole plan in rounds of the size you name:

```sh
butcher rebalance-cluster --shards-per-round=5 cluster.conf
```

Or carry it out yourself: the last line is printed as the flags themselves, so the first step is a copy and
a paste:

```sh
butcher prepare-migration --target=node-03 --shards=0,1,2,3, ... ,340 cluster.conf
```

> **The plan is a target state, not a single operation.** Those 341 shards do not have to move in one go.
> The three migration commands act on whatever shards are prepared at that moment, so you can take the whole
> plan in one round — or walk through it one shard at a time, ten at a time, fifty at a time — repeating
> prepare → move → finalize for each batch. `rebalance-cluster --shards-per-round=N` is that walk, done for
> you: every round is a prepare, a move and a finalize of at most N shards, and the next round reads the
> plan again.
>
> The size of a batch is a trade-off about *pause*, not about speed. Between `prepare-migration` and
> `finalize-migration` the shards being moved are consumed by **no** node, so the messages sitting in them
> wait. Preparing all 341 at once leaves a third of the queue unread for however long `move-data` takes;
> preparing ten leaves 1% of it unread, and for much less time. Small batches also fail smaller: if
> something looks wrong, you have moved ten shards rather than three hundred. Against that, each round costs
> its own prepare and finalize, and the whole rebalance takes longer in wall-clock time.
>
> Nothing has to be planned ahead. After each round `check-cluster --checks=shard-balance` looks at the new
> distribution and proposes whatever is still left to do, so you can stop between batches for as long as you
> like — a cluster with some shards moved and some not is a perfectly ordinary cluster, not a half-finished
> migration.

> **The proposal counts shards and nothing else.** butcher does not look at how many messages a shard holds,
> how much traffic it takes, or how big its rows are — it only makes the *number* of shards per node as equal
> as it can. It treats all 1024 shards as interchangeable because, as far as the shard table is concerned,
> they are.
>
> They usually are not, in practice. Which shard a message lands on comes from
> [ShardStrategy](Cluster%20architecture.md#how-a-messages-shard-is-chosen), and some strategies concentrate
> traffic: `Fixed` sends everything to one shard, `ThreadLocal` pins a thread's whole output to one for the
> life of that thread, and a hand-picked `SendOptions.shard` goes wherever you put it. So an evenly numbered
> cluster can still be lopsided by rows, and a plan that looks fair by count can move the one shard that
> carries most of the queue.
>
> Two things follow. First, `move-data` takes as long as the *data* behind the shards you prepared, not as
> long as their count suggests. Second, when the shards are known to be uneven, it is worth looking at the
> real numbers before pasting the proposal — a plain query against the queue table answers it (ad-hoc reads
> of `q_*` tables are expected, see [Architecture.md](Architecture.md)):
>
> ```sql
> select shard, count(*) from q_orders group by shard order by count(*) desc limit 20;
> ```
>
> and then you choose `--shards` yourself instead of taking the proposal as given. The proposal is advice
> about counts; only you know what the counts are worth.

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

Goal: add a third node, `node-03`, to a two-node cluster and move a third of the shards onto it.

1. Bring the new node's database up and add a `node-03` line to the config file. Run `check-cluster`: it
   gives the node its id and prints `Initialized node node-03`. Only then add the node to the application's
   `Cluster`, see [Node ids](#node-ids) for why the order matters. A fresh node joins owning no shards, and
   its schema is created when the application's `Cluster` next starts. Run
   `check-cluster --checks=schema-consistency` to confirm it matches before migrating onto it.

2. Ask for a balanced plan and pick the shards to move:

   ```sh
   butcher check-cluster --checks=shard-balance cluster.conf
   # → proposes moving shards 0..340 to node-03
   ```

3. Carry the plan out. In one command, in rounds of five shards:

   ```sh
   butcher rebalance-cluster --shards-per-round=5 cluster.conf
   ```

   ```
   Shard balance: 1024 shard(s) across 3 node(s), 341 should move
   ┌─────────┬────────┬────────┐
   │ Node    │ Shards │ Change │
   ├─────────┼────────┼────────┤
   │ node-01 │    512 │   -171 │
   │ node-02 │    512 │   -170 │
   │ node-03 │      0 │   +341 │
   └─────────┴────────┴────────┘
   Proposed moves (341):
     node-03 receives 341 shard(s):
       --target=node-03 --shards=0,1,2,3, ... ,340
   341 shard(s) will move, in rounds of 5: each round prepares its shards, copies their rows and hands
   them over before the next one starts, and the shards of a round are consumed by no node while their
   rows are copied. Continue? [y/N]: y

   Round 1: 5 to node-03 (341 shard(s) left to move)
   Prepared 5 shard(s) for node-03
   Move table 'q_orders' from DataSource(node-01 (db01.internal:5432/orders)) to DataSource(node-03 (db03.internal:5432/orders)) finished, migrated rows: 12043
   Move table 'q_orders' from DataSource(node-02 (db02.internal:5432/orders)) to DataSource(node-03 (db03.internal:5432/orders)) finished, migrated rows: 11890
   Finalized 5 shard(s)

   Round 2: 5 to node-03 (336 shard(s) left to move)
   ...

   Moved 341 shard(s) in 7 round(s)
   Completed successfully.
   Shard balance: 1024 shard(s) evenly spread across 3 node(s)
   ┌─────────┬────────┐
   │ Node    │ Shards │
   ├─────────┼────────┤
   │ node-01 │    342 │
   │ node-02 │    341 │
   │ node-03 │    341 │
   └─────────┴────────┘
   ```

   The plan goes on screen before the question, so `n` is a preview and `y` is the go-ahead. Inside a
   rebalance the three steps report by count: the round header has already said which shards are going
   where. Driven by hand they print a line per shard, as below.

**The same expansion, step by step.** This is what those rounds did, one round's worth at a time. It is
also how to do an expansion where you pick the shards yourself:

3. Prepare those shards for `node-03`:

   ```sh
   butcher prepare-migration --target=node-03 --shards=0,1,2, ... ,340 cluster.conf
   ```

   ```
   Prepare
   Shards: [0, 1, 2, ..., 340]
   Target node: node-03
   Shards prepared to move (341):
       shard[0]: stable on node-01 => migrating to node-03
       ...
   Completed successfully.
   ```

4. Move the data (re-run until it reports zero moved):

   ```sh
   butcher move-data cluster.conf
   ```

   ```
   Move table 'q_orders' from DataSource(node-01 (db01.internal:5432/orders)) to DataSource(node-03 (db03.internal:5432/orders)) finished, migrated rows: 84213
   Move table 'q_orders' from DataSource(node-02 (db02.internal:5432/orders)) to DataSource(node-03 (db03.internal:5432/orders)) finished, migrated rows: 81902
   Completed successfully.
   ```

5. Finalize:

   ```sh
   butcher finalize-migration cluster.conf
   ```

   ```
   Finalize
   Shards moved to stable state (341):
       shard[0]: migrating to node-03 => stable on node-03
       ...
   Completed successfully.
   ```

A `check-cluster --checks=shard-balance` now shows roughly 341 / 341 / 342.

### Cluster shrinking — remove a node

Goal: drain `node-03` and decommission it. Shrinking is a migration whose target is one of the
**remaining** nodes, repeated until `node-03` owns nothing. Emptying nodes is what `drain-nodes` does, so
the migration part is one command.

1. Empty the node, in rounds of fifty shards:

   ```sh
   butcher drain-nodes --nodes=node-03 --shards-per-round=5 cluster.conf
   ```

   ```
   Draining node-03: 341 shard(s) to move onto 2 node(s)
   ┌─────────┬────────┬────────┐
   │ Node    │ Shards │ Change │
   ├─────────┼────────┼────────┤
   │ node-01 │    342 │   +170 │
   │ node-02 │    341 │   +171 │
   │ node-03 │    341 │   -341 │
   └─────────┴────────┴────────┘
   341 shard(s) will move off node-03, in rounds of 5. They go to the other nodes, but no shards move
   between those nodes. If the other nodes are uneven now, they may stay uneven. 'rebalance-cluster'
   evens them out.
   Continue? [y/N]: y

   Round 1: 5 to node-01, 5 to node-02 (341 shard(s) left to move)
   Prepared 5 shard(s) for node-01
   Prepared 5 shard(s) for node-02
   ...
   Finalized 5 shard(s)

   ...

   Moved 341 shard(s) in 7 round(s)
   Completed successfully.
   Node node-03 owns no shards: nothing is produced to it and nothing is consumed from it
   ┌─────────┬────────┐
   │ Node    │ Shards │
   ├─────────┼────────┤
   │ node-01 │    512 │
   │ node-02 │    512 │
   │ node-03 │      0 │
   └─────────┴────────┘
   ```

   The headline is about the node, not about the cluster. A drain spreads one node's shards over the rest
   and touches nothing else, so 0 / 512 / 512 is the honest outcome. Here the two remaining nodes happen to
   be even as well. Had they started out lopsided they would still be lopsided, and `rebalance-cluster`
   would be the next command.

   **By hand** it is the ordinary three steps, with a target among the nodes that stay.
   `shard-balance`'s current distribution lists the exact shard numbers next to each node's count, and
   `migration-state` confirms none are already in flight. You can send them all to one node and let a later
   rebalance even things out, or split them yourself:

   ```sh
   butcher prepare-migration --target=node-01 --shards=<node-03's shards> cluster.conf
   butcher move-data cluster.conf            # re-run until zero
   butcher finalize-migration cluster.conf
   ```

2. Confirm `node-03` is empty:

   ```sh
   butcher check-cluster --checks=shard-balance cluster.conf   # node-03 → 0 shards
   ```

3. Remove that node's `node-03` line from butcher's config file and decommission its database.

4. **Before shutting the database down, remove the node from your application's `Cluster` configuration too** —
   that is the separate node list your services pass to `Cluster` (not this butcher config file). If a service
   still has the drained node in its list when the database goes away, it will keep trying to reach a node that
   is no longer there. Drop it from the application config and redeploy first; only then power the database off.

### Single-shard migration — a minimal test

The smallest possible migration — useful for rehearsing the three steps in staging. Move just shard `5` from its
current owner to `node-02`:

```sh
butcher prepare-migration --target=node-02 --shards=5 cluster.conf
butcher move-data cluster.conf
butcher finalize-migration cluster.conf
```

Between prepare and finalize, `butcher check-cluster --checks=migration-state cluster.conf` shows shard 5 in flight:

```
Migration state: 1 shard(s) in migration
┌───────────┬────────┐
│ Moving to │ Shards │
├───────────┼────────┤
│ node-02   │      1 │
└───────────┴────────┘
  node-02: 5
```

## Troubleshooting

Every failure prints `Error: <what went wrong>` and exits `1`. There are two kinds, and they differ only in
whether butcher also prints the command's usage:

| Kind | What it looks like | What it means / how to fix |
|---|---|---|
| bad input | usage, then `Error: …` | Malformed command line or config file: a missing required option (`--target` / `--shards` / `--nodes`), an unknown option, a shard number outside `0..1023`, a `--shards-per-round` outside `1..1024`, no config file given, a file that does not exist, a node missing `host`/`dbname`, a bad port, or a duplicate line-id in one file. The message names the specific problem; fix it and re-run. |
| runtime | `Error: …` alone | Something went wrong reaching or operating the cluster. The two you'll actually hit: **a node is unreachable** (the message lists which nodes and the connection error), or **this isn't an initialized cluster** — `Initialized shard table q__shard not found. Is it a Kolbasa cluster?`, meaning no node has a fully-seeded 1024-row `q__shard` (the cluster has never been started by an application, or you pointed butcher at the wrong databases). |

Six migration-specific refusals are worth knowing by name, because the message states the rule rather than
the fix:

| Refusal | Cause | Fix |
|---|---|---|
| shard already belongs to the target | `prepare-migration --target=X` for a shard node X already owns. | Pick a different target, or drop that shard from `--shards`. |
| target is not a node of this cluster | `--target` isn't a known node id in the cluster (remember it's the internal `q__node` id, not the config line-id). | Re-run `check-cluster --checks=shard-balance` and copy the exact node id from the `--target=…` line it prints. |
| table schemas differ between nodes | `move-data` found a table whose shape differs between nodes. | Resolve the drift first — run `check-cluster --checks=schema-consistency` to see exactly what differs, then re-run schema generation (start a `Cluster` against the lagging node) or restore the missing objects. Then re-run `move-data`. |
| shards are already migrating | `rebalance-cluster` or `drain-nodes` on a cluster where something is mid-migration: an interrupted run of either, or a hand-driven migration in progress. The message counts the shards and names the nodes they are going to. | Finish that migration: `move-data` until it reports zero, then `finalize-migration`. Then run the command again; it picks up from the distribution it finds. `check-cluster --checks=migration-state` is the same information in advance. |
| node is not a member of this cluster | `drain-nodes --nodes=X` where X is not a node id of the cluster. Most often a config line-id typed instead of the internal `q__node` id. The message lists the ids that do exist. | Copy the id from `check-cluster --checks=shard-balance`. |
| draining would leave the cluster without a node | `drain-nodes` naming every node of the cluster. Their shards would have nowhere to go. | Add a node and let it join first, or drop the queues instead of draining them. |

Two questions that come up often:

- **"`move-data` got interrupted — did I corrupt anything?"** No. It is safe to re-run; copies are idempotent
  and it only deletes rows from the source after they're safely on the target. Just run it again until it
  reports zero rows moved.
- **"`rebalance-cluster` / `drain-nodes` got interrupted — what now?"** The rounds that finished need
  nothing. The round that was running is a migration in flight, and the command will refuse to start again
  until it is closed: `move-data` until it reports zero, then `finalize-migration`. Then run it again. It
  works from the distribution as it is now, so it continues with what is left.
- **"I ran `prepare-migration` with the wrong shards."** Prepare only flips metadata; no data has moved yet.
  Re-run `prepare-migration` with the correct shards (you can prepare additional shards), and to undo a shard
  you flipped by mistake, prepare it back to its original owner. As long as you haven't run `move-data`, the
  rows never left their node.

## The same thing from code

Every check, every migration step and both high-level commands are methods on
`kolbasa.cluster.admin.ClusterAdmin`, so a rebalance can be driven by a program instead of by hand. The
CLI is a frontend over this class and uses nothing it does not offer — `butcher` is the name of the tool,
not of the API.

```kotlin
val admin = ClusterAdmin(listOf(node01, node02, node03))   // the same DataSource list a Cluster is given

// What the two high-level commands are, as one call each
admin.rebalance(shardsPerRound = 50)
admin.drainNodes(setOf(NodeId("node-03")), shardsPerRound = 50)
```

Each has a read-only report in front of it: `checkShardBalance()` for the first, `planNodesDrain(nodes)` for
the second. That report is what they work from, and the CLI shows you the same thing before it asks for a
yes. Both are a loop over the three low-level steps, which stay there to be driven directly when the loop
is not what you want:

```kotlin
val balance = admin.checkShardBalance()
if (!balance.isBalanced) {
    balance.proposedMoves.forEach { (target, shards) ->
        admin.prepareMigration(target, shards)
        admin.moveData()
        admin.finalizeMigration()
    }
}
```

Creating a `ClusterAdmin` touches no database: it only remembers the nodes. That matters because you reach for it
when a cluster is already in a bad state, and a tool that repaired the schema or seeded the shard table on
its way in would destroy the evidence it was asked to look at. (`Cluster` does initialize what it needs when
it starts, which is why it is the wrong thing to diagnose a cluster with.)

The check results are plain data — `balance.proposedMoves`, `balance.netChangeByNode`,
`migrationState.isClean`, `schemaConsistency.divergencesByTable` — so a program decides on the numbers
rather than on parsed text. Progress during a long `moveData` is reported through `ProgressCallback`; pass
`ProgressCallback.Ignore` when nobody is watching. A `rebalance` or a `drainNodes` reports every round
through the same callback (`rebalanceRoundStart`, `rebalanceFinished`), and the three steps of each round
report themselves as usual.

## Limitations

- **butcher never rebalances on its own.** `rebalance-cluster` and `drain-nodes` carry a whole plan out in
  one command, but somebody has to run them, say how big a round is and answer the question they ask first.
  Nothing in butcher watches a cluster or reacts to one; `check-cluster` only ever proposes.
- **The high-level commands take no table filters, and do not pick shards selectively.** They move every
  table of the shards they move (see [`rebalance-cluster`](#rebalance-cluster) for why), and the shards
  come from the plan. Choosing shards yourself, or moving only some tables, means driving the low-level
  commands.
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
