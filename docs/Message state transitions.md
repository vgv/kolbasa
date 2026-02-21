# Message State Transitions

## Simple Diagram

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

## States Description

| State | Description                                                                                       |
|-------|---------------------------------------------------------------------------------------------------|
| SCHEDULED | Message placed in queue, waiting for delay to expire, not yet visible to consumers                |
| READY | Message is visible and can be received for processing, has never been attempted                   |
| IN_FLIGHT | Message is being processed by a consumer                                                          |
| RETRY | Processing attempt failed (visibility timeout expired); message is available for the next attempt |
| DEAD | All attempts exhausted, waiting for sweep to remove                                               |
| COMPLETED | Successfully processed and removed from queue                                                     |

## Uniqueness Scopes

### UNTOUCHED_UNIQUE

Uniqueness enforced only for SCHEDULED + READY states (messages that have never been processed before):

```
              ┌─────────────────────────────────────────┐                                ┌────────────────────> DEAD
              │         UNTOUCHED_UNIQUE scope          │                                │ (no attempts left)
              │                                         │                                │
 ● ───────────│──> SCHEDULED ─────────────────> READY ──│─────────────────> IN_FLIGHT ─────────────────> COMPLETED
     send()   │               (delay expired)           │   ↑   receive()                │    delete()
              │                                         │   │                            │
              │                                         │   │                            │ (timeout)
              └─────────────────────────────────────────┘   └──────────────── RETRY <────┘
```

### ALL_LIVE_UNIQUE

Uniqueness enforced for all live states (SCHEDULED + READY + IN_FLIGHT + RETRY):

```

              ┌───────────────────────────────────────────────────────────────────────────┐
              │                           ALL_LIVE_UNIQUE scope                           │       ┌─────────────────> DEAD
              │                                                                           │       │ (no attempts left)
              │                                                                           │       │
 ● ───────────│──> SCHEDULED ─────────────────> READY ─────────────────> IN_FLIGHT ───────│───────┴──────> COMPLETED
     send()   │               (delay expired)            ↑    receive()      │            │     delete()
              │                                          │                   │            │
              │                                          │                   │ (timeout)  │
              │                                          └─────── RETRY <────┘            │
              │                                                                           │
              └───────────────────────────────────────────────────────────────────────────┘
```

## Uniqueness Comparison

| Option | States in uniqueness check | Use case |
|--------|---------------------------|----------|
| `ALL_LIVE_UNIQUE` | SCHEDULED + READY + IN_FLIGHT + RETRY | Dedupe all active messages, including retries |
| `UNTOUCHED_UNIQUE` | SCHEDULED + READY | Dedupe only untouched messages, allow reprocessing duplicates |
