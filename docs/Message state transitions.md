# Message State Transitions

## Simple Diagram

```
                                                                                ┌────────────────────> DEAD
                                                                                │ (no attempts left)
                                                                                │
 ● ───────────> SCHEDULED ─────────────────> AVAILABLE ─────────────────> IN_FLIGHT ─────────────────> COMPLETED ──────> ●
     send()                (delay expired)        ↑        receive()            │         delete()
                                                  │                             │
                                                  │                             │ (timeout)
                                                  │                             │
                                                  │                             ↓
                                                  │                      RETRY_SCHEDULED
                                                  │                             │
                                                  │                             │ (delay expired)
                                                  │                             │
                                                  └─────────────────────────────┘
```

## States Description

| State | Description |
|-------|-------------|
| SCHEDULED | Message placed in queue, waiting for delay to expire, not yet visible to consumers |
| AVAILABLE | Message is visible and can be received for processing |
| IN_FLIGHT | Message is being processed by a consumer |
| RETRY_SCHEDULED | Processing failed, waiting for next attempt |
| DEAD | All attempts exhausted, waiting for sweep to remove |
| COMPLETED | Successfully processed and removed from queue |

## Uniqueness Scopes

### UNTOUCHED_UNIQUE

Uniqueness enforced only for SCHEDULED + AVAILABLE states (messages not yet picked up for processing):

```
              ┌──────────────────────────────────────────────────────────────────┐
              │                      UNTOUCHED_UNIQUE scope                      │
              │                                                                  │         ┌────────────────────> DEAD
              │                                                                  │         │ (no attempts left)
              │                                                                  │         │
 ● ───────────│──> SCHEDULED ─────────────────> AVAILABLE ───────────────────────│───> IN_FLIGHT ─────────────────> COMPLETED ──────> ●
     send()   │               (delay expired)         ↑        receive()         │         │         delete()
              │                                       │                          │         │
              │                                       │                          │         │ (timeout)
              │                                       │ (delay expired)          │         │
              │                                       │                          │         ↓
              │                                       │                          │  RETRY_SCHEDULED
              │                                       │                          │         │
              │                                       │                          │         │ (delay expired)
              │                                       └──────────────────────────│─────────┘
              │                                          (back in scope)         │
              └──────────────────────────────────────────────────────────────────┘
```

### ALL_LIVE_UNIQUE

Uniqueness enforced for all live states (SCHEDULED + AVAILABLE + IN_FLIGHT + RETRY_SCHEDULED):

```
              ┌────────────────────────────────────────────────────────────────────────────────────────┐
              │                               ALL_LIVE_UNIQUE scope                                    │
              │                                                                                        │  ┌─────────────────> DEAD
              │                                                                                        │  │ (no attempts left)
              │                                                                                        │  │
 ● ───────────│──> SCHEDULED ─────────────────> AVAILABLE ─────────────────> IN_FLIGHT ────────────────│──┴─────────────────> COMPLETED ──────> ●
     send()   │               (delay expired)        ↑        receive()            │                   │        delete()
              │                                      │                             │                   │
              │                                      │                             │ (timeout)         │
              │                                      │                             │                   │
              │                                      │                             ↓                   │
              │                                      │                      RETRY_SCHEDULED            │
              │                                      │                             │                   │
              │                                      │                             │ (delay expired)   │
              │                                      │                             │                   │
              │                                      └─────────────────────────────┘                   │
              │                                                                                        │
              └────────────────────────────────────────────────────────────────────────────────────────┘
```

## Uniqueness Comparison

| Option | States in uniqueness check | Use case |
|--------|---------------------------|----------|
| `ALL_LIVE_UNIQUE` | SCHEDULED + AVAILABLE + IN_FLIGHT + RETRY_SCHEDULED | Dedupe all active messages, including retries |
| `UNTOUCHED_UNIQUE` | SCHEDULED + AVAILABLE | Dedupe only untouched messages, allow reprocessing duplicates |
