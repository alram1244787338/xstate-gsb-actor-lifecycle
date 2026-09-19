---
'xstate': patch
---

Fixed the scheduler so that scheduling a delayed event with the same `id` twice atomically replaces the pending event. Previously, the stale timeout was never cancelled, so the outdated event could still be delivered, it could remove the replacing event's records, and a later `cancel()` of that `id` could fail to prevent delivery.

```ts
const machine = createMachine({
  on: {
    reschedule: {
      // scheduling with the same id again replaces the pending event;
      // only the latest event is delivered, after the new delay
      actions: raise({ type: 'ping' }, { id: 'myPing', delay: 1000 })
    }
  }
});
```
