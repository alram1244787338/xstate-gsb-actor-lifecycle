---
'xstate': patch
---

Scheduling a delayed event with the same `id` from the same actor now atomically replaces the previously scheduled event. The old event is no longer delivered and the new event can still be canceled with `cancel(...)`.

```ts
const machine = createMachine({
  on: {
    SCHEDULE: {
      // re-scheduling with the same id replaces the pending event
      actions: raise({ type: 'REFRESH' }, { delay: 1000, id: 'refresh' })
    }
  }
});
```
