import { of } from 'rxjs';
import { CallbackActorRef, fromCallback } from '../src/actors/callback.ts';
import {
  ActorRef,
  ActorRefFrom,
  AnyActorRef,
  AnyStateMachine,
  EventObject,
  Snapshot,
  SimulatedClock,
  assign,
  cancel,
  createActor,
  createMachine,
  fromEventObservable,
  fromObservable,
  fromPromise,
  fromTransition,
  raise,
  sendTo,
  setup,
  spawnChild,
  stopChild
} from '../src/index.ts';
import { ActorSystem } from '../src/system.ts';

describe('system', () => {
  it('should register an invoked actor', () => {
    const { resolve, promise } = Promise.withResolvers<void>();
    type MySystem = ActorSystem<{
      actors: {
        receiver: ActorRef<Snapshot<unknown>, { type: 'HELLO' }>;
      };
    }>;

    const machine = createMachine({
      id: 'parent',
      initial: 'a',
      states: {
        a: {
          invoke: [
            {
              src: fromCallback(({ receive }) => {
                receive((event) => {
                  expect(event.type).toBe('HELLO');
                  resolve();
                });
              }),
              systemId: 'receiver'
            },
            {
              src: createMachine({
                id: 'childmachine',
                entry: ({ system }) => {
                  const receiver = (system as MySystem)?.get('receiver');

                  if (receiver) {
                    receiver.send({ type: 'HELLO' });
                  }
                }
              })
            }
          ]
        }
      }
    });

    createActor(machine).start();

    return promise;
  });

  it('should register a spawned actor', () => {
    const { resolve, promise } = Promise.withResolvers<void>();
    type MySystem = ActorSystem<{
      actors: {
        receiver: ActorRef<Snapshot<unknown>, { type: 'HELLO' }>;
      };
    }>;

    const machine = createMachine({
      types: {} as {
        context: {
          ref: CallbackActorRef<EventObject, unknown>;
          machineRef?: ActorRefFrom<AnyStateMachine>;
        };
      },
      id: 'parent',
      context: ({ spawn }) => ({
        ref: spawn(
          fromCallback(({ receive }) => {
            receive((event) => {
              expect(event.type).toBe('HELLO');
              resolve();
            });
          }),
          { systemId: 'receiver' }
        )
      }),
      on: {
        toggle: {
          actions: assign({
            machineRef: ({ spawn }) => {
              return spawn(
                createMachine({
                  id: 'childmachine',
                  entry: ({ system }) => {
                    const receiver = (system as MySystem)?.get('receiver');

                    if (receiver) {
                      receiver.send({ type: 'HELLO' });
                    } else {
                      throw new Error('no');
                    }
                  }
                })
              );
            }
          })
        }
      }
    });

    const actor = createActor(machine).start();

    actor.send({ type: 'toggle' });

    return promise;
  });

  it('system can be immediately accessed outside the actor', () => {
    const machine = createMachine({
      invoke: {
        systemId: 'someChild',
        src: createMachine({})
      }
    });

    // no .start() here is important for the test
    const actor = createActor(machine);

    expect(actor.system.get('someChild')).toBeDefined();
  });

  it('root actor can be given the systemId', () => {
    const machine = createMachine({});
    const actor = createActor(machine, { systemId: 'test' });
    expect(actor.system.get('test')).toBe(actor);
  });

  it('should remove invoked actor from receptionist if stopped', () => {
    const machine = createMachine({
      initial: 'active',
      states: {
        active: {
          invoke: {
            src: createMachine({}),
            systemId: 'test'
          },
          on: {
            toggle: 'inactive'
          }
        },
        inactive: {}
      }
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();

    actor.send({ type: 'toggle' });

    expect(actor.system.get('test')).toBeUndefined();
  });

  it('should remove spawned actor from receptionist if stopped', () => {
    const childMachine = createMachine({});
    const machine = createMachine({
      types: {} as {
        context: {
          ref: ActorRefFrom<typeof childMachine>;
        };
      },
      context: ({ spawn }) => ({
        ref: spawn(childMachine, {
          systemId: 'test'
        })
      }),
      on: {
        toggle: {
          actions: stopChild(({ context }) => context.ref)
        }
      }
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();

    actor.send({ type: 'toggle' });

    expect(actor.system.get('test')).toBeUndefined();
  });

  it('should throw an error if an actor with the system ID already exists', () => {
    const machine = createMachine({
      initial: 'inactive',
      states: {
        inactive: {
          on: {
            toggle: 'active'
          }
        },
        active: {
          invoke: [
            {
              src: createMachine({}),
              systemId: 'test'
            },
            {
              src: createMachine({}),
              systemId: 'test'
            }
          ]
        }
      }
    });

    const errorSpy = vi.fn();

    const actorRef = createActor(machine, { systemId: 'test' });
    actorRef.subscribe({
      error: errorSpy
    });
    actorRef.start();
    actorRef.send({ type: 'toggle' });

    expect(errorSpy.mock.calls).toMatchInlineSnapshot(`
      [
        [
          [Error: Actor with system ID 'test' already exists.],
        ],
      ]
    `);
  });

  it('should cleanup stopped actors', () => {
    const machine = createMachine({
      types: {
        context: {} as {
          ref: AnyActorRef;
        }
      },
      context: ({ spawn }) => ({
        ref: spawn(
          fromPromise(() => Promise.resolve()),
          {
            systemId: 'test'
          }
        )
      }),
      on: {
        stop: {
          actions: stopChild(({ context }) => context.ref)
        },
        start: {
          actions: spawnChild(
            fromPromise(() => Promise.resolve()),
            {
              systemId: 'test'
            }
          )
        }
      }
    });

    const actor = createActor(machine).start();

    actor.send({ type: 'stop' });

    expect(() => {
      actor.send({ type: 'start' });
    }).not.toThrow();
  });

  it('should be accessible in inline custom actions', () => {
    const machine = createMachine({
      invoke: {
        src: createMachine({}),
        systemId: 'test'
      },
      entry: ({ system }) => {
        expect(system.get('test')).toBeDefined();
      }
    });

    createActor(machine).start();
  });

  it('should be accessible in referenced custom actions', () => {
    const machine = createMachine(
      {
        invoke: {
          src: createMachine({}),
          systemId: 'test'
        },
        entry: 'myAction'
      },
      {
        actions: {
          myAction: ({ system }) => {
            expect(system.get('test')).toBeDefined();
          }
        }
      }
    );

    createActor(machine).start();
  });

  it('should be accessible in assign actions', () => {
    const machine = createMachine({
      invoke: {
        src: createMachine({}),
        systemId: 'test'
      },
      initial: 'a',
      states: {
        a: {
          entry: assign(({ system }) => {
            expect(system.get('test')).toBeDefined();
          })
        }
      }
    });

    createActor(machine).start();
  });

  it('should be accessible in sendTo actions', () => {
    const machine = createMachine({
      invoke: {
        src: createMachine({}),
        systemId: 'test'
      },
      initial: 'a',
      states: {
        a: {
          entry: sendTo(
            ({ system }) => {
              expect(system.get('test')).toBeDefined();
              return system.get('test');
            },
            { type: 'FOO' }
          )
        }
      }
    });

    createActor(machine).start();
  });

  it('should be accessible in promise logic', () => {
    expect.assertions(2);
    const machine = createMachine({
      invoke: [
        {
          src: createMachine({}),
          systemId: 'test'
        },
        {
          src: fromPromise(({ system }) => {
            expect(system.get('test')).toBeDefined();
            return Promise.resolve();
          })
        }
      ]
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();
  });

  it('should be accessible in transition logic', () => {
    expect.assertions(2);
    const machine = createMachine({
      invoke: [
        {
          src: createMachine({}),
          systemId: 'test'
        },

        {
          src: fromTransition((_state, _event, { system }) => {
            expect(system.get('test')).toBeDefined();
            return 0;
          }, 0),
          systemId: 'reducer'
        }
      ]
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();

    // The assertion won't be checked until the transition function gets an event
    actor.system.get('reducer')!.send({ type: 'a' });
  });

  it('should be accessible in observable logic', () => {
    expect.assertions(2);
    const machine = createMachine({
      invoke: [
        {
          src: createMachine({}),
          systemId: 'test'
        },

        {
          src: fromObservable(({ system }) => {
            expect(system.get('test')).toBeDefined();
            return of(0);
          })
        }
      ]
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();
  });

  it('should be accessible in event observable logic', () => {
    expect.assertions(2);
    const machine = createMachine({
      invoke: [
        {
          src: createMachine({}),
          systemId: 'test'
        },

        {
          src: fromEventObservable(({ system }) => {
            expect(system.get('test')).toBeDefined();
            return of({ type: 'a' });
          })
        }
      ]
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();
  });

  it('should be accessible in callback logic', () => {
    expect.assertions(2);
    const machine = createMachine({
      invoke: [
        {
          src: createMachine({}),
          systemId: 'test'
        },
        {
          src: fromCallback(({ system }) => {
            expect(system.get('test')).toBeDefined();
          })
        }
      ]
    });

    const actor = createActor(machine).start();

    expect(actor.system.get('test')).toBeDefined();
  });

  it('should gracefully handle re-registration of a `systemId` during a reentering transition', () => {
    const spy = vi.fn();

    let counter = 0;

    const machine = createMachine({
      initial: 'listening',
      states: {
        listening: {
          invoke: {
            systemId: 'listener',
            src: fromCallback(({ receive }) => {
              const localId = counter++;

              receive((event) => {
                spy(localId, event);
              });

              return () => {};
            })
          }
        }
      },
      on: {
        RESTART: {
          target: '.listening'
        }
      }
    });

    const actorRef = createActor(machine).start();

    actorRef.send({ type: 'RESTART' });
    actorRef.system.get('listener')!.send({ type: 'a' });

    expect(spy.mock.calls).toEqual([
      [
        1,
        {
          type: 'a'
        }
      ]
    ]);
  });

  it('should be able to send an event to an ancestor with a registered `systemId` from an initial entry action', () => {
    const spy = vi.fn();

    const child = createMachine({
      entry: sendTo(({ system }) => system.get('myRoot'), {
        type: 'EV'
      })
    });

    const machine = createMachine({
      invoke: {
        src: child
      },
      on: {
        EV: {
          actions: spy
        }
      }
    });
    createActor(machine, { systemId: 'myRoot' }).start();

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it('system ID should be accessible on the actor', () => {
    const machine = createMachine({});
    const actor = createActor(machine, { systemId: 'test' });
    expect(actor.systemId).toBe('test');
  });

  it('should give a list of runnings actors', () => {
    const machine = createMachine({
      id: 'root',
      initial: 'happy path',
      states: {
        'happy path': {
          entry: [spawnChild(createMachine({}), { systemId: 'child1' })],
          invoke: [
            {
              src: createMachine({
                id: 'machine'
              }),
              systemId: 'child2'
            }
          ],
          on: {
            stopChild1: 'sad path'
          }
        },
        'sad path': {
          entry: stopChild(({ system }) => system.get('child1'))
        }
      }
    });

    const actor = createActor(machine).start();

    expect(actor.system.getAll()).toEqual({
      child1: actor.system.get('child1'),
      child2: actor.system.get('child2')
    });

    actor.send({ type: 'stopChild1' });

    expect(actor.system.getAll()).toEqual({});
  });

  it('should unregister nested child systemIds when stopping a parent actor', () => {
    const subchild = createMachine({});

    const child = setup({
      actors: {
        subchild
      }
    }).createMachine({
      id: 'childSystem',
      invoke: {
        src: 'subchild',
        systemId: 'subchild'
      }
    });

    const parent = setup({
      actors: { child }
    }).createMachine({
      entry: spawnChild('child', { id: 'childId' }),
      on: {
        restart: {
          actions: [
            stopChild('childId'),
            spawnChild('child', { id: 'childId' })
          ]
        }
      }
    });

    const root = createActor(parent).start();

    expect(root.system.get('subchild')).toBeDefined();

    // This should not throw "Actor with system ID 'subchild' already exists"
    expect(() => root.send({ type: 'restart' })).not.toThrow();

    expect(root.system.get('subchild')).toBeDefined();
  });
});

describe('scheduler', () => {
  it('should replace a pending event when re-scheduling with the same id', () => {
    const clock = new SimulatedClock();
    const received: number[] = [];

    const machine = createMachine({
      types: {
        events: {} as
          | { type: 'SCHEDULE'; n: number }
          | { type: 'PING'; n: number }
      },
      on: {
        SCHEDULE: {
          actions: raise(({ event }) => ({ type: 'PING', n: event.n }), {
            id: 'ping',
            delay: 100
          })
        },
        PING: {
          actions: ({ event }) => {
            received.push(event.n);
          }
        }
      }
    });

    const actor = createActor(machine, { clock }).start();

    actor.send({ type: 'SCHEDULE', n: 1 });
    clock.increment(50);

    // re-scheduling with the same id replaces the pending event
    actor.send({ type: 'SCHEDULE', n: 2 });

    // the stale event is not delivered at the original delay
    clock.increment(50);
    expect(received).toEqual([]);

    // the replacing event is delivered exactly once, after the new delay
    clock.increment(50);
    expect(received).toEqual([2]);

    clock.increment(1000);
    expect(received).toEqual([2]);
  });

  it('should be able to cancel a re-scheduled event', () => {
    const clock = new SimulatedClock();
    const received: string[] = [];

    const machine = createMachine({
      types: {
        events: {} as
          | { type: 'SCHEDULE' }
          | { type: 'CANCEL' }
          | { type: 'PING' }
      },
      on: {
        SCHEDULE: {
          actions: raise({ type: 'PING' }, { id: 'ping', delay: 100 })
        },
        CANCEL: {
          actions: cancel('ping')
        },
        PING: {
          actions: () => {
            received.push('PING');
          }
        }
      }
    });

    const actor = createActor(machine, { clock }).start();

    actor.send({ type: 'SCHEDULE' });
    clock.increment(50);
    actor.send({ type: 'SCHEDULE' });

    // advance past the original delay: the stale timeout must not
    // clear the records of the replacing event
    clock.increment(50);

    actor.send({ type: 'CANCEL' });
    clock.increment(1000);

    expect(received).toEqual([]);
  });

  it('should not interfere between different actors scheduling with the same id', () => {
    const clock = new SimulatedClock();
    const received: string[] = [];

    const child = createMachine({
      types: { events: {} as { type: 'CHILD_PING' } },
      entry: raise({ type: 'CHILD_PING' }, { id: 'ping', delay: 100 }),
      on: {
        CHILD_PING: {
          actions: () => {
            received.push('child');
          }
        }
      }
    });

    const parent = createMachine({
      types: { events: {} as { type: 'PARENT_PING' } },
      invoke: { id: 'child', src: child },
      entry: raise({ type: 'PARENT_PING' }, { id: 'ping', delay: 200 }),
      on: {
        PARENT_PING: {
          actions: () => {
            received.push('parent');
          }
        }
      }
    });

    createActor(parent, { clock }).start();

    clock.increment(100);
    expect(received).toEqual(['child']);

    clock.increment(100);
    expect(received).toEqual(['child', 'parent']);
  });

  it('should keep a single scheduled event in the system snapshot when re-scheduling with the same id', () => {
    const clock = new SimulatedClock();

    const machine = createMachine({
      types: {
        events: {} as { type: 'SCHEDULE' } | { type: 'PING' }
      },
      on: {
        SCHEDULE: {
          actions: raise({ type: 'PING' }, { id: 'ping', delay: 100 })
        }
      }
    });

    const actor = createActor(machine, { clock }).start();

    actor.send({ type: 'SCHEDULE' });
    clock.increment(50);
    actor.send({ type: 'SCHEDULE' });

    const scheduledEvents = Object.values(
      actor.system.getSnapshot()._scheduledEvents
    );
    expect(scheduledEvents).toHaveLength(1);
    expect(scheduledEvents[0].id).toBe('ping');
    expect(scheduledEvents[0].event).toEqual({ type: 'PING' });

    clock.increment(1000);
    expect(actor.system.getSnapshot()._scheduledEvents).toEqual({});
  });

  it('should ignore a stale timeout that fires after being replaced', () => {
    // a clock that cannot unschedule an already queued timeout
    const timeouts = new Map<number, () => void>();
    let timeoutId = 0;
    const clock = {
      setTimeout: (fn: () => void, _delay: number) => {
        const id = ++timeoutId;
        timeouts.set(id, fn);
        return id;
      },
      clearTimeout: (_id: number) => {}
    };
    const received: number[] = [];

    const machine = createMachine({
      types: {
        events: {} as
          | { type: 'SCHEDULE'; n: number }
          | { type: 'PING'; n: number }
      },
      on: {
        SCHEDULE: {
          actions: raise(({ event }) => ({ type: 'PING', n: event.n }), {
            id: 'ping',
            delay: 100
          })
        },
        PING: {
          actions: ({ event }) => {
            received.push(event.n);
          }
        }
      }
    });

    const actor = createActor(machine, { clock }).start();

    actor.send({ type: 'SCHEDULE', n: 1 });
    actor.send({ type: 'SCHEDULE', n: 2 });

    const [staleTimeout, currentTimeout] = [...timeouts.values()];

    // the stale timeout must not deliver its event
    // nor remove the replacing event from the scheduler
    staleTimeout();
    expect(received).toEqual([]);
    expect(
      Object.keys(actor.system.getSnapshot()._scheduledEvents)
    ).toHaveLength(1);

    currentTimeout();
    expect(received).toEqual([2]);
    expect(actor.system.getSnapshot()._scheduledEvents).toEqual({});
  });
});
