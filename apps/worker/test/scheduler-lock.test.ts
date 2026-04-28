import { describe, expect, it } from 'vitest';

import { acquireLease, releaseLease } from '../src/scheduler/lock';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

describe('scheduler/lock', () => {
  it('returns true when the lease row is inserted or updated', async () => {
    let boundArgs: unknown[] | null = null;
    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'insert into locks',
        run: (args) => {
          boundArgs = args;
          return { meta: { changes: 1 } };
        },
      },
    ];

    const db = createFakeD1Database(handlers);
    const ok = await acquireLease(db, 'scheduler:tick', 100, 55);

    expect(ok).toBe(true);
    expect(boundArgs).toEqual(['scheduler:tick', 155, 100]);
  });

  it('returns false when a lease cannot be claimed', async () => {
    const db = createFakeD1Database([
      {
        match: 'insert into locks',
        run: () => ({ meta: { changes: 0 } }),
      },
    ]);

    await expect(acquireLease(db, 'retention', 200, 600)).resolves.toBe(false);
  });

  it('treats missing changes metadata as a failed lease claim', async () => {
    const db = createFakeD1Database([
      {
        match: 'insert into locks',
        run: () => ({ success: true }),
      },
    ]);

    await expect(acquireLease(db, 'retention', 200, 600)).resolves.toBe(false);
  });

  it('releases only the lease row matching the claimed expiry', async () => {
    let boundArgs: unknown[] | null = null;
    const db = createFakeD1Database([
      {
        match: 'delete from locks',
        run: (args) => {
          boundArgs = args;
          return { meta: { changes: 1 } };
        },
      },
    ]);

    await expect(releaseLease(db, 'scheduler:tick', 155)).resolves.toBeUndefined();
    expect(boundArgs).toEqual(['scheduler:tick', 155]);
  });
});
