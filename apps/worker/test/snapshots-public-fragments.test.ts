import { describe, expect, it } from 'vitest';

import {
  readPublicSnapshotFragments,
  writePublicSnapshotFragments,
} from '../src/snapshots/public-fragments';
import { createFakeD1Database } from './helpers/fake-d1';

describe('snapshots/public-fragments', () => {
  it('writes public snapshot fragments in a D1 batch', async () => {
    const writes: unknown[][] = [];
    const db = createFakeD1Database([
      {
        match: 'insert into public_snapshot_fragments',
        run: (args) => {
          writes.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const results = await writePublicSnapshotFragments(db, [
      {
        snapshotKey: 'status',
        fragmentKey: 'monitor:1',
        generatedAt: 200,
        bodyJson: '{"id":1}',
        updatedAt: 205,
      },
      {
        snapshotKey: 'status',
        fragmentKey: 'monitor:2',
        generatedAt: 200,
        bodyJson: '{"id":2}',
        updatedAt: 205,
      },
    ]);

    expect(results).toHaveLength(2);
    expect(writes).toEqual([
      ['status', 'monitor:1', 200, '{"id":1}', 205],
      ['status', 'monitor:2', 200, '{"id":2}', 205],
    ]);
  });

  it('reads public snapshot fragments ordered by fragment key', async () => {
    const db = createFakeD1Database([
      {
        match: 'from public_snapshot_fragments',
        all: (args) => {
          expect(args).toEqual(['status']);
          return [
            {
              fragment_key: 'monitor:1',
              generated_at: 200,
              body_json: '{"id":1}',
              updated_at: 205,
            },
          ];
        },
      },
    ]);

    await expect(readPublicSnapshotFragments(db, 'status')).resolves.toEqual([
      {
        fragment_key: 'monitor:1',
        generated_at: 200,
        body_json: '{"id":1}',
        updated_at: 205,
      },
    ]);
  });

  it('rejects empty fragment identifiers before preparing SQL', async () => {
    const db = createFakeD1Database([]);

    await expect(
      writePublicSnapshotFragments(db, [
        {
          snapshotKey: 'status',
          fragmentKey: '',
          generatedAt: 200,
          bodyJson: '{"id":1}',
          updatedAt: 205,
        },
      ]),
    ).rejects.toThrow('fragmentKey must not be empty');
  });
});
