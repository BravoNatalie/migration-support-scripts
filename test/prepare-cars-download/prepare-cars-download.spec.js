import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { createInitialState, serializeState } from '@storacha/filecoin-pin-migration'
import {
  buildDownloadRecords,
  prepareCarsDownload,
  selectSpaceDID,
  serializeAria2Input,
} from '../../scripts/prepare-cars-download/prepare-cars-download.js'
import { registerSuite } from '../register-suite.js'

registerSuite('testPrepareCarsDownload', {
  'buildDownloadRecords deduplicates shard CIDs across roots and sources': () => {
    const records = buildDownloadRecords({
      spaceDID: 'did:key:zDownload',
      inventory: {
        did: 'did:key:zDownload',
        uploads: ['root-a', 'root-b'],
        shards: [
          {
            root: 'root-a',
            cid: 'bafy-shared',
            pieceCID: 'bafk-piece-1',
            sourceURL: 'https://example.invalid/shared',
            sizeBytes: 100n,
          },
        ],
        shardsToStore: [
          {
            root: 'root-b',
            cid: 'bafy-shared',
            pieceCID: 'bafk-piece-1',
            sourceURL: 'https://example.invalid/shared',
            sizeBytes: 100n,
          },
          {
            root: 'root-b',
            cid: 'bafy-store',
            sourceURL: 'https://example.invalid/store',
            sizeBytes: 200n,
          },
        ],
        skippedUploads: [],
        totalBytes: 300n,
        totalSizeToMigrate: 300n,
      },
    })

    assert.equal(records.length, 2)
    assert.deepEqual(records[0], {
      spaceDID: 'did:key:zDownload',
      shardCid: 'bafy-shared',
      pieceCID: 'bafk-piece-1',
      sizeBytes: 100n,
      sourceURL: 'https://example.invalid/shared',
      relativePath: path.join('cars', 'bafy-shared.car'),
      roots: ['root-a', 'root-b'],
      from: 'both',
    })
    assert.equal(records[1].from, 'shardsToStore')
  },

  'buildDownloadRecords allows duplicate shard CIDs with different source URLs and prefers direct R2': () => {
    const records = buildDownloadRecords({
      spaceDID: 'did:key:zDownload',
      inventory: {
        did: 'did:key:zDownload',
        uploads: ['root-a', 'root-b'],
        shards: [
          {
            root: 'root-a',
            cid: 'bafy-shared',
            pieceCID: 'bafk-piece-1',
            sourceURL: 'https://gateway.example.invalid/bafy-shared.car',
            sizeBytes: 100n,
          },
        ],
        shardsToStore: [
          {
            root: 'root-b',
            cid: 'bafy-shared',
            pieceCID: 'bafk-piece-1',
            sourceURL: 'https://test.r2.w3s.link/bafy-shared.car',
            sizeBytes: 100n,
          },
        ],
        skippedUploads: [],
        totalBytes: 100n,
        totalSizeToMigrate: 100n,
      },
    })

    assert.equal(records.length, 1)
    assert.deepEqual(records[0], {
      spaceDID: 'did:key:zDownload',
      shardCid: 'bafy-shared',
      pieceCID: 'bafk-piece-1',
      sizeBytes: 100n,
      sourceURL: 'https://test.r2.w3s.link/bafy-shared.car',
      relativePath: path.join('cars', 'bafy-shared.car'),
      roots: ['root-a', 'root-b'],
      from: 'both',
    })

    const aria2 = serializeAria2Input('/tmp/download', records)
    assert.match(aria2, /^https:\/\/test\.r2\.w3s\.link\/bafy-shared\.car\n/)
  },

  'prepareCarsDownload writes manifests, skips complete files, and moves mismatches aside': async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storacha-prepare-cars-'))

    try {
      const stateFile = path.join(tmpDir, 'state.json')
      const downloadDir = path.join(tmpDir, 'download')
      await fs.mkdir(path.join(downloadDir, 'cars'), { recursive: true })

      const state = createDownloadState()
      await fs.writeFile(stateFile, JSON.stringify(serializeState(state)))

      await fs.writeFile(path.join(downloadDir, 'cars', 'bafy-complete.car'), 'a'.repeat(10))
      await fs.writeFile(path.join(downloadDir, 'cars', 'bafy-mismatch.car'), 'bad')
      await fs.writeFile(path.join(downloadDir, 'cars', 'bafy-mismatch.car.aria2'), 'control')

      const result = await prepareCarsDownload({
        stateFile,
        downloadDir,
      })

      assert.equal(result.spaceDID, 'did:key:zDownloadState')
      assert.equal(result.summary.uniqueShardCount, 3)
      assert.equal(result.summary.alreadyPresentCount, 1)
      assert.equal(result.summary.queuedDownloadCount, 2)
      assert.equal(result.summary.skippedUploadCount, 1)

      const aria2 = await fs.readFile(path.join(downloadDir, 'download.aria2'), 'utf8')
      assert.match(aria2, /https:\/\/example\.invalid\/mismatch/)
      assert.match(aria2, /https:\/\/example\.invalid\/missing/)
      assert.doesNotMatch(aria2, /https:\/\/example\.invalid\/complete/)

      // bafy-mismatch has a .aria2 control file so it is a partial download —
      // it should be left in place for aria2 to resume, not moved to conflicts.
      const conflicts = await fs.readFile(path.join(downloadDir, 'conflicts.ndjson'), 'utf8')
      assert.equal(conflicts.trim(), '')

      const mismatchCarExists = await fs
        .access(path.join(downloadDir, 'cars', 'bafy-mismatch.car'))
        .then(() => true)
        .catch(() => false)
      assert.ok(mismatchCarExists, 'partial .car file should remain at original path')
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true })
    }
  },

  'selectSpaceDID requires --space when state contains multiple spaces': () => {
    const state = createInitialState()
    state.spaces['did:key:zOne'] = {
      did: 'did:key:zOne',
      phase: 'pending',
      copies: [],
    }
    state.spaces['did:key:zTwo'] = {
      did: 'did:key:zTwo',
      phase: 'pending',
      copies: [],
    }

    assert.throws(() => selectSpaceDID(state, undefined), /--space is required/)
    assert.equal(selectSpaceDID(state, 'did:key:zTwo'), 'did:key:zTwo')
  },

  'prepareCarsDownload leaves partial downloads with .aria2 control files in place': async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storacha-prepare-cars-partial-'))
    try {
      const stateFile = path.join(tmpDir, 'state.json')
      const downloadDir = path.join(tmpDir, 'download')
      await fs.mkdir(path.join(downloadDir, 'cars'), { recursive: true })

      const state = createInitialState()
      const spaceDID = 'did:key:zPartial'
      state.spaces[spaceDID] = {
        did: spaceDID,
        phase: 'pending',
        copies: createSeededCopies(),
      }
      state.spacesInventories[spaceDID] = {
        did: spaceDID,
        uploads: ['root-partial'],
        shards: [
          {
            root: 'root-partial',
            cid: 'bafy-partial',
            pieceCID: 'piece-partial',
            sourceURL: 'https://example.invalid/partial',
            sizeBytes: 1000n,
          },
        ],
        shardsToStore: [],
        skippedUploads: [],
        totalBytes: 1000n,
        totalSizeToMigrate: 1000n,
      }
      await fs.writeFile(stateFile, JSON.stringify(serializeState(state)))

      // Simulate a partial download: wrong size + aria2 control file
      await fs.writeFile(path.join(downloadDir, 'cars', 'bafy-partial.car'), 'partial')
      await fs.writeFile(path.join(downloadDir, 'cars', 'bafy-partial.car.aria2'), 'control')

      const result = await prepareCarsDownload({ stateFile, downloadDir })

      // The partial file should stay in place and be requeued
      assert.equal(result.summary.alreadyPresentCount, 0)
      assert.equal(result.summary.queuedDownloadCount, 1)

      // No conflicts: partial + control file must still exist at original path
      const carExists = await fs
        .access(path.join(downloadDir, 'cars', 'bafy-partial.car'))
        .then(() => true)
        .catch(() => false)
      assert.ok(carExists, 'partial .car file should remain at original path')

      const conflicts = await fs.readFile(path.join(downloadDir, 'conflicts.ndjson'), 'utf8')
      assert.equal(conflicts.trim(), '', 'conflicts.ndjson should be empty')
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true })
    }
  },

  'selectSpaceDID auto-selects from spacesInventories when one inventory exists': () => {
    const state = createInitialState()
    state.spaces['did:key:zA'] = {
      did: 'did:key:zA',
      phase: 'pending',
      copies: [],
    }
    state.spaces['did:key:zB'] = {
      did: 'did:key:zB',
      phase: 'pending',
      copies: [],
    }
    // Only one space has an inventory
    state.spacesInventories['did:key:zA'] = {
      did: 'did:key:zA',
      uploads: [],
      shards: [],
      shardsToStore: [],
      skippedUploads: [],
      totalBytes: 0n,
      totalSizeToMigrate: 0n,
    }

    // Auto-select should pick the space that has an inventory, not throw because there are 2 spaces
    assert.equal(selectSpaceDID(state, undefined), 'did:key:zA')
  },

  'serializeAria2Input writes URL plus destination metadata': () => {
    const text = serializeAria2Input('/tmp/download', [
      {
        spaceDID: 'did:key:zDownload',
        shardCid: 'bafy-test',
        pieceCID: undefined,
        sizeBytes: 100n,
        sourceURL: 'https://example.invalid/file',
        relativePath: path.join('cars', 'bafy-test.car'),
        roots: ['root-a'],
        from: 'shards',
      },
    ])

    assert.match(text, /^https:\/\/example\.invalid\/file\n/)
    assert.match(text, /dir=\/tmp\/download\/cars/)
    assert.match(text, /out=bafy-test\.car/)
  },
})

function createDownloadState() {
  const state = createInitialState()
  const spaceDID = 'did:key:zDownloadState'
  state.spaces[spaceDID] = {
    did: spaceDID,
    phase: 'pending',
    copies: createSeededCopies(),
  }
  state.spacesInventories[spaceDID] = {
    did: spaceDID,
    uploads: ['root-complete', 'root-mismatch', 'root-missing'],
    shards: [
      {
        root: 'root-complete',
        cid: 'bafy-complete',
        pieceCID: 'piece-complete',
        sourceURL: 'https://example.invalid/complete',
        sizeBytes: 10n,
      },
      {
        root: 'root-mismatch',
        cid: 'bafy-mismatch',
        pieceCID: 'piece-mismatch',
        sourceURL: 'https://example.invalid/mismatch',
        sizeBytes: 20n,
      },
    ],
    shardsToStore: [
      {
        root: 'root-missing',
        cid: 'bafy-missing',
        sourceURL: 'https://example.invalid/missing',
        sizeBytes: 30n,
      },
    ],
    skippedUploads: ['root-skipped'],
    totalBytes: 60n,
    totalSizeToMigrate: 60n,
  }
  return state
}

function createSeededCopies() {
  return [
    {
      copyIndex: 0,
      providerId: 11n,
      serviceProvider: '0x1111111111111111111111111111111111111111',
      providerURL: 'https://provider-1.invalid',
      dataSetId: 101n,
      pulled: new Set(),
      committed: new Set(),
      failedUploads: new Set(),
      storedShards: {},
    },
  ]
}
