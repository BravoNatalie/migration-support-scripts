import { mkdtempSync, rmSync } from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, describe, expect, it } from 'vitest'

import { runAggregatePlan } from '../src/commands/aggregate-plan.mjs'
import { pieceAggregateCommP } from '../src/lib/piece-aggregate.mjs'
import { openTrackingDb } from '../src/lib/tracking-db.mjs'

const fixture = {
  members: [
    {
      pieceCid: 'bafkzcibf3ck4uais4fgennh4hbfx5z3i6hue4xgq2cdeamtus4hjbsrjs5lf2azbxmsa',
      rawSize: 5010728,
    },
    {
      pieceCid: 'bafkzcibewpkqwewyhz3yxutlxbpt2nkb6si5qilg4qqtzzij32uw7ammsc73a4wkgi',
      rawSize: 8131917,
    },
    {
      pieceCid: 'bafkzcibftk7jmaytlewyhwwszv2ej4wws45pkn2dsquy4mfaytqap4xnaegks3nfxyhq',
      rawSize: 9986278,
    },
    {
      pieceCid: 'bafkzcibfywz24aiswrwkb7fp3garu3xmbztlzrfs5tnjhcs2flqj2uwgmlz4226qkq5q',
      rawSize: 5465659,
    },
    {
      pieceCid: 'bafkzcibf2wepqaytigd6ubsmcli2kcykixhxucikocupwbrtuf4ugtdrh62sm3365qbq',
      rawSize: 8387499,
    },
    {
      pieceCid: 'bafkzcibevkergel4lcnclf654q66sb573y4hxhbgatphkv4kn7ffcdipn72nllzxey',
      rawSize: 3849046,
    },
    {
      pieceCid: 'bafkzcibfrsfzuaisf25aejnvxcjttnilwjcwikefrggjfsllqh37ljnihkjqae7laqhq',
      rawSize: 5798516,
    },
    {
      pieceCid: 'bafkzcibeuxfqedw2eu4aonj3b4ukwt7nbztcuqg2fkdpfd4wkmynwp6kz3cmelifde',
      rawSize: 477787,
    },
    {
      pieceCid: 'bafkzcibftwfpqaqtfxjn372452uukqi7tdp4x7an52og64hlnec74s2zj3tetcm77q2a',
      rawSize: 10484451,
    },
    {
      pieceCid: 'bafkzcibet6irceizvphuyaguz5duoanefmeaq4musuqrhw2bqhgvxadci4golk2idi',
      rawSize: 3880801,
    },
    {
      pieceCid: 'bafkzcibe4xib6d3rekjzko3hjdd7uieqsvujomiwzgmbw37wtlaxbegdkg6bmsoebi',
      rawSize: 522139,
    },
    {
      pieceCid: 'bafkzcibfsloz4ait55tlodbxeld24hsgkq74ht43zf67x7v6guuqt4wuyc74fvhlka6a',
      rawSize: 14045550,
    },
    {
      pieceCid: 'bafkzcibevwtaqdx6g4ut7ecrl747cwbjug42wz77fvyuiisddbgmwimvxn6fai6pfa',
      rawSize: 384211,
    },
    {
      pieceCid: 'bafkzciberdyagdtwvtywrxfjpy5ux24jug5ueuemwn63ifhl4mejfqyt2nnun2gpbe',
      rawSize: 456696,
    },
    {
      pieceCid: 'bafkzcibf5s4n4div4c5csy26sn2embdvzgecstiytq3ksoy7ayszxqmh5gpkv7pvxelq',
      rawSize: 37774228,
    },
    {
      pieceCid: 'bafkzcibeyd5audtir5hjskjt4nny6a4xwzvjhiaxl6ft3kdwqvxl3pvqv5r5pfvbei',
      rawSize: 340672,
    },
    {
      pieceCid: 'bafkzcibetcrese3353i2n6itsmsw7w46v2j6iktgciovxpwrd4omdlhppqbpsakphq',
      rawSize: 15445736,
    },
    {
      pieceCid: 'bafkzcibf632iaaiskwf2xhlnwfu3lyzkbm74basbwqkbo3g3kyxoids5a47bccrq347a',
      rawSize: 6210954,
    },
    {
      pieceCid: 'bafkzcibexdhr6d2tl633vicwhyonakdeeom4mmsidtnkdyeljlrlseryhyob2hl7ha',
      rawSize: 522312,
    },
    {
      pieceCid: 'bafkzcibe22equdsmjoovpxl3pixn346inodko5vbrnysy562wa4peyj3mjxkgumgbe',
      rawSize: 355114,
    },
    {
      pieceCid: 'bafkzcibev3jaodtfvhairc2nriggnje3elsqtclurk4fpow5icitbu3umg3rgqtyeu',
      rawSize: 394962,
    },
    {
      pieceCid: 'bafkzcibeyo7gie3mntxn3ixmm45l4s7mhsibbia762rc5kkvrkjrmfngvpphc6gkgy',
      rawSize: 14999741,
    },
    {
      pieceCid: 'bafkzcibftd3i4bauoalbw77ytalkeugb6klfnkn2uyeeo5lh625sfvnb4375jp7liqlq',
      rawSize: 24659176,
    },
    {
      pieceCid: 'bafkzcibeqkbxwe6qiho54ppof37nuu76prxcyvwlbcumzq7fjhrt5unatyq7zyt5ci',
      rawSize: 14630526,
    },
    {
      pieceCid: 'bafkzcibe236asdu7b7xorgtsc6srur6yvxolxplck2vz7zdvp7smideylja2ulsece',
      rawSize: 356778,
    },
    {
      pieceCid: 'bafkzcibfy2i2iaisb34qexxoe3npnuwnk7cqsxmtpktwymeguz6rqh3i5azb7nap4yxq',
      rawSize: 5633850,
    },
    {
      pieceCid: 'bafkzcibftspyyaisfzettpxzxy7gaah3ebep6zvkfgjpjs7zf77s45bizqov5qgny4ga',
      rawSize: 6025316,
    },
    {
      pieceCid: 'bafkzcibe43baief3tf4g3u22ki6ha62z3ilsrozoxcsuo4ozmsijineqrlrbrjadai',
      rawSize: 2006682,
    },
    {
      pieceCid: 'bafkzcibeyprvsexa6gcw3zytnia3sbhijdiz747wwdqipy52etei4dohvxvycv5dei',
      rawSize: 6852157,
    },
    {
      pieceCid: 'bafkzcibe4hvaudw6be6rvv24ahz4mvkgedvs6tutplqedznt3z5u3j7lnxs5fegjcu',
      rawSize: 342687,
    },
    {
      pieceCid: 'bafkzcibe3w6amdvxp6so6ph5mxgpzuwrf56l2vbofiuebhust25x4b2a5c6awz5fhe',
      rawSize: 414115,
    },
    {
      pieceCid: 'bafkzciberxyqwdqyztnvff3gtmt42rzazqmfzoz4rn7qwog3sxpyubevktyzo7xccy',
      rawSize: 325491,
    },
    {
      pieceCid: 'bafkzcibeyhpa4drujqcnxr6ganqeftr2ciercnw6d6isf6oxrlvgybjcds4kntm2hy',
      rawSize: 278719,
    },
    {
      pieceCid: 'bafkzcibey2caudq2czqzepa4uetgmij2fyduuoqqfsb4yrxc6zs5dbezbdwekjl7em',
      rawSize: 355770,
    },
    {
      pieceCid: 'bafkzcibeyxbqidujg3wtyvdj2atcwvewnw77lsclxrrmnv46ld2kgpymtpnwde2kby',
      rawSize: 446011,
    },
    {
      pieceCid: 'bafkzcibftgtl6aiscrvsdksvsa3col6ldac3l46ate46ejfuv4yrvfcexqpzz4sipu5a',
      rawSize: 5188839,
    },
    {
      pieceCid: 'bafkzcibe2gcasdqhjhgu2qutb6spdhubprphxbctw2sic7vqqnzqsqe4ek6exekiea',
      rawSize: 372143,
    },
    {
      pieceCid: 'bafkzcibezlwacdqh4nyegflo3xgwcklxyhx5ruegaxbpq23j5n3mn4kzm2mb6taaba',
      rawSize: 489910,
    },
    {
      pieceCid: 'bafkzcibe4giq4dt5s3awr2q34dts65jpxnu2jahhtaxhsu24zkdkpvtg3glst5rcbe',
      rawSize: 288543,
    },
    {
      pieceCid: 'bafkzcibe6seq4dsse5ew5skh7ezdjtclssbwistmfskjvd225ga2mgwduijfng5nai',
      rawSize: 289548,
    },
    {
      pieceCid: 'bafkzcibes7ovwexuvh53yspakcsz5gkc33ims7nulnzd42hqqyxt4hysatpv472qay',
      rawSize: 6820201,
    },
    {
      pieceCid: 'bafkzcibe2hgqsdua2qmm5b7zbfixiqrtdprrnopcxqelzhqc36pjii63hmabid37ey',
      rawSize: 362799,
    },
    {
      pieceCid: 'bafkzcibeqkfq2dtbysct4sejjwsqdc4o4cgqsrxfsb5ivkiyekeerupvbkasy3sifu',
      rawSize: 305790,
    },
    {
      pieceCid: 'bafkzcibesglu2ei4xsq4aooosjtik5rptmba3upyioneh22lmdqewzjo7fnfuxdlei',
      rawSize: 2897007,
    },
    {
      pieceCid: 'bafkzcibew3zxaekghnmvngmbecktrbrfxqdo4vwo4mmyscc5tljc263nzllrwdfgei',
      rawSize: 2311754,
    },
    {
      pieceCid: 'bafkzcibfshj4kaisomxcki7suzkg6zi4jirpmxyc4s6gr26qqnoaypp2gj6mvqmoteqa',
      rawSize: 5084783,
    },
    {
      pieceCid: 'bafkzcibesd4qsdvskid22citxzhcrlnolqhbor2fg3sgfdrqf2r55atu6iuqxql4de',
      rawSize: 357232,
    },
    {
      pieceCid: 'bafkzcibf2cdncaisijfollen3dy2fixtvcc4bamyka34dxq6usgw4xyuarsek3ktcira',
      rawSize: 4897968,
    },
  ],
  expectedRoot: 'bafkzcibf7cvzwcqxm5cdi3s3pavdxy65bylec3rho3frrrx4f2wc7e4jxaofhsvku4gq',
  expectedRawSize: 244918792,
}

const cleanups = []
afterEach(() => {
  while (cleanups.length) cleanups.pop()()
})

function tempDir(prefix) {
  const dir = mkdtempSync(path.join('/private/tmp', prefix))
  cleanups.push(() => rmSync(dir, { recursive: true, force: true }))
  return dir
}

describe('pieceAggregateCommP', () => {
  it('matches the upstream Curio-verified aggregate fixture', () => {
    const aggregate = pieceAggregateCommP(fixture.members)

    expect(aggregate.rootPieceCid).toBe(fixture.expectedRoot)
    expect(aggregate.rawSize).toBe(fixture.expectedRawSize)
  })

  it('returns the original PieceCID for a single sub-piece', () => {
    const member = fixture.members[0]
    const aggregate = pieceAggregateCommP([member])

    expect(aggregate.rootPieceCid).toBe(member.pieceCid)
    expect(aggregate.orderedSubPieceCids).toEqual([member.pieceCid])
    expect(aggregate.rawSize).toBe(member.rawSize)
  })
})

function addCommittedPieces(dir, pieceCids, startIndex = 0) {
  const raw = new DatabaseSync(path.join(dir, 'tracking.db'))
  raw.exec('PRAGMA foreign_keys = OFF')
  const insert = raw.prepare(
    "INSERT INTO root_shards (root_cid, shard_cid, piece_cid, commit_status, commit_attempts, updated_at) VALUES (?, ?, ?, 'committed', 0, 0)",
  )
  pieceCids.forEach((pieceCid, index) => {
    const rowIndex = startIndex + index
    insert.run(`root-${rowIndex}`, `shard-${rowIndex}`, pieceCid)
  })
  raw.close()
}

function makeTrackingWithCommittedPieces(pieceCids) {
  const dir = tempDir('aggregate-tracking-')
  const tracking = openTrackingDb(dir)
  tracking.close()
  addCommittedPieces(dir, pieceCids)

  return dir
}

describe('runAggregatePlan', () => {
  it('stores aggregate plan rows with provider-ordered committed pieces', async () => {
    const members = fixture.members.slice(0, 3)
    const dir = makeTrackingWithCommittedPieces(members.map((member) => member.pieceCid))

    await runAggregatePlan({ dir, maxSizeBytes: 1n << 30n })

    const tracking = openTrackingDb(dir)
    cleanups.push(() => tracking.close())

    const aggregates = tracking.listPlannedAggregatePieces(10)
    expect(aggregates).toHaveLength(1)

    const subPieces = tracking.listAggregateSubPieces(aggregates[0].aggregateId, 10)
    const aggregateMembers = subPieces.map((row) => {
      const fixtureMember = members.find((member) => member.pieceCid === row.subPieceCid)
      return { pieceCid: row.subPieceCid, rawSize: fixtureMember.rawSize }
    })
    const expected = pieceAggregateCommP(aggregateMembers)

    expect(aggregates[0].aggregatePieceCid).toBe(expected.rootPieceCid)
    expect(subPieces.map((row) => row.position)).toEqual([0, 1, 2])
    expect(subPieces.map((row) => row.subPieceCid)).toEqual(expected.orderedSubPieceCids)
    expect(new Set(subPieces.map((row) => row.subPieceCid))).toEqual(new Set(members.map((member) => member.pieceCid)))
  })

  it('does not re-plan committed pieces already present in aggregate sub-pieces', async () => {
    const members = fixture.members.slice(0, 3)
    const dir = makeTrackingWithCommittedPieces(members.slice(0, 2).map((member) => member.pieceCid))

    await runAggregatePlan({ dir, maxSizeBytes: 1n << 30n })
    addCommittedPieces(dir, [members[2].pieceCid], 2)
    await runAggregatePlan({ dir, maxSizeBytes: 1n << 30n })

    const tracking = openTrackingDb(dir)
    cleanups.push(() => tracking.close())

    const aggregates = tracking.listPlannedAggregatePieces(10)
    const subPieces = aggregates.flatMap((aggregate) =>
      tracking.listAggregateSubPieces(aggregate.aggregateId, 10).map((row) => row.subPieceCid),
    )

    expect(aggregates).toHaveLength(2)
    expect(subPieces).toHaveLength(3)
    expect(new Set(subPieces)).toEqual(new Set(members.map((member) => member.pieceCid)))
    expect(tracking.countUnplannedCommittedPieceCids()).toBe(0)
  })

  it('supports planned aggregate and sub-piece batch queries', async () => {
    const members = fixture.members.slice(0, 3)
    const dir = makeTrackingWithCommittedPieces(members.map((member) => member.pieceCid))

    await runAggregatePlan({ dir, maxSizeBytes: 1n << 30n })

    const tracking = openTrackingDb(dir)
    cleanups.push(() => tracking.close())

    const [aggregate] = tracking.listPlannedAggregatePieces(1, 0)
    expect(aggregate.aggregatePieceCid).toBeTruthy()
    expect(tracking.listPlannedAggregatePieces(1, aggregate.aggregateId)).toEqual([])

    const firstSubPieces = tracking.listAggregateSubPieces(aggregate.aggregateId, 2, -1)
    expect(firstSubPieces).toHaveLength(2)
    const nextSubPieces = tracking.listAggregateSubPieces(aggregate.aggregateId, 2, firstSubPieces[1].position)
    expect(nextSubPieces).toHaveLength(1)
  })

  it('does not store oversized committed pieces in the aggregate plan', async () => {
    const member = fixture.members[0]
    const dir = makeTrackingWithCommittedPieces([member.pieceCid])

    await expect(runAggregatePlan({ dir, maxSizeBytes: 1n })).rejects.toThrow(/oversized committed pieces/)

    const tracking = openTrackingDb(dir)
    cleanups.push(() => tracking.close())
    expect(tracking.listPlannedAggregatePieces(10)).toEqual([])
  })
})
