import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Bindings } from "../../../models/db";
import { createMigratedSqliteD1 } from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import type { AgentReportBlock } from "../domain/models";
import { preserveReplacedHours, readLegacyArchiveSamples, type PreservedArchiveHour } from "./archive-compat";
import * as codec from "./decode";
import { queryAgentSamples, queryAgentSamplesBatch } from "./query";
import { CODEC_VERSION, ENCODING_RAW, FLAG_PRESENCE, HEADER_SIZE, MAGIC } from "./registry";

const HOUR = Date.UTC(2026, 9, 25, 12) / 1000;
let sqlite: SqliteD1;
let env: Bindings;

/** 独立的未压缩小时块，包含管理端需要保留而公开投影不包含的 ping 字段。 */
function hourBlock(slots: number[], base: number, bucketStart = HOUR) {
  const dimensions: number[] = [];
  const appendName = (name: string) => {
    const encoded = new TextEncoder().encode(name);
    dimensions.push(encoded.length, ...encoded);
  };
  dimensions.push(1);
  appendName("/private-mount");
  const total = new Uint8Array(8);
  new DataView(total.buffer).setBigUint64(0, 1000n, true);
  dimensions.push(...total, 1);
  appendName("private-nic");
  dimensions.push(1);
  appendName("private-target");
  const descriptors = [
    { id: 1, scale: 2 }, { id: 8, scale: 0 }, { id: 100, scale: 0 },
    { id: 200, scale: 0 }, { id: 500, scale: 2 },
  ];
  const header = new Uint8Array(HEADER_SIZE);
  header[0] = MAGIC;
  header[1] = CODEC_VERSION;
  header[2] = 60;
  header[3] = FLAG_PRESENCE;
  header[4] = 3;
  const view = new DataView(header.buffer);
  view.setUint16(6, descriptors.length, true);
  view.setUint16(8, 60, true);
  view.setUint32(12, bucketStart, true);
  view.setBigUint64(16, 1000n, true);
  const data = [...header, ...dimensions];
  for (const descriptor of descriptors) {
    data.push(descriptor.id & 255, descriptor.id >> 8, ENCODING_RAW, descriptor.scale);
  }
  const bitmap = new Uint8Array(8);
  for (const slot of slots) bitmap[slot >> 3] |= 1 << (slot & 7);
  for (const descriptor of descriptors) {
    for (let aggregate = 0; aggregate < 3; aggregate++) {
      data.push(...bitmap);
      for (const slot of slots) {
        const value = descriptor.id === 1 ? (base + slot) * 100 : descriptor.id * 100 + slot;
        let encoded = BigInt(value + aggregate) << 1n;
        while (encoded >= 128n) {
          data.push(Number(encoded & 127n) | 128);
          encoded >>= 7n;
        }
        data.push(Number(encoded));
      }
    }
  }
  const raw = Uint8Array.from(data);
  const block: AgentReportBlock = {
    resolution: 60, bucket_start: bucketStart, point_count: slots.length,
    codec: 1, data: raw,
  };
  return { block, raw };
}

function insertHour(input: ReturnType<typeof hourBlock>, summary: string | null = null) {
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id,resolution,bucket_start,point_count,codec,byte_size,data,summary_json)
     VALUES(1,60,?,?,1,?,?,?)`
  ).run(input.block.bucket_start, input.block.point_count, input.raw.length, input.raw, summary);
}

function replaceHour(input: ReturnType<typeof hourBlock>, preserved: PreservedArchiveHour) {
  sqlite.raw.prepare(
    `UPDATE agent_metric_blocks SET point_count=?,byte_size=?,data=?,summary_json=?,public_json=?
     WHERE agent_id=1 AND resolution=60 AND bucket_start=?`
  ).run(input.block.point_count, input.raw.length, input.raw, preserved.summaryJson,
    preserved.publicJson, input.block.bucket_start);
}

beforeEach(() => {
  sqlite = createMigratedSqliteD1();
  sqlite.raw.prepare("INSERT INTO agents(id,name,token,created_at,updated_at) VALUES(1,?,?,?,?)")
    .run("fixture", "unused", "2026-01-01", "2026-01-01");
  sqlite.raw.prepare(
    "INSERT INTO agent_nodes(id,name,collect_interval_ms,report_interval_ms,created_at_ms,updated_at_ms) VALUES(1,?,1000,60000,0,0)"
  ).run("fixture");
  env = { DB: sqlite.DB } as unknown as Bindings;
});

afterEach(() => { sqlite.close(); vi.restoreAllMocks(); });

describe("v5 与 v6 小时归档替换", () => {
  for (const oldCount of [10, 50]) {
    it(`旧 ${oldCount} 分钟与新 ${60 - oldCount} 分钟合并，保留完整管理端历史`, async () => {
      const old = hourBlock(Array.from({ length: oldCount }, (_, slot) => slot), 10);
      const incoming = hourBlock(Array.from({ length: 60 - oldCount }, (_, slot) => oldCount + slot), 20);
      insertHour(old);

      const saved = (await preserveReplacedHours(env, 1, [incoming])).get(HOUR)!;
      expect(readLegacyArchiveSamples(saved.summaryJson)).toHaveLength(oldCount);
      replaceHour(incoming, saved);

      const result = await queryAgentSamples(env, 1, HOUR, HOUR + 7200, { resolution: 60 });
      expect(result.corruptBlocks).toBe(0);
      expect(result.samples).toHaveLength(60);
      expect(result.samples.map((sample) => sample.timestampMs))
        .toEqual(Array.from({ length: 60 }, (_, slot) => (HOUR + slot * 60) * 1000));
      expect(result.samples[0].pings[0].key).toBe("private-target");
      expect(result.samples[0].nets[0].iface).toBe("private-nic");
      expect(result.samples[0].disks[0].mountPoint).toBe("/private-mount");
      expect(result.samples[0].cpuUsage).toBe(10);
      expect(result.samples[oldCount].cpuUsage).toBe(20 + oldCount);
      const batch = await queryAgentSamplesBatch(env, [1], HOUR, HOUR + 7200, { resolution: 60 });
      expect(batch.get(1)).toEqual(result.samples);

      const publicData = JSON.parse(saved.publicJson!);
      expect(Object.keys(publicData.p)).toHaveLength(60);
      expect(saved.publicJson).not.toContain("private-target");
    });
  }

  it("再次替换保留更早的兼容样本，并让新归档覆盖同一时间槽", async () => {
    insertHour(hourBlock(Array.from({ length: 50 }, (_, slot) => slot), 10));
    const first = hourBlock(Array.from({ length: 10 }, (_, slot) => 50 + slot), 20);
    replaceHour(first, (await preserveReplacedHours(env, 1, [first])).get(HOUR)!);
    const second = hourBlock(Array.from({ length: 20 }, (_, slot) => 40 + slot), 30);
    const preserved = (await preserveReplacedHours(env, 1, [second])).get(HOUR)!;
    expect(readLegacyArchiveSamples(preserved.summaryJson)).toHaveLength(40);
    replaceHour(second, preserved);
    const result = await queryAgentSamples(env, 1, HOUR, HOUR + 7200, { resolution: 60 });
    expect(result.samples).toHaveLength(60);
    expect(result.samples[39].cpuUsage).toBe(49);
    expect(result.samples[40].cpuUsage).toBe(70);
    expect(result.samples[59].cpuUsage).toBe(89);
  });

  it("同字节归档与空 placeholder 只做一次索引查询，不解码", async () => {
    const existing = hourBlock([0], 10);
    const summary = JSON.stringify({ sealed: true, legacy_samples: [] });
    insertHour(existing, summary);
    const fresh = hourBlock([0], 20, HOUR + 3600);
    sqlite.raw.prepare(
      `INSERT INTO agent_metric_blocks(agent_id,resolution,bucket_start,point_count,codec,byte_size,data)
       VALUES(1,60,?,0,1,0,?)`
    ).run(HOUR + 3600, new Uint8Array());
    const decode = vi.spyOn(codec, "decodeBlock");
    const prepare = vi.spyOn(sqlite.DB, "prepare");

    const result = await preserveReplacedHours(env, 1, [existing, fresh]);

    expect(prepare).toHaveBeenCalledOnce();
    expect(decode).not.toHaveBeenCalled();
    expect(result.get(HOUR)).toEqual({ summaryJson: summary, publicJson: null });
    expect(result.has(HOUR + 3600)).toBe(false);
  });

  it("相同字节的旧 v5 块仍补齐公开投影，再标记封存", async () => {
    const existing = hourBlock([0, 1, 2], 10);
    insertHour(existing);
    const decode = vi.spyOn(codec, "decodeBlock");

    const saved = (await preserveReplacedHours(env, 1, [existing])).get(HOUR)!;

    expect(decode).toHaveBeenCalledOnce();
    expect(JSON.parse(saved.summaryJson)).toEqual({ sealed: true, legacy_samples: [] });
    expect(Object.keys(JSON.parse(saved.publicJson!).p)).toEqual(["0", "1", "2"]);
  });

  it("较早的未封存小时仍补分钟摘要，已封存小时不读旧 hot 摘要", async () => {
    insertHour(hourBlock([0], 10));
    sqlite.raw.prepare("UPDATE agent_metric_blocks SET public_json=? WHERE agent_id=1")
      .run(JSON.stringify({ v: 1, r: 1, p: {} }));
    const at = HOUR + 1800;
    const summary = JSON.stringify({ collected_at: new Date(at * 1000).toISOString(), cpu: { usage: 42 } });
    sqlite.raw.prepare(
      `INSERT INTO agent_metric_blocks(agent_id,resolution,bucket_start,point_count,codec,byte_size,data,summary_json)
       VALUES(1,1,?,60,1,0,?,?)`
    ).run(at, new Uint8Array(), summary);
    const pending = await queryAgentSamples(env, 1, HOUR, HOUR + 7200, { resolution: 60 });
    expect(pending.samples.map((sample) => sample.cpuUsage)).toEqual([10, 42]);

    sqlite.raw.prepare("UPDATE agent_metric_blocks SET summary_json=? WHERE resolution=60")
      .run(JSON.stringify({ sealed: true }));
    const sealed = await queryAgentSamples(env, 1, HOUR, HOUR + 7200, { resolution: 60 });
    expect(sealed.samples.map((sample) => sample.cpuUsage)).toEqual([10]);
  });
});
