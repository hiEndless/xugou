import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Bindings } from "../../../models/db";
import { createMigratedSqliteD1 } from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import { parseBinaryReport } from "../http/binary-report";
import { D1AgentReportIngestor, AgentBlockRejected } from "./D1AgentReportIngestor";
import { queryAgentSamples } from "../metricblock/query";
import { decodeBlock } from "../metricblock/decode";
import { blockSamples } from "../metricblock/materialize";

const fixture = () => new Uint8Array(readFileSync(resolve(process.cwd(), "../agent/pkg/reporter/testdata/report-v6.bin")));
let sqlite: SqliteD1;
let env: Bindings;

beforeEach(() => {
  sqlite = createMigratedSqliteD1();
  sqlite.raw.exec(`
    INSERT INTO agents (id,name,token,created_at,updated_at) VALUES (1,'fixture','unused','2026-09-30','2026-09-30');
    INSERT INTO agent_nodes (id,name,collect_interval_ms,report_interval_ms,created_at_ms,updated_at_ms) VALUES (1,'fixture',1000,60000,0,0);
    INSERT INTO agent_runtime (agent_id,status,created_at_ms,updated_at_ms) VALUES (1,'active',0,0);
    INSERT INTO notification_rules (target_type,target_id,enabled,on_cpu_threshold,cpu_threshold,on_memory_threshold,memory_threshold,on_disk_threshold,disk_threshold,created_at_ms,updated_at_ms)
    VALUES ('agent',1,1,1,80,1,80,1,90,0,0);
  `);
  env = { DB: sqlite.DB } as unknown as Bindings;
});
afterEach(() => { sqlite.close(); vi.restoreAllMocks(); });

describe("Go v6 跨语言摄入", () => {
  it("有界二进制解析拒绝截断、尾部和元数据长度错误", () => {
    const input = fixture();
    expect(parseBinaryReport(input).protocol_version).toBe(6);
    expect(() => parseBinaryReport(input.subarray(0, input.length - 1))).toThrow();
    expect(() => parseBinaryReport(new Uint8Array([...input, 0]))).toThrow();
    const changed = input.slice();
    new DataView(changed.buffer).setUint32(4, 0xffffffff, true);
    expect(() => parseBinaryReport(changed)).toThrow();
  });

  it("不解压归档即可保存原字节、记账和发布分钟投影", async () => {
    const report = parseBinaryReport(fixture());
    const inflate = vi.spyOn(globalThis, "DecompressionStream");
    await new D1AgentReportIngestor(env).process(1, report, { receivedAt: "2026-10-01T00:00:10.000Z" });
    expect(inflate).not.toHaveBeenCalled();
    const current = sqlite.raw.prepare("SELECT metrics_json,month_rx,month_tx FROM agent_current_metrics WHERE agent_id=1").get()!;
    const metrics = JSON.parse(String(current.metrics_json));
    expect(metrics.threshold_state).toEqual({ cpu: true, memory: true, disk: true });
    expect(metrics.tcp_connections).toBe(0);
    expect(metrics.udp_connections).toBe(0);
    expect(report.summary!.cpu_usage_peak).toBe(88.89);
    expect(report.summary!.memory_usage_peak).toBe(90);
    expect(report.summary!.disk_usage_peak).toBe(99);
    expect(Number(current.month_rx)).toBe(40);
    expect(Number(current.month_tx)).toBe(50);
    for (const block of report.blocks) {
      const row = sqlite.raw.prepare("SELECT data,summary_json,byte_size FROM agent_metric_blocks WHERE agent_id=1 AND resolution=? AND bucket_start=?")
        .get(block.resolution, block.bucket_start)!;
      expect(new Uint8Array(row.data as Uint8Array)).toEqual(block.data);
      expect(Number(row.byte_size)).toBe((block.data as Uint8Array).byteLength +
        new TextEncoder().encode(String(row.summary_json ?? "")).byteLength);
    }
    const hours = sqlite.raw.prepare("SELECT public_json FROM agent_metric_blocks WHERE agent_id=1 AND resolution=60 ORDER BY bucket_start").all();
    expect(hours).toHaveLength(2);
    expect(hours.every(row => JSON.parse(String(row.public_json)).v === 1)).toBe(true);
    const first = report.minute_summaries!.at(-1)!;
    const window = await queryAgentSamples(env, 1, first.bucket_start, first.bucket_start + 3599, { resolution: 60 });
    expect(window.samples).toHaveLength(1);
    expect(window.samples[0].timestampMs).toBe(first.bucket_start * 1000);
    expect(window.samples[0].tcpConnections).toBe(0);
    expect(window.samples[0].udpConnections).toBe(0);
  });

  it("旧v5仍完整校验，摘要与旧样本的末值一致", async () => {
    const report = parseBinaryReport(fixture());
    const lastHot = report.blocks.filter(block => block.resolution === 1).at(-1)!;
    const decoded = await decodeBlock(lastHot.data as Uint8Array);
    const last = blockSamples(decoded).at(-1)!;
    const legacy = { ...report, protocol_version: 5 as const, summary: undefined, minute_summaries: undefined };
    await new D1AgentReportIngestor(env).process(1, legacy, { receivedAt: "2026-10-01T00:00:10.000Z" });
    const current = sqlite.raw.prepare("SELECT collected_at_ms FROM agent_current_metrics WHERE agent_id=1").get()!;
    expect(current.collected_at_ms).toBe(last.timestampMs);
    const broken = (legacy.blocks[0].data as Uint8Array).slice();
    broken[broken.length - 1] ^= 0xff;
    legacy.blocks[0] = { ...legacy.blocks[0], data: broken };
    await expect(new D1AgentReportIngestor(env).process(1, legacy, { receivedAt: "2026-10-01T00:01:10.000Z" }))
      .rejects.toBeInstanceOf(AgentBlockRejected);
  });

  it("摘要必须与块的分钟、点数和最新采样时间匹配", async () => {
    const report = parseBinaryReport(fixture());
    report.minute_summaries![0].point_count = 59;
    await expect(new D1AgentReportIngestor(env).process(1, report, { receivedAt: "2026-10-01T00:00:10.000Z" }))
      .rejects.toBeInstanceOf(AgentBlockRejected);
    expect(sqlite.raw.prepare("SELECT COUNT(*) AS n FROM agent_metric_blocks").get()!.n).toBe(0);
  });

  it("旧v5覆盖同一原始块时，保留的摘要仍计入热数据字节预算", async () => {
    const report = parseBinaryReport(fixture());
    const ingestor = new D1AgentReportIngestor(env);
    await ingestor.process(1, report, { receivedAt: "2026-10-01T00:00:10.000Z" });
    const legacy = { ...report, protocol_version: 5 as const, summary: undefined, minute_summaries: undefined };
    await ingestor.process(1, legacy, { receivedAt: "2026-10-01T00:00:20.000Z" });
    const rows = sqlite.raw.prepare("SELECT data,summary_json,byte_size FROM agent_metric_blocks WHERE resolution=1").all();
    expect(rows.length).toBeGreaterThan(0);
    for (const row of rows) {
      expect(row.summary_json).not.toBeNull();
      expect(Number(row.byte_size)).toBe((row.data as Uint8Array).byteLength +
        new TextEncoder().encode(String(row.summary_json)).byteLength);
    }
  });
});
