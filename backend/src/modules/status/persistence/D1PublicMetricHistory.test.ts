import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Bindings } from "../../../models/db";
import { createMigratedSqliteD1 } from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import type { AgentReportSample } from "../../agents/domain/models";
import { preparePublicMinuteProjection, publicMinutePoint, type PublicMetricHour } from "../../agents/metricblock/public-projection";
import fixtures from "../../agents/metricblock/__fixtures__/blocks.json";
import { D1PublicMetricHistory } from "./D1PublicMetricHistory";
import { D1StatusRepository } from "./D1StatusRepository";
import { StatusPublicationConsumer } from "../queue/StatusPublicationConsumer";
import { Hono } from "hono";
import type { AuthVariables } from "../../../types";
import { adminSessionMiddleware } from "../../../middlewares/auth";
import { statusV2 } from "../http/routes";

const legacy = fixtures.cases.find((item) => item.name === "rollup-60s")!;
const HOUR = legacy.expect.bucketStart;
const NOW = (HOUR + 2 * 3600) * 1000;
let sqlite: SqliteD1;
let env: Bindings;

function sample(slot: number, cpu = 12): AgentReportSample {
  return {
    collected_at: new Date((HOUR + slot * 60) * 1000).toISOString(),
    cpu: { usage: cpu, model_name: "private-model" },
    memory: { total: 1000, used: 250, free: 750, usage_rate: 25 },
    load: { load1: 1, load5: 2, load15: 3 },
    network: [{ interface: "eth0", bytes_sent: 2000, bytes_recv: 3000,
      packets_sent: 20, packets_recv: 30, token: "private-token" }],
    disks: [{ mount_point: "/", total: 10000, used: 3000, free: 7000, usage_rate: 30,
      password: "private-password" }],
    ping: { "private-target": { target: "10.0.0.1", latency_ms: 10 } },
    ipv4_reachable: true,
  };
}

function insertHot(slot: number, pointCount = 60) {
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id, resolution, bucket_start, point_count, codec, byte_size, data)
     VALUES (1, 1, ?, ?, 1, 1, X'00')`
  ).run(HOUR + slot * 60, pointCount);
}

function insertLegacy() {
  const bytes = Buffer.from(legacy.data, "base64");
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id, resolution, bucket_start, point_count, codec, byte_size, data)
     VALUES (1, 60, ?, ?, 1, ?, ?)`
  ).run(HOUR, legacy.pointCount, bytes.length, bytes);
}

function readHour() {
  const row = sqlite.raw.prepare(
    "SELECT public_json FROM agent_metric_blocks WHERE resolution = 60 AND bucket_start = ?"
  ).get(HOUR) as { public_json: string };
  return JSON.parse(row.public_json) as PublicMetricHour;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW);
  sqlite = createMigratedSqliteD1();
  env = { DB: sqlite.DB } as unknown as Bindings;
  sqlite.raw.exec(`
    INSERT INTO agents (id, name, token, created_at, updated_at)
    VALUES (1, 'public', 'legacy-fixture', '2026-10-01', '2026-10-01');
    INSERT INTO agent_nodes
      (id, name, collect_interval_ms, report_interval_ms, created_at_ms, updated_at_ms)
    VALUES (1, 'public', 1000, 60000, 0, 0);
    INSERT INTO agent_runtime (agent_id, status, created_at_ms, updated_at_ms)
    VALUES (1, 'active', 0, 0);
    INSERT INTO status_pages (id, singleton_key, title, theme, created_at_ms, updated_at_ms)
    VALUES (1, 1, 'public', 'mono', 0, 0);
    INSERT INTO status_components VALUES (1, 'agent', 1, 0, 0, 0);
  `);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  sqlite.close();
});

describe("incremental public metric hours", () => {
  it("appends only the new minute and keeps missing minutes absent", async () => {
    insertHot(0);
    insertHot(2);
    await env.DB.batch([
      preparePublicMinuteProjection(env.DB, 1, sample(0), 60),
      preparePublicMinuteProjection(env.DB, 1, sample(2, 34), 60),
    ]);
    expect(readHour()).toMatchObject({ v: 1, r: 2, p: { "0": { c: 12 }, "2": { c: 34 } } });
    expect(Object.keys(readHour().p)).toEqual(["0", "2"]);
    expect(sqlite.raw.prepare("SELECT point_count, byte_size, length(data) AS bytes FROM agent_metric_blocks WHERE resolution = 60").get())
      .toMatchObject({ point_count: 0, byte_size: 0, bytes: 0 });
  });

  it("a less complete minute cannot overwrite a full public point", async () => {
    insertHot(0, 60);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    await preparePublicMinuteProjection(env.DB, 1, sample(0, 99), 30).run();
    expect(readHour().r).toBe(1);
    expect(readHour().p["0"].c).toBe(12);
  });

  it("projects a fixed whitelist and preserves null rather than inventing zero", () => {
    const projected = publicMinutePoint(sample(0));
    const json = JSON.stringify(projected);
    for (const secret of ["private-model", "private-token", "private-password", "private-target", "10.0.0.1"]) {
      expect(json).not.toContain(secret);
    }
    expect(projected.v4).toBe(1);
    expect(projected.v6).toBeNull();
    expect(publicMinutePoint({ collected_at: sample(0).collected_at }).m).toEqual([null, null, null, null]);
  });

  it("manifest returns metadata only; new hours work even with no archive payload", async () => {
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    const repository = new D1PublicMetricHistory(env);
    const first = await repository.manifest(1);
    expect(first?.hours).toEqual([{ bucket_start: HOUR, revision: "1:0:0" }]);
    expect(JSON.stringify(first)).not.toContain("private-model");
    expect(JSON.stringify(first)).not.toContain('"chunk"');
    expect(JSON.parse((await repository.hour(1, HOUR))!.payloadJson).chunk.p["0"].c).toBe(12);
    insertHot(1);
    await preparePublicMinuteProjection(env.DB, 1, sample(1), 60).run();
    expect((await repository.manifest(1))?.hours[0].revision).toBe("2:0:0");
  });

  it("converts a legacy hour once on demand, retaining every existing sample", async () => {
    insertLegacy();
    const repository = new D1PublicMetricHistory(env);
    expect(await repository.manifest(1)).not.toBeNull();
    expect(sqlite.raw.prepare("SELECT public_json FROM agent_metric_blocks").get()?.public_json).toBeNull();
    const first = await repository.hour(1, HOUR);
    expect(Object.keys(JSON.parse(first!.payloadJson).chunk.p)).toHaveLength(legacy.pointCount);
    const changes = sqlite.raw.prepare("SELECT total_changes() AS n").get()?.n;
    await repository.hour(1, HOUR);
    expect(sqlite.raw.prepare("SELECT total_changes() AS n").get()?.n).toBe(changes);
  });

  it("bulk returns ready public hours without decoding old archives", async () => {
    insertLegacy();
    insertHot(60);
    await preparePublicMinuteProjection(env.DB, 1, sample(60), 60).run();
    const prepare = vi.spyOn(env.DB, "prepare");
    const result = await new D1PublicMetricHistory(env).hours(1, [HOUR, HOUR + 3600]);
    const payload = JSON.parse(result!.payloadJson) as {
      hours: { bucket_start: number; chunk: PublicMetricHour }[];
      missing: number[];
    };
    expect(payload.hours).toHaveLength(1);
    expect(payload.hours[0].bucket_start).toBe(HOUR + 3600);
    expect(payload.missing).toEqual([HOUR]);
    expect(prepare.mock.calls.filter(([sql]) => sql.includes("FROM agent_metric_blocks"))).toHaveLength(1);
    expect(prepare.mock.calls.some(([sql]) => sql.includes("UPDATE agent_metric_blocks"))).toBe(false);
  });

  it("fills legacy gaps without replacing a new public minute or its null fields", async () => {
    insertLegacy();
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, { collected_at: sample(0).collected_at,
      cpu: { usage: 99 } }, 60).run();
    await new D1PublicMetricHistory(env).hour(1, HOUR);
    expect(Object.keys(readHour().p)).toHaveLength(legacy.pointCount);
    expect(readHour().p["0"]).toMatchObject({ c: 99, p: null, m: [null, null, null, null] });
  });

  it("merges the old prefix even when the new suffix has more public points", async () => {
    insertLegacy();
    for (let slot = 12; slot < 60; slot++) {
      insertHot(slot);
      await preparePublicMinuteProjection(env.DB, 1, sample(slot, 99), 60).run();
    }
    expect(Object.keys(readHour().p)).toHaveLength(48);
    await new D1PublicMetricHistory(env).hour(1, HOUR);
    expect(Object.keys(readHour().p)).toHaveLength(60);
    expect(readHour().p["0"].c).not.toBe(99);
    expect(readHour().p["59"].c).toBe(99);
  });

  it("does not decode a v6 sealed archive whose public minutes already exist", async () => {
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    sqlite.raw.exec(`UPDATE agent_metric_blocks SET point_count = 60, byte_size = 1,
      data = X'00', summary_json = '{"sealed":true}' WHERE resolution = 60`);
    expect(await new D1PublicMetricHistory(env).hour(1, HOUR)).not.toBeNull();
  });

  it("does not expose hours after a node becomes hidden or is removed from the page", async () => {
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    const repository = new D1PublicMetricHistory(env);
    expect(await repository.hour(1, HOUR)).not.toBeNull();
    sqlite.raw.exec("UPDATE agent_nodes SET is_hidden = 1 WHERE id = 1");
    expect(await repository.manifest(1)).toBeNull();
    expect(await repository.hour(1, HOUR)).toBeNull();
    sqlite.raw.exec("UPDATE agent_nodes SET is_hidden = 0 WHERE id = 1; DELETE FROM status_components");
    expect(await repository.hour(1, HOUR)).toBeNull();
  });

  it("keeps the old metric endpoint compatible without prebuilding daily publications", async () => {
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    const publication = await new D1StatusRepository(env).getActiveMetricPublication(1);
    expect(JSON.parse(publication!.payloadJson).agent).toMatchObject([
      { cpu_usage: 12, memory_used: 250, disk_metrics: [{ mount_point: "/" }] },
    ]);
  });

  it("serves manifest and hour anonymously but rejects hidden and deleted nodes", async () => {
    insertHot(0);
    await preparePublicMinuteProjection(env.DB, 1, sample(0), 60).run();
    const app = new Hono<{ Bindings: Bindings; Variables: AuthVariables }>();
    app.use("*", adminSessionMiddleware);
    app.route("/api/v2/status", statusV2);
    const manifestPath = "/api/v2/status/public/agents/1/metrics/manifest";
    const hourPath = `/api/v2/status/public/agents/1/metrics/hours/${HOUR}`;
    const bulkPath = `/api/v2/status/public/agents/1/metrics/hours?buckets=${HOUR}`;
    expect((await app.request(manifestPath, {}, env)).status).toBe(200);
    expect((await app.request(bulkPath, {}, env)).status).toBe(200);
    expect((await app.request(`${bulkPath},1`, {}, env)).status).toBe(400);
    const hour = await app.request(hourPath, {}, env);
    expect(hour.status).toBe(200);
    const payload = await hour.json() as { chunk: PublicMetricHour };
    expect(payload.chunk.p["0"].c).toBe(12);
    const conditional = await app.request(hourPath, { headers: { "If-None-Match": hour.headers.get("ETag")! } }, env);
    expect(conditional.status).toBe(304);
    sqlite.raw.exec("UPDATE agent_nodes SET is_hidden = 1 WHERE id = 1");
    expect((await app.request(manifestPath, {}, env)).status).toBe(404);
    expect((await app.request(hourPath, {}, env)).status).toBe(404);
    expect((await app.request(bulkPath, {}, env)).status).toBe(404);
    sqlite.raw.exec("UPDATE agent_nodes SET is_hidden = 0, deleted_at_ms = 1 WHERE id = 1");
    expect((await app.request(manifestPath, {}, env)).status).toBe(404);
    expect((await app.request(hourPath, {}, env)).status).toBe(404);
    expect((await app.request(bulkPath, {}, env)).status).toBe(404);
  });

  it("status publication never reads or decodes historical blocks", async () => {
    sqlite.raw.prepare(
      `INSERT INTO agent_metric_blocks
       (agent_id, resolution, bucket_start, point_count, codec, byte_size, data)
       VALUES (1, 60, ?, 60, 1, 1, X'00')`
    ).run(HOUR);
    const prepare = vi.spyOn(env.DB, "prepare");
    await new StatusPublicationConsumer(env).process({
      event_id: "status-test", event_type: "status.rebuild.requested",
      aggregate_type: "status_page", aggregate_id: "1", payload_json: "{}", status: "published",
    });
    expect(prepare.mock.calls.some(([sql]) => sql.includes("FROM agent_metric_blocks"))).toBe(false);
    expect(sqlite.raw.prepare("SELECT COUNT(*) AS n FROM status_metric_publications").get()?.n).toBe(0);
    expect(sqlite.raw.prepare("SELECT COUNT(*) AS n FROM status_publications").get()?.n).toBe(1);
  });
});
