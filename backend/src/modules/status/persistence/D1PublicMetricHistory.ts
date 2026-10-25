import type { Bindings } from "../../../models/db";
import { decodeBlock } from "../../agents/metricblock/decode";
import { blockSamples } from "../../agents/metricblock/materialize";
import {
  publicMinutePointFromBlock,
  type PublicMetricHour,
  type PublicMinutePoint,
} from "../../agents/metricblock/public-projection";
import { writeStructuredLog } from "../../../platform/observability/StructuredLogger";
import type { PublicMetricManifest } from "../domain/models";

interface HourRow {
  id: number;
  bucket_start: number;
  point_count: number;
  byte_size: number;
  data: ArrayBuffer | Uint8Array;
  public_json: string | null;
  revision: number;
  legacy_points: number | null;
  sealed: number | null;
}

function revision(row: Pick<HourRow, "revision" | "point_count" | "byte_size">) {
  return `${row.revision ?? 0}:${row.point_count}:${row.byte_size}`;
}

export function expandPublicMinute(
  agentId: number,
  timestamp: number,
  point: PublicMinutePoint
) {
  return {
    id: `${agentId}:${timestamp}`,
    agent_id: agentId,
    timestamp: new Date(timestamp).toISOString(),
    cpu_usage: point.c,
    memory_total: point.m[0], memory_used: point.m[1],
    memory_free: point.m[2], memory_usage_rate: point.m[3],
    load_1: point.l[0], load_5: point.l[1], load_15: point.l[2],
    swap_total: point.s[0], swap_used: point.s[1],
    process_count: point.p, tcp_connections: point.t, udp_connections: point.u,
    ipv4_reachable: point.v4, ipv6_reachable: point.v6,
    disk_metrics: point.d.map(([mount, total, used, free, rate]) => ({
      device: String(mount ?? ""), mount_point: String(mount ?? ""),
      total: Number(total ?? 0), used: Number(used ?? 0), free: Number(free ?? 0),
      usage_rate: Number(rate ?? 0), fs_type: "",
    })),
    network_metrics: point.n.map(([iface, sent, recv, packetsSent, packetsRecv]) => ({
      interface: String(iface ?? ""), bytes_sent: Number(sent ?? 0),
      bytes_recv: Number(recv ?? 0), packets_sent: Number(packetsSent ?? 0),
      packets_recv: Number(packetsRecv ?? 0),
    })),
  };
}

/** Public historical data shares the block table; publication never materializes it. */
export class D1PublicMetricHistory {
  constructor(private readonly env: Bindings) {}

  async isPublic(agentId: number) {
    return Boolean(await this.env.DB.prepare(
      `SELECT node.id FROM agent_nodes node
       JOIN status_components component ON component.component_id = node.id
         AND component.component_type = 'agent'
       JOIN status_pages page ON page.id = component.page_id
       WHERE page.singleton_key = 1 AND node.id = ?
         AND node.deleted_at_ms IS NULL AND node.is_hidden <> 1 LIMIT 1`
    ).bind(agentId).first());
  }

  async manifest(agentId: number): Promise<PublicMetricManifest | null> {
    if (!await this.isPublic(agentId)) return null;
    const to = Math.floor(Date.now() / 1000);
    const from = to - 86_400;
    // Only 25 small metadata rows cross the Worker boundary, regardless of samples.
    const { results } = await this.env.DB.prepare(
      `SELECT bucket_start, point_count, byte_size,
              COALESCE(json_extract(public_json, '$.r'), 0) AS revision
       FROM agent_metric_blocks
       WHERE agent_id = ? AND resolution = 60 AND bucket_start >= ? AND bucket_start <= ?
         AND (public_json IS NOT NULL OR point_count > 0)
       ORDER BY bucket_start ASC LIMIT 25`
    ).bind(agentId, Math.floor(from / 3600) * 3600, to)
      .all<Pick<HourRow, "bucket_start" | "revision" | "point_count" | "byte_size">>();
    return { success: true, agent_id: agentId, from, to,
      hours: results.map((row) => ({ bucket_start: row.bucket_start, revision: revision(row) })) };
  }

  async hour(agentId: number, bucketStart: number) {
    if (!await this.isPublic(agentId)) return null;
    const to = Math.floor(Date.now() / 1000);
    if (bucketStart < Math.floor((to - 86_400) / 3600) * 3600 || bucketStart > to) return null;
    const row = await this.env.DB.prepare(
      `SELECT id, bucket_start, point_count, byte_size, data, public_json,
              COALESCE(json_extract(public_json, '$.r'), 0) AS revision,
              json_extract(public_json, '$.a') AS legacy_points,
              json_extract(summary_json, '$.sealed') AS sealed
       FROM agent_metric_blocks
       WHERE agent_id = ? AND resolution = 60 AND bucket_start = ? LIMIT 1`
    ).bind(agentId, bucketStart).first<HourRow>();
    if (!row || (!row.public_json && row.point_count === 0)) return null;

    let payload = row.public_json;
    // Old data is migrated only when a visitor asks for that hour. A new Go report
    // already supplies the public points, so its normal read path never decodes.
    if (row.point_count > 0 && !row.sealed && row.legacy_points !== row.point_count) {
      const points: PublicMetricHour["p"] = {};
      try {
        const decoded = await decodeBlock(
          row.data instanceof Uint8Array ? row.data : new Uint8Array(row.data)
        );
        for (const sample of blockSamples(decoded)) {
          const slot = (sample.timestampMs / 1000 - bucketStart) / 60;
          if (Number.isInteger(slot) && slot >= 0 && slot < 60) {
            points[String(slot)] = publicMinutePointFromBlock(sample);
          }
        }
      } catch (error) {
        writeStructuredLog(this.env, { service: "status", operation: "legacy_public_hour",
          result: "failure", entityType: "agent", entityId: agentId,
          errorCode: "METRIC_BLOCK_DECODE_FAILED", error,
          fields: { bucket_start: bucketStart } });
        if (!payload) throw error;
      }
      if (Object.keys(points).length > 0) {
        const legacy = JSON.stringify({ v: 1, p: points });
        // Existing public minutes win, including minutes inserted concurrently.
        // The metadata guard keeps a concurrent sealed-block replacement visible.
        const migrated = await this.env.DB.prepare(
          `UPDATE agent_metric_blocks SET public_json = json_set(
             COALESCE(public_json, '{"v":1,"r":0,"p":{}}'),
             '$.p', json((
               SELECT json_group_object(key, json(value)) FROM (
                 SELECT legacy.key, legacy.value FROM json_each(?, '$.p') legacy
                 WHERE NOT EXISTS (
                   SELECT 1 FROM json_each(agent_metric_blocks.public_json, '$.p') current
                   WHERE current.key = legacy.key
                 )
                 UNION ALL
                 SELECT key, value FROM json_each(agent_metric_blocks.public_json, '$.p')
               )
             )),
             '$.r', COALESCE(json_extract(public_json, '$.r'), 0) + 1,
             '$.a', ?
           ) WHERE id = ? AND point_count = ? AND byte_size = ?
           RETURNING public_json`
        ).bind(legacy, row.point_count, row.id, row.point_count, row.byte_size)
          .first<{ public_json: string }>();
        if (migrated) {
          payload = migrated.public_json;
        } else {
          const existing = row.public_json ? JSON.parse(row.public_json) as PublicMetricHour : null;
          payload = JSON.stringify({ v: 1, r: row.revision, p: { ...points, ...existing?.p } });
        }
      }
    }
    if (!payload) return null;
    return {
      payloadJson: `{"success":true,"agent_id":${agentId},"bucket_start":${bucketStart},"chunk":${payload}}`,
      etag: `"public-hour-${agentId}-${bucketStart}-${revision(row)}"`,
    };
  }

  async hours(agentId: number, buckets: number[]) {
    if (!await this.isPublic(agentId)) return null;
    const to = Math.floor(Date.now() / 1000);
    const earliest = Math.floor((to - 86_400) / 3600) * 3600;
    const selected = [...new Set(buckets)].slice(0, 25);
    const { results } = await this.env.DB.prepare(
      `SELECT bucket_start, public_json
       FROM agent_metric_blocks
       WHERE agent_id = ? AND resolution = 60
         AND bucket_start IN (SELECT CAST(value AS INTEGER) FROM json_each(?))
         AND bucket_start >= ? AND bucket_start <= ?
         AND public_json IS NOT NULL
         AND (point_count = 0 OR json_extract(summary_json, '$.sealed') = 1
              OR json_extract(public_json, '$.a') = point_count)
       ORDER BY bucket_start ASC LIMIT 25`
    ).bind(agentId, JSON.stringify(selected), earliest, to)
      .all<{ bucket_start: number; public_json: string }>();
    const ready = new Set(results.map((row) => row.bucket_start));
    const missing = selected.filter((bucket) => !ready.has(bucket));
    // The public projection was validated during ingestion. Return stored bytes;
    // a bulk read never inflates archives or expands any historical samples.
    const hours = results.map((row) =>
      `{"bucket_start":${row.bucket_start},"chunk":${row.public_json}}`
    ).join(",");
    return {
      payloadJson: `{"success":true,"agent_id":${agentId},"hours":[${hours}],"missing":${JSON.stringify(missing)}}`,
    };
  }
}
