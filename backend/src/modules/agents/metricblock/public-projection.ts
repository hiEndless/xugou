import type { AgentReportSample } from "../domain/models";
import type { BlockSample } from "./materialize";

/** Only these numeric fields and bounded device names may cross the public boundary. */
export interface PublicMinutePoint {
  c: number | null;
  m: (number | null)[];
  l: (number | null)[];
  s: (number | null)[];
  p: number | null;
  t: number | null;
  u: number | null;
  v4: number | null;
  v6: number | null;
  d: (string | number | null)[][];
  n: (string | number | null)[][];
}

export interface PublicMetricHour {
  v: 1;
  r: number;
  /** A missing minute is absent, never represented as a measured zero. */
  p: Record<string, PublicMinutePoint>;
}

function numeric(value: unknown, maximum = Number.MAX_SAFE_INTEGER): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.min(value, maximum)
    : null;
}

function name(value: unknown): string {
  return typeof value === "string" ? value.slice(0, 512) : "";
}

export function publicMinutePoint(sample: AgentReportSample): PublicMinutePoint {
  return {
    c: numeric(sample.cpu?.usage, 100),
    m: [numeric(sample.memory?.total), numeric(sample.memory?.used),
      numeric(sample.memory?.free), numeric(sample.memory?.usage_rate, 100)],
    l: [numeric(sample.load?.load1), numeric(sample.load?.load5), numeric(sample.load?.load15)],
    s: [numeric(sample.swap?.total), numeric(sample.swap?.used)],
    p: numeric(sample.process_count),
    t: numeric(sample.tcp_connections),
    u: numeric(sample.udp_connections),
    v4: sample.ipv4_reachable == null ? null : Number(sample.ipv4_reachable),
    v6: sample.ipv6_reachable == null ? null : Number(sample.ipv6_reachable),
    d: (sample.disks ?? []).slice(0, 64).map((disk) => [
      name(disk.mount_point), numeric(disk.total), numeric(disk.used),
      numeric(disk.free), numeric(disk.usage_rate, 100),
    ]),
    n: (sample.network ?? []).slice(0, 64).map((network) => [
      name(network.interface), numeric(network.bytes_sent), numeric(network.bytes_recv),
      numeric(network.packets_sent), numeric(network.packets_recv),
    ]),
  };
}

/** Legacy blocks are projected once per requested hour, not during status publication. */
export function publicMinutePointFromBlock(sample: BlockSample): PublicMinutePoint {
  return publicMinutePoint({
    collected_at: new Date(sample.timestampMs).toISOString(),
    cpu: { usage: sample.cpuUsage ?? undefined },
    memory: {
      total: sample.memoryTotal,
      used: sample.memoryUsed ?? undefined,
      free: sample.memoryFree ?? undefined,
      usage_rate: sample.memoryUsageRate ?? undefined,
    },
    load: { load1: sample.load1 ?? undefined, load5: sample.load5 ?? undefined,
      load15: sample.load15 ?? undefined },
    swap: { total: sample.swapTotal, used: sample.swapUsed ?? undefined },
    process_count: sample.processCount ?? undefined,
    tcp_connections: sample.tcpConnections ?? undefined,
    udp_connections: sample.udpConnections ?? undefined,
    ipv4_reachable: sample.ipv4Reachable,
    ipv6_reachable: sample.ipv6Reachable,
    disks: sample.disks.map((disk) => ({ mount_point: disk.mountPoint,
      total: disk.total, used: disk.used, free: disk.free, usage_rate: disk.usageRate })),
    network: sample.nets.map((network) => ({ interface: network.iface,
      bytes_sent: network.bytesSent, bytes_recv: network.bytesRecv,
      packets_sent: network.packetsSent, packets_recv: network.packetsRecv })),
  });
}

/**
 * Run after the matching hot-block upsert in the same D1 batch. The hot row's
 * point_count guards the public minute against a less complete input. SQL updates
 * one slot without reading, parsing or serializing an existing hour in JavaScript.
 */
export function preparePublicMinuteProjection(
  db: D1Database,
  agentId: number,
  sample: AgentReportSample,
  pointCount: number
): D1PreparedStatement {
  const timestamp = Date.parse(sample.collected_at);
  if (!Number.isFinite(timestamp)) throw new Error("Invalid public minute timestamp");
  const minuteStart = Math.floor(timestamp / 60_000) * 60;
  const hourStart = Math.floor(minuteStart / 3600) * 3600;
  const slot = String((minuteStart - hourStart) / 60);
  const projected = publicMinutePoint(sample);
  const point = JSON.stringify(projected);
  const initial: PublicMetricHour = { v: 1, r: 1, p: { [slot]: projected } };
  return db.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id, resolution, bucket_start, point_count, codec, byte_size, data, public_json)
     SELECT ?, 60, ?, 0, 1, 0, X'', ?
     WHERE EXISTS (
       SELECT 1 FROM agent_metric_blocks
       WHERE agent_id = ? AND resolution = 1 AND bucket_start = ? AND point_count = ?
     )
     ON CONFLICT(agent_id, resolution, bucket_start) DO UPDATE SET
       public_json = json_set(
         COALESCE(agent_metric_blocks.public_json, '{"v":1,"r":0,"p":{}}'),
         '$.r', COALESCE(json_extract(agent_metric_blocks.public_json, '$.r'), 0) + 1,
         ?, json(?)
       )`
  ).bind(agentId, hourStart, JSON.stringify(initial), agentId, minuteStart, pointCount,
    `$.p."${slot}"`, point);
}
