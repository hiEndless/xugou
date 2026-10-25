import type { PublicMetricHistory } from "../../types/agents";
import type { components } from "../../api/generated/v2-schema";

/** The compact hour protocol contains only public numeric series and device names. */
export type PublicMinutePoint = components["schemas"]["PublicMinutePoint"];
export type PublicMetricHourResponse = components["schemas"]["PublicMetricHourResult"];
export type PublicMetricHour = PublicMetricHourResponse["chunk"];

export interface MetricHistoryWork {
  agentId: number;
  from: number;
  to: number;
  hours: PublicMetricHourResponse[];
}

/** Run off the main thread so opening a full day does not interrupt live cards. */
export function expandMetricHours(work: MetricHistoryWork): PublicMetricHistory[] {
  const metrics: PublicMetricHistory[] = [];
  for (const hour of work.hours) {
    if (hour.agent_id !== work.agentId || hour.chunk.v !== 1) continue;
    for (const [key, point] of Object.entries(hour.chunk.p)) {
      const slot = Number(key);
      if (!Number.isInteger(slot) || slot < 0 || slot >= 60) continue;
      const seconds = hour.bucket_start + slot * 60;
      if (seconds < work.from || seconds > work.to) continue;
      const timestamp = seconds * 1000;
      metrics.push({
        id: timestamp,
        agent_id: work.agentId,
        timestamp: new Date(timestamp).toISOString(),
        cpu_usage: point.c ?? undefined,
        memory_total: point.m[0] ?? undefined, memory_used: point.m[1] ?? undefined,
        memory_free: point.m[2] ?? undefined, memory_usage_rate: point.m[3] ?? undefined,
        load_1: point.l[0] ?? undefined, load_5: point.l[1] ?? undefined,
        load_15: point.l[2] ?? undefined,
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
      });
    }
  }
  metrics.sort((left, right) => Date.parse(left.timestamp ?? "") - Date.parse(right.timestamp ?? ""));
  if (metrics.length <= 288) return metrics;
  const step = Math.ceil(metrics.length / 288);
  const sampled = metrics.filter((_, index) => index % step === 0);
  const last = metrics[metrics.length - 1];
  if (sampled[sampled.length - 1] !== last) sampled.push(last);
  return sampled;
}
