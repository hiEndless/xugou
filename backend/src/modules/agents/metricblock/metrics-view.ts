import type { Metrics } from "../../../models/agent";
import type { BlockSample } from "./materialize";

/** 仪表盘历史曲线在旧 JSON 与浏览器解码中使用同一点数上限。 */
export const DASHBOARD_CHART_POINTS = 160;

/** 把还原出的块样本映射成仪表盘沿用的 Metrics 形状。 */
export function blockSampleToMetrics(agentId: number, sample: BlockSample): Metrics {
  return {
    agent_id: agentId,
    timestamp: new Date(sample.timestampMs).toISOString(),
    cpu_usage: sample.cpuUsage ?? undefined,
    // CPU 型号与核数属于静态元数据，不入块；历史图不需要，留空
    memory_total: sample.memoryTotal,
    memory_used: sample.memoryUsed ?? undefined,
    memory_free: sample.memoryFree ?? undefined,
    memory_usage_rate: sample.memoryUsageRate ?? undefined,
    load_1: sample.load1 ?? undefined,
    load_5: sample.load5 ?? undefined,
    load_15: sample.load15 ?? undefined,
    disk_metrics: JSON.stringify(
      sample.disks.map((disk) => ({
        mount_point: disk.mountPoint,
        total: disk.total,
        used: disk.used,
        free: disk.free,
        usage_rate: disk.usageRate,
      }))
    ),
    network_metrics: JSON.stringify(
      sample.nets.map((net) => ({
        interface: net.iface,
        bytes_sent: net.bytesSent,
        bytes_recv: net.bytesRecv,
        packets_sent: net.packetsSent,
        packets_recv: net.packetsRecv,
      }))
    ),
    swap_total: sample.swapTotal,
    swap_used: sample.swapUsed ?? undefined,
    process_count: sample.processCount ?? undefined,
    tcp_connections: sample.tcpConnections ?? undefined,
    udp_connections: sample.udpConnections ?? undefined,
    ping_json: JSON.stringify(
      Object.fromEntries(
        sample.pings.map((ping) => [
          ping.key,
          { latency_ms: ping.latencyMs, loss: ping.loss },
        ])
      )
    ),
    ipv4_reachable:
      sample.ipv4Reachable === null ? undefined : sample.ipv4Reachable ? 1 : 0,
    ipv6_reachable:
      sample.ipv6Reachable === null ? undefined : sample.ipv6Reachable ? 1 : 0,
  } as Metrics;
}

/**
 * 等距抽样到不超过 maxPoints 个点，并始终保留最后一个点
 * （最新数据点是用户最关心的，不能因为步长对不齐被丢掉）。
 */
export function downsample<T>(items: T[], maxPoints: number): T[] {
  if (maxPoints <= 0 || items.length <= maxPoints) return items;
  const step = Math.ceil(items.length / maxPoints);
  const out: T[] = [];
  for (let i = 0; i < items.length; i += step) out.push(items[i]);
  const last = items[items.length - 1];
  if (out[out.length - 1] !== last) out.push(last);
  return out;
}
