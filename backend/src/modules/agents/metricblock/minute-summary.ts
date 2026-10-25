import type { AgentReportSample } from "../domain/models";
import type { BlockSample } from "./materialize";

/** Go 预聚合的分钟摘要恢复为查询层已有的样本形状，无归档解压。 */
export function minuteSummarySample(sample: AgentReportSample): BlockSample {
  return {
    timestampMs: Date.parse(sample.collected_at),
    cpuUsage: sample.cpu?.usage ?? null,
    memoryTotal: sample.memory?.total ?? 0,
    memoryUsed: sample.memory?.used ?? null,
    memoryFree: sample.memory?.free ?? null,
    memoryUsageRate: sample.memory?.usage_rate ?? null,
    load1: sample.load?.load1 ?? null,
    load5: sample.load?.load5 ?? null,
    load15: sample.load?.load15 ?? null,
    swapTotal: sample.swap?.total ?? 0,
    swapUsed: sample.swap?.used ?? null,
    swapUsageRate: sample.swap?.usage_rate ?? null,
    processCount: sample.process_count ?? null,
    tcpConnections: sample.tcp_connections ?? null,
    udpConnections: sample.udp_connections ?? null,
    ipv4Reachable: sample.ipv4_reachable ?? null,
    ipv6Reachable: sample.ipv6_reachable ?? null,
    disks: (sample.disks ?? []).map(disk => ({
      mountPoint: String(disk.mount_point ?? ""),
      total: typeof disk.total === "number" ? disk.total : 0,
      used: typeof disk.used === "number" ? disk.used : null,
      free: typeof disk.free === "number" ? disk.free : null,
      usageRate: typeof disk.usage_rate === "number" ? disk.usage_rate : null,
    })),
    nets: (sample.network ?? []).map(net => ({
      iface: String(net.interface ?? ""),
      bytesSent: typeof net.bytes_sent === "number" ? net.bytes_sent : null,
      bytesRecv: typeof net.bytes_recv === "number" ? net.bytes_recv : null,
      packetsSent: typeof net.packets_sent === "number" ? net.packets_sent : null,
      packetsRecv: typeof net.packets_recv === "number" ? net.packets_recv : null,
    })),
    pings: Object.entries(sample.ping ?? {}).map(([key, ping]) => ({
      key,
      latencyMs: typeof ping.latency_ms === "number" ? ping.latency_ms : null,
      loss: typeof ping.loss === "boolean" ? ping.loss : null,
    })),
  };
}
