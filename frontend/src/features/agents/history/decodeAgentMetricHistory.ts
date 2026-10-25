import { decodeBlock } from "../../../../../backend/src/modules/agents/metricblock/decode";
import type { AgentMetricHistoryMetadata } from "../../../../../backend/src/modules/agents/metricblock/history-wire";
import { blockSamples, type BlockSample } from "../../../../../backend/src/modules/agents/metricblock/materialize";
import { blockSampleToMetrics, DASHBOARD_CHART_POINTS, downsample } from "../../../../../backend/src/modules/agents/metricblock/metrics-view";
import { minuteSummarySample } from "../../../../../backend/src/modules/agents/metricblock/minute-summary";
import type { AgentMetric } from "../../../api/agents";

function parseMetadata(buffer: ArrayBuffer, agentId: number) {
  const bytes = new Uint8Array(buffer);
  if (bytes.length < 8 || String.fromCharCode(...bytes.subarray(0, 4)) !== "XHR1") {
    throw new Error("Invalid metric history envelope");
  }
  const metadataLength = new DataView(buffer).getUint32(4, true);
  if (metadataLength === 0 || metadataLength > bytes.length - 8) {
    throw new Error("Truncated metric history metadata");
  }
  const metadata = JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(8, 8 + metadataLength))
  ) as AgentMetricHistoryMetadata;
  if (
    !metadata || typeof metadata !== "object" ||
    metadata.agent_id !== agentId ||
    !Number.isSafeInteger(metadata.from) || !Number.isSafeInteger(metadata.to) ||
    metadata.from > metadata.to ||
    (metadata.resolution !== 1 && metadata.resolution !== 60) ||
    metadata.max_points !== DASHBOARD_CHART_POINTS ||
    !Array.isArray(metadata.blocks) || !Array.isArray(metadata.pending)
  ) {
    throw new Error("Invalid metric history metadata");
  }
  return { bytes, metadata, offset: 8 + metadataLength };
}

/** 仅在 WebWorker 执行：复用 Go/Worker 共用的 codec 和旧指标映射。 */
export async function decodeAgentMetricHistory(
  buffer: ArrayBuffer,
  agentId: number
): Promise<AgentMetric[]> {
  const parsed = parseMetadata(buffer, agentId);
  const { bytes, metadata } = parsed;
  let offset = parsed.offset;
  const byTime = new Map<number, BlockSample>();
  const add = (sample: BlockSample) => {
    if (!Number.isFinite(sample.timestampMs)) {
      throw new Error("Invalid metric history sample timestamp");
    }
    if (sample.timestampMs < metadata.from * 1000 || sample.timestampMs > metadata.to * 1000) return;
    byTime.set(sample.timestampMs, sample);
  };

  for (const block of metadata.blocks) {
    if (
      !block || !Number.isSafeInteger(block.byte_length) || block.byte_length <= 0 ||
      block.byte_length > bytes.length - offset || !Array.isArray(block.legacy_samples)
    ) {
      throw new Error("Invalid metric history block length or legacy samples");
    }
    // bucketStart、维度与数值都由原 codec 自描述，不能从传输顺序猜时间。
    const decoded = await decodeBlock(bytes.subarray(offset, offset + block.byte_length));
    if (decoded.interval !== metadata.resolution) {
      throw new Error("Metric history block resolution mismatch");
    }
    offset += block.byte_length;
    for (const sample of block.legacy_samples) add(sample);
    for (const sample of blockSamples(decoded)) add(sample);
  }
  if (offset !== bytes.length) {
    throw new Error("Unexpected trailing metric history bytes");
  }
  for (const pending of metadata.pending) add(minuteSummarySample(pending));
  const samples = [...byTime.values()].sort((left, right) => left.timestampMs - right.timestampMs);
  const metrics = downsample(samples, metadata.max_points)
    .map((sample) => blockSampleToMetrics(agentId, sample));

  // JSON API 会省略 undefined；Worker structured clone 默认会保留这些键。
  return JSON.parse(JSON.stringify(metrics)) as AgentMetric[];
}
