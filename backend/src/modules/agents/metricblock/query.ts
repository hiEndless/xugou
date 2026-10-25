/**
 * 按时间窗口读取指标块并还原成样本序列。
 *
 * 状态页与仪表盘共用本层，两处都不再直接碰 agent_metric_blocks。
 */

import type { Bindings } from "../../../models/db";
import { writeStructuredLog } from "../../../platform/observability/StructuredLogger";
import { decodeBlock } from "./decode";
import { blockSamples, type BlockSample } from "./materialize";
import { minuteSummarySample } from "./minute-summary";
import type { AgentReportSample } from "../domain/models";
import { readLegacyArchiveSamples } from "./archive-compat";

/**
 * 窗口超过这个跨度就改查 1 分钟层。
 *
 * 依据：图表宽度约 1500 px，1 秒精度超过约 25 分钟就必然要降采样，
 * 与其读 60 倍的块再丢掉，不如直接读聚合层。
 */
export const HOT_WINDOW_SECONDS = 25 * 60;

/** 单次查询最多读取的块数，防止超大窗口把 Worker 拖垮。 */
const MAX_BLOCKS_PER_QUERY = 400;

export type Resolution = 1 | 60;

/** 一个块覆盖的秒数：1 秒块跨 1 分钟，1 分钟块跨 1 小时。 */
function bucketSpan(resolution: Resolution): number {
  return resolution * 60;
}

export function pickResolution(fromSec: number, toSec: number): Resolution {
  return toSec - fromSec <= HOT_WINDOW_SECONDS ? 1 : 60;
}

export interface AgentSampleWindow {
  resolution: Resolution;
  samples: BlockSample[];
  /** 因解码失败被跳过的块数，非 0 说明存储里有损坏数据。 */
  corruptBlocks: number;
}

export interface MetricArchiveRow {
  agent_id: number;
  bucket_start: number;
  data: ArrayBuffer | Uint8Array;
  summary_json: string | null;
}

function toBytes(data: ArrayBuffer | Uint8Array): Uint8Array {
  return data instanceof Uint8Array ? data : new Uint8Array(data);
}

/** 同一个有界索引窗口同时供旧 JSON 查询与二进制归档透传使用。 */
export async function readAgentMetricArchives(
  env: Bindings,
  agentIds: number[],
  fromSec: number,
  toSec: number,
  resolution: Resolution
): Promise<MetricArchiveRow[]> {
  if (agentIds.length === 0) return [];
  const rows = await env.DB.prepare(
    `SELECT agent_id, bucket_start, data, summary_json
     FROM agent_metric_blocks
     WHERE agent_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))
       AND resolution = ? AND bucket_start >= ? AND bucket_start <= ? AND point_count > 0
     ORDER BY agent_id ASC, bucket_start ASC
     LIMIT ?`
  ).bind(JSON.stringify(agentIds), resolution, fromSec - bucketSpan(resolution),
    toSec, MAX_BLOCKS_PER_QUERY * agentIds.length).all<MetricArchiveRow>();
  return rows.results;
}

/**
 * 批量读取多个 agent 的样本，一次 D1 往返。
 *
 * 状态页要同时渲染多台机器，逐个查会把往返次数放大到 agent 数量。
 */
export async function queryAgentSamplesBatch(
  env: Bindings,
  agentIds: number[],
  fromSec: number,
  toSec: number,
  options: { resolution?: Resolution; aggregate?: number } = {}
): Promise<Map<number, BlockSample[]>> {
  const result = new Map<number, BlockSample[]>();
  if (agentIds.length === 0) return result;

  const resolution = options.resolution ?? pickResolution(fromSec, toSec);
  const rows = await readAgentMetricArchives(env, agentIds, fromSec, toSec, resolution);

  for (const row of rows) {
    let decoded;
    try {
      decoded = await decodeBlock(toBytes(row.data));
    } catch (error) {
      // 单个坏块不该让整张图空掉
      writeStructuredLog(env, {
        service: "http",
        operation: "decode_metric_block",
        result: "failure",
        entityType: "agent",
        entityId: row.agent_id,
        errorCode: "METRIC_BLOCK_DECODE_FAILED",
        fields: { bucket_start: row.bucket_start, resolution },
        error,
      });
      continue;
    }
    const bucket = result.get(row.agent_id) ?? [];
    const preserved = resolution === 60 ? readLegacyArchiveSamples(row.summary_json) : [];
    for (const sample of mergeSamples(preserved, blockSamples(decoded, options.aggregate ?? 0))) {
      const sec = sample.timestampMs / 1000;
      if (sec < fromSec || sec > toSec) continue;
      bucket.push(sample);
    }
    result.set(row.agent_id, bucket);
  }

  for (const [id, samples] of result) {
    result.set(id, mergeSamples([], samples));
  }
  if (resolution === 60) {
    const pending = await queryUnsealedMinuteSamples(env, agentIds, fromSec, toSec);
    for (const [id, samples] of pending) {
      result.set(id, mergeSamples(result.get(id) ?? [], samples));
    }
  }
  return result;
}

/**
 * 读取 [fromSec, toSec] 内的样本。
 *
 * `aggregate` 只对 1 分钟层有意义：0=avg（默认）、1=min、2=max。
 */
export async function queryAgentSamples(
  env: Bindings,
  agentId: number,
  fromSec: number,
  toSec: number,
  options: { resolution?: Resolution; aggregate?: number } = {}
): Promise<AgentSampleWindow> {
  const resolution = options.resolution ?? pickResolution(fromSec, toSec);
  const rows = await readAgentMetricArchives(env, [agentId], fromSec, toSec, resolution);

  const samples: BlockSample[] = [];
  let corruptBlocks = 0;

  for (const row of rows) {
    try {
      const decoded = await decodeBlock(toBytes(row.data));
      const preserved = resolution === 60 ? readLegacyArchiveSamples(row.summary_json) : [];
      for (const sample of mergeSamples(preserved, blockSamples(decoded, options.aggregate ?? 0))) {
        const sec = sample.timestampMs / 1000;
        if (sec < fromSec || sec > toSec) continue;
        samples.push(sample);
      }
    } catch (error) {
      // 单个坏块不该让整张图空掉：跳过它，记日志，其余照常返回
      corruptBlocks++;
      writeStructuredLog(env, {
        // 读路径都由 HTTP 请求驱动
        service: "http",
        operation: "decode_metric_block",
        result: "failure",
        entityType: "agent",
        entityId: agentId,
        errorCode: "METRIC_BLOCK_DECODE_FAILED",
        fields: { bucket_start: row.bucket_start, resolution },
        error,
      });
    }
  }

  if (resolution === 60) {
    samples.push(...(await queryUnsealedMinuteSamples(env, [agentId], fromSec, toSec)).get(agentId) ?? []);
  }
  return { resolution, samples: mergeSamples([], samples), corruptBlocks };
}

function mergeSamples(archived: BlockSample[], pending: BlockSample[]) {
  const byTime = new Map([...archived, ...pending].map(sample => [sample.timestampMs, sample]));
  return [...byTime.values()].sort((left, right) => left.timestampMs - right.timestampMs);
}

/** 仅补未封存小时；已有小时块时不再扫描其 60 份分钟摘要。 */
async function queryUnsealedMinuteSamples(
  env: Bindings,
  agentIds: number[],
  fromSec: number,
  toSec: number
): Promise<Map<number, BlockSample[]>> {
  const rows = await readUnsealedMinuteSummaries(env, agentIds, fromSec, toSec);
  return new Map([...rows].map(([id, samples]) => [id, samples.map(minuteSummarySample)]));
}

/** 未封存分钟保持小摘要形状，二进制响应无需先物化成管理端样本。 */
export async function readUnsealedMinuteSummaries(
  env: Bindings,
  agentIds: number[],
  fromSec: number,
  toSec: number
): Promise<Map<number, AgentReportSample[]>> {
  if (agentIds.length === 0) return new Map();
  const rows = await env.DB.prepare(
    `SELECT hot.agent_id, hot.summary_json
     FROM agent_metric_blocks hour
     JOIN agent_metric_blocks hot
       ON hot.agent_id = hour.agent_id AND hot.resolution = 1
       AND hot.bucket_start >= hour.bucket_start AND hot.bucket_start < hour.bucket_start + 3600
     WHERE hour.agent_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))
       AND hour.resolution = 60 AND hour.bucket_start >= ? AND hour.bucket_start <= ?
       AND hour.public_json IS NOT NULL
       AND COALESCE(json_extract(hour.summary_json, '$.sealed'), 0) <> 1
       AND hot.bucket_start >= ? AND hot.bucket_start <= ?
       AND hot.summary_json IS NOT NULL
     ORDER BY hot.agent_id, hot.bucket_start LIMIT ?`
  ).bind(JSON.stringify(agentIds), fromSec - 3600, toSec,
    fromSec - 60, toSec, MAX_BLOCKS_PER_QUERY * 60 * agentIds.length)
    .all<{ agent_id: number; summary_json: string }>();
  const result = new Map<number, AgentReportSample[]>();
  for (const row of rows.results) {
    try {
      const sample = JSON.parse(row.summary_json) as AgentReportSample;
      const timestamp = Date.parse(sample.collected_at);
      if (!Number.isFinite(timestamp) || timestamp < fromSec * 1000 || timestamp > toSec * 1000) continue;
      const list = result.get(row.agent_id) ?? [];
      list.push(sample);
      result.set(row.agent_id, list);
    } catch (error) {
      writeStructuredLog(env, {
        service: "http", operation: "read_minute_summary", result: "failure",
        entityId: row.agent_id, errorCode: "MINUTE_SUMMARY_INVALID", error,
      });
    }
  }
  return result;
}

export { downsample } from "./metrics-view";
