import type { Bindings } from "../../../models/db";
import type { AgentReportBlock } from "../domain/models";
import { decodeBlock } from "./decode";
import { blockSamples, type BlockSample } from "./materialize";
import { publicMinutePointFromBlock, type PublicMetricHour } from "./public-projection";

interface ExistingHour {
  bucket_start: number;
  point_count: number;
  data: ArrayBuffer | Uint8Array;
  summary_json: string | null;
}

export interface PreservedArchiveHour {
  summaryJson: string;
  publicJson: string | null;
}

/** 兼容元数据只保留完整管理端样本；公开数据另走数值白名单投影。 */
export function readLegacyArchiveSamples(summaryJson: string | null): BlockSample[] {
  if (!summaryJson) return [];
  try {
    const metadata = JSON.parse(summaryJson) as { legacy_samples?: unknown };
    if (!Array.isArray(metadata.legacy_samples)) return [];
    return metadata.legacy_samples.filter((sample): sample is BlockSample =>
      sample !== null && typeof sample === "object" &&
      Number.isFinite(sample.timestampMs) && Array.isArray(sample.disks) &&
      Array.isArray(sample.nets) && Array.isArray(sample.pings)
    ).slice(0, 60);
  } catch {
    // 坏元数据不能连带丢弃仍可解码的归档块。
    return [];
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array) {
  return left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);
}

function isSealed(summaryJson: string | null) {
  try {
    return summaryJson !== null && JSON.parse(summaryJson).sealed === true;
  } catch {
    return false;
  }
}

/**
 * v5 半小时块与 v6 封存块可能覆盖不同分钟。只在已有不同归档时解码两侧，
 * 留下新块未覆盖的旧分钟，再由调用方与替换操作一并提交。常规 placeholder 不解压。
 */
export async function preserveReplacedHours(
  env: Pick<Bindings, "DB">,
  agentId: number,
  blocks: Array<{ block: AgentReportBlock; raw: Uint8Array }>
): Promise<Map<number, PreservedArchiveHour>> {
  const sealed = blocks.filter(({ block }) => block.resolution === 60);
  const result = new Map<number, PreservedArchiveHour>();
  if (sealed.length === 0) return result;
  const { results } = await env.DB.prepare(
    `SELECT bucket_start, point_count, data, summary_json
     FROM agent_metric_blocks
     WHERE agent_id = ? AND resolution = 60
       AND bucket_start IN (SELECT CAST(value AS INTEGER) FROM json_each(?))`
  ).bind(agentId, JSON.stringify(sealed.map(({ block }) => block.bucket_start)))
    .all<ExistingHour>();
  const existingByHour = new Map(results.map((row) => [row.bucket_start, row]));
  for (const { block, raw } of sealed) {
    const previous = existingByHour.get(block.bucket_start);
    if (!previous || previous.point_count === 0) continue;
    const oldRaw = previous.data instanceof Uint8Array ? previous.data : new Uint8Array(previous.data);
    const identical = sameBytes(oldRaw, raw);
    if (identical && isSealed(previous.summary_json)) {
      result.set(block.bucket_start, {
        summaryJson: previous.summary_json ?? JSON.stringify({ sealed: true }),
        publicJson: null,
      });
      continue;
    }
    const oldSamples = blockSamples(await decodeBlock(oldRaw));
    const newSamples = identical ? oldSamples : blockSamples(await decodeBlock(raw));
    const covered = new Set(newSamples.map((sample) => sample.timestampMs));
    const merged = new Map([
      ...readLegacyArchiveSamples(previous.summary_json), ...oldSamples,
    ].map((sample) => [sample.timestampMs, sample]));
    const startMs = block.bucket_start * 1000;
    const legacy = [...merged.values()].filter((sample) =>
      sample.timestampMs >= startMs && sample.timestampMs < startMs + 3_600_000 &&
      (sample.timestampMs - startMs) % 60_000 === 0 && !covered.has(sample.timestampMs)
    ).sort((left, right) => left.timestampMs - right.timestampMs).slice(0, 60);
    const points: PublicMetricHour["p"] = {};
    for (const sample of [...legacy, ...newSamples]) {
      points[String((sample.timestampMs - startMs) / 60_000)] = publicMinutePointFromBlock(sample);
    }
    result.set(block.bucket_start, {
      summaryJson: JSON.stringify({ sealed: true, legacy_samples: legacy }),
      publicJson: Object.keys(points).length ? JSON.stringify({ v: 1, r: 1, p: points }) : null,
    });
  }
  return result;
}
