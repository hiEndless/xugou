import type { Bindings } from "../../../models/db";
import { readLegacyArchiveSamples } from "../metricblock/archive-compat";
import { METRIC_HISTORY_CONTENT_TYPE, type AgentMetricHistoryMetadata } from "../metricblock/history-wire";
import { pickResolution, readAgentMetricArchives, readUnsealedMinuteSummaries } from "../metricblock/query";
import { DASHBOARD_CHART_POINTS } from "../metricblock/metrics-view";

/** 管理指标仅透传有界的归档窗口；解压、还原与降采样由浏览器 Worker 完成。 */
export async function createMetricHistoryResponse(
  env: Bindings,
  agentId: number,
  hours: number
): Promise<Response | null> {
  const exists = await env.DB.prepare(
    `SELECT 1 AS ok FROM agent_nodes WHERE id = ? AND deleted_at_ms IS NULL LIMIT 1`
  ).bind(agentId).first();
  if (!exists) return null;
  const to = Math.floor(Date.now() / 1000);
  const from = to - hours * 3600;
  const resolution = pickResolution(from, to);
  const [rows, pending] = await Promise.all([
    readAgentMetricArchives(env, [agentId], from, to, resolution),
    resolution === 60 ? readUnsealedMinuteSummaries(env, [agentId], from, to)
      : Promise.resolve(new Map<number, AgentMetricHistoryMetadata["pending"]>()),
  ]);
  const raw = rows.map((row) => row.data instanceof Uint8Array ? row.data : new Uint8Array(row.data));
  const metadata: AgentMetricHistoryMetadata = {
    agent_id: agentId, from, to, resolution, max_points: DASHBOARD_CHART_POINTS,
    blocks: rows.map((row, index) => ({
      byte_length: raw[index].byteLength,
      legacy_samples: resolution === 60 ? readLegacyArchiveSamples(row.summary_json) : [],
    })),
    pending: pending.get(agentId) ?? [],
  };
  const encoded = new TextEncoder().encode(JSON.stringify(metadata));
  const prefix = new Uint8Array(8);
  prefix.set([88, 72, 82, 49]);
  new DataView(prefix.buffer).setUint32(4, encoded.byteLength, true);
  const chunks = [prefix, encoded, ...raw];
  let index = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) controller.enqueue(chunks[index++]);
      else controller.close();
    },
  });
  return new Response(stream, {
    headers: {
      "Content-Type": METRIC_HISTORY_CONTENT_TYPE,
      "Cache-Control": "private, no-store",
      "Content-Length": String(chunks.reduce((size, chunk) => size + chunk.byteLength, 0)),
    },
  });
}
