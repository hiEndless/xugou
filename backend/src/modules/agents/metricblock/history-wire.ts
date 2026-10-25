import type { AgentReportSample } from "../domain/models";
import type { BlockSample } from "./materialize";

export const METRIC_HISTORY_CONTENT_TYPE = "application/vnd.xugou.metrics+binary";

/** XHR1 | u32le metadata byte length | UTF-8 JSON | unchanged archive bytes. */
export interface AgentMetricHistoryMetadata {
  agent_id: number;
  from: number;
  to: number;
  resolution: 1 | 60;
  max_points: number;
  blocks: Array<{ byte_length: number; legacy_samples: BlockSample[] }>;
  pending: AgentReportSample[];
}
