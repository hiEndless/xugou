import type { AgentTrafficSegment } from "../modules/agents/domain/models";
import {
  getTrafficPeriodStart,
  isLoopbackInterface,
  MAX_TRACKED_INTERFACES,
  type TrafficComputation,
  type TrafficBaselines,
  type TrafficState,
} from "./traffic";

/**
 * v6 在 Go 侧扫描每秒计数器，Worker 只合并逐接口首末值与内部增量。
 * 跨批次的第一笔差分仍由服务端持久化基准决定，不能直接对全机总量做差。
 */
export function computeSummarizedTraffic(
  prev: TrafficState | null,
  segments: AgentTrafficSegment[],
  resetDay: number
): TrafficComputation {
  const state: TrafficState = {
    month_rx: prev?.month_rx ?? 0,
    month_tx: prev?.month_tx ?? 0,
    baselines: Object.assign(Object.create(null) as TrafficBaselines, prev?.baselines),
    last_total_rx: prev?.last_total_rx ?? null,
    last_total_tx: prev?.last_total_tx ?? null,
    month_reset_at: prev?.month_reset_at ?? null,
    last_ts: prev?.last_ts ?? null,
  };
  const speeds: TrafficComputation["speeds"] = [];
  for (const segment of segments) {
    if (state.last_ts !== null && segment.last_ts <= state.last_ts) continue;
    const period = getTrafficPeriodStart(new Date(segment.last_ts), resetDay);
    if (state.month_reset_at !== period) {
      state.month_rx = 0;
      state.month_tx = 0;
      state.month_reset_at = period;
    }
    const baselines = state.baselines ?? {};
    let boundaryRx = 0;
    let boundaryTx = 0;
    let matched = false;
    let reset = false;
    const seen = new Set<string>();
    let lastNetworkTs = -1;
    for (const item of segment.interfaces) {
      if (isLoopbackInterface(item.name)) continue;
      seen.add(item.name);
      lastNetworkTs = Math.max(lastNetworkTs, item.last_ts);
      const baseline = baselines[item.name];
      if (baseline) {
        const rx = item.first_rx - baseline.rx;
        const tx = item.first_tx - baseline.tx;
        matched = true;
        reset ||= rx < 0 || tx < 0;
        boundaryRx += rx < 0 ? item.first_rx : rx;
        boundaryTx += tx < 0 ? item.first_tx : tx;
      }
      state.month_rx += item.delta_rx;
      state.month_tx += item.delta_tx;
      baselines[item.name] = { rx: item.last_rx, tx: item.last_tx };
    }
    state.month_rx += boundaryRx;
    state.month_tx += boundaryTx;
    // 单点没有内部速率时，用上次报告末尾的基准补齐；明确缺值不可补零。
    const elapsed = state.last_ts === null ? 0 : segment.last_ts - state.last_ts;
    speeds.push(segment.last_speed ?? (
      segment.first_ts === segment.last_ts && matched && !reset && elapsed > 0
        ? { rx: boundaryRx * 1000 / elapsed, tx: boundaryTx * 1000 / elapsed }
        : { rx: null, tx: null }
    ));
    if (Object.keys(baselines).length > MAX_TRACKED_INTERFACES) {
      for (const name of Object.keys(baselines)) {
        if (!seen.has(name)) delete baselines[name];
      }
    }
    if (seen.size > 0) {
      state.baselines = baselines;
      state.last_total_rx = Object.values(baselines).reduce((sum, item) => sum + item.rx, 0);
      state.last_total_tx = Object.values(baselines).reduce((sum, item) => sum + item.tx, 0);
      state.last_ts = lastNetworkTs;
    }
  }
  if (!state.baselines || Object.keys(state.baselines).length === 0) state.baselines = null;
  return { state, speeds };
}
