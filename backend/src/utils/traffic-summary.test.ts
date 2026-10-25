import { describe, expect, it } from "vitest";
import type { AgentTrafficSegment } from "../modules/agents/domain/models";
import { computeTraffic, type TrafficState } from "./traffic";
import { computeSummarizedTraffic } from "./traffic-summary";

const T = Date.UTC(2026, 9, 25, 12);
function previous(): TrafficState {
  return { month_rx: 200, month_tx: 100, baselines: { eth0: { rx: 1000, tx: 500 }, tun0: { rx: 2000, tx: 1000 } },
    last_total_rx: 3000, last_total_tx: 1500, month_reset_at: "2026-10-01", last_ts: T };
}

describe("v6 流量摘要与逐样本记账一致", () => {
  it("新增/消失网卡和计数器归零保留逐接口语义", () => {
    const input = [
      { ts: T + 1000, interfaces: [{ name: "eth0", rx: 1100, tx: 550 }, { name: "docker0", rx: 9000, tx: 5000 }] },
      { ts: T + 2000, interfaces: [{ name: "eth0", rx: 30, tx: 20 }] },
      { ts: T + 3000, interfaces: [{ name: "eth0", rx: 80, tx: 40 }, { name: "docker0", rx: 9300, tx: 5100 }] },
    ];
    const segment: AgentTrafficSegment = {
      utc_day: "2026-10-25", first_ts: T + 1000, last_ts: T + 3000,
      interfaces: [
        { name: "eth0", first_ts: T + 1000, last_ts: T + 3000, first_rx: 1100, first_tx: 550, last_rx: 80, last_tx: 40, delta_rx: 80, delta_tx: 40 },
        { name: "docker0", first_ts: T + 1000, last_ts: T + 3000, first_rx: 9000, first_tx: 5000, last_rx: 9300, last_tx: 5100, delta_rx: 300, delta_tx: 100 },
      ], last_speed: { rx: 350, tx: 120 },
    };
    const expected = computeTraffic(previous(), input, "2026-10-01");
    const actual = computeSummarizedTraffic(previous(), [segment], 1);
    expect(actual.state).toEqual(expected.state);
    expect(actual.speeds.at(-1)).toEqual(expected.speeds.at(-1));
    expect(actual.state.baselines?.tun0).toEqual({ rx: 2000, tx: 1000 });
  });

  it("单点使用上批基准补网速，回环接口不记账", () => {
    const segment: AgentTrafficSegment = { utc_day: "2026-10-25", first_ts: T + 1000, last_ts: T + 1000,
      interfaces: [
        { name: "eth0", first_ts: T + 1000, last_ts: T + 1000, first_rx: 1100, first_tx: 550, last_rx: 1100, last_tx: 550, delta_rx: 0, delta_tx: 0 },
        { name: "lo", first_ts: T + 1000, last_ts: T + 1000, first_rx: 10000, first_tx: 10000, last_rx: 10000, last_tx: 10000, delta_rx: 9999, delta_tx: 9999 },
      ], last_speed: null };
    const actual = computeSummarizedTraffic(previous(), [segment], 1);
    expect(actual.state.month_rx).toBe(300);
    expect(actual.speeds).toEqual([{ rx: 100, tx: 50 }]);
    expect(actual.state.baselines).not.toHaveProperty("lo");
  });

  it("跨计费日分段清零，保留接口基准衔接", () => {
    const midnight = Date.UTC(2026, 10, 1);
    const segments: AgentTrafficSegment[] = [0, 1].map(index => ({
      utc_day: index === 0 ? "2026-10-31" : "2026-11-01",
      first_ts: midnight + (index - 1) * 1000, last_ts: midnight + (index - 1) * 1000,
      interfaces: [{ name: "eth0", first_ts: midnight + (index - 1) * 1000, last_ts: midnight + (index - 1) * 1000,
        first_rx: 1100 + index * 100, first_tx: 550 + index * 50, last_rx: 1100 + index * 100,
        last_tx: 550 + index * 50, delta_rx: 0, delta_tx: 0 }], last_speed: null,
    }));
    const actual = computeSummarizedTraffic({ ...previous(), last_ts: midnight - 2000 }, segments, 1);
    expect(actual.state.month_rx).toBe(100);
    expect(actual.state.month_tx).toBe(50);
    expect(actual.state.month_reset_at).toBe("2026-11-01");
  });

  it("无有效网卡时保留基准，最新速率保持缺值", () => {
    const before = previous();
    const result = computeSummarizedTraffic(before, [{ utc_day: "2026-10-25", first_ts: T + 1000,
      last_ts: T + 2000, interfaces: [], last_speed: { rx: null, tx: null } }], 1);
    expect(result.state).toEqual(before);
    expect(result.speeds).toEqual([{ rx: null, tx: null }]);
  });

  it("窗口最后切换到服务器已知接口时补齐边界网速", () => {
    const segments: AgentTrafficSegment[] = ["eth0", "tun0"].map((name, index) => ({
      utc_day: "2026-10-25", first_ts: T + (index + 1) * 1000, last_ts: T + (index + 1) * 1000,
      interfaces: [{ name, first_ts: T + (index + 1) * 1000, last_ts: T + (index + 1) * 1000,
        first_rx: index === 0 ? 1100 : 2100, first_tx: index === 0 ? 600 : 1100,
        last_rx: index === 0 ? 1100 : 2100, last_tx: index === 0 ? 600 : 1100, delta_rx: 0, delta_tx: 0 }],
      last_speed: null,
    }));
    const actual = computeSummarizedTraffic(previous(), segments, 1);
    expect(actual.speeds.at(-1)).toEqual({ rx: 100, tx: 100 });
    expect(actual.state.month_rx).toBe(400);
  });
});
