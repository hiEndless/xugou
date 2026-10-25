import {
  StatusPageConfig,
  StatusPageConfigResponse,
  StatusPageData,
} from "../types/status";
import { PublicMetricHistory } from "../types/agents";
import { unwrapOpenApi, v2Client } from "./generated/v2-client";
import { materializeMetricHours } from "../features/status/metric-history-client";
import type { PublicMetricHourResponse } from "../features/status/metric-history";

// Entries are keyed by the server's hour revision. Closed hours are reused when
// the rolling window advances; only the changing head hour is downloaded again.
const publicHourCache = new Map<string, PublicMetricHourResponse>();
const MAX_CACHED_PUBLIC_HOURS = 128;

// 获取状态页配置
export const getStatusPageConfig = async (
  signal?: AbortSignal
): Promise<StatusPageConfigResponse> => {
    const response = await v2Client.GET("/api/v2/status/config", { signal });
    return unwrapOpenApi(response).data;
  };

// 保存状态页配置
export const saveStatusPageConfig = async (
  config: StatusPageConfig
): Promise<StatusPageConfigResponse> => {
  const response = await v2Client.PUT("/api/v2/status/config", {
    body: config,
  });
  return unwrapOpenApi(response).data;
};

// 获取状态页数据
export const getStatusPageData = async (
  signal?: AbortSignal
): Promise<StatusPageData> => {
  const response = await v2Client.GET("/api/v2/status/public", { signal });
  return unwrapOpenApi(response);
};

export const getPublicAgentMetrics = async (
  agentId: number,
  signal?: AbortSignal
): Promise<{
  success: boolean;
  agent?: PublicMetricHistory[];
  message?: string;
}> => {
  const manifestResult = await v2Client.GET(
    "/api/v2/status/public/agents/{agentId}/metrics/manifest",
    { params: { path: { agentId } }, signal }
  );
  // Allows the current frontend to run during a rolling backend deployment.
  if (manifestResult.response.status === 404) return getLegacyPublicAgentMetrics(agentId, signal);
  const manifest = unwrapOpenApi(manifestResult);
  const hours: PublicMetricHourResponse[] = [];
  const pending: typeof manifest.hours = [];
  const remember = (item: typeof manifest.hours[number], hour: PublicMetricHourResponse) => {
    const prefix = `${agentId}:${item.bucket_start}:`;
    for (const cachedKey of publicHourCache.keys()) {
      if (cachedKey.startsWith(prefix)) publicHourCache.delete(cachedKey);
    }
    publicHourCache.set(`${prefix}${item.revision}`, hour);
    while (publicHourCache.size > MAX_CACHED_PUBLIC_HOURS) {
      const oldest = publicHourCache.keys().next().value;
      if (oldest !== undefined) publicHourCache.delete(oldest);
    }
    hours.push(hour);
  };
  for (const item of manifest.hours) {
    const cached = publicHourCache.get(`${agentId}:${item.bucket_start}:${item.revision}`);
    if (cached) hours.push(cached);
    else pending.push(item);
  }

  let unready = pending;
  if (pending.length > 0) {
    // One round trip loads every ready hour, including a cold 24-hour view. Legacy
    // archives are left for the bounded single-hour path rather than bulk-decoded.
    const response = await v2Client.GET(
      "/api/v2/status/public/agents/{agentId}/metrics/hours",
      { params: { path: { agentId }, query: { buckets: pending.map((item) => item.bucket_start).join(",") } },
        signal, cache: "no-cache" }
    );
    if (response.response.status !== 404) {
      const batch = unwrapOpenApi(response);
      if (batch.agent_id !== agentId) throw new Error("Metric history agent mismatch");
      const ready = new Map(batch.hours.map((hour) => [hour.bucket_start, hour]));
      unready = pending.filter((item) => {
        const hour = ready.get(item.bucket_start);
        if (!hour) return true;
        remember(item, { success: true, agent_id: agentId, ...hour });
        return false;
      });
    }
  }
  let cursor = 0;
  const fetchNext = async () => {
    while (cursor < unready.length) {
      const item = unready[cursor++];
      const response = await v2Client.GET(
        "/api/v2/status/public/agents/{agentId}/metrics/hours/{bucketStart}",
        { params: { path: { agentId, bucketStart: item.bucket_start } }, signal, cache: "no-cache" }
      );
      // A manifest can cross the left edge of the 24-hour window while loading.
      if (response.response.status === 404) continue;
      remember(item, unwrapOpenApi(response));
    }
  };
  await Promise.all(Array.from({ length: Math.min(4, unready.length) }, fetchNext));
  return {
    success: true,
    agent: await materializeMetricHours({ agentId, from: manifest.from, to: manifest.to, hours }, signal),
  };
};

const getLegacyPublicAgentMetrics = async (
  agentId: number,
  signal?: AbortSignal
): Promise<{ success: boolean; agent?: PublicMetricHistory[]; message?: string }> => {
  const response = await v2Client.GET(
    "/api/v2/status/public/agents/{agentId}/metrics",
    {
      params: { path: { agentId } },
      signal,
    }
  );
  const payload = unwrapOpenApi(response);
  return {
    ...payload,
    agent: payload.agent?.map((metric) => ({
      ...metric,
      id: metric.id === undefined ? undefined : Number(metric.id),
    })),
  };
};
