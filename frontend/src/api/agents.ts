import type { components } from "./generated/v2-schema";
import {
  OpenApiRequestError,
  unwrapOpenApi,
  v2Client,
} from "./generated/v2-client";
import { decodeAgentMetricHistoryInWorker } from "../features/agents/history/workerClient";

export type AgentV2 = components["schemas"]["Agent"];
export type AgentPage = components["schemas"]["AgentPage"];
export type AgentUpdate = components["schemas"]["AgentUpdate"];
export type AgentMetric = components["schemas"]["AgentMetric"];
export type AgentCredentialPage = components["schemas"]["AgentCredentialPage"];
export type AgentEnrollmentMetadata = components["schemas"]["AgentEnrollment"];
export type AgentExportItem = components["schemas"]["AgentExportItem"];
export type AgentImportItem = components["schemas"]["AgentImportItem"];
export type AgentImportResult = components["schemas"]["AgentImportResult"];

function assertOk(result: { response: Response; error?: unknown }) {
  if (!result.response.ok) {
    throw new OpenApiRequestError(result.response.status, result.error);
  }
}

export async function getAgentsPage(
  input: {
    cursor?: string;
    limit?: number;
    includeLatestMetrics?: boolean;
  } = {},
  signal?: AbortSignal
): Promise<AgentPage> {
  const result = await v2Client.GET("/api/v2/agents", {
    params: {
      query: {
        cursor: input.cursor,
        limit: input.limit ?? 50,
        include_latest_metrics: input.includeLatestMetrics ? "true" : "false",
      },
    },
    signal,
  });
  return unwrapOpenApi(result);
}

export async function getAgent(
  id: number,
  signal?: AbortSignal
): Promise<AgentV2> {
  const result = await v2Client.GET("/api/v2/agents/{id}", {
    params: { path: { id } },
    signal,
  });
  return unwrapOpenApi(result).data;
}

export async function updateAgent(id: number, input: AgentUpdate) {
  const result = await v2Client.PATCH("/api/v2/agents/{id}", {
    params: { path: { id } },
    body: input,
  });
  return unwrapOpenApi(result).data;
}

export async function deleteAgent(id: number): Promise<void> {
  const result = await v2Client.DELETE("/api/v2/agents/{id}", {
    params: { path: { id } },
  });
  assertOk(result);
}

export async function updateAgentsOrder(ids: number[]) {
  const result = await v2Client.PUT("/api/v2/agents/order", { body: { ids } });
  return unwrapOpenApi(result).data;
}

export async function exportAgents(): Promise<AgentExportItem[]> {
  const result = await v2Client.GET("/api/v2/agents/export");
  return unwrapOpenApi(result).data;
}

export async function importAgents(items: unknown[]): Promise<AgentImportResult> {
  const result = await v2Client.POST("/api/v2/agents/import", {
    body: items as AgentImportItem[],
  });
  return unwrapOpenApi(result).data;
}

export async function getAgentMetrics(
  id: number,
  signal?: AbortSignal,
  hours: "1" | "6" | "12" | "24" | "168" = "24"
): Promise<AgentMetric[]> {
  try {
    const blocks = await v2Client.GET("/api/v2/agents/{id}/metrics/blocks", {
      params: { path: { id }, query: { hours } },
      parseAs: "arrayBuffer",
      cache: "no-store",
      signal,
    });
    const buffer = unwrapOpenApi(blocks);
    if (!(buffer instanceof ArrayBuffer)) {
      throw new Error("Invalid metric history binary response");
    }
    return await decodeAgentMetricHistoryInWorker(buffer, id, signal);
  } catch (error) {
    // 取消包括离开页面/切换时间窗口，不应再触发旧接口请求。
    if (signal?.aborted || (error instanceof Error && error.name === "AbortError")) {
      throw error;
    }
  }

  const result = await v2Client.GET("/api/v2/agents/{id}/metrics", {
    params: { path: { id }, query: { hours } },
    signal,
  });
  return unwrapOpenApi(result).data;
}

export async function getLatestAgentMetrics(
  id: number,
  signal?: AbortSignal
): Promise<AgentMetric | null> {
  const result = await v2Client.GET("/api/v2/agents/{id}/metrics/latest", {
    params: { path: { id } },
    signal,
  });
  return unwrapOpenApi(result).data;
}

export async function generateToken() {
  const result = await v2Client.POST("/api/v2/agents/enrollments");
  return unwrapOpenApi(result).data;
}

export async function getAgentEnrollments(): Promise<AgentEnrollmentMetadata[]> {
  const result = await v2Client.GET("/api/v2/agents/enrollments");
  return unwrapOpenApi(result).data;
}

export async function revokeAgentEnrollment(id: number): Promise<void> {
  const result = await v2Client.DELETE("/api/v2/agents/enrollments/{id}", {
    params: { path: { id } },
  });
  assertOk(result);
}

export async function rotateAgentCredential(agentId: number) {
  const result = await v2Client.POST("/api/v2/agents/{id}/credentials", {
    params: { path: { id: agentId } },
  });
  return unwrapOpenApi(result).data;
}

export async function revokeAgentCredential(
  agentId: number,
  credentialId: number
): Promise<void> {
  const result = await v2Client.DELETE(
    "/api/v2/agents/{id}/credentials/{credentialId}",
    {
      params: {
        path: { id: agentId, credentialId },
      },
    }
  );
  assertOk(result);
}
