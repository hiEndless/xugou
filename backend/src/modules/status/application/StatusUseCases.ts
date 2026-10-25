import { ApplicationProblem } from "../../../shared/errors/ApplicationProblem";
import type {
  ActiveStatusPublication,
  ActiveMetricPublication,
  StatusPageConfigCommand,
  PublicMetricManifest,
} from "../domain/models";

export interface StatusRepositoryPort {
  getConfig(): Promise<unknown>;
  saveConfig(input: StatusPageConfigCommand): Promise<unknown>;
  getActivePublication(): Promise<ActiveStatusPublication | null>;
  getActiveMetricPublication(agentId: number): Promise<ActiveMetricPublication | null>;
  getPublicMetricManifest(agentId: number): Promise<PublicMetricManifest | null>;
  getPublicMetricHour(agentId: number, bucketStart: number): Promise<{ payloadJson: string; etag: string } | null>;
  getPublicMetricHours(agentId: number, buckets: number[]): Promise<{ payloadJson: string } | null>;
}

export class StatusUseCases {
  constructor(private readonly repository: StatusRepositoryPort) {}

  getConfig() {
    return this.repository.getConfig();
  }

  async saveConfig(input: StatusPageConfigCommand) {
    if (
      input.monitors.length > 100 ||
      input.agents.length > 100 ||
      new Set(input.monitors).size !== input.monitors.length ||
      new Set(input.agents).size !== input.agents.length
    ) {
      throw new ApplicationProblem(
        400,
        "STATUS_CONFIG_RESOURCE_LIMIT_INVALID",
        "Status configuration resource IDs must be unique and at most 100 per type"
      );
    }
    try {
      await this.repository.saveConfig(input);
      return await this.repository.getConfig();
    } catch (error) {
      if (error instanceof ApplicationProblem) throw error;
      if (error instanceof Error && error.name === "StatusPageConfigValidationError") {
        throw new ApplicationProblem(400, "STATUS_CONFIG_INVALID", error.message);
      }
      throw error;
    }
  }

  async getPublicData() {
    const publication = await this.repository.getActivePublication();
    if (publication) return publication;
    throw new ApplicationProblem(
      503,
      "PUBLICATION_NOT_READY",
      "Public status publication is not ready"
    );
  }

  async getPublicAgentMetrics(agentId: number) {
    const publication = await this.repository.getActiveMetricPublication(agentId);
    if (publication) return publication;
    throw new ApplicationProblem(
      404,
      "PUBLIC_AGENT_NOT_FOUND",
      "Public agent metric publication not found"
    );
  }

  async getPublicMetricManifest(agentId: number) {
    const manifest = await this.repository.getPublicMetricManifest(agentId);
    if (manifest) return manifest;
    throw new ApplicationProblem(404, "PUBLIC_AGENT_NOT_FOUND", "Public agent not found");
  }

  async getPublicMetricHour(agentId: number, bucketStart: number) {
    const hour = await this.repository.getPublicMetricHour(agentId, bucketStart);
    if (hour) return hour;
    throw new ApplicationProblem(404, "PUBLIC_METRIC_HOUR_NOT_FOUND", "Public metric hour not found");
  }

  async getPublicMetricHours(agentId: number, buckets: number[]) {
    const hours = await this.repository.getPublicMetricHours(agentId, buckets);
    if (hours) return hours;
    throw new ApplicationProblem(404, "PUBLIC_AGENT_NOT_FOUND", "Public agent not found");
  }
}
