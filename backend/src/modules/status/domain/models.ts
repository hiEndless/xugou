export interface StatusPageConfigCommand {
  title: string;
  description: string;
  logoUrl: string;
  customCss: string;
  theme: string;
  monitors: number[];
  agents: number[];
}

export interface ActiveStatusPublication {
  payloadJson: string;
  etag: string;
  generatedAt: string;
}

export interface ActiveMetricPublication extends ActiveStatusPublication {
  agentId: number;
}

export interface PublicMetricManifest {
  success: true;
  agent_id: number;
  from: number;
  to: number;
  hours: { bucket_start: number; revision: string }[];
}
