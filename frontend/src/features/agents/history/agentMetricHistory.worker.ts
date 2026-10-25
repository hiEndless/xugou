import { decodeAgentMetricHistory } from "./decodeAgentMetricHistory";
import type { HistoryDecodeRequest, HistoryDecodeResponse } from "./workerClient";

const workerScope = self as unknown as {
  onmessage: (event: MessageEvent<HistoryDecodeRequest>) => void;
  postMessage(response: HistoryDecodeResponse): void;
};

workerScope.onmessage = async (event) => {
  try {
    const metrics = await decodeAgentMetricHistory(event.data.buffer, event.data.agentId);
    workerScope.postMessage({ ok: true, metrics });
  } catch (error) {
    workerScope.postMessage({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};
