import type { AgentMetric } from "../../../api/agents";

export interface HistoryDecodeRequest {
  agentId: number;
  buffer: ArrayBuffer;
}

export type HistoryDecodeResponse =
  | { ok: true; metrics: AgentMetric[] }
  | { ok: false; error: string };

export interface HistoryDecodeWorker {
  postMessage(message: HistoryDecodeRequest, transfer: Transferable[]): void;
  terminate(): void;
  onmessage: ((event: MessageEvent<HistoryDecodeResponse>) => void) | null;
  onerror: ((event: ErrorEvent) => void) | null;
  onmessageerror: ((event: MessageEvent<unknown>) => void) | null;
}

function createHistoryWorker(): HistoryDecodeWorker {
  return new Worker(new URL("./agentMetricHistory.worker.ts", import.meta.url), {
    type: "module",
  });
}

function abortReason(signal?: AbortSignal): unknown {
  return signal?.reason ?? new DOMException("The operation was aborted.", "AbortError");
}

/** 每次查询独占一个 Worker，取消时可立即终止解压与解码。 */
export function decodeAgentMetricHistoryInWorker(
  buffer: ArrayBuffer,
  agentId: number,
  signal?: AbortSignal,
  createWorker: () => HistoryDecodeWorker = createHistoryWorker
): Promise<AgentMetric[]> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortReason(signal));
      return;
    }

    let worker: HistoryDecodeWorker;
    try {
      worker = createWorker();
    } catch (error) {
      reject(error);
      return;
    }

    const cleanup = () => {
      worker.onmessage = null;
      worker.onerror = null;
      worker.onmessageerror = null;
      signal?.removeEventListener("abort", onAbort);
      worker.terminate();
    };
    const onAbort = () => {
      cleanup();
      reject(abortReason(signal));
    };
    const fail = (error: unknown) => {
      cleanup();
      reject(error);
    };

    worker.onmessage = (event) => {
      const response = event.data;
      if (!response || typeof response !== "object") {
        fail(new Error("Invalid metric history Worker response"));
        return;
      }
      if (response.ok === true && Array.isArray(response.metrics)) {
        cleanup();
        resolve(response.metrics);
      } else {
        fail(new Error(response.ok === false ? response.error : "Invalid metric history Worker response"));
      }
    };
    worker.onerror = (event) => {
      event.preventDefault();
      fail(new Error(event.message || "Metric history Worker failed"));
    };
    worker.onmessageerror = () => fail(new Error("Metric history Worker message could not be decoded"));
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) {
      onAbort();
      return;
    }
    try {
      worker.postMessage({ agentId, buffer }, [buffer]);
    } catch (error) {
      fail(error);
    }
  });
}
