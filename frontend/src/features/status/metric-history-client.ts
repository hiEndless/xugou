import type { PublicMetricHistory } from "../../types/agents";
import { expandMetricHours, type MetricHistoryWork } from "./metric-history";

export async function materializeMetricHours(work: MetricHistoryWork, signal?: AbortSignal) {
  if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
  if (typeof Worker === "undefined") return expandMetricHours(work);
  let worker: Worker;
  try {
    worker = new Worker(new URL("./metric-history.worker.ts", import.meta.url), { type: "module" });
  } catch {
    return expandMetricHours(work);
  }
  return new Promise<PublicMetricHistory[]>((resolve, reject) => {
    const cleanup = () => {
      worker.terminate();
      signal?.removeEventListener("abort", abort);
    };
    const abort = () => {
      cleanup();
      reject(new DOMException("Aborted", "AbortError"));
    };
    worker.onmessage = (event: MessageEvent<PublicMetricHistory[]>) => {
      cleanup();
      resolve(event.data);
    };
    const fallback = () => {
      cleanup();
      try {
        resolve(expandMetricHours(work));
      } catch (error) {
        reject(error);
      }
    };
    worker.onerror = fallback;
    worker.onmessageerror = fallback;
    signal?.addEventListener("abort", abort, { once: true });
    try {
      worker.postMessage(work);
    } catch {
      fallback();
    }
  });
}
