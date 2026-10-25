import { expandMetricHours, type MetricHistoryWork } from "./metric-history";

globalThis.onmessage = (event: MessageEvent<MetricHistoryWork>) => {
  globalThis.postMessage(expandMetricHours(event.data));
};
