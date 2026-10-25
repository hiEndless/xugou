import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeAgentMetricHistoryInWorker,
  type HistoryDecodeRequest,
  type HistoryDecodeResponse,
  type HistoryDecodeWorker,
} from "./workerClient";

class FakeWorker implements HistoryDecodeWorker {
  onmessage: HistoryDecodeWorker["onmessage"] = null;
  onerror: HistoryDecodeWorker["onerror"] = null;
  onmessageerror: HistoryDecodeWorker["onmessageerror"] = null;
  terminated = false;
  requests: Array<{ message: HistoryDecodeRequest; transfer: Transferable[] }> = [];

  postMessage(message: HistoryDecodeRequest, transfer: Transferable[]) {
    this.requests.push({ message, transfer });
  }

  terminate() {
    this.terminated = true;
  }

  respond(response: HistoryDecodeResponse) {
    this.onmessage?.({ data: response } as MessageEvent<HistoryDecodeResponse>);
  }
}

test("transfers the binary response and terminates its Worker after success", async () => {
  const worker = new FakeWorker();
  const buffer = new ArrayBuffer(8);
  const result = decodeAgentMetricHistoryInWorker(buffer, 7, undefined, () => worker);
  assert.deepEqual(worker.requests, [{ message: { agentId: 7, buffer }, transfer: [buffer] }]);
  worker.respond({ ok: true, metrics: [] });
  assert.deepEqual(await result, []);
  assert.equal(worker.terminated, true);
  assert.equal(worker.onmessage, null);
});

test("decode and Worker startup errors reject for legacy JSON fallback", async () => {
  const worker = new FakeWorker();
  const result = decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, undefined, () => worker);
  worker.respond({ ok: false, error: "Unsupported codec" });
  await assert.rejects(result, /Unsupported codec/);
  assert.equal(worker.terminated, true);
  await assert.rejects(decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, undefined, () => {
    throw new Error("Worker blocked");
  }), /Worker blocked/);
});

test("aborting during decode immediately terminates the Worker", async () => {
  const worker = new FakeWorker();
  const controller = new AbortController();
  const result = decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, controller.signal, () => worker);
  controller.abort();
  await assert.rejects(result, { name: "AbortError" });
  assert.equal(worker.terminated, true);
  assert.equal(worker.onmessage, null);
});

test("an already cancelled query never creates a Worker", async () => {
  const controller = new AbortController();
  controller.abort();
  let created = false;
  await assert.rejects(decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, controller.signal, () => {
    created = true;
    return new FakeWorker();
  }), { name: "AbortError" });
  assert.equal(created, false);
});

test("postMessage and message deserialization errors terminate the Worker", async () => {
  const worker = new FakeWorker();
  worker.postMessage = () => { throw new Error("Transfer failed"); };
  await assert.rejects(decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, undefined, () => worker), /Transfer failed/);
  assert.equal(worker.terminated, true);

  const badMessageWorker = new FakeWorker();
  const result = decodeAgentMetricHistoryInWorker(new ArrayBuffer(0), 7, undefined, () => badMessageWorker);
  badMessageWorker.onmessageerror?.({} as MessageEvent<unknown>);
  await assert.rejects(result, /could not be decoded/);
  assert.equal(badMessageWorker.terminated, true);
});
