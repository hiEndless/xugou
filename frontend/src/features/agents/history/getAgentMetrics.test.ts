import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { getAgentMetrics } from "../../../api/agents";
import { v2Client } from "../../../api/generated/v2-client";
import type { HistoryDecodeResponse, HistoryDecodeWorker } from "./workerClient";

const metric = { agent_id: 7, timestamp: "2026-08-06T08:00:00.000Z", cpu_usage: 12 };

function mockGet(context: TestContext, handler: (path: string, options: {
  signal?: AbortSignal;
  parseAs?: string;
  cache?: RequestCache;
  params?: { query?: { hours?: string } };
}) => Promise<unknown>) {
  const original = v2Client.GET;
  v2Client.GET = handler as typeof v2Client.GET;
  context.after(() => { v2Client.GET = original; });
}

function mockWorker(context: TestContext, constructor: unknown) {
  const original = Object.getOwnPropertyDescriptor(globalThis, "Worker");
  Object.defineProperty(globalThis, "Worker", { configurable: true, value: constructor });
  context.after(() => {
    if (original) Object.defineProperty(globalThis, "Worker", original);
    else Reflect.deleteProperty(globalThis, "Worker");
  });
}

class FakeWorker implements HistoryDecodeWorker {
  onmessage: HistoryDecodeWorker["onmessage"] = null;
  onerror: HistoryDecodeWorker["onerror"] = null;
  onmessageerror: HistoryDecodeWorker["onmessageerror"] = null;
  terminated = false;

  postMessage() {
    queueMicrotask(() => this.onmessage?.({ data: { ok: true, metrics: [metric] } } as MessageEvent<HistoryDecodeResponse>));
  }

  terminate() { this.terminated = true; }
}

test("getAgentMetrics returns Worker output without calling the legacy endpoint", async (context) => {
  const calls: string[] = [];
  mockWorker(context, FakeWorker);
  mockGet(context, async (path, options) => {
    calls.push(path);
    assert.equal(options.parseAs, "arrayBuffer");
    assert.equal(options.cache, "no-store");
    assert.equal(options.params?.query?.hours, "168");
    return { data: new ArrayBuffer(8), response: new Response() };
  });
  assert.deepEqual(await getAgentMetrics(7, undefined, "168"), [metric]);
  assert.deepEqual(calls, ["/api/v2/agents/{id}/metrics/blocks"]);
});

test("a binary HTTP failure falls back to the existing JSON endpoint", async (context) => {
  const calls: string[] = [];
  mockGet(context, async (path) => {
    calls.push(path);
    return path.endsWith("/blocks")
      ? { error: { title: "Unsupported" }, response: new Response(null, { status: 404 }) }
      : { data: { data: [metric] }, response: new Response() };
  });
  assert.deepEqual(await getAgentMetrics(7), [metric]);
  assert.deepEqual(calls, ["/api/v2/agents/{id}/metrics/blocks", "/api/v2/agents/{id}/metrics"]);
});

test("a blocked Worker falls back to JSON", async (context) => {
  mockWorker(context, class { constructor() { throw new Error("Worker blocked"); } });
  const calls: string[] = [];
  mockGet(context, async (path) => {
    calls.push(path);
    return { data: path.endsWith("/blocks") ? new ArrayBuffer(8) : { data: [metric] }, response: new Response() };
  });
  assert.deepEqual(await getAgentMetrics(7), [metric]);
  assert.equal(calls.length, 2);
});

test("aborting the binary fetch never starts JSON fallback", async (context) => {
  const controller = new AbortController();
  const calls: string[] = [];
  mockGet(context, async (path) => {
    calls.push(path);
    controller.abort();
    throw controller.signal.reason;
  });
  await assert.rejects(getAgentMetrics(7, controller.signal), { name: "AbortError" });
  assert.equal(calls.length, 1);
});

test("aborting during Worker decoding terminates it without JSON fallback", async (context) => {
  const controller = new AbortController();
  let notifyCreated: (worker: FakeWorker) => void = () => {};
  const created = new Promise<FakeWorker>(resolve => { notifyCreated = resolve; });
  mockWorker(context, class extends FakeWorker {
    constructor() {
      super();
      notifyCreated(this);
    }
    postMessage() {}
  });
  const calls: string[] = [];
  mockGet(context, async (path) => {
    calls.push(path);
    return { data: new ArrayBuffer(8), response: new Response() };
  });
  const result = getAgentMetrics(7, controller.signal);
  const worker = await created;
  controller.abort();
  await assert.rejects(result, { name: "AbortError" });
  assert.equal(worker.terminated, true);
  assert.equal(calls.length, 1);
});
