import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { decodeBlock } from "../../../../../backend/src/modules/agents/metricblock/decode";
import type { AgentMetricHistoryMetadata } from "../../../../../backend/src/modules/agents/metricblock/history-wire";
import { blockSamples } from "../../../../../backend/src/modules/agents/metricblock/materialize";
import { blockSampleToMetrics } from "../../../../../backend/src/modules/agents/metricblock/metrics-view";
import { decodeAgentMetricHistory } from "./decodeAgentMetricHistory";

const fixtures = JSON.parse(readFileSync(new URL(
  "../../../../../backend/src/modules/agents/metricblock/__fixtures__/blocks.json",
  import.meta.url
), "utf8")) as { cases: Array<{ name: string; data: string }> };
const archive = Uint8Array.from(Buffer.from(fixtures.cases.find(item => item.name === "rollup-60s")!.data, "base64"));

function envelope(metadata: AgentMetricHistoryMetadata, blocks: Uint8Array[] = []): ArrayBuffer {
  const encoded = new TextEncoder().encode(JSON.stringify(metadata));
  const bytes = new Uint8Array(8 + encoded.length + blocks.reduce((total, block) => total + block.byteLength, 0));
  bytes.set(new TextEncoder().encode("XHR1"));
  new DataView(bytes.buffer).setUint32(4, encoded.length, true);
  bytes.set(encoded, 8);
  let offset = 8 + encoded.length;
  for (const block of blocks) {
    bytes.set(block, offset);
    offset += block.length;
  }
  return bytes.buffer;
}

function metadata(overrides: Partial<AgentMetricHistoryMetadata> = {}): AgentMetricHistoryMetadata {
  return {
    agent_id: 7,
    from: 1785999600,
    to: 1786086000,
    resolution: 60,
    max_points: 160,
    blocks: [],
    pending: [],
    ...overrides,
  };
}

test("Go archive fixture produces exactly the legacy JSON mapping", async () => {
  const samples = blockSamples(await decodeBlock(archive));
  const wire = metadata({ blocks: [{ byte_length: archive.length, legacy_samples: [] }] });
  const actual = await decodeAgentMetricHistory(envelope(wire, [archive]), 7);
  const expected = JSON.parse(JSON.stringify(samples.map(sample => blockSampleToMetrics(7, sample))));
  assert.deepEqual(actual, expected);
  assert.equal(Object.prototype.hasOwnProperty.call(actual[0], "cpu_model"), false);
});

test("history sorts archive buckets, deduplicates, overlays pending and filters the window", async () => {
  const samples = blockSamples(await decodeBlock(archive));
  const later = archive.slice();
  new DataView(later.buffer).setUint32(12, 1786003200, true);
  const first = samples[0];
  const wire = metadata({
    from: first.timestampMs / 1000,
    to: 1786006800,
    blocks: [
      { byte_length: later.length, legacy_samples: [] },
      { byte_length: archive.length, legacy_samples: [
        { ...first, cpuUsage: -1 },
        { ...first, timestampMs: first.timestampMs - 60000 },
        { ...first, timestampMs: first.timestampMs + 1000, cpuUsage: 88 },
      ] },
    ],
    pending: [{ collected_at: new Date(first.timestampMs).toISOString(), cpu: { usage: 99 } }],
  });
  const actual = await decodeAgentMetricHistory(envelope(wire, [later, archive]), 7);
  assert.equal(actual[0].cpu_usage, 99);
  assert.equal(actual[1].cpu_usage, 88);
  assert.equal(actual.filter(item => item.timestamp === new Date(first.timestampMs).toISOString()).length, 1);
  assert.deepEqual(actual.map(item => Date.parse(item.timestamp)), actual.map(item => Date.parse(item.timestamp)).sort((a, b) => a - b));
  assert.ok(actual.every(item => Date.parse(item.timestamp) >= wire.from * 1000 && Date.parse(item.timestamp) <= wire.to * 1000));
});

test("pending minute summaries use the same sampling stride and retain the latest point", async () => {
  const wire = metadata({
    pending: Array.from({ length: 170 }, (_, index) => ({
      collected_at: new Date((1785999600 + index * 60) * 1000).toISOString(),
      cpu: { usage: index },
    })),
  });
  const actual = await decodeAgentMetricHistory(envelope(wire), 7);
  assert.deepEqual(actual.map(item => item.cpu_usage), [...Array.from({ length: 85 }, (_, index) => index * 2), 169]);
});

test("malformed, mismatched and truncated envelopes reject for JSON fallback", async () => {
  await assert.rejects(decodeAgentMetricHistory(new ArrayBuffer(0), 7));
  await assert.rejects(decodeAgentMetricHistory(envelope(metadata()), 8));
  await assert.rejects(decodeAgentMetricHistory(envelope(metadata({
    blocks: [{ byte_length: archive.length + 1, legacy_samples: [] }],
  }), [archive]), 7));
  await assert.rejects(decodeAgentMetricHistory(envelope(metadata(), [archive]), 7));
  const corrupt = archive.slice();
  corrupt[0] = 0;
  await assert.rejects(decodeAgentMetricHistory(envelope(metadata({
    blocks: [{ byte_length: corrupt.length, legacy_samples: [] }],
  }), [corrupt]), 7));
});
