import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { bench, describe } from "vitest";
import { parseBinaryReport } from "../http/binary-report";
import { agentV5ReportSchema } from "../http/schemas";
import { base64ToBytes, decodeBlock, inspectBlock } from "./decode";
import { blockSamples } from "./materialize";
import { computeSummarizedTraffic } from "../../../utils/traffic-summary";

const binary = new Uint8Array(readFileSync(resolve(process.cwd(), "../agent/pkg/reporter/testdata/report-v6.bin")));
const report = parseBinaryReport(binary);
const { summary: _summary, minute_summaries: _minutes, ...legacyFields } = report;
void _summary;
void _minutes;
const legacy = gzipSync(JSON.stringify({ ...legacyFields, protocol_version: 5,
  blocks: report.blocks.map(block => ({ ...block, data: Buffer.from(block.data as Uint8Array).toString("base64") })) }));

/** Same cross-language fixture; excludes database/network latency and browser work. */
describe("report request parsing and archive validation", () => {
  bench("v5 gzip JSON, schema, full block decode and materialize", async () => {
    const compressed: ReadableStream<BufferSource> = new Response(new Uint8Array(legacy).buffer).body!;
    const value = await new Response(compressed.pipeThrough(new DecompressionStream("gzip"))).json();
    const parsed = agentV5ReportSchema.parse(value);
    for (const block of parsed.blocks) blockSamples(await decodeBlock(base64ToBytes(block.data)));
  });
  bench("v6 metadata, bounded block inspection and traffic summary", () => {
    const parsed = parseBinaryReport(binary);
    for (const block of parsed.blocks) inspectBlock(block.data as Uint8Array);
    computeSummarizedTraffic(null, parsed.summary!.traffic, 1);
  });
});
