/** 从 backend/ 运行 pnpm exec vitest bench metricblock.bench.ts --run。 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { bench, describe } from "vitest";

import { base64ToBytes, decodeBlock, inspectBlock } from "./decode";
import { blockSamples } from "./materialize";

const fixtures = JSON.parse(readFileSync(join(
  process.cwd(), "src/modules/agents/metricblock/__fixtures__/blocks.json"
), "utf8")) as { cases: Array<{ name: string; data: string }> };

for (const fixture of fixtures.cases.filter(({ name }) =>
  name === "dense-1s" || name === "rollup-60s"
)) {
  describe(fixture.name, () => {
    const raw = base64ToBytes(fixture.data);
    const options = { time: 200, warmupTime: 100, iterations: 100 };

    bench("archive header inspection", () => { inspectBlock(raw); }, options);
    bench("decode and count occupied slots", async () => {
      await decodeBlock(raw);
    }, options);
    bench("decode and materialize samples", async () => {
      blockSamples(await decodeBlock(raw));
    }, options);
  });
}
