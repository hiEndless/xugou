/**
 * 跨语言往返测试：解码 Go 编码器产出的 fixture 并逐值比对。
 *
 * fixture 由 `cd agent && go test ./pkg/metricblock/` 生成。
 * 两端序列注册表或字节布局一旦漂移，这里立刻失败。
 */

import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

import {
  base64ByteLength,
  base64ToBytes,
  decodeBlock,
  inspectBlock,
  MetricBlockError,
} from "./decode";
import {
  CODEC_VERSION,
  ENCODING_DELTA,
  ENCODING_DELTA_OF_DELTA,
  ENCODING_RAW,
  FLAG_PRESENCE,
  HEADER_SIZE,
  MAGIC,
  PING_FIELD_LATENCY_MS,
  PING_FIELD_LOSS,
  SERIES_CPU_USAGE,
  SERIES_MEMORY_USED,
  SERIES_PROCESS_COUNT,
  pingSeriesId,
} from "./registry";

interface FixtureCase {
  name: string;
  data: string;
  pointCount: number;
  expect: {
    interval: number;
    bucketStart: number;
    slotCount: number;
    aggregateCount: number;
    memoryTotal: number;
    swapTotal: number;
    dims: {
      disks: { Name: string; Total: number }[];
      nets: string[];
      pings: string[];
    };
    series: Record<string, (number | null)[][]>;
  };
}

// tsconfig 的 module 是 CommonJS，用不了 import.meta；vitest 由 backend/ 目录启动，
// 因此这里用 cwd 相对路径。fixture 缺失时给出明确的重建指引，而不是一个 ENOENT。
const FIXTURE_PATH = join(
  process.cwd(),
  "src/modules/agents/metricblock/__fixtures__/blocks.json"
);
if (!existsSync(FIXTURE_PATH)) {
  throw new Error(
    `跨语言 fixture 缺失: ${FIXTURE_PATH}\n` +
      "请先运行 `cd agent && go test ./pkg/metricblock/` 生成。"
  );
}
const fixtures = JSON.parse(readFileSync(FIXTURE_PATH, "utf8")) as {
  codecVersion: number;
  cases: FixtureCase[];
};

function bytesOf(base64: string): Uint8Array {
  return Uint8Array.from(Buffer.from(base64, "base64"));
}

interface TestSeries {
  id: number;
  encoding: number;
  values: (bigint | null)[][];
}

/** 未压缩的独立测试块，覆盖 Go fixture 中没有出现的整数边界和不同 presence。 */
function integerBlock(
  series: TestSeries[],
  options: { slotCount?: number; aggregateCount?: number; presence?: boolean } = {}
): Uint8Array {
  const slotCount = options.slotCount ?? series[0]?.values[0].length ?? 1;
  const aggregateCount = options.aggregateCount ?? series[0]?.values.length ?? 1;
  const presence = options.presence ?? true;
  const header = new Uint8Array(HEADER_SIZE + 3 + series.length * 4);
  header[0] = MAGIC;
  header[1] = CODEC_VERSION;
  header[2] = 1;
  header[3] = presence ? FLAG_PRESENCE : 0;
  header[4] = aggregateCount;
  const view = new DataView(header.buffer);
  view.setUint16(6, series.length, true);
  view.setUint16(8, slotCount, true);
  const payload: number[] = [];
  const appendSigned = (value: bigint) => {
    let encoded = value < 0n ? (-value << 1n) - 1n : value << 1n;
    while (encoded >= 128n) {
      payload.push(Number(encoded & 0x7fn) | 0x80);
      encoded >>= 7n;
    }
    payload.push(Number(encoded));
  };
  series.forEach((column, index) => {
    const descriptorOffset = HEADER_SIZE + 3 + index * 4;
    view.setUint16(descriptorOffset, column.id, true);
    header[descriptorOffset + 2] = column.encoding;
    for (const values of column.values) {
      if (presence) {
        const bitmap = new Uint8Array(Math.ceil(slotCount / 8));
        values.forEach((value, slot) => {
          if (value !== null) bitmap[slot >> 3] |= 1 << (slot & 7);
        });
        payload.push(...bitmap);
      }
      let previous = 0n;
      let previousDelta = 0n;
      let count = 0;
      for (const value of values) {
        if (value === null) continue;
        const delta = value - previous;
        const encoded = column.encoding === ENCODING_RAW || count === 0
          ? value
          : column.encoding === ENCODING_DELTA_OF_DELTA && count > 1
            ? delta - previousDelta
            : delta;
        appendSigned(encoded);
        previous = value;
        previousDelta = delta;
        count++;
      }
    }
  });
  return Uint8Array.from([...header, ...payload]);
}

describe("跨语言往返", () => {
  it("fixture 的 codec 版本与本端一致", () => {
    expect(fixtures.codecVersion).toBe(CODEC_VERSION);
  });

  it("fixture 覆盖了稠密/稀疏/单点/丢包/聚合五种形态", () => {
    expect(fixtures.cases.map((c) => c.name)).toEqual([
      "dense-1s",
      "sparse-1s",
      "single-point-1s",
      "ping-loss-1s",
      "rollup-60s",
    ]);
  });

  for (const testCase of fixtures.cases) {
    describe(testCase.name, () => {
      it("块头与维度头解码一致", async () => {
        const block = await decodeBlock(bytesOf(testCase.data));
        expect(block.interval).toBe(testCase.expect.interval);
        expect(block.bucketStart).toBe(testCase.expect.bucketStart);
        expect(block.slotCount).toBe(testCase.expect.slotCount);
        expect(block.actualPointCount).toBe(testCase.pointCount);
        expect(block.aggregateCount).toBe(testCase.expect.aggregateCount);
        expect(block.memoryTotal).toBe(testCase.expect.memoryTotal);
        expect(block.swapTotal).toBe(testCase.expect.swapTotal);
        expect(block.dims.nets).toEqual(testCase.expect.dims.nets);
        expect(block.dims.pings).toEqual(testCase.expect.dims.pings);
        expect(block.dims.disks.map((d) => d.name)).toEqual(
          testCase.expect.dims.disks.map((d) => d.Name)
        );
        expect(block.dims.disks.map((d) => d.total)).toEqual(
          testCase.expect.dims.disks.map((d) => d.Total)
        );
      });

      it("每个序列每个槽的值都与 Go 侧一致", async () => {
        const block = await decodeBlock(bytesOf(testCase.data));
        const expectedIds = Object.keys(testCase.expect.series)
          .map(Number)
          .sort((a, b) => a - b);
        expect([...block.series.keys()].sort((a, b) => a - b)).toEqual(
          expectedIds
        );

        for (const [idText, expectedAggs] of Object.entries(
          testCase.expect.series
        )) {
          const id = Number(idText);
          const actualAggs = block.series.get(id);
          expect(actualAggs, `series ${id} 缺失`).toBeDefined();
          expect(actualAggs!.length).toBe(expectedAggs.length);

          for (let agg = 0; agg < expectedAggs.length; agg++) {
            const expected = expectedAggs[agg];
            const actual = actualAggs![agg];
            expect(actual.length, `series ${id} agg ${agg} 槽数不符`).toBe(
              expected.length
            );
            for (let slot = 0; slot < expected.length; slot++) {
              const want = expected[slot];
              const got = actual[slot];
              if (want === null) {
                expect(got, `series ${id} agg ${agg} 槽 ${slot} 应缺失`).toBeNull();
                continue;
              }
              expect(got, `series ${id} agg ${agg} 槽 ${slot} 不应缺失`).not.toBeNull();
              // 两端都已定点化，理论上应精确相等；留极小容差防浮点除法末位差异
              expect(
                Math.abs((got as number) - want),
                `series ${id} agg ${agg} 槽 ${slot}: 期望 ${want}，实际 ${got}`
              ).toBeLessThan(1e-9);
            }
          }
        }
      });
    });
  }

  it("稀疏块的缺失槽落在预期区间", async () => {
    const sparse = fixtures.cases.find((c) => c.name === "sparse-1s")!;
    const block = await decodeBlock(bytesOf(sparse.data));
    const cpu = block.series.get(SERIES_CPU_USAGE)![0];
    for (let slot = 20; slot < 40; slot++) {
      expect(cpu[slot], `槽 ${slot} 应缺失`).toBeNull();
    }
    for (const slot of [0, 19, 40, 59]) {
      expect(cpu[slot], `槽 ${slot} 不应缺失`).not.toBeNull();
    }
  });

  it("丢包时 latency 保真为 -1（未被哨兵值吞掉）", async () => {
    const lossy = fixtures.cases.find((c) => c.name === "ping-loss-1s")!;
    const block = await decodeBlock(bytesOf(lossy.data));
    const slot = block.dims.pings.indexOf("ct");
    expect(slot).toBeGreaterThanOrEqual(0);
    const latency = block.series.get(
      pingSeriesId(slot, PING_FIELD_LATENCY_MS)
    )![0][0];
    expect(latency).toBeCloseTo(-1, 6);
    const loss = block.series.get(pingSeriesId(slot, PING_FIELD_LOSS))![0][0];
    expect(loss).toBe(1);
  });

  it("聚合块满足 min <= avg <= max", async () => {
    const rollup = fixtures.cases.find((c) => c.name === "rollup-60s")!;
    const block = await decodeBlock(bytesOf(rollup.data));
    const [avg, min, max] = block.series.get(SERIES_CPU_USAGE)!;
    for (let slot = 0; slot < block.slotCount; slot++) {
      if (avg[slot] === null) continue;
      expect(min[slot]!).toBeLessThanOrEqual(avg[slot]!);
      expect(avg[slot]!).toBeLessThanOrEqual(max[slot]!);
    }
  });
});

describe("结构检查与实际点数", () => {
  it("inspectBlock 与完整解码共用块头，但不解压载荷", async () => {
    const corrupt = bytesOf(fixtures.cases[0].data);
    corrupt[corrupt.length - 1] ^= 0xff;
    const inspected = inspectBlock(corrupt);
    expect(inspected).toMatchObject({
      codec: CODEC_VERSION,
      interval: 1,
      slotCount: 60,
      aggregateCount: 1,
      seriesCount: 35,
    });
    expect(inspected.payloadOffset).toBeLessThan(corrupt.length);
    expect(inspected.descriptors).toHaveLength(inspected.seriesCount);
    await expect(decodeBlock(corrupt)).rejects.toThrow(MetricBlockError);
  });

  it("inspectBlock 拒绝越界维度、截断描述符和非法序列", () => {
    const valid = integerBlock([
      { id: SERIES_CPU_USAGE, encoding: ENCODING_RAW, values: [[0n]] },
    ]);
    const badDims = valid.slice();
    badDims[HEADER_SIZE] = 1;
    badDims[HEADER_SIZE + 1] = 255;
    expect(() => inspectBlock(badDims)).toThrow(MetricBlockError);
    const payloadOffset = inspectBlock(valid).payloadOffset;
    expect(() => inspectBlock(valid.subarray(0, payloadOffset - 1)))
      .toThrow(MetricBlockError);
    const badSeries = valid.slice();
    new DataView(badSeries.buffer).setUint16(HEADER_SIZE + 3, 0xffff, true);
    expect(() => inspectBlock(badSeries)).toThrow(MetricBlockError);
  });

  it("点数取聚合位 0 的 presence 并集，零值存在且其他聚合位不计入", async () => {
    const cpu = [0n, null, null, null, null, null, null, null, null, null];
    const memory = [null, null, null, null, null, null, null, null, null, 5n];
    const minimum = [null, 1n, null, null, null, null, null, null, null, null];
    const maximum = [null, null, 9n, null, null, null, null, null, null, null];
    const block = await decodeBlock(integerBlock([
      { id: SERIES_CPU_USAGE, encoding: ENCODING_RAW, values: [cpu, minimum, maximum] },
      { id: SERIES_MEMORY_USED, encoding: ENCODING_RAW, values: [memory, minimum, maximum] },
    ]));
    expect(block.actualPointCount).toBe(2);
    expect(block.series.get(SERIES_CPU_USAGE)?.[0][0]).toBe(0);
  });

  it("忽略 presence 最后一个字节中超出 slot_count 的填充位", async () => {
    const raw = integerBlock([
      { id: SERIES_CPU_USAGE, encoding: ENCODING_RAW, values: [[null, null, null]] },
    ]);
    raw[inspectBlock(raw).payloadOffset] = 0xf8;
    expect((await decodeBlock(raw)).actualPointCount).toBe(0);
  });

  it("没有 presence 时仅有序列的块才计为稠密", async () => {
    const dense = integerBlock([
      { id: SERIES_CPU_USAGE, encoding: ENCODING_RAW, values: [[0n, 0n, 0n]] },
    ], { presence: false });
    expect((await decodeBlock(dense)).actualPointCount).toBe(3);
    const empty = integerBlock([], { slotCount: 3, presence: false });
    expect((await decodeBlock(empty)).actualPointCount).toBe(0);
  });
});

describe("整数快路径与 64 位回退", () => {
  for (const encoding of [ENCODING_DELTA, ENCODING_DELTA_OF_DELTA]) {
    it(`编码 ${encoding} 保留超过 2^53 的计数器逐点累积`, async () => {
      const start = 2n ** 53n;
      const values = Array.from({ length: 8 }, (_, index) => start + BigInt(index));
      const block = await decodeBlock(integerBlock([
        { id: SERIES_PROCESS_COUNT, encoding, values: [values] },
      ]));
      expect(block.series.get(SERIES_PROCESS_COUNT)?.[0]).toEqual(values.map(Number));
    });

    it(`编码 ${encoding} 在小 varint 累计越界时转用精确整数`, async () => {
      const step = 2n ** 48n - 1n;
      const values = Array.from({ length: 40 }, (_, index) =>
        encoding === ENCODING_DELTA
          ? step * BigInt(index)
          : step * BigInt(index) * BigInt(index + 1) / 2n
      );
      const block = await decodeBlock(integerBlock([
        { id: SERIES_PROCESS_COUNT, encoding, values: [values] },
      ]));
      expect(block.series.get(SERIES_PROCESS_COUNT)?.[0]).toEqual(values.map(Number));
    });
  }

  it("raw 编码支持 int64 正负边界与负数 zigzag", async () => {
    const values = [2n ** 63n - 1n, -(2n ** 63n), -1n, 0n, 1n];
    const block = await decodeBlock(integerBlock([
      { id: SERIES_PROCESS_COUNT, encoding: ENCODING_RAW, values: [values] },
    ]));
    expect(block.series.get(SERIES_PROCESS_COUNT)?.[0]).toEqual(values.map(Number));
  });

  it("拒绝第十字节超过 uint64 范围的 varint", async () => {
    const valid = integerBlock([
      { id: SERIES_PROCESS_COUNT, encoding: ENCODING_RAW, values: [[0n]] },
    ]);
    const offset = inspectBlock(valid).payloadOffset;
    const overflow = Uint8Array.from([
      ...valid.subarray(0, offset + 1), ...new Array<number>(9).fill(0x80), 2,
    ]);
    await expect(decodeBlock(overflow)).rejects.toThrow("varint 超出 64 位");
  });
});

describe("畸形输入", () => {
  const valid = bytesOf(fixtures.cases[0].data);

  const mutate = (fn: (b: Uint8Array) => Uint8Array) => fn(valid.slice());

  const cases: [string, () => Uint8Array][] = [
    ["空输入", () => new Uint8Array(0)],
    ["块头被截断", () => valid.slice(0, HEADER_SIZE - 1)],
    ["仅块头", () => valid.slice(0, HEADER_SIZE)],
    [
      "magic 错误",
      () =>
        mutate((b) => {
          b[0] = 0xff;
          return b;
        }),
    ],
    [
      "codec 版本错误",
      () =>
        mutate((b) => {
          b[1] = 9;
          return b;
        }),
    ],
    [
      "interval 非法",
      () =>
        mutate((b) => {
          b[2] = 7;
          return b;
        }),
    ],
    [
      "aggregate_count 非法",
      () =>
        mutate((b) => {
          b[4] = 2;
          return b;
        }),
    ],
    [
      "slot_count 为 0",
      () =>
        mutate((b) => {
          b[8] = 0;
          b[9] = 0;
          return b;
        }),
    ],
    [
      "series_count 超上限",
      () =>
        mutate((b) => {
          b[6] = 0xff;
          b[7] = 0xff;
          return b;
        }),
    ],
    ["尾部被截断", () => valid.slice(0, valid.length - 5)],
    [
      "gzip 载荷损坏",
      () =>
        mutate((b) => {
          b[b.length - 1] ^= 0xff;
          return b;
        }),
    ],
  ];

  for (const [name, build] of cases) {
    it(`${name} 应抛错而非返回半截数据`, async () => {
      await expect(decodeBlock(build())).rejects.toThrow();
    });
  }

  it("抛出的是 MetricBlockError 而非通用异常", async () => {
    await expect(decodeBlock(new Uint8Array(4))).rejects.toBeInstanceOf(
      MetricBlockError
    );
  });
});

describe("base64 辅助函数", () => {
  it("base64ByteLength 与实际解码长度一致", () => {
    // byte_size 列参与 GC 预算统计，算错会让保留策略失准
    for (const testCase of fixtures.cases) {
      expect(
        base64ByteLength(testCase.data),
        `${testCase.name} 的长度推算与实际解码不符`
      ).toBe(bytesOf(testCase.data).length);
    }
  });

  it("base64ByteLength 覆盖三种 padding 情形", () => {
    // 分别产生 0 / 2 / 1 个 '='
    expect(base64ByteLength(Buffer.from("abc").toString("base64"))).toBe(3);
    expect(base64ByteLength(Buffer.from("a").toString("base64"))).toBe(1);
    expect(base64ByteLength(Buffer.from("ab").toString("base64"))).toBe(2);
    expect(base64ByteLength("")).toBe(0);
  });

  it("base64ToBytes 对非法输入抛 MetricBlockError", () => {
    expect(() => base64ToBytes("!!!not base64!!!")).toThrow(MetricBlockError);
  });
});
