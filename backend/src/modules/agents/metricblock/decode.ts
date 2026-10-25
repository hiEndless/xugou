/**
 * 指标块解码器（codec v1）。编码在 Agent 侧（Go），此处只解码。
 *
 * 规格见 docs/指标存储重构设计.md §3，常量见 ./registry.ts。
 * 本解码器直接消费网络输入，任何不合规格的字节流都必须抛错而非返回半截数据。
 */

import {
  CODEC_VERSION,
  FLAG_GZIP,
  FLAG_PRESENCE,
  HEADER_SIZE,
  MAGIC,
  MAX_DECOMPRESSED_BYTES,
  MAX_DIM_ENTRIES,
  MAX_POINT_COUNT,
  MAX_SERIES_COUNT,
  ENCODING_DELTA_OF_DELTA,
  ENCODING_RAW,
  specFor,
} from "./registry";

export class MetricBlockError extends Error {
  constructor(message: string) {
    super(`metricblock: ${message}`);
    this.name = "MetricBlockError";
  }
}

export interface DiskDim {
  name: string;
  total: number;
}

export interface BlockDims {
  disks: DiskDim[];
  nets: string[];
  pings: string[];
}

export interface DecodedBlock {
  interval: number;
  bucketStart: number;
  /** 时间槽总数，恒为 60。与上报体里"实际存在点数"的 point_count 不同。 */
  slotCount: number;
  /** 聚合位 0 中至少有一个序列存在的槽数，不需要还原样本对象即可校验 point_count。 */
  actualPointCount: number;
  aggregateCount: number;
  dims: BlockDims;
  memoryTotal: number;
  swapTotal: number;
  /** series_id -> [agg][slot]，null 表示该槽缺值。 */
  series: Map<number, (number | null)[][]>;
}

export interface BlockSeriesDescriptor {
  id: number;
  encoding: number;
  scale: number;
}

/** 可在归档写入时检查的自描述结构；不代表压缩载荷或数值序列已经通过校验。 */
export interface InspectedBlock {
  codec: number;
  interval: number;
  bucketStart: number;
  slotCount: number;
  aggregateCount: number;
  seriesCount: number;
  flags: number;
  memoryTotal: number;
  swapTotal: number;
  dims: BlockDims;
  descriptors: BlockSeriesDescriptor[];
  payloadOffset: number;
}

/** base64 解成字节。非法输入抛 MetricBlockError 而不是 atob 的原生异常。 */
export function base64ToBytes(value: string): Uint8Array {
  let binary: string;
  try {
    binary = atob(value);
  } catch {
    throw new MetricBlockError("block.data 不是合法 base64");
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

/**
 * 不做解码，直接由 base64 长度算出原始字节数。
 *
 * `byte_size` 列参与 GC 的预算统计，每块都真解一遍只为拿长度太浪费。
 * 每 4 个 base64 字符对应 3 字节，末尾每个 '=' 抵掉一个字节。
 */
export function base64ByteLength(value: string): number {
  if (value.length === 0) return 0;
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  return (value.length / 4) * 3 - padding;
}

/**
 * 解析 base64 编码的块。上报体里的块以 base64 传输，这里统一处理解码。
 */
export async function decodeBlockBase64(value: string): Promise<DecodedBlock> {
  return decodeBlock(base64ToBytes(value));
}

function readVarint(buf: Uint8Array, offset: number): [number | bigint, number] {
  // 七个字节最多承载 49 位，乘加始终精确。常见数值与小 delta 无需创建 BigInt。
  let result = 0;
  let factor = 1;
  for (let index = 0; index < 7; index++) {
    if (offset >= buf.length) throw new MetricBlockError("varint 被截断");
    const byte = buf[offset++];
    result += (byte & 0x7f) * factor;
    if ((byte & 0x80) === 0) return [result, offset];
    factor *= 128;
  }

  let largeResult = BigInt(result);
  for (let shift = 49n; shift <= 63n; shift += 7n) {
    if (offset >= buf.length) throw new MetricBlockError("varint 被截断");
    const byte = buf[offset++];
    if (shift === 63n && byte > 1) {
      throw new MetricBlockError("varint 超出 64 位");
    }
    largeResult |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return [largeResult, offset];
  }
  throw new MetricBlockError("varint 超出 64 位");
}

function unzigzagNumber(value: number): number {
  // 位运算会把 Number 截成 32 位，这里的 varint 可达 49 位。
  return value % 2 === 0 ? value / 2 : -(value + 1) / 2;
}

function countOccupiedSlots(bitmap: Uint8Array, slotCount: number): number {
  let count = 0;
  for (let index = 0; index < bitmap.length; index++) {
    const remaining = slotCount - index * 8;
    let bits = bitmap[index] & (remaining < 8 ? (1 << remaining) - 1 : 0xff);
    bits -= (bits >>> 1) & 0x55;
    bits = (bits & 0x33) + ((bits >>> 2) & 0x33);
    count += (bits + (bits >>> 4)) & 0x0f;
  }
  return count;
}

function bitmapLen(n: number): number {
  return (n + 7) >> 3;
}

function getBit(bitmap: Uint8Array, i: number): boolean {
  return (bitmap[i >> 3] & (1 << (i & 7))) !== 0;
}

async function gunzip(src: Uint8Array): Promise<Uint8Array> {
  let stream: ReadableStream<Uint8Array>;
  try {
    stream = new Blob([src as BlobPart])
      .stream()
      .pipeThrough(new DecompressionStream("gzip"));
  } catch (error) {
    throw new MetricBlockError(`gzip 解压初始化失败: ${String(error)}`);
  }

  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      // 边读边判上限，避免 zip bomb 在拼接前就打爆内存
      if (total > MAX_DECOMPRESSED_BYTES) {
        throw new MetricBlockError(
          `解压后超过 ${MAX_DECOMPRESSED_BYTES} 字节上限`
        );
      }
      chunks.push(value);
    }
  } catch (error) {
    reader.cancel().catch(() => {});
    if (error instanceof MetricBlockError) throw error;
    throw new MetricBlockError(`gzip 解压失败: ${String(error)}`);
  }

  const out = new Uint8Array(total);
  let pos = 0;
  for (const chunk of chunks) {
    out.set(chunk, pos);
    pos += chunk.byteLength;
  }
  return out;
}

function decodeDimHeader(
  raw: Uint8Array,
  start: number
): { dims: BlockDims; offset: number } {
  let offset = start;
  const decoder = new TextDecoder("utf-8", { fatal: false });

  const readName = (): string => {
    if (offset >= raw.length) throw new MetricBlockError("维度头被截断");
    const len = raw[offset++];
    if (offset + len > raw.length) throw new MetricBlockError("维度名被截断");
    const name = decoder.decode(raw.subarray(offset, offset + len));
    offset += len;
    return name;
  };

  const readCount = (what: string): number => {
    if (offset >= raw.length) throw new MetricBlockError(`${what}计数缺失`);
    const n = raw[offset++];
    if (n > MAX_DIM_ENTRIES) {
      throw new MetricBlockError(`${what}数 ${n} 超上限 ${MAX_DIM_ENTRIES}`);
    }
    return n;
  };

  const disks: DiskDim[] = [];
  const diskCount = readCount("磁盘");
  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  for (let i = 0; i < diskCount; i++) {
    const name = readName();
    if (offset + 8 > raw.length) {
      throw new MetricBlockError("磁盘 total 被截断");
    }
    const total = Number(view.getBigUint64(offset, true));
    offset += 8;
    disks.push({ name, total });
  }

  const nets: string[] = [];
  const netCount = readCount("网卡");
  for (let i = 0; i < netCount; i++) nets.push(readName());

  const pings: string[] = [];
  const pingCount = readCount("ping");
  for (let i = 0; i < pingCount; i++) pings.push(readName());

  return { dims: { disks, nets, pings }, offset };
}

/** 检查块头、维度与描述符边界，不解压或遍历数值载荷。完整数值校验使用 decodeBlock。 */
export function inspectBlock(raw: Uint8Array): InspectedBlock {
  if (raw.length < HEADER_SIZE) {
    throw new MetricBlockError(`长度 ${raw.length} 不足以容纳块头`);
  }
  if (raw[0] !== MAGIC) {
    throw new MetricBlockError(`magic 错误 0x${raw[0].toString(16)}`);
  }
  if (raw[1] !== CODEC_VERSION) {
    throw new MetricBlockError(`不支持的 codec 版本 ${raw[1]}`);
  }

  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  const interval = raw[2];
  if (interval !== 1 && interval !== 60) {
    throw new MetricBlockError(`非法 interval ${interval}`);
  }
  const flags = raw[3];
  const aggregateCount = raw[4];
  if (aggregateCount !== 1 && aggregateCount !== 3) {
    throw new MetricBlockError(`非法 aggregate_count ${aggregateCount}`);
  }
  const seriesCount = view.getUint16(6, true);
  if (seriesCount > MAX_SERIES_COUNT) {
    throw new MetricBlockError(`序列数 ${seriesCount} 超上限`);
  }
  const slotCount = view.getUint16(8, true);
  if (slotCount === 0 || slotCount > MAX_POINT_COUNT) {
    throw new MetricBlockError(`非法 slot_count ${slotCount}`);
  }
  const bucketStart = view.getUint32(12, true);
  const memoryTotal = Number(view.getBigUint64(16, true));
  const swapTotal = Number(view.getBigUint64(24, true));

  const { dims, offset: afterDims } = decodeDimHeader(raw, HEADER_SIZE);

  let offset = afterDims;
  if (offset + seriesCount * 4 > raw.length) {
    throw new MetricBlockError("描述符区被截断");
  }
  const descriptors: BlockSeriesDescriptor[] = [];
  let prevId = -1;
  for (let i = 0; i < seriesCount; i++) {
    const id = view.getUint16(offset, true);
    const encoding = raw[offset + 2];
    const scale = raw[offset + 3];
    offset += 4;
    if (specFor(id) === null) {
      throw new MetricBlockError(`非法 series_id ${id}`);
    }
    if (id <= prevId) {
      throw new MetricBlockError("描述符未按 series_id 升序排列");
    }
    if (encoding > ENCODING_RAW) {
      throw new MetricBlockError(`非法 encoding ${encoding}`);
    }
    prevId = id;
    descriptors.push({ id, encoding, scale });
  }

  return {
    codec: raw[1],
    interval,
    bucketStart,
    slotCount,
    aggregateCount,
    seriesCount,
    flags,
    memoryTotal,
    swapTotal,
    dims,
    descriptors,
    payloadOffset: offset,
  };
}

/** 解析一个指标块，包含压缩载荷与全部数值序列的校验。 */
export async function decodeBlock(raw: Uint8Array): Promise<DecodedBlock> {
  const {
    interval, bucketStart, slotCount, aggregateCount, seriesCount,
    flags, memoryTotal, swapTotal, dims, descriptors, payloadOffset,
  } = inspectBlock(raw);
  let payload = raw.subarray(payloadOffset);
  if ((flags & FLAG_GZIP) !== 0) {
    payload = await gunzip(payload);
  }

  const hasPresence = (flags & FLAG_PRESENCE) !== 0;
  const occupiedSlots = hasPresence ? new Uint8Array(bitmapLen(slotCount)) : null;
  const series = new Map<number, (number | null)[][]>();
  let pos = 0;

  for (const desc of descriptors) {
    const factor = 10 ** desc.scale;
    const aggs: (number | null)[][] = [];
    for (let agg = 0; agg < aggregateCount; agg++) {
      let present: Uint8Array | null = null;
      if (hasPresence) {
        const n = bitmapLen(slotCount);
        if (pos + n > payload.length) {
          throw new MetricBlockError("presence bitmap 被截断");
        }
        present = payload.subarray(pos, pos + n);
        pos += n;
        if (agg === 0 && occupiedSlots) {
          for (let index = 0; index < n; index++) {
            occupiedSlots[index] |= present[index];
          }
        }
      }

      const values: (number | null)[] = new Array(slotCount).fill(null);
      let prev: number | bigint = 0;
      let prevDelta: number | bigint = 0;
      let count = 0;
      for (let slot = 0; slot < slotCount; slot++) {
        if (present !== null && !getBit(present, slot)) continue;
        const [rawVal, next] = readVarint(payload, pos);
        pos = next;
        if (
          typeof rawVal === "number" && typeof prev === "number" &&
          typeof prevDelta === "number"
        ) {
          const signed = unzigzagNumber(rawVal);
          let cur: number;
          let delta: number = prevDelta;
          if (desc.encoding === ENCODING_RAW || count === 0) {
            cur = signed;
          } else if (desc.encoding === ENCODING_DELTA_OF_DELTA) {
            delta = count === 1 ? signed : delta + signed;
            cur = prev + delta;
          } else {
            cur = prev + signed;
          }
          if (Number.isSafeInteger(cur) && Number.isSafeInteger(delta)) {
            values[slot] = cur / factor;
            prev = cur;
            prevDelta = delta;
            count++;
            continue;
          }
        }

        // 初值或累计 delta 越过安全整数后，整条序列保持 BigInt 运算；仅输出时转 Number。
        const signed = typeof rawVal === "bigint"
          ? (rawVal >> 1n) ^ -(rawVal & 1n)
          : BigInt(unzigzagNumber(rawVal));
        const previous = typeof prev === "bigint" ? prev : BigInt(prev);
        prevDelta = typeof prevDelta === "bigint" ? prevDelta : BigInt(prevDelta);
        let cur: bigint;
        if (desc.encoding === ENCODING_RAW) {
          cur = signed;
        } else if (desc.encoding === ENCODING_DELTA_OF_DELTA) {
          if (count === 0) {
            cur = signed;
          } else if (count === 1) {
            cur = previous + signed;
            prevDelta = signed;
          } else {
            const delta: bigint = prevDelta + signed;
            cur = previous + delta;
            prevDelta = delta;
          }
        } else {
          cur = count === 0 ? signed : previous + signed;
        }
        values[slot] = Number(cur) / factor;
        prev = cur;
        count++;
      }
      aggs.push(values);
    }
    series.set(desc.id, aggs);
  }

  if (pos !== payload.length) {
    throw new MetricBlockError(
      `Payload 有 ${payload.length - pos} 字节残留，与描述符不符`
    );
  }

  return {
    interval,
    bucketStart,
    slotCount,
    actualPointCount: occupiedSlots
      ? countOccupiedSlots(occupiedSlots, slotCount)
      : seriesCount > 0 ? slotCount : 0,
    aggregateCount,
    dims,
    memoryTotal,
    swapTotal,
    series,
  };
}
