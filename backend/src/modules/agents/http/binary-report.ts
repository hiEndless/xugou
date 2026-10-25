import type { AgentReportCommand } from "../domain/models";
import { agentV6ReportMetadataSchema } from "./schemas";

export const BINARY_REPORT_CONTENT_TYPE = "application/vnd.xugou.report+binary";
export const MAX_BINARY_REPORT_BYTES = 2 * 1024 * 1024;
const MAX_METADATA_BYTES = 512 * 1024;

/** XG06 | metadata length (u32 little endian) | JSON metadata | compressed blocks. */
export function parseBinaryReport(body: Uint8Array): AgentReportCommand {
  if (body.byteLength < 8 || body.byteLength > MAX_BINARY_REPORT_BYTES ||
      body[0] !== 88 || body[1] !== 71 || body[2] !== 48 || body[3] !== 54) {
    throw new Error("Invalid binary report envelope");
  }
  const metadataLength = new DataView(body.buffer, body.byteOffset, body.byteLength).getUint32(4, true);
  if (metadataLength === 0 || metadataLength > MAX_METADATA_BYTES || 8 + metadataLength > body.byteLength) {
    throw new Error("Invalid binary report metadata length");
  }
  const text = new TextDecoder("utf-8", { fatal: true }).decode(body.subarray(8, 8 + metadataLength));
  const metadata = agentV6ReportMetadataSchema.parse(JSON.parse(text));
  let offset = 8 + metadataLength;
  const blocks = metadata.blocks.map(({ byte_length, ...block }) => {
    if (offset + byte_length > body.byteLength) throw new Error("Truncated binary report block");
    const data = body.subarray(offset, offset + byte_length);
    offset += byte_length;
    return { ...block, data };
  });
  if (offset !== body.byteLength) throw new Error("Trailing binary report bytes");
  return { ...metadata, blocks };
}
