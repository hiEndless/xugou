import type { Metrics } from "../../../models/agent";
import type { Bindings } from "../../../models/db";
import {
  authenticateAgentToken,
  consumeAgentEnrollmentToken,
  createCredentialForAgent,
  generateAgentCredentialToken,
  linkAgentEnrollmentToken,
  releaseAgentEnrollmentToken,
} from "./D1AgentCredentialStore";
import type { AgentMutation } from "../domain/models";
import { downsample, queryAgentSamples } from "../metricblock/query";
import { blockSampleToMetrics, DASHBOARD_CHART_POINTS } from "../metricblock/metrics-view";

import { createAgentUseCases } from "../composition";

export { normalizeAgentMetricsHours } from "../../../utils/agentMetricsHours";

interface LatestMetricRow {
  agent_id: number;
  metrics_json: string;
  collected_at: string | null;
  reported_at: string | null;
}

function parseLatestMetric(row: LatestMetricRow): Metrics | null {
  try {
    const parsed = JSON.parse(row.metrics_json) as Metrics;
    return {
      ...parsed,
      agent_id: row.agent_id,
      timestamp: parsed.timestamp ?? row.collected_at ?? row.reported_at ?? "",
    };
  } catch {
    return null;
  }
}

export function toAgentMutation(input: Record<string, unknown>): AgentMutation {
  const mutation: AgentMutation = {};
  if (typeof input.name === "string") mutation.name = input.name;
  if (input.hostname === null || typeof input.hostname === "string") {
    mutation.hostname = input.hostname;
  }
  if (Array.isArray(input.ip_addresses)) {
    mutation.ip_addresses = input.ip_addresses.filter(
      (item): item is string => typeof item === "string"
    );
  }
  if (input.os === null || typeof input.os === "string") mutation.os = input.os;
  if (input.version === null || typeof input.version === "string") {
    mutation.version = input.version;
  }
  if (input.status === null || typeof input.status === "string") {
    mutation.status = input.status;
  }
  if (typeof input.collect_interval === "number") {
    mutation.collect_interval_seconds = input.collect_interval;
  } else if (typeof input.collect_interval_seconds === "number") {
    mutation.collect_interval_seconds = input.collect_interval_seconds;
  }
  if (typeof input.report_interval === "number") {
    mutation.report_interval_seconds = input.report_interval;
  } else if (typeof input.report_interval_seconds === "number") {
    mutation.report_interval_seconds = input.report_interval_seconds;
  }
  if (input.price === null || typeof input.price === "number") mutation.price = input.price;
  if (input.currency === null || typeof input.currency === "string") {
    mutation.currency = input.currency;
  }
  if (input.billing_cycle === null || typeof input.billing_cycle === "string") {
    mutation.billing_cycle = input.billing_cycle;
  }
  if (input.expire_date === null || typeof input.expire_date === "string") {
    mutation.expire_date = input.expire_date;
  }
  if (typeof input.auto_renewal === "number" || typeof input.auto_renewal === "boolean") {
    mutation.auto_renewal = input.auto_renewal === true || input.auto_renewal === 1;
  }
  if (typeof input.is_hidden === "number" || typeof input.is_hidden === "boolean") {
    mutation.is_hidden = input.is_hidden === true || input.is_hidden === 1;
  }
  if (input.traffic_limit_gb === null || typeof input.traffic_limit_gb === "number") {
    mutation.traffic_limit_gb = input.traffic_limit_gb;
  }
  if (typeof input.traffic_reset_day === "number") {
    mutation.traffic_reset_day = input.traffic_reset_day;
  }
  if (typeof input.traffic_calc_type === "string") {
    mutation.traffic_calc_type = input.traffic_calc_type;
  }
  if (typeof input.auto_update === "number" || typeof input.auto_update === "boolean") {
    mutation.auto_update = input.auto_update === true || input.auto_update === 1;
  }
  if (input.group_name === null || typeof input.group_name === "string") {
    mutation.group_name = input.group_name;
  }
  if (Array.isArray(input.tags)) {
    mutation.tags = input.tags.filter((tag): tag is string => typeof tag === "string");
  } else if (input.tags === null || typeof input.tags === "string") {
    mutation.tags = String(input.tags ?? "")
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);
  }
  return mutation;
}

export async function updateLegacyAgentOrder(env: Bindings, ids: number[]) {
  const uniqueIds = [...new Set(ids)];
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS count FROM agent_nodes
     WHERE deleted_at_ms IS NULL
       AND id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))`
  )
    .bind(JSON.stringify(uniqueIds))
    .first<{ count: number }>();
  if (Number(row?.count ?? 0) !== uniqueIds.length) return false;
  for (let offset = 0; offset < uniqueIds.length; offset += 50) {
    const nowMs = Date.now();
    await env.DB.batch(
      uniqueIds.slice(offset, offset + 50).map((id, index) =>
        env.DB.prepare(
          `UPDATE agent_nodes SET sort_order = ?, updated_at_ms = ? WHERE id = ?`
        ).bind(offset + index, nowMs, id)
      )
    );
  }
  return true;
}

async function insertAgent(
  env: Bindings,
  input: {
    name: string;
    token: string;
    status: string;
    hostname?: string | null;
    ip_addresses?: string[] | null;
    os?: string | null;
    version?: string | null;
  }
) {
  const now = new Date().toISOString();
  const nowMs = Date.parse(now);
  // agents 表如今只当 id 锚点用（agent_metric_blocks.agent_id 外键指向它），
  // 业务字段一律落在 agent_nodes / agent_runtime，这里写占位值即可。
  const row = await env.DB.prepare(
    `INSERT INTO agents
     (name, token, status, hostname, ip_addresses, os, version,
      last_seen_at, last_state_changed_at, created_at, updated_at)
     VALUES (?, ?, 'retired', NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
     RETURNING id`
  )
    .bind(
      `contract-anchor:${crypto.randomUUID()}`,
      `contract-anchor:${crypto.randomUUID()}`,
      now,
      now
    )
    .first<{ id: number }>();
  if (!row) throw new Error("创建客户端失败");
  try {
    await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO agent_nodes
         (id, name, collect_interval_ms, report_interval_ms, group_name,
          tags_json, price, currency, billing_cycle, expire_date,
          auto_renewal, is_hidden, traffic_limit_gb, traffic_reset_day,
          traffic_calc_type, auto_update, sort_order, created_at_ms,
          updated_at_ms, deleted_at_ms)
         VALUES (?, ?, 1000, 60000, NULL, '[]', NULL, 'USD', NULL, NULL,
                 0, 0, NULL, 1, 'sum', 0, 0, ?, ?, NULL)`
      ).bind(row.id, input.name, nowMs, nowMs),
      env.DB.prepare(
        `INSERT INTO agent_runtime
         (agent_id, status, hostname, ip_addresses_json, os, agent_version,
          keepalive_seconds, boot_time, last_seen_at_ms,
          last_state_changed_at_ms, next_offline_at_ms, region,
          geo_latitude, geo_longitude, geo_city, geo_region_name, version,
          created_at_ms, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL, NULL, NULL,
                 NULL, NULL, 0, ?, ?)`
      ).bind(
        row.id,
        input.status,
        input.hostname ?? null,
        JSON.stringify(input.ip_addresses ?? []),
        input.os ?? null,
        input.version ?? null,
        nowMs,
        nowMs,
        nowMs,
        nowMs
      ),
    ]);
  } catch (error) {
    await env.DB.prepare(`DELETE FROM agents WHERE id = ?`).bind(row.id).run();
    throw error;
  }
  return row.id;
}

export async function registerAgent(
  env: Bindings,
  input: {
    token: string;
    name: string;
    hostname?: string | null;
    ip_addresses?: string[] | null;
    os?: string | null;
    version?: string | null;
  }
) {
  const existing = await authenticateAgentToken(env, input.token);
  if (existing) return { id: existing.id, created: false };
  const enrollment = await consumeAgentEnrollmentToken(env, input.token);
  if (!enrollment) return null;
  try {
    const id = await insertAgent(env, { ...input, status: "active" });
    await createCredentialForAgent(env, id, input.token);
    await linkAgentEnrollmentToken(env, enrollment.id, id);
    return { id, created: true };
  } catch (error) {
    await releaseAgentEnrollmentToken(env, enrollment.id);
    throw error;
  }
}

export async function importLegacyAgents(
  env: Bindings,
  items: Array<Record<string, unknown> & { name: string; token?: string }>
) {
  const candidateNames = [...new Set(items.map((item) => item.name))];
  const existingRows = await env.DB.prepare(
    `SELECT name FROM agent_nodes
     WHERE deleted_at_ms IS NULL
       AND name IN (SELECT value FROM json_each(?))`
  )
    .bind(JSON.stringify(candidateNames))
    .all<{ name: string }>();
  const existingNames = new Set(existingRows.results.map((row) => row.name));
  let created = 0;
  let skipped = 0;
  const issuedCredentials: Array<{ name: string; token: string }> = [];
  for (const item of items) {
    if (existingNames.has(item.name)) {
      skipped += 1;
      continue;
    }
    let token = item.token ?? "";
    let generated = false;
    if (!token || (await authenticateAgentToken(env, token))) {
      token = generateAgentCredentialToken();
      generated = true;
    }
    const id = await insertAgent(env, {
      name: item.name,
      token,
      status: "inactive",
      hostname: typeof item.hostname === "string" ? item.hostname : null,
      ip_addresses: Array.isArray(item.ip_addresses)
        ? item.ip_addresses.filter((value): value is string => typeof value === "string")
        : null,
      os: typeof item.os === "string" ? item.os : null,
      version: typeof item.version === "string" ? item.version : null,
    });
    await createCredentialForAgent(env, id, token);
    const mutation = toAgentMutation(item);
    if (Object.keys(mutation).length > 0) {
      await createAgentUseCases(env).update(id, mutation);
    }
    if (typeof item.sort_order === "number") {
      await env.DB.prepare(
        `UPDATE agent_nodes SET sort_order = ?, updated_at_ms = ? WHERE id = ?`
      )
        .bind(item.sort_order, Date.now(), id)
        .run();
    }
    if (generated) issuedCredentials.push({ name: item.name, token });
    existingNames.add(item.name);
    created += 1;
  }
  return { created, skipped, issuedCredentials };
}

export async function queryLegacyAgentMetrics(env: Bindings, agentId: number, hours: number) {
  const agent = await env.DB.prepare(
    `SELECT 1 AS ok FROM agent_nodes
     WHERE id = ? AND deleted_at_ms IS NULL LIMIT 1`
  )
    .bind(agentId)
    .first<{ ok: number }>();
  if (!agent) return null;

  const toSec = Math.floor(Date.now() / 1000);
  const fromSec = toSec - hours * 3600;
  const { samples } = await queryAgentSamples(env, agentId, fromSec, toSec);

  return downsample(
    samples.map((sample) => blockSampleToMetrics(agentId, sample)),
    DASHBOARD_CHART_POINTS
  );
}

export async function queryLatestLegacyAgentMetric(env: Bindings, agentId: number) {
  const exists = await env.DB.prepare(
    `SELECT id FROM agent_nodes WHERE id = ? AND deleted_at_ms IS NULL LIMIT 1`
  )
    .bind(agentId)
    .first<{ id: number }>();
  if (!exists) return null;
  const row = await env.DB.prepare(
    `SELECT agent_id, metrics_json,
            CASE WHEN collected_at_ms IS NULL THEN NULL
                 ELSE strftime('%Y-%m-%dT%H:%M:%fZ',
                               collected_at_ms / 1000.0, 'unixepoch') END
              AS collected_at,
            strftime('%Y-%m-%dT%H:%M:%fZ',
                     reported_at_ms / 1000.0, 'unixepoch') AS reported_at
     FROM agent_current_metrics WHERE agent_id = ? LIMIT 1`
  )
    .bind(agentId)
    .first<LatestMetricRow>();
  return row ? parseLatestMetric(row) : undefined;
}

export async function queryLatestAgentMetricsForIds(
  env: Bindings,
  agentIds: number[]
) {
  if (agentIds.length === 0) return new Map<number, Metrics>();
  const rows = await env.DB.prepare(
    `SELECT agent_id, metrics_json,
            CASE WHEN collected_at_ms IS NULL THEN NULL
                 ELSE strftime('%Y-%m-%dT%H:%M:%fZ',
                               collected_at_ms / 1000.0, 'unixepoch') END
              AS collected_at,
            strftime('%Y-%m-%dT%H:%M:%fZ',
                     reported_at_ms / 1000.0, 'unixepoch') AS reported_at
     FROM agent_current_metrics
     WHERE agent_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))`
  )
    .bind(JSON.stringify(agentIds))
    .all<LatestMetricRow>();
  const metrics = new Map<number, Metrics>();
  for (const row of rows.results) {
    const parsed = parseLatestMetric(row);
    if (parsed) metrics.set(row.agent_id, parsed);
  }
  return metrics;
}
