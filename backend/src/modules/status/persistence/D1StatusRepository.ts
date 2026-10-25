import type { Bindings } from "../../../models/db";
import { writeStructuredLog } from "../../../platform/observability/StructuredLogger";
import { QueueJobPublisher } from "../../../platform/queues/QueuePublisher";
import type { StatusRepositoryPort } from "../application/StatusUseCases";
import type { StatusPageConfigCommand } from "../domain/models";
import { downsample } from "../../agents/metricblock/query";
import type { PublicMetricHour } from "../../agents/metricblock/public-projection";
import { D1PublicMetricHistory, expandPublicMinute } from "./D1PublicMetricHistory";

import {
  projectPublicDiskMetrics,
  projectPublicNetworkMetrics,
  toPublicAgent,
  type PublicAgentSource,
} from "../domain/public-contract";

const DEFAULT_CONFIG = {
  title: "系统状态",
  description: "实时监控系统运行状态",
  logoUrl: "",
  customCss: "",
  theme: "mono",
};
const MAX_PUBLIC_METRIC_POINTS = 288;
const MAX_PUBLIC_COMPONENTS_PER_TYPE = 100;
const MAX_STATUS_CONFIG_CANDIDATES = 500;
const PUBLIC_DAILY_STATS_DAYS = 90;

type ConfigRow = {
  id: number;
  title: string;
  description: string | null;
  logo_url: string | null;
  custom_css: string | null;
  theme: string | null;
};

type IdNameRow = { id: number; name: string; selected: number };
type MetricRow = Record<string, unknown> & { agent_id: number };

function uniquePositiveIds(values: number[]) {
  return [...new Set(values.filter((value) => Number.isInteger(value) && value > 0))];
}

type InvalidMetricHandler = (field: "disk_metrics" | "network_metrics") => void;

function publicMetricArrays(
  diskValue: unknown,
  networkValue: unknown,
  onInvalid: InvalidMetricHandler
) {
  const diskMetrics = projectPublicDiskMetrics(diskValue);
  const networkMetrics = projectPublicNetworkMetrics(networkValue);
  if (diskMetrics === null) onInvalid("disk_metrics");
  if (networkMetrics === null) onInvalid("network_metrics");
  return {
    disk_metrics: diskMetrics ?? [],
    network_metrics: networkMetrics ?? [],
  };
}

function toLatestMetric(row: MetricRow | undefined, onInvalid: InvalidMetricHandler) {
  if (!row) return null;
  return {
    id: row.agent_id,
    agent_id: row.agent_id,
    timestamp: row.collected_at ?? row.reported_at,
    cpu_usage: row.cpu_usage,
    memory_usage_rate: row.memory_usage_rate,
    ...publicMetricArrays(row.disk_metrics, row.network_metrics, onInvalid),
    swap_total: row.swap_total,
    swap_used: row.swap_used,
    process_count: row.process_count,
    tcp_connections: row.tcp_connections,
    udp_connections: row.udp_connections,
    ipv4_reachable: row.ipv4_reachable,
    ipv6_reachable: row.ipv6_reachable,
    network_rx_speed: row.network_rx_speed,
    network_tx_speed: row.network_tx_speed,
    month_rx: row.month_rx,
    month_tx: row.month_tx,
  };
}

export class D1StatusRepository implements StatusRepositoryPort {
  private readonly publisher: QueueJobPublisher;

  constructor(private readonly env: Bindings) {
    this.publisher = new QueueJobPublisher(env.XUGOU_JOBS);
  }

  private projectionWarning(
    agentId: number,
    source: string,
    field: "disk_metrics" | "network_metrics"
  ) {
    writeStructuredLog(this.env, {
      service: "status",
      operation: "public_metric_projection",
      result: "rejected",
      entityType: "agent",
      entityId: agentId,
      errorCode: "PUBLIC_METRIC_LEGACY_JSON_INVALID",
      fields: { source, field },
    });
  }

  private async ensureConfig() {
    const nowMs = Date.now();
    await this.env.DB.prepare(
      `INSERT INTO status_pages
       (id, singleton_key, title, description, logo_url, custom_css, theme,
        created_at_ms, updated_at_ms)
       VALUES (1, 1, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(singleton_key) DO NOTHING`
    )
      .bind(
        DEFAULT_CONFIG.title,
        DEFAULT_CONFIG.description,
        DEFAULT_CONFIG.logoUrl,
        DEFAULT_CONFIG.customCss,
        DEFAULT_CONFIG.theme,
        nowMs,
        nowMs
      )
      .run();
  }

  async getConfig() {
    await this.ensureConfig();
    const monitorSelectionQuery = `SELECT m.id, m.name,
              CASE WHEN component.component_id IS NULL THEN 0 ELSE 1 END AS selected
       FROM monitor_definitions m
       LEFT JOIN status_components component
         ON component.component_id = m.id AND component.component_type = 'monitor'
        AND component.page_id = (SELECT id FROM status_pages WHERE singleton_key = 1)
       WHERE m.deleted_at_ms IS NULL
       ORDER BY selected DESC, m.id ASC LIMIT ?`;
    const agentSelectionQuery = `SELECT a.id, a.name,
              CASE WHEN component.component_id IS NULL THEN 0 ELSE 1 END AS selected
       FROM agent_nodes a
       LEFT JOIN status_components component
         ON component.component_id = a.id AND component.component_type = 'agent'
        AND component.page_id = (SELECT id FROM status_pages WHERE singleton_key = 1)
       WHERE a.deleted_at_ms IS NULL
       ORDER BY selected DESC, a.id ASC LIMIT ?`;
    const [config, monitors, agents] = await Promise.all([
      this.env.DB.prepare(
        `SELECT id, title, description, logo_url, custom_css, theme
         FROM status_pages WHERE singleton_key = 1 LIMIT 1`
      ).first<ConfigRow>(),
      this.env.DB.prepare(monitorSelectionQuery)
        .bind(MAX_STATUS_CONFIG_CANDIDATES + 1)
        .all<IdNameRow>(),
      this.env.DB.prepare(agentSelectionQuery)
        .bind(MAX_STATUS_CONFIG_CANDIDATES + 1)
        .all<IdNameRow>(),
    ]);
    if (!config) throw new Error("Status page singleton config was not created");
    return {
      title: config.title,
      description: config.description ?? "",
      logoUrl: config.logo_url ?? "",
      customCss: config.custom_css ?? "",
      theme: config.theme ?? "mono",
      monitors: monitors.results
        .slice(0, MAX_STATUS_CONFIG_CANDIDATES)
        .map((row) => ({ ...row, selected: row.selected === 1 })),
      agents: agents.results
        .slice(0, MAX_STATUS_CONFIG_CANDIDATES)
        .map((row) => ({ ...row, selected: row.selected === 1 })),
      monitors_has_more: monitors.results.length > MAX_STATUS_CONFIG_CANDIDATES,
      agents_has_more: agents.results.length > MAX_STATUS_CONFIG_CANDIDATES,
    };
  }

  async saveConfig(input: StatusPageConfigCommand) {
    await this.ensureConfig();
    const monitorIds = uniquePositiveIds(input.monitors);
    const agentIds = uniquePositiveIds(input.agents);
    const [monitorCount, agentCount] = await Promise.all([
      this.countIds("monitor_definitions", monitorIds, "deleted_at_ms IS NULL"),
      this.countIds("agent_nodes", agentIds, "deleted_at_ms IS NULL")
    ]);
    if (monitorCount !== monitorIds.length || agentCount !== agentIds.length) {
      const error = new Error("状态页配置包含不存在的资源");
      error.name = "StatusPageConfigValidationError";
      throw error;
    }

    const nowMs = Date.now();
    const now = new Date(nowMs).toISOString();
    const eventId = `status.rebuild:${crypto.randomUUID()}`;
    const payload = JSON.stringify({ reason: "config.updated", requested_at: now });
    await this.env.DB.batch([
      this.env.DB.prepare(
        `UPDATE status_pages SET title = ?, description = ?, logo_url = ?,
         custom_css = ?, theme = ?, updated_at_ms = ? WHERE singleton_key = 1`
      ).bind(
        input.title,
        input.description,
        input.logoUrl,
        input.customCss,
        input.theme,
        nowMs
      ),
      this.env.DB.prepare(
        `DELETE FROM status_components
         WHERE page_id = (SELECT id FROM status_pages WHERE singleton_key = 1)`
      ),
      this.env.DB.prepare(
        `INSERT INTO status_components
         (page_id, component_type, component_id, sort_order,
          created_at_ms, updated_at_ms)
         SELECT (SELECT id FROM status_pages WHERE singleton_key = 1),
                'monitor', CAST(value AS INTEGER), CAST(key AS INTEGER), ?, ?
         FROM json_each(?)`
      ).bind(nowMs, nowMs, JSON.stringify(monitorIds)),
      this.env.DB.prepare(
        `INSERT INTO status_components
         (page_id, component_type, component_id, sort_order,
          created_at_ms, updated_at_ms)
         SELECT (SELECT id FROM status_pages WHERE singleton_key = 1),
                'agent', CAST(value AS INTEGER), CAST(key AS INTEGER), ?, ?
         FROM json_each(?)`
      ).bind(nowMs, nowMs, JSON.stringify(agentIds)),
      this.env.DB.prepare(
        `INSERT INTO domain_outbox
         (event_id, event_type, aggregate_type, aggregate_id, payload_json, status,
          attempts, available_at, created_at, updated_at)
         VALUES (?, 'status.rebuild.requested', 'status_page', '1', ?, 'pending', 0, ?, ?, ?)`
      ).bind(eventId, payload, now, now, now),
    ]);

    try {
      await this.publisher.publishOutbox(eventId);
      await this.env.DB.prepare(
        `UPDATE domain_outbox SET status = 'published', attempts = attempts + 1,
         published_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
      )
        .bind(now, now, eventId)
        .run();
    } catch (error) {
      writeStructuredLog(this.env, {
        service: "queue",
        operation: "publish_status_config_outbox",
        result: "deferred",
        eventId,
        entityType: "status_page",
        entityId: 1,
        errorCode: "STATUS_CONFIG_PUBLISH_DEFERRED",
        error,
      });
    }
    return { success: true, message: "配置已保存" };
  }

  private async countIds(
    table: "monitors" | "agents" | "monitor_definitions" | "agent_nodes",
    ids: number[],
    extra = "1 = 1"
  ) {
    if (ids.length === 0) return 0;
    const row = await this.env.DB.prepare(
      `SELECT COUNT(*) AS count FROM ${table}
       WHERE id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) AND ${extra}`
    )
      .bind(JSON.stringify(ids))
      .first<{ count: number }>();
    return Number(row?.count ?? 0);
  }

  async getActivePublication() {
    const row = await this.env.DB.prepare(
      `SELECT p.payload_json, p.etag, p.generated_at
       FROM status_publication_state s
       JOIN status_publications p ON p.id = s.active_publication_id
       WHERE s.singleton_key = 1 LIMIT 1`
    ).first<{ payload_json: string; etag: string; generated_at: string }>();
    return row
      ? { payloadJson: row.payload_json, etag: row.etag, generatedAt: row.generated_at }
      : null;
  }

  async getActiveMetricPublication(agentId: number) {
    const history = new D1PublicMetricHistory(this.env);
    const manifest = await history.manifest(agentId);
    if (!manifest) return null;
    // Compatibility for older clients only. The current frontend fetches and
    // caches individual hours and expands their compact points in a Web Worker.
    const metrics = [];
    for (const item of manifest.hours) {
      const hour = await history.hour(agentId, item.bucket_start);
      if (!hour) continue;
      const { chunk } = JSON.parse(hour.payloadJson) as { chunk: PublicMetricHour };
      for (const [slot, point] of Object.entries(chunk.p)) {
        const timestamp = (item.bucket_start + Number(slot) * 60) * 1000;
        if (timestamp < manifest.from * 1000 || timestamp > manifest.to * 1000) continue;
        metrics.push(expandPublicMinute(agentId, timestamp, point));
      }
    }
    metrics.sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp));
    return {
      agentId,
      payloadJson: JSON.stringify({ success: true, agent: downsample(metrics, MAX_PUBLIC_METRIC_POINTS) }),
      etag: `"public-metrics-${agentId}-${Math.floor(manifest.from / 60)}-${manifest.hours.map((hour) => hour.revision).join("-")}"`,
      generatedAt: new Date(manifest.to * 1000).toISOString(),
    };
  }

  getPublicMetricManifest(agentId: number) {
    return new D1PublicMetricHistory(this.env).manifest(agentId);
  }

  getPublicMetricHour(agentId: number, bucketStart: number) {
    return new D1PublicMetricHistory(this.env).hour(agentId, bucketStart);
  }

  getPublicMetricHours(agentId: number, buckets: number[]) {
    return new D1PublicMetricHistory(this.env).hours(agentId, buckets);
  }

  async buildPublicData() {
    await this.ensureConfig();
    const config = await this.env.DB.prepare(
      `SELECT id, title, description, logo_url, custom_css, theme
       FROM status_pages WHERE singleton_key = 1 LIMIT 1`
    ).first<ConfigRow>();
    if (!config) return { ...DEFAULT_CONFIG, monitors: [], agents: [] };

    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const dailySince = new Date(
      Date.now() - PUBLIC_DAILY_STATS_DAYS * 24 * 60 * 60 * 1000
    )
      .toISOString()
      .slice(0, 10);
    const selectedMonitorSql = `SELECT component_id FROM status_components
         WHERE page_id = ? AND component_type = 'monitor'
         ORDER BY sort_order ASC, component_id ASC
         LIMIT ${MAX_PUBLIC_COMPONENTS_PER_TYPE}`;
    const selectedAgentSql = `SELECT component_id FROM status_components
         WHERE page_id = ? AND component_type = 'agent'
         ORDER BY sort_order ASC, component_id ASC
         LIMIT ${MAX_PUBLIC_COMPONENTS_PER_TYPE}`;
    const dailyStatsQuery = `SELECT monitor_id, substr(bucket_start, 1, 10) AS date,
                total_checks, up_checks, down_checks,
                response_time_avg AS avg_response_time,
                response_time_min AS min_response_time,
                response_time_max AS max_response_time,
                CASE WHEN total_checks > 0
                     THEN (CAST(up_checks AS REAL) / total_checks) * 100 ELSE 0 END
                  AS availability,
                created_at
         FROM monitor_check_rollups
         WHERE bucket_size_seconds = 86400
           AND monitor_id IN (${selectedMonitorSql})
           AND bucket_start >= ?
         ORDER BY bucket_start ASC`;
    const publicMonitorQuery = `SELECT d.id, d.name, r.status,
                r.response_time_ms AS response_time,
                CASE WHEN r.last_checked_at_ms IS NULL THEN NULL
                     ELSE strftime('%Y-%m-%dT%H:%M:%fZ',
                                   r.last_checked_at_ms / 1000.0, 'unixepoch') END
                  AS last_checked,
                strftime('%Y-%m-%dT%H:%M:%fZ',
                         d.created_at_ms / 1000.0, 'unixepoch') AS created_at,
                strftime('%Y-%m-%dT%H:%M:%fZ',
                         d.updated_at_ms / 1000.0, 'unixepoch') AS updated_at
         FROM monitor_definitions d
         JOIN monitor_runtime r ON r.monitor_id = d.id
         WHERE d.deleted_at_ms IS NULL AND d.id IN (${selectedMonitorSql})
         ORDER BY d.id ASC`;
    const publicAgentQuery = `SELECT n.id, n.name, r.status, r.hostname, r.os,
                r.agent_version AS version, r.region,
                r.geo_city AS city, r.geo_region_name AS region_name,
                r.geo_latitude AS map_latitude,
                r.geo_longitude AS map_longitude,
                strftime('%Y-%m-%dT%H:%M:%fZ',
                         n.created_at_ms / 1000.0, 'unixepoch') AS created_at,
                strftime('%Y-%m-%dT%H:%M:%fZ',
                         n.updated_at_ms / 1000.0, 'unixepoch') AS updated_at,
                n.traffic_limit_gb, n.traffic_reset_day, n.traffic_calc_type
         FROM agent_nodes n JOIN agent_runtime r ON r.agent_id = n.id
         WHERE n.id IN (${selectedAgentSql}) AND n.deleted_at_ms IS NULL
           AND n.is_hidden <> 1
         ORDER BY n.id ASC`;
    const [monitorResult, dailyResult, rollupResult, historyResult, agentResult, metricResult] =
      await Promise.all([
        this.env.DB.prepare(publicMonitorQuery)
          .bind(config.id)
          .all<Record<string, unknown>>(),
        this.env.DB.prepare(dailyStatsQuery)
          .bind(config.id, dailySince)
          .all<Record<string, unknown>>(),
        this.env.DB.prepare(
          `SELECT id, monitor_id, bucket_start, last_status, response_time_avg
           FROM monitor_check_rollups
           WHERE monitor_id IN (${selectedMonitorSql})
             AND bucket_size_seconds = 300 AND bucket_start >= ?
           ORDER BY bucket_start ASC`
        ).bind(config.id, since).all<Record<string, unknown>>(),
        this.env.DB.prepare(
            `SELECT job_id AS id, monitor_id, status, checked_at AS timestamp,
                      response_time_ms AS response_time, status_code, error
               FROM monitor_check_samples
               WHERE monitor_id IN (${selectedMonitorSql}) AND checked_at >= ?
                 AND NOT EXISTS (
                   SELECT 1 FROM monitor_check_rollups rollup
                   WHERE rollup.monitor_id = monitor_check_samples.monitor_id
                     AND rollup.bucket_size_seconds = 300 AND rollup.bucket_start >= ?
                 )
               ORDER BY checked_at ASC LIMIT 10000`
        ).bind(config.id, since, since).all<Record<string, unknown>>(),
        this.env.DB.prepare(publicAgentQuery)
          .bind(config.id)
          .all<PublicAgentSource>(),
        this.env.DB.prepare(
          `SELECT agent_id,
                  CASE WHEN collected_at_ms IS NULL THEN NULL
                       ELSE strftime('%Y-%m-%dT%H:%M:%fZ',
                                     collected_at_ms / 1000.0, 'unixepoch') END
                    AS collected_at,
                  strftime('%Y-%m-%dT%H:%M:%fZ',
                           reported_at_ms / 1000.0, 'unixepoch') AS reported_at,
                  cpu_usage, memory_usage_rate, disk_usage_rate, swap_total,
                  swap_used, process_count, tcp_connections, udp_connections,
                  ipv4_reachable, ipv6_reachable, network_rx_speed,
                  network_tx_speed, month_rx, month_tx,
                  json_extract(metrics_json, '$.disk_metrics') AS disk_metrics,
                  json_extract(metrics_json, '$.network_metrics') AS network_metrics
           FROM agent_current_metrics
           WHERE agent_id IN (${selectedAgentSql})`
        ).bind(config.id).all<MetricRow>(),
      ]);

    const daily = new Map<number, Record<string, unknown>[]>();
    for (const row of dailyResult.results) {
      const id = Number(row.monitor_id);
      const rows = daily.get(id) ?? [];
      rows.push(row);
      daily.set(id, rows);
    }
    const rollups = new Map<number, Record<string, unknown>[]>();
    for (const row of rollupResult.results) {
      const id = Number(row.monitor_id);
      const mapped = {
        id: row.id,
        monitor_id: id,
        status: row.last_status ?? "pending",
        timestamp: row.bucket_start,
        response_time: row.response_time_avg,
        status_code: null,
        error: null,
      };
      const rows = rollups.get(id) ?? [];
      rows.push(mapped);
      rollups.set(id, rows);
    }
    const rawHistory = new Map<number, Record<string, unknown>[]>();
    for (const row of historyResult.results) {
      const id = Number(row.monitor_id);
      const rows = rawHistory.get(id) ?? [];
      rows.push(row);
      rawHistory.set(id, rows);
    }
    const latestByAgent = new Map(
      metricResult.results.map((row) => [
        row.agent_id,
        toLatestMetric(row, (field) =>
          this.projectionWarning(row.agent_id, "latest", field)
        ),
      ])
    );

    return {
      title: config.title,
      description: config.description ?? "",
      logoUrl: config.logo_url ?? "",
      customCss: config.custom_css ?? "",
      theme: config.theme ?? "mono",
      monitors: monitorResult.results.map((row) => ({
        id: row.id,
        name: row.name,
        status: row.status ?? "pending",
        response_time: row.response_time ?? 0,
        last_checked: row.last_checked ?? null,
        created_at: row.created_at,
        updated_at: row.updated_at,
        dailyStats: daily.get(Number(row.id)) ?? [],
        history:
          (rollups.get(Number(row.id))?.length ?? 0) > 0
            ? rollups.get(Number(row.id))
            : rawHistory.get(Number(row.id)) ?? [],
      })),
      agents: agentResult.results.map((row) => ({
        ...toPublicAgent(row),
        metrics: latestByAgent.get(Number(row.id)) ?? null,
      })),
    };
  }

}
