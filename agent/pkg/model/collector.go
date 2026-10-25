package model

import "time"

// DynamicMetrics 每次采集都会变化的动态指标，由 SystemInfo 与 Sample 匿名内嵌共用。
// 内嵌时不带 json tag，字段在两者的 JSON 中平铺，字段名由此处的 tag 唯一决定
// （服务端 zod 依赖这些字段名，修改前先看 backend/src/modules/agents/http/schemas.ts）。
type DynamicMetrics struct {
	CPU            CPUInfo               `json:"cpu"`
	Memory         MemoryInfo            `json:"memory"`
	Load           LoadInfo              `json:"load"`
	Disks          []DiskInfo            `json:"disks,omitempty"`
	Network        []NetworkInfo         `json:"network,omitempty"`
	Swap           *SwapInfo             `json:"swap,omitempty"`           // Swap 信息（取不到时省略）
	ProcessCount   int                   `json:"process_count"`            // 进程数；0 是有效观测值
	TCPConnections int                   `json:"tcp_connections"`          // TCP 连接数
	UDPConnections int                   `json:"udp_connections"`          // UDP 连接数
	Ping           map[string]PingResult `json:"ping,omitempty"`           // 四线路 TCP 拨测结果（ct/cu/cm/bd）
	IPv4Reachable  *bool                 `json:"ipv4_reachable,omitempty"` // IPv4 出网可达性
	IPv6Reachable  *bool                 `json:"ipv6_reachable,omitempty"` // IPv6 出网可达性
}

// SystemInfo 包含系统的各种信息
type SystemInfo struct {
	Token                  string    `json:"token"`
	AgentVersion           string    `json:"-"`
	Timestamp              time.Time `json:"timestamp"`
	Hostname               string    `json:"hostname"`
	Platform               string    `json:"platform"`
	OS                     string    `json:"os"`
	Version                string    `json:"version"`      // 操作系统版本
	IPAddresses            []string  `json:"ip_addresses"` // IP地址列表
	Keepalive              int       `json:"keepalive"`
	CollectIntervalSeconds int       `json:"collect_interval_seconds"`
	ReportIntervalSeconds  int       `json:"report_interval_seconds"`
	DynamicMetrics
	BootTime int64 `json:"boot_time,omitempty"` // 主机启动时间（Unix 秒）
}

// SwapInfo 包含 Swap 相关信息
type SwapInfo struct {
	Total     uint64  `json:"total"`
	Used      uint64  `json:"used"`
	UsageRate float64 `json:"usage_rate"`
}

// PingResult 单条线路 TCP 拨测结果（用 TCP 连接耗时代替 ICMP，避免 raw socket 权限）
type PingResult struct {
	Target    string  `json:"target"`
	LatencyMs float64 `json:"latency_ms"` // 丢包时为 -1
	Loss      bool    `json:"loss"`       // 超时/失败视为丢包
}

// AgentReportSample 是 v4 数据面的一条采样。时间使用带时区的 RFC3339，
// 其余动态指标与采集模型共用同一组字段定义。
type AgentReportSample struct {
	CollectedAt string `json:"collected_at"`
	DynamicMetrics
}

// AgentReportProtocolVersion 是当前数据面协议版本。
// v6 保留 v5 指标块编码，在线路上直接传二进制，并附带分钟与流量摘要。
const AgentReportProtocolVersion = 6

// AgentReportBlock 是一个列式压缩的指标块。编码规格见 pkg/metricblock，
// 服务端按 (agent_id, resolution, bucket_start) 幂等 upsert。
type AgentReportBlock struct {
	// Resolution 为 1（1 秒块，桶跨 1 分钟）或 60（1 分钟聚合块，桶跨 1 小时）
	Resolution  int   `json:"resolution"`
	BucketStart int64 `json:"bucket_start"`
	// PointCount 是【实际存在】的槽数，服务端用它做单调守卫，
	// 与块头里恒为 60 的 slot_count 不是一回事。
	PointCount int    `json:"point_count"`
	Codec      int    `json:"codec"`
	Data       string `json:"data,omitempty"` // 仅本地持久化使用 base64
	ByteLength int    `json:"byte_length,omitempty"`
	RawData    []byte `json:"-"`
}

// AgentReport 是数据面持久化的稳定信封。report_id 在本地 Spool
// 首次组批时生成，网络重试和进程重启后保持不变。
type AgentReport struct {
	ProtocolVersion       int                 `json:"protocol_version"`
	AgentVersion          string              `json:"agent_version,omitempty"`
	ReportID              string              `json:"report_id"`
	Hostname              string              `json:"hostname,omitempty"`
	IPAddresses           []string            `json:"ip_addresses,omitempty"`
	OS                    string              `json:"os,omitempty"`
	Version               string              `json:"version,omitempty"`
	BootTime              int64               `json:"boot_time,omitempty"`
	KeepaliveSeconds      int                 `json:"keepalive_seconds,omitempty"`
	ReportIntervalSeconds int                 `json:"report_interval_seconds,omitempty"`
	Blocks                []*AgentReportBlock `json:"blocks"`
	// Latest 是本批次最后一条原始样本，服务端据此更新 agent_current_metrics
	// 以及 CPU 型号、设备名等不进块的静态元数据。
	Latest          *AgentReportSample          `json:"latest,omitempty"`
	MinuteSummaries []*AgentReportMinuteSummary `json:"minute_summaries,omitempty"`
	Summary         *AgentReportSummary         `json:"summary,omitempty"`
}

// AgentReportMinuteSummary 让服务端直接保存新增分钟，无需解压原始秒级块。
type AgentReportMinuteSummary struct {
	BucketStart int64              `json:"bucket_start"`
	PointCount  int                `json:"point_count"`
	Avg         *AgentReportSample `json:"avg"`
}

// AgentReportSummary 保留整个批次的峰值与逐网卡流量，不能以最新样本替代。
type AgentReportSummary struct {
	FirstTS         int64                        `json:"first_ts"`
	LastTS          int64                        `json:"last_ts"`
	CPUUsagePeak    *float64                     `json:"cpu_usage_peak"`
	MemoryUsagePeak *float64                     `json:"memory_usage_peak"`
	DiskUsagePeak   *float64                     `json:"disk_usage_peak"`
	Traffic         []*AgentReportTrafficSegment `json:"traffic"`
}

// AgentReportTrafficSegment 按 UTC 日拆段，使后端可按自定义月重置日结算。
// FirstTS/LastTS 是该日样本边界；每块网卡还保留自己的实际出现时间。
type AgentReportTrafficSegment struct {
	UTCDay     string                         `json:"utc_day"`
	FirstTS    int64                          `json:"first_ts"`
	LastTS     int64                          `json:"last_ts"`
	Interfaces []*AgentReportTrafficInterface `json:"interfaces"`
	LastSpeed  *AgentReportTrafficSpeed       `json:"last_speed"`
}

type AgentReportTrafficInterface struct {
	Name    string `json:"name"`
	FirstTS int64  `json:"first_ts"`
	LastTS  int64  `json:"last_ts"`
	FirstRx uint64 `json:"first_rx"`
	FirstTx uint64 `json:"first_tx"`
	LastRx  uint64 `json:"last_rx"`
	LastTx  uint64 `json:"last_tx"`
	DeltaRx uint64 `json:"delta_rx"`
	DeltaTx uint64 `json:"delta_tx"`
}

type AgentReportTrafficSpeed struct {
	Rx *float64 `json:"rx"`
	Tx *float64 `json:"tx"`
}

// SampleCount 汇总本次上报覆盖的采样点数，仅用于日志。
func (r *AgentReport) SampleCount() int {
	total := 0
	for _, block := range r.Blocks {
		if block.Resolution == 1 {
			total += block.PointCount
		}
	}
	return total
}

// LiveMetricSample 是实时协议里的单个采样点。样本只承载指标，不携带 Agent
// Credential 和静态身份；身份在 WebSocket 握手阶段通过 Authorization Header
// 完成绑定。
type LiveMetricSample struct {
	CollectedAt    string                `json:"collected_at"`
	CPU            CPUInfo               `json:"cpu"`
	Memory         MemoryInfo            `json:"memory"`
	Load           LoadInfo              `json:"load"`
	Disks          []DiskInfo            `json:"disks,omitempty"`
	Network        []NetworkInfo         `json:"network,omitempty"`
	Swap           *SwapInfo             `json:"swap,omitempty"`
	ProcessCount   int                   `json:"process_count,omitempty"`
	TCPConnections int                   `json:"tcp_connections,omitempty"`
	UDPConnections int                   `json:"udp_connections,omitempty"`
	Ping           map[string]PingResult `json:"ping,omitempty"`
	IPv4Reachable  *bool                 `json:"ipv4_reachable,omitempty"`
	IPv6Reachable  *bool                 `json:"ipv6_reachable,omitempty"`
	NetworkRxSpeed *float64              `json:"network_rx_speed"`
	NetworkTxSpeed *float64              `json:"network_tx_speed"`
}

// LiveMetricBatch 是 Agent 到 Worker 独立上行 WebSocket 的实时协议（v2）。
//
// 一批承载 live-interval 窗口内的全部采样点，而不是每个采样点发一帧：
// Durable Object 的每条入站 WebSocket 消息都单独计一次 Worker 请求，
// 秒级一帧的话单台探针就是 86400 次/天，免费额度一台就用光。
// 攒批只增加实时视图的延迟，不降低采样精度——批内仍是逐秒的样本。
type LiveMetricBatch struct {
	Type            string              `json:"type"`
	ProtocolVersion int                 `json:"protocol_version"`
	Sequence        uint64              `json:"sequence"`
	Samples         []*LiveMetricSample `json:"samples"`
}

// NewAgentReportSample 从一次完整采集中生成不含凭据的 v4 样本。
func NewAgentReportSample(info *SystemInfo) *AgentReportSample {
	return &AgentReportSample{
		CollectedAt:    info.Timestamp.UTC().Format(time.RFC3339Nano),
		DynamicMetrics: info.DynamicMetrics,
	}
}

// CPUInfo 包含CPU相关信息
type CPUInfo struct {
	Usage     float64 `json:"usage"`
	Cores     int     `json:"cores"`
	ModelName string  `json:"model_name"`
}

// MemoryInfo 包含内存相关信息
type MemoryInfo struct {
	Total     uint64  `json:"total"`
	Used      uint64  `json:"used"`
	Free      uint64  `json:"free"`
	UsageRate float64 `json:"usage_rate"`
}

// DiskInfo 包含磁盘相关信息
type DiskInfo struct {
	Device     string  `json:"device"`
	MountPoint string  `json:"mount_point"`
	Total      uint64  `json:"total"`
	Used       uint64  `json:"used"`
	Free       uint64  `json:"free"`
	UsageRate  float64 `json:"usage_rate"`
	FSType     string  `json:"fs_type"`
}

// NetworkInfo 包含网络相关信息
type NetworkInfo struct {
	Interface   string `json:"interface"`
	BytesSent   uint64 `json:"bytes_sent"`
	BytesRecv   uint64 `json:"bytes_recv"`
	PacketsSent uint64 `json:"packets_sent"`
	PacketsRecv uint64 `json:"packets_recv"`
}

// LoadInfo 包含系统负载信息
type LoadInfo struct {
	Load1  float64 `json:"load1"`
	Load5  float64 `json:"load5"`
	Load15 float64 `json:"load15"`
}
