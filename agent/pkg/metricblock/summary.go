package metricblock

import (
	"math"
	"time"

	"github.com/xugou/agent/pkg/model"
)

// AverageSample 直接从已计算的分钟聚合值生成读端摘要，不再编码后解码。
// 网络计数器与布尔量遵循 Aggregate 的最后值语义，其余序列取平均。
func (p *MinutePoint) AverageSample(latest *model.AgentReportSample) *model.AgentReportSample {
	value := func(id uint16) float64 { return p.Values[id][AggAvg] }
	count := func(id uint16) int { return int(math.Round(value(id))) }
	bytes := func(id uint16) uint64 { return uint64(math.Round(math.Max(0, value(id)))) }
	rate := func(used, total uint64) float64 {
		if total == 0 {
			return 0
		}
		return float64(used) / float64(total) * 100
	}
	sample := &model.AgentReportSample{
		CollectedAt: time.Unix(p.MinuteStart, 0).UTC().Format(time.RFC3339),
		DynamicMetrics: model.DynamicMetrics{
			CPU:          model.CPUInfo{Usage: value(SeriesCPUUsage), Cores: latest.CPU.Cores, ModelName: latest.CPU.ModelName},
			Memory:       model.MemoryInfo{Total: p.MemoryTotal, Used: bytes(SeriesMemoryUsed), Free: bytes(SeriesMemoryFree)},
			Load:         model.LoadInfo{Load1: value(SeriesLoad1), Load5: value(SeriesLoad5), Load15: value(SeriesLoad15)},
			ProcessCount: count(SeriesProcessCount), TCPConnections: count(SeriesTCPConnections), UDPConnections: count(SeriesUDPConnections),
		},
	}
	sample.Memory.UsageRate = rate(sample.Memory.Used, sample.Memory.Total)
	if _, ok := p.Values[SeriesSwapUsed]; ok {
		used := bytes(SeriesSwapUsed)
		sample.Swap = &model.SwapInfo{Total: p.SwapTotal, Used: used, UsageRate: rate(used, p.SwapTotal)}
	}
	for i, disk := range p.Dims.Disks {
		if _, ok := p.Values[DiskSeriesID(i)]; !ok {
			continue
		}
		used := bytes(DiskSeriesID(i))
		free := uint64(0)
		if disk.Total > used {
			free = disk.Total - used
		}
		sample.Disks = append(sample.Disks, model.DiskInfo{MountPoint: disk.Name, Total: disk.Total, Used: used, Free: free, UsageRate: rate(used, disk.Total)})
	}
	for i, name := range p.Dims.Nets {
		if _, ok := p.Values[NetSeriesID(i, NetFieldBytesRecv)]; !ok {
			continue
		}
		sample.Network = append(sample.Network, model.NetworkInfo{
			Interface: name, BytesRecv: bytes(NetSeriesID(i, NetFieldBytesRecv)), BytesSent: bytes(NetSeriesID(i, NetFieldBytesSent)),
			PacketsRecv: bytes(NetSeriesID(i, NetFieldPacketsRecv)), PacketsSent: bytes(NetSeriesID(i, NetFieldPacketsSent)),
		})
	}
	if len(p.Dims.Pings) > 0 {
		sample.Ping = make(map[string]model.PingResult, len(p.Dims.Pings))
		for i, name := range p.Dims.Pings {
			if _, ok := p.Values[PingSeriesID(i, PingFieldLatencyMs)]; !ok {
				continue
			}
			sample.Ping[name] = model.PingResult{LatencyMs: value(PingSeriesID(i, PingFieldLatencyMs)), Loss: value(PingSeriesID(i, PingFieldLoss)) != 0}
		}
	}
	if _, ok := p.Values[SeriesIPv4Reachable]; ok {
		reachable := value(SeriesIPv4Reachable) != 0
		sample.IPv4Reachable = &reachable
	}
	if _, ok := p.Values[SeriesIPv6Reachable]; ok {
		reachable := value(SeriesIPv6Reachable) != 0
		sample.IPv6Reachable = &reachable
	}
	return sample
}
