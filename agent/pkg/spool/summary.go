package spool

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xugou/agent/pkg/model"
)

const maxSafeJSONInteger uint64 = 1<<53 - 1

// summarizeReport 只遍历新增原始样本，内部差分不依赖 latest 的接口拓扑，
// 因而中途消失、重新出现的网卡也保留自己的基准与计数器归零语义。
func summarizeReport(buckets []*sampleBucket) (*model.AgentReportSummary, error) {
	summary := &model.AgentReportSummary{Traffic: make([]*model.AgentReportTrafficSegment, 0, 2)}
	var segment *model.AgentReportTrafficSegment
	var interfaces map[string]*model.AgentReportTrafficInterface
	var topology map[string]model.NetworkInfo
	var previous = make(map[string]model.NetworkInfo)
	var lastNetworkTS int64
	var haveNetworkTS bool
	var samples int
	for _, bucket := range buckets {
		for _, sample := range bucket.samples {
			at, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
			if err != nil {
				return nil, fmt.Errorf("解析摘要采样时间失败: %w", err)
			}
			ts := at.UnixMilli()
			if samples == 0 {
				summary.FirstTS = ts
			}
			summary.LastTS = ts
			samples++
			setPeak(&summary.CPUUsagePeak, math.Round(sample.CPU.Usage*100)/100)
			if sample.Memory.Total > 0 {
				setPeak(&summary.MemoryUsagePeak, float64(sample.Memory.Used)/float64(sample.Memory.Total)*100)
			}
			for _, disk := range sample.Disks {
				if disk.Total > 0 {
					setPeak(&summary.DiskUsagePeak, float64(disk.Used)/float64(disk.Total)*100)
				}
			}

			current := make(map[string]model.NetworkInfo, len(sample.Network))
			for _, iface := range sample.Network {
				if iface.Interface == "" || isLoopback(iface.Interface) {
					continue
				}
				if iface.BytesRecv > maxSafeJSONInteger || iface.BytesSent > maxSafeJSONInteger {
					return nil, fmt.Errorf("网卡 %q 的流量计数器超出 JSON 安全整数范围", iface.Interface)
				}
				current[iface.Interface] = iface
			}
			day := at.UTC().Format("2006-01-02")
			// 拓扑改变也切段，使后端的64接口基准裁剪等价于逐样本执行；
			// 稳定拓扑仍只需一个小摘要，接口消失不会丢掉先前的基准。
			if segment == nil || segment.UTCDay != day || !sameTopology(topology, current) {
				segment = &model.AgentReportTrafficSegment{UTCDay: day, FirstTS: ts, Interfaces: make([]*model.AgentReportTrafficInterface, 0)}
				interfaces = make(map[string]*model.AgentReportTrafficInterface)
				topology = current
				summary.Traffic = append(summary.Traffic, segment)
			}
			segment.LastTS = ts
			segment.LastSpeed = &model.AgentReportTrafficSpeed{}
			if haveNetworkTS && ts <= lastNetworkTS {
				continue
			}
			if len(current) == 0 {
				continue
			}
			var deltaRx, deltaTx uint64
			matched, reset, unknownBaseline := false, false, false
			// 按原始切片顺序处理，使 manifest 与二进制摘要顺序稳定。
			seen := make(map[string]bool, len(current))
			for _, raw := range sample.Network {
				iface, exists := current[raw.Interface]
				if !exists || seen[iface.Interface] {
					continue
				}
				seen[iface.Interface] = true
				name := iface.Interface
				if baseline, exists := previous[name]; exists {
					matched = true
					rx, rxReset := counterDelta(baseline.BytesRecv, iface.BytesRecv)
					tx, txReset := counterDelta(baseline.BytesSent, iface.BytesSent)
					deltaRx += rx
					deltaTx += tx
					reset = reset || rxReset || txReset
				} else {
					unknownBaseline = true
				}
				previous[name] = iface
				entry := interfaces[name]
				if entry == nil {
					entry = &model.AgentReportTrafficInterface{Name: name, FirstTS: ts, FirstRx: iface.BytesRecv, FirstTx: iface.BytesSent}
					interfaces[name] = entry
					segment.Interfaces = append(segment.Interfaces, entry)
				} else {
					rx, _ := counterDelta(entry.LastRx, iface.BytesRecv)
					tx, _ := counterDelta(entry.LastTx, iface.BytesSent)
					if rx > maxSafeJSONInteger-entry.DeltaRx || tx > maxSafeJSONInteger-entry.DeltaTx {
						return nil, fmt.Errorf("网卡 %q 的流量增量超出 JSON 安全整数范围", name)
					}
					entry.DeltaRx += rx
					entry.DeltaTx += tx
				}
				entry.LastTS, entry.LastRx, entry.LastTx = ts, iface.BytesRecv, iface.BytesSent
			}
			if !haveNetworkTS || unknownBaseline {
				// 单点无内部速率，服务端可用持久化的前一批基准补齐。
				segment.LastSpeed = nil
			} else if matched && !reset && ts > lastNetworkTS {
				rx := float64(deltaRx) / float64(ts-lastNetworkTS) * 1000
				tx := float64(deltaTx) / float64(ts-lastNetworkTS) * 1000
				segment.LastSpeed = &model.AgentReportTrafficSpeed{Rx: &rx, Tx: &tx}
			}
			lastNetworkTS, haveNetworkTS = ts, true
			if len(previous) > 64 {
				for name := range previous {
					if _, exists := current[name]; !exists {
						delete(previous, name)
					}
				}
			}
		}
	}
	return summary, nil
}

func setPeak(peak **float64, value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	if *peak == nil || value > **peak {
		copy := value
		*peak = &copy
	}
}

func sameTopology(previous, current map[string]model.NetworkInfo) bool {
	if len(previous) != len(current) {
		return false
	}
	for name := range current {
		if _, exists := previous[name]; !exists {
			return false
		}
	}
	return true
}

func counterDelta(previous, current uint64) (uint64, bool) {
	if current < previous {
		return current, true
	}
	return current - previous, false
}

func isLoopback(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if strings.Contains(lower, "loopback") || lower == "lo" {
		return true
	}
	if strings.HasPrefix(lower, "lo") && len(lower) > 2 {
		_, err := strconv.ParseUint(lower[2:], 10, 64)
		return err == nil
	}
	return false
}
