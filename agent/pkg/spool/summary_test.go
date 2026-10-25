package spool

import (
	"math"
	"testing"
	"time"

	"github.com/xugou/agent/pkg/model"
)

func summarySample(at time.Time, cpu float64, networks ...model.NetworkInfo) *model.AgentReportSample {
	return &model.AgentReportSample{
		CollectedAt: at.Format(time.RFC3339Nano),
		DynamicMetrics: model.DynamicMetrics{
			CPU: model.CPUInfo{Usage: cpu}, Memory: model.MemoryInfo{Total: 100, Used: uint64(cpu)},
			Disks: []model.DiskInfo{{MountPoint: "/", Total: 100, Used: uint64(cpu)}}, Network: networks,
		},
	}
}

func netCounter(name string, rx, tx uint64) model.NetworkInfo {
	return model.NetworkInfo{Interface: name, BytesRecv: rx, BytesSent: tx}
}

func TestSummaryPreservesPeaksAndInterfaceTransitions(t *testing.T) {
	at := time.Date(2026, 10, 1, 1, 2, 3, 0, time.UTC)
	samples := []*model.AgentReportSample{
		summarySample(at, 88.885, netCounter("eth0", 100, 200), netCounter("tun0", 1000, 2000), netCounter("lo0", 999, 999)),
		summarySample(at.Add(time.Second), 10, netCounter("eth0", 150, 250)),
		summarySample(at.Add(2*time.Second), 20, netCounter("eth0", 10, 20), netCounter("tun0", 1050, 2050)),
		summarySample(at.Add(3*time.Second), 5, netCounter("eth0", 30, 50), netCounter("tun0", 1100, 2150)),
	}
	summary, err := summarizeReport([]*sampleBucket{{samples: samples}})
	if err != nil {
		t.Fatal(err)
	}
	if *summary.CPUUsagePeak != 88.89 || *summary.MemoryUsagePeak != 88 || *summary.DiskUsagePeak != 88 {
		t.Fatalf("峰值不能被最后一条样本覆盖: %+v", summary)
	}
	if len(summary.Traffic) != 3 || len(summary.Traffic[0].Interfaces) != 2 || len(summary.Traffic[1].Interfaces) != 1 {
		t.Fatalf("应排除回环且按拓扑改变切段: %+v", summary.Traffic)
	}
	segment := summary.Traffic[2]
	eth, tun := segment.Interfaces[0], segment.Interfaces[1]
	if eth.FirstRx != 10 || eth.DeltaRx != 20 || eth.DeltaTx != 30 || tun.FirstRx != 1050 || tun.DeltaRx != 50 || tun.DeltaTx != 100 {
		t.Fatalf("归零后的首值与段内增量错误: eth=%+v tun=%+v", eth, tun)
	}
	if *segment.LastSpeed.Rx != 70 || *segment.LastSpeed.Tx != 130 {
		t.Fatalf("最后网速应只使用最后两个有效样本: %+v", segment.LastSpeed)
	}
	// 最新样本网络无效时必须返回空速率，不能沿用上一条的70/130。
	samples = append(samples, summarySample(at.Add(4*time.Second), 1))
	summary, err = summarizeReport([]*sampleBucket{{samples: samples}})
	if err != nil {
		t.Fatal(err)
	}
	last := summary.Traffic[len(summary.Traffic)-1]
	if len(last.Interfaces) != 0 || last.LastSpeed == nil || last.LastSpeed.Rx != nil || last.LastSpeed.Tx != nil {
		t.Fatal("最新采样缺网卡时网速必须为null")
	}
}

func TestTrafficSummarySplitsAtUTCDay(t *testing.T) {
	at := time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC)
	samples := []*model.AgentReportSample{
		summarySample(at, 10, netCounter("eth0", 100, 200)),
		summarySample(at.Add(time.Second), 20, netCounter("eth0", 150, 250)),
		summarySample(at.Add(2*time.Second), 30, netCounter("eth0", 180, 290)),
	}
	summary, err := summarizeReport([]*sampleBucket{{samples: samples}})
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Traffic) != 2 {
		t.Fatalf("应按UTC跨日拆为两段: %+v", summary.Traffic)
	}
	first, second := summary.Traffic[0], summary.Traffic[1]
	if first.UTCDay != "2026-09-30" || second.UTCDay != "2026-10-01" {
		t.Fatalf("UTC日期错误: %s %s", first.UTCDay, second.UTCDay)
	}
	if first.Interfaces[0].DeltaRx != 0 || second.Interfaces[0].FirstRx != 150 || second.Interfaces[0].DeltaRx != 30 {
		t.Fatal("跨日增量应由服务端按首点桥接，段内只累计当天后续增量")
	}
	if first.LastSpeed != nil || *second.LastSpeed.Rx != 30 {
		t.Fatal("首次无基准网速为null，第二天最后网速应为30")
	}
}

func TestTrafficSummaryRejectsUnsafeIntegers(t *testing.T) {
	sample := summarySample(time.Unix(1000, 0), math.NaN(), netCounter("eth0", maxSafeJSONInteger+1, 0))
	if _, err := summarizeReport([]*sampleBucket{{samples: []*model.AgentReportSample{sample}}}); err == nil {
		t.Fatal("超出JSON精确整数范围的计数器不能被静默截断")
	}
}
