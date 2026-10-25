package metricblock

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/xugou/agent/pkg/model"
)

func TestZeroCountsRemainExplicitInLatestAndMinuteSummary(t *testing.T) {
	minute := BucketStartFor(1786000000, 1)
	samples := syntheticMinute(t, minute)
	for _, sample := range samples {
		sample.ProcessCount, sample.TCPConnections, sample.UDPConnections = 0, 0, 0
	}
	latest := samples[len(samples)-1]
	average := Aggregate(minute, samples).AverageSample(latest)
	for _, sample := range []*model.AgentReportSample{latest, average} {
		encoded, err := json.Marshal(sample)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"process_count", "tcp_connections", "udp_connections"} {
			if string(fields[field]) != "0" {
				t.Fatalf("%s 必须显式传输0，实际为%s", field, fields[field])
			}
		}
	}
}

func TestMinuteAverageReusesRollupSemantics(t *testing.T) {
	minute := BucketStartFor(1786000000, 1)
	samples := syntheticMinute(t, minute)
	point := Aggregate(minute, samples)
	avg := point.AverageSample(samples[len(samples)-1])
	if avg.CollectedAt != time.Unix(minute, 0).UTC().Format(time.RFC3339) {
		t.Fatal("摘要时间必须对齐分钟，不使用末样本时间")
	}
	if math.Abs(avg.CPU.Usage-point.Values[SeriesCPUUsage][AggAvg]) > 1e-9 {
		t.Fatal("CPU应该取分钟均值")
	}
	if avg.Network[0].BytesRecv != samples[len(samples)-1].Network[0].BytesRecv {
		t.Fatal("网络计数器必须取最后值，不能平均")
	}
	if avg.Memory.UsageRate != float64(avg.Memory.Used)/float64(avg.Memory.Total)*100 {
		t.Fatal("内存百分比必须从平均Used推导")
	}
	// 可选序列缺席时不能制造false/0样本。
	for _, sample := range samples {
		sample.Swap, sample.IPv4Reachable, sample.IPv6Reachable = nil, nil, nil
		sample.Disks, sample.Network, sample.Ping = nil, nil, nil
	}
	point = Aggregate(minute, samples)
	avg = point.AverageSample(samples[len(samples)-1])
	if avg.Swap != nil || avg.IPv4Reachable != nil || avg.IPv6Reachable != nil || len(avg.Disks) != 0 || len(avg.Network) != 0 || len(avg.Ping) != 0 {
		t.Fatal("缺失指标应保持缺失")
	}
}

func BenchmarkMinuteAverageSummary(b *testing.B) {
	minute := BucketStartFor(1786000000, 1)
	samples := make([]*model.AgentReportSample, 60)
	for i := range samples {
		samples[i] = &model.AgentReportSample{CollectedAt: time.Unix(minute+int64(i), 0).UTC().Format(time.RFC3339), DynamicMetrics: model.DynamicMetrics{
			CPU: model.CPUInfo{Usage: float64(i)}, Memory: model.MemoryInfo{Total: 1 << 30, Used: uint64(i) << 20}, Network: []model.NetworkInfo{{Interface: "eth0", BytesRecv: uint64(i) * 1000}},
		}}
	}
	point := Aggregate(minute, samples)
	b.ReportAllocs()
	for b.Loop() {
		_ = point.AverageSample(samples[len(samples)-1])
	}
}
