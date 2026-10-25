package spool

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xugou/agent/pkg/metricblock"
	"github.com/xugou/agent/pkg/model"
)

// anchor 固定在一个整分钟起点，方便按秒偏移构造样本。
const anchor int64 = 1786000020 // 落在某分钟的第 20 秒之前对齐后使用

func minuteStart(offsetMinutes int64) int64 {
	return metricblock.BucketStartFor(anchor, 1) + offsetMinutes*60
}

func newStore(t *testing.T, now time.Time) *Store {
	t.Helper()
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("打开 spool 失败: %v", err)
	}
	store.now = func() time.Time { return now }
	return store
}

func sampleAt(unix int64) *model.SystemInfo {
	ts := time.Unix(unix, 0).UTC()
	swap := &model.SwapInfo{Total: 1 << 30, Used: 1 << 28}
	return &model.SystemInfo{
		Hostname:     "test-host",
		OS:           "linux",
		Version:      "1.0",
		Keepalive:    60,
		Timestamp:    ts,
		AgentVersion: "v1.2.9",
		DynamicMetrics: model.DynamicMetrics{
			CPU:    model.CPUInfo{Usage: 12.5, Cores: 2, ModelName: "test-cpu"},
			Memory: model.MemoryInfo{Total: 1 << 31, Used: 1 << 29, Free: 1 << 30},
			Load:   model.LoadInfo{Load1: 0.5, Load5: 0.4, Load15: 0.3},
			Disks: []model.DiskInfo{
				{Device: "/dev/sda1", MountPoint: "/", Total: 1 << 40, Used: 1 << 38, FSType: "ext4"},
			},
			Network: []model.NetworkInfo{
				{Interface: "eth0", BytesSent: uint64(unix) * 1000, BytesRecv: uint64(unix) * 2000},
			},
			Swap:           swap,
			ProcessCount:   100,
			TCPConnections: 20,
			UDPConnections: 5,
		},
	}
}

func addRange(t *testing.T, store *Store, start int64, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if _, err := store.Add(sampleAt(start + int64(i))); err != nil {
			t.Fatalf("写入样本失败: %v", err)
		}
	}
}

func blocksByResolution(report *model.AgentReport, resolution int) []*model.AgentReportBlock {
	var out []*model.AgentReportBlock
	for _, b := range report.Blocks {
		if b.Resolution == resolution {
			out = append(out, b)
		}
	}
	return out
}

func TestNextBoundsTopologySegmentsWithoutSplittingMinutes(t *testing.T) {
	minute := metricblock.BucketStartFor(anchor, 60) + 600
	store := newStore(t, time.Unix(minute+180, 0))
	// A partial first minute plus two complete minutes can otherwise yield
	// 30+60+60=150 samples, each creating a different traffic topology segment.
	for i := 0; i < 150; i++ {
		info := sampleAt(minute + 30 + int64(i))
		if i%2 == 0 {
			info.Network = nil
		}
		if _, err := store.Add(info); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := store.Next(DefaultMaxSamples, DefaultMaxCompressedBytes)
	if err != nil || !ok {
		t.Fatalf("首批必须能推进: ok=%v err=%v", ok, err)
	}
	if first.SampleCount() != 90 || len(first.Summary.Traffic) != 90 {
		t.Fatalf("首批应保留完整30+60样本，实际样本%d、摘要段%d", first.SampleCount(), len(first.Summary.Traffic))
	}
	if err := store.Ack(first.ReportID); err != nil {
		t.Fatal(err)
	}
	second, ok, err := store.Next(DefaultMaxSamples, DefaultMaxCompressedBytes)
	if err != nil || !ok {
		t.Fatalf("末分钟必须留给下一批: ok=%v err=%v", ok, err)
	}
	if second.SampleCount() != 60 || len(second.Summary.Traffic) != 60 {
		t.Fatalf("末分钟不能丢失或截断，实际样本%d、摘要段%d", second.SampleCount(), len(second.Summary.Traffic))
	}
	if err := store.Ack(second.ReportID); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats()
	if err != nil || stats.Samples != 0 {
		t.Fatalf("150个样本确认后不应残留，stats=%+v err=%v", stats, err)
	}
}

func decodeReportBlock(t *testing.T, block *model.AgentReportBlock) *metricblock.DecodedBlock {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(block.Data)
	if err != nil {
		t.Fatalf("base64 解码失败: %v", err)
	}
	decoded, err := metricblock.Decode(raw)
	if err != nil {
		t.Fatalf("块解码失败: %v", err)
	}
	return decoded
}

// TestNextSkipsIncompleteBucket 锁住"只发已完整的分钟桶"这条规则：
// 当前分钟仍在写入，先发半个块会导致下一轮再发一次完整块，白白多写。
func TestNextSkipsIncompleteBucket(t *testing.T) {
	m0 := minuteStart(0)
	// 时钟停在 m0 这一分钟内 —— 该桶尚未完整
	store := newStore(t, time.Unix(m0+30, 0))
	addRange(t, store, m0, 30)

	report, ok, err := store.Next(DefaultMaxSamples, DefaultMaxCompressedBytes)
	if err != nil {
		t.Fatalf("Next 出错: %v", err)
	}
	if ok || report != nil {
		t.Fatalf("当前分钟未完整，不应组批，实际返回 %+v", report)
	}

	// 时钟推进到下一分钟，桶完整了
	store.now = func() time.Time { return time.Unix(m0+60, 0) }
	report, ok, err = store.Next(DefaultMaxSamples, DefaultMaxCompressedBytes)
	if err != nil {
		t.Fatalf("Next 出错: %v", err)
	}
	if !ok || report == nil {
		t.Fatal("桶完整后应能组批")
	}
	hot := blocksByResolution(report, 1)
	if len(hot) != 1 {
		t.Fatalf("期望 1 个 1 秒块，实际 %d", len(hot))
	}
	if hot[0].BucketStart != m0 {
		t.Fatalf("桶起点应为 %d，实际 %d", m0, hot[0].BucketStart)
	}
	if hot[0].PointCount != 30 {
		t.Fatalf("期望 30 个点，实际 %d", hot[0].PointCount)
	}
}

func TestNextGroupsMultipleBuckets(t *testing.T) {
	m0 := minuteStart(0)
	store := newStore(t, time.Unix(m0+180, 0))
	addRange(t, store, m0, 60)     // 第 0 分钟满
	addRange(t, store, m0+60, 60)  // 第 1 分钟满
	addRange(t, store, m0+120, 10) // 第 2 分钟部分（但已完整，因为时钟在第 3 分钟）

	report, ok, err := store.Next(DefaultMaxSamples, 1<<20)
	if err != nil || !ok {
		t.Fatalf("Next 失败: ok=%v err=%v", ok, err)
	}
	hot := blocksByResolution(report, 1)
	if len(hot) != 2 {
		t.Fatalf("maxSamples=100 应覆盖 2 个完整桶，实际 %d 个", len(hot))
	}
	if hot[0].BucketStart != m0 || hot[1].BucketStart != m0+60 {
		t.Fatalf("桶顺序错误: %d, %d", hot[0].BucketStart, hot[1].BucketStart)
	}
	// 关键：第二个桶必须完整发出 60 点，而不是被 maxSamples 截断成 40 点
	if hot[0].PointCount != 60 || hot[1].PointCount != 60 {
		t.Fatalf("桶应完整不被截断，实际点数: %d, %d", hot[0].PointCount, hot[1].PointCount)
	}
}

// TestBucketsAreAtomic 是上面那条规则的回归测试。
//
// 若 collectBuckets 按样本数硬性截断，第二个桶会只发 40 点，Ack 删掉这 40 条后，
// 剩余 20 条在下一轮组成同一桶的 20 点块 —— 服务端的单调守卫
// (excluded.point_count >= 现有值) 会拒绝它，这 20 秒数据永久丢失。
func TestBucketsAreAtomic(t *testing.T) {
	m0 := minuteStart(0)
	store := newStore(t, time.Unix(m0+180, 0))
	addRange(t, store, m0, 60)
	addRange(t, store, m0+60, 60)

	// 故意把上限设成 70：既不足以容纳两个完整桶，也会诱发半桶截断
	report, ok, err := store.Next(70, 1<<20)
	if err != nil || !ok {
		t.Fatalf("Next 失败: ok=%v err=%v", ok, err)
	}
	hot := blocksByResolution(report, 1)
	for _, block := range hot {
		if block.PointCount != 60 {
			t.Fatalf("桶 %d 被截断成 %d 点，会导致数据永久丢失",
				block.BucketStart, block.PointCount)
		}
	}

	// Ack 后剩余样本必须仍能组成完整的桶
	if err := store.Ack(report.ReportID); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}
	seen := map[int64]int{}
	for _, block := range hot {
		seen[block.BucketStart] = block.PointCount
	}
	for {
		next, ok, err := store.Next(70, 1<<20)
		if err != nil {
			t.Fatalf("后续 Next 失败: %v", err)
		}
		if !ok {
			break
		}
		for _, block := range blocksByResolution(next, 1) {
			if prev, dup := seen[block.BucketStart]; dup {
				t.Fatalf("桶 %d 被重复发送（先 %d 点，后 %d 点），说明发生了截断",
					block.BucketStart, prev, block.PointCount)
			}
			seen[block.BucketStart] = block.PointCount
		}
		if err := store.Ack(next.ReportID); err != nil {
			t.Fatalf("Ack 失败: %v", err)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("应恰好发出 2 个桶，实际 %d 个", len(seen))
	}
	for start, count := range seen {
		if count != 60 {
			t.Fatalf("桶 %d 只发了 %d 点", start, count)
		}
	}
}

// TestRollupUploadedOnlyWhenHourSeals 确保当前小时只传新增分钟摘要，小时结束只编码一次。
func TestRollupUploadedOnlyWhenHourSeals(t *testing.T) {
	hour := metricblock.BucketStartFor(anchor, 60)
	store := newStore(t, time.Unix(hour+60, 0))
	for _, minute := range []int64{hour, hour + 60, hour + 59*60} {
		store.now = func() time.Time { return time.Unix(minute+60, 0) }
		addRange(t, store, minute, 60)
		report, ok, err := store.Next(DefaultMaxSamples, 1<<20)
		if err != nil || !ok {
			t.Fatalf("Next 失败: ok=%v err=%v", ok, err)
		}
		if len(report.MinuteSummaries) != 1 || report.MinuteSummaries[0].BucketStart != minute {
			t.Fatalf("每轮只应含新增分钟摘要: %+v", report.MinuteSummaries)
		}
		rollups := blocksByResolution(report, 60)
		if minute == hour+59*60 {
			if len(rollups) != 1 || rollups[0].PointCount != 3 {
				t.Fatalf("小时结束应只封存一次，共3个稀疏分钟: %+v", rollups)
			}
			decoded := decodeReportBlock(t, rollups[0])
			if decoded.AggCount != 3 || decoded.Interval != 60 {
				t.Fatalf("小时块编码错误: %+v", decoded)
			}
		} else if len(rollups) != 0 {
			t.Fatalf("当前小时不应重复上传聚合块: %+v", rollups)
		}
		if err := store.Ack(report.ReportID); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.rollupPoints) != 0 {
		t.Fatalf("封存小时应立即从累积器移除，实际剩%d", len(store.rollupPoints))
	}
	addRange(t, store, hour+3600, 10)
	store.now = func() time.Time { return time.Unix(hour+3660, 0) }
	report, ok, err := store.Next(DefaultMaxSamples, 1<<20)
	if err != nil || !ok {
		t.Fatalf("新小时 Next 失败: %v", err)
	}
	if len(blocksByResolution(report, 60)) != 0 || len(store.rollupPoints) != 1 {
		t.Fatal("新小时只能保留新增分钟，不应再次上传已封存小时")
	}
}

// TestRollupWatermarkFollowsBacklog 验证离线积压不能按现在时刻提前封存历史小时。
func TestRollupWatermarkFollowsBacklog(t *testing.T) {
	hour := metricblock.BucketStartFor(anchor, 60)
	store := newStore(t, time.Unix(hour+7200, 0))
	addRange(t, store, hour, 60)
	addRange(t, store, hour+60, 60)
	report, ok, err := store.Next(60, 1<<20)
	if err != nil || !ok {
		t.Fatalf("Next 失败: %v", err)
	}
	if len(blocksByResolution(report, 60)) != 0 {
		t.Fatal("积压尚未处理完整小时，不能封存第一个分钟")
	}
	if err := store.Ack(report.ReportID); err != nil {
		t.Fatal(err)
	}
	// 重启后继续累积旧小时，不丢失已确认分钟。
	reopened, err := Open(Options{Dir: store.dir})
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = store.now
	if len(reopened.rollupPoints) != 1 {
		t.Fatalf("重启应恢复1个已确认分钟，实际%d", len(reopened.rollupPoints))
	}
	report, ok, err = reopened.Next(60, 1<<20)
	if err != nil || !ok {
		t.Fatalf("重启后 Next 失败: %v", err)
	}
	if len(blocksByResolution(report, 60)) != 0 || len(reopened.rollupPoints) != 2 {
		t.Fatal("第二个历史分钟仍只能累积，不能提前封存")
	}
}

// TestCorruptSampleDoesNotWedgeQueue 验证时间戳无法解析的样本文件仍会被
// 纳入 Ack 范围删除，否则一个坏文件会永久堵住队列。
func TestCorruptSampleDoesNotWedgeQueue(t *testing.T) {
	m0 := minuteStart(0)
	store := newStore(t, time.Unix(m0+60, 0))
	addRange(t, store, m0, 5)

	// 挑一个样本文件，把 collected_at 改成非法值
	files, err := store.sampleFiles()
	if err != nil {
		t.Fatalf("列出样本失败: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("样本文件为空")
	}
	victim := filepath.Join(store.dir, files[0])
	raw, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("读取样本失败: %v", err)
	}
	broken := replaceFirst(string(raw), `"collected_at":"`, `"collected_at":"NOT-A-TIME`)
	if err := os.WriteFile(victim, []byte(broken), 0o600); err != nil {
		t.Fatalf("写入损坏样本失败: %v", err)
	}

	report, ok, err := store.Next(DefaultMaxSamples, 1<<20)
	if err != nil {
		t.Fatalf("Next 不应因损坏样本而失败: %v", err)
	}
	if !ok {
		t.Fatal("其余样本仍应能组批")
	}
	if err := store.Ack(report.ReportID); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}

	remaining, err := store.sampleFiles()
	if err != nil {
		t.Fatalf("列出样本失败: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("Ack 后队列应清空（含损坏文件），实际剩 %d 个", len(remaining))
	}
}

func replaceFirst(s, old, new string) string {
	idx := indexOf(s, old)
	if idx < 0 {
		return s
	}
	return s[:idx] + new + s[idx+len(old):]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestInflightSurvivesRestart 验证已固化的批次在重启后原样重发，
// report_id 与块内容都不变。
func TestInflightSurvivesRestart(t *testing.T) {
	m0 := minuteStart(0)
	dir := t.TempDir()

	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("打开 spool 失败: %v", err)
	}
	store.now = func() time.Time { return time.Unix(m0+60, 0) }
	addRange(t, store, m0, 60)

	first, ok, err := store.Next(DefaultMaxSamples, 1<<20)
	if err != nil || !ok {
		t.Fatalf("首次 Next 失败: ok=%v err=%v", ok, err)
	}

	// 模拟进程重启：重新 Open 同一目录，不 Ack
	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("重开 spool 失败: %v", err)
	}
	reopened.now = func() time.Time { return time.Unix(m0+120, 0) }

	second, ok, err := reopened.Next(DefaultMaxSamples, 1<<20)
	if err != nil || !ok {
		t.Fatalf("重启后 Next 失败: ok=%v err=%v", ok, err)
	}
	if second.ReportID != first.ReportID {
		t.Fatalf("重启后 report_id 应保持不变: %s vs %s", first.ReportID, second.ReportID)
	}
	firstHot := blocksByResolution(first, 1)
	secondHot := blocksByResolution(second, 1)
	if len(firstHot) != len(secondHot) {
		t.Fatalf("重启后块数变化: %d vs %d", len(firstHot), len(secondHot))
	}
	if firstHot[0].Data != secondHot[0].Data {
		t.Fatal("重启后块内容应逐字节不变")
	}
	if err := reopened.Ack(second.ReportID); err != nil {
		t.Fatal(err)
	}
	// 在重启恢复的未确认分钟之后封存小时，必须包含恢复前的聚合点。
	hour := metricblock.BucketStartFor(m0, 60)
	lastMinute := hour + 59*60
	if lastMinute > m0 {
		addRange(t, reopened, lastMinute, 5)
		reopened.now = func() time.Time { return time.Unix(hour+3600, 0) }
		sealed, ok, err := reopened.Next(DefaultMaxSamples, 1<<20)
		if err != nil || !ok {
			t.Fatalf("封存恢复的小时失败: %v", err)
		}
		rollups := blocksByResolution(sealed, 60)
		if len(rollups) != 1 || rollups[0].PointCount != 2 {
			t.Fatal("重启后封存必须保留先前inflight携带的聚合分钟")
		}
	}
}
