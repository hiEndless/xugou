package reporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xugou/agent/pkg/model"
	"github.com/xugou/agent/pkg/spool"
)

// fixtureReport 由真实 spool 管线生成，固定的跨 UTC 月界样本用于两端协议测试。
func fixtureReport(t testing.TB) *model.AgentReport {
	t.Helper()
	store, err := spool.Open(spool.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 30, 23, 59, 58, 0, time.UTC)
	rx, tx := []uint64{100, 160, 10, 40}, []uint64{200, 260, 20, 50}
	for i, cpu := range []float64{88.885, 40, 20, 5} {
		info := &model.SystemInfo{
			Timestamp: at.Add(time.Duration(i) * time.Second), Hostname: "fixture-agent", AgentVersion: "v6-fixture", OS: "linux", Version: "6.1",
			Keepalive: 60, ReportIntervalSeconds: 60,
			DynamicMetrics: model.DynamicMetrics{
				CPU: model.CPUInfo{Usage: cpu, Cores: 2, ModelName: "fixture-cpu"}, Memory: model.MemoryInfo{Total: 1000, Used: uint64(900 - i*100), Free: uint64(100 + i*100), UsageRate: float64(90 - i*10)},
				Load: model.LoadInfo{Load1: 1, Load5: 0.5, Load15: 0.25}, Disks: []model.DiskInfo{{Device: "/dev/vda1", MountPoint: "/", Total: 100, Used: uint64(99 - i), Free: uint64(1 + i), UsageRate: float64(99 - i), FSType: "ext4"}},
				Network: []model.NetworkInfo{{Interface: "eth0", BytesRecv: rx[i], BytesSent: tx[i]}}, ProcessCount: 20,
			},
		}
		if _, err := store.Add(info); err != nil {
			t.Fatal(err)
		}
	}
	report, ok, err := store.Next(100, 1<<20)
	if err != nil || !ok {
		t.Fatalf("组批fixture失败: ok=%v err=%v", ok, err)
	}
	report.ReportID = "00000000-0000-4000-8000-000000000006"
	return report
}

func TestBinaryReportCrossLanguageFixture(t *testing.T) {
	report := fixtureReport(t)
	payload, contentType, encoding, err := encodeReport(report)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != model.AgentReportBinaryContentType || encoding != "" || string(payload[:4]) != "XG06" {
		t.Fatal("v6必须直接发送二进制，无外层gzip")
	}
	metadataLength := int(binary.LittleEndian.Uint32(payload[4:8]))
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(payload[8:8+metadataLength], &metadata); err != nil {
		t.Fatal(err)
	}
	var blocks []map[string]json.RawMessage
	if err := json.Unmarshal(metadata["blocks"], &blocks); err != nil {
		t.Fatal(err)
	}
	offset := 8 + metadataLength
	for i, block := range blocks {
		if _, hasData := block["data"]; hasData {
			t.Fatal("metadata不能重复携带base64块")
		}
		var length int
		if err := json.Unmarshal(block["byte_length"], &length); err != nil {
			t.Fatal(err)
		}
		raw, err := report.Blocks[i].BlockBytes()
		if err != nil || !bytes.Equal(payload[offset:offset+length], raw) {
			t.Fatal("二进制块边界或内容错误")
		}
		offset += length
	}
	if offset != len(payload) {
		t.Fatal("二进制信封不能有尾随数据")
	}
	// 验证重启后的本地base64 inflight也生成完全一样的线路字节。
	persisted, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var reopened model.AgentReport
	if err := json.Unmarshal(persisted, &reopened); err != nil {
		t.Fatal(err)
	}
	encoded, err := model.EncodeBinaryReport(&reopened)
	if err != nil || !bytes.Equal(encoded, payload) {
		t.Fatal("恢复inflight后二进制上报必须逐字节一致")
	}
	path := filepath.Join("testdata", "report-v6.bin")
	if os.Getenv("XUGOU_UPDATE_FIXTURES") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(golden, payload) {
		t.Fatalf("跨语言fixture不一致，可显式使用XUGOU_UPDATE_FIXTURES=1更新: %v", err)
	}
}

func TestReportKeepsLegacyInflightWireFormat(t *testing.T) {
	report := fixtureReport(t)
	for _, version := range []int{5, 6} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			copy := *report
			copy.ProtocolVersion = version
			if version == 5 {
				copy.Summary, copy.MinuteSummaries = nil, nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer fixture-token" {
					t.Error("上报凭据应保留在请求头")
				}
				if version == 5 {
					if req.Header.Get("Content-Type") != "application/json" || req.Header.Get("Content-Encoding") != "gzip" {
						t.Error("旧inflight必须继续JSON gzip")
					}
					reader, err := gzip.NewReader(req.Body)
					if err != nil {
						t.Error(err)
						return
					}
					defer reader.Close()
					var decoded model.AgentReport
					if err := json.NewDecoder(reader).Decode(&decoded); err != nil || decoded.ProtocolVersion != 5 {
						t.Errorf("旧版JSON无效: %v", err)
					}
				} else {
					payload, err := io.ReadAll(req.Body)
					if err != nil || req.Header.Get("Content-Encoding") != "" || req.Header.Get("Content-Type") != model.AgentReportBinaryContentType || !bytes.HasPrefix(payload, []byte("XG06")) {
						t.Errorf("新版二进制上报无效: %v", err)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"config":{"collect_interval_seconds":1,"report_interval_seconds":60}}`)
			}))
			defer server.Close()
			reporter := &DefaultReporter{reporter: &model.HTTPReporter{ServerURL: server.URL, ApiToken: "fixture-token", Client: server.Client(), Registered: true}}
			if _, err := reporter.Report(context.Background(), &copy); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkReportWireEncoding(b *testing.B) {
	report := fixtureReport(b)
	for _, version := range []int{5, 6} {
		b.Run(string(rune('0'+version)), func(b *testing.B) {
			copy := *report
			copy.ProtocolVersion = version
			if version == 5 {
				copy.Summary, copy.MinuteSummaries = nil, nil
			}
			b.ReportAllocs()
			payload, _, _, err := encodeReport(&copy)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for b.Loop() {
				if _, _, _, err := encodeReport(&copy); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(payload)), "wire_B/report")
		})
	}
}
