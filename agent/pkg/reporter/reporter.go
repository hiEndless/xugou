package reporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xugou/agent/pkg/config"
	"github.com/xugou/agent/pkg/model"
	"github.com/xugou/agent/pkg/utils"
)

// setDefaultHeaders 设置所有请求的通用头部
func setDefaultHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://www.google.com/")
}

// setAgentVersionHeader 上报请求附带探针版本。
//
// 这里曾经还发 X-Agent-Config-Schema 与 X-Agent-Config-Md5，用于 v2/v3 的配置
// 协商；协商本身在 v4 就被上报响应里的 JSON 取代了，服务端此后再没读过这两个头，
// 探针却仍在每次上报前算一次配置串的 MD5。头和 MD5 实现已一并删除。
// 版本头保留：服务端虽然也不读（它从上报 body 的 agent_version 取版本），
// 但它在 Cloudflare 请求日志里直接可见，是排查探针版本的第一手信息。
func setAgentVersionHeader(req *http.Request) {
	if config.AgentVersion != "" {
		req.Header.Set(config.HeaderAgentVersion, config.AgentVersion)
	}
}

// Reporter 定义数据上报器接口
type Reporter interface {
	// Report 上报已由持久化 Spool 固化 report_id 的 v4 批次。
	Report(ctx context.Context, report *model.AgentReport) (*config.RemoteConfig, error)
}

type DefaultReporter struct {
	reporter *model.HTTPReporter
}

func NewReporter() Reporter {
	return &DefaultReporter{
		reporter: NewHTTPReporter(),
	}
}

// NewHTTPReporter 创建一个新的HTTP数据上报器
func NewHTTPReporter() *model.HTTPReporter {
	// 创建HTTP客户端
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// 如果设置了代理，配置代理
	if config.ProxyURL != "" {
		proxy, err := url.Parse(config.ProxyURL)
		if err != nil {
			fmt.Printf("警告: 代理URL解析失败: %v，将不使用代理\n", err)
		} else {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxy),
			}
		}
	}

	reporter := &model.HTTPReporter{
		ServerURL:  utils.NormalizeURL(config.ServerURL),
		ApiToken:   config.Token,
		ProxyURL:   config.ProxyURL,
		Client:     client,
		Registered: false,
	}

	return reporter
}

// Report 对新批次直接发送 v6 二进制；升级前已持久化的 v5 仍使用 JSON gzip。
// 调用方只在 2xx 后 Ack Spool，因此 HTTP 超时、
// 5xx 或进程退出都会在下一轮重用同一个 report_id 和 Payload。
func (r *DefaultReporter) Report(ctx context.Context, report *model.AgentReport) (*config.RemoteConfig, error) {
	if report == nil || report.ReportID == "" || len(report.Blocks) == 0 {
		return nil, errors.New("没有可上报的批次")
	}

	if !r.reporter.Registered {
		if err := r.register(ctx, report); err != nil {
			log.Printf("注册客户端失败: %v", err)
			return nil, err
		}
	}

	reportURL := fmt.Sprintf("%s/api/v2/agents/reports", r.reporter.ServerURL)
	payload, contentType, contentEncoding, err := encodeReport(report)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reportURL, bytes.NewReader(payload))
	if err != nil {
		log.Println("创建请求失败：", err)
		return nil, err
	}
	setDefaultHeaders(req)
	setAgentVersionHeader(req)
	req.Header.Set("Authorization", "Bearer "+r.reporter.ApiToken)
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := r.reporter.Client.Do(req)
	if err != nil {
		log.Println("上报数据失败：", err)
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkResponse(resp, "上报数据失败"); err != nil {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusGone {
			r.reporter.Registered = false
		}
		log.Println(err)
		return nil, err
	}
	log.Printf("v%d 批量上报成功: report_id=%s blocks=%d samples=%d wire_bytes=%d",
		report.ProtocolVersion, report.ReportID, len(report.Blocks), report.SampleCount(), len(payload))
	return parseV4ConfigResponse(resp), nil
}

func encodeReport(report *model.AgentReport) ([]byte, string, string, error) {
	if report.ProtocolVersion == model.AgentReportProtocolVersion {
		payload, err := model.EncodeBinaryReport(report)
		return payload, model.AgentReportBinaryContentType, "", err
	}
	if report.ProtocolVersion != 5 {
		return nil, "", "", fmt.Errorf("不支持的上报协议版本 %d", report.ProtocolVersion)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, "", "", fmt.Errorf("序列化旧版上报数据失败: %w", err)
	}
	var compressed bytes.Buffer
	zipper, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, "", "", fmt.Errorf("创建 gzip 编码器失败: %w", err)
	}
	if _, err := zipper.Write(encoded); err != nil {
		return nil, "", "", fmt.Errorf("压缩旧版上报数据失败: %w", err)
	}
	if err := zipper.Close(); err != nil {
		return nil, "", "", fmt.Errorf("结束旧版上报压缩失败: %w", err)
	}
	return compressed.Bytes(), "application/json", "gzip", nil
}

type v4ReportResponse struct {
	Config struct {
		CollectIntervalSeconds int  `json:"collect_interval_seconds"`
		ReportIntervalSeconds  int  `json:"report_interval_seconds"`
		Update                 bool `json:"update"`
	} `json:"config"`
}

const maxRegisterResponseBytes = 64 * 1024

func parseV4ConfigResponse(resp *http.Response) *config.RemoteConfig {
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(body) == 0 || len(body) > 4096 {
		log.Printf("读取 v4 配置响应失败: bytes=%d err=%v", len(body), err)
		return nil
	}
	var decoded v4ReportResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		log.Printf("解析 v4 配置响应失败: %v", err)
		return nil
	}
	if err := config.ValidateIntervals(
		decoded.Config.CollectIntervalSeconds,
		decoded.Config.ReportIntervalSeconds,
	); err != nil {
		log.Printf("v4 配置响应值域校验失败: %v", err)
		return nil
	}
	return &config.RemoteConfig{
		CollectInterval: decoded.Config.CollectIntervalSeconds,
		ReportInterval:  decoded.Config.ReportIntervalSeconds,
		Update:          decoded.Config.Update,
	}
}

func (r *DefaultReporter) register(ctx context.Context, report *model.AgentReport) error {

	log.Println("开始检查是否客户端已经注册，未注册将会自动注册")

	registerURL := fmt.Sprintf("%s/api/v2/agents/register", r.reporter.ServerURL)
	registerPaylod := &model.RegisterPayload{
		Name:        report.Hostname,
		Hostname:    report.Hostname,
		IPAddresses: report.IPAddresses,
		OS:          report.OS,
		Version:     report.Version,
	}

	data, err := json.Marshal(registerPaylod)

	if err != nil {
		log.Println("注册客户端失败: ", err)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", registerURL, bytes.NewBuffer(data))
	if err != nil {
		log.Println("创建请求失败: ", err)
		return err
	}
	setDefaultHeaders(req)
	req.Header.Set("Authorization", "Bearer "+r.reporter.ApiToken)
	req.Header.Set("Accept", "application/json")

	resp, err := r.reporter.Client.Do(req)
	if err != nil {
		log.Println("注册客户端失败: ", err)
		return err
	}
	defer resp.Body.Close()
	if err := checkResponse(resp, "注册客户端失败"); err != nil {
		log.Println(err)
		return err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRegisterResponseBytes+1))

	if err != nil {
		log.Println("注册客户端失败: ", err)
		return err
	}
	if len(body) > maxRegisterResponseBytes {
		return fmt.Errorf("注册响应超过 %d 字节限制", maxRegisterResponseBytes)
	}

	var respData model.RegisterResponse

	if err := json.Unmarshal(body, &respData); err != nil {
		log.Println("注册客户端失败: ", err)
		return err
	}

	if respData.Data.AgentID <= 0 {
		return errors.New("注册响应缺少有效的 agent_id")
	}

	log.Printf("客户端 ID: %d", respData.Data.AgentID)

	r.reporter.Registered = true

	return nil
}

func checkResponse(resp *http.Response, message string) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if len(body) == 0 {
		return fmt.Errorf("%s: HTTP %d", message, resp.StatusCode)
	}
	return fmt.Errorf("%s: HTTP %d: %s", message, resp.StatusCode, string(body))
}
