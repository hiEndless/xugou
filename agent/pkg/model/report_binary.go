package model

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

const AgentReportBinaryContentType = "application/vnd.xugou.report+binary"

// BlockBytes 复用刚编码的字节；从旧 inflight 恢复时只解一次本地 base64。
func (b *AgentReportBlock) BlockBytes() ([]byte, error) {
	if b.RawData != nil {
		return b.RawData, nil
	}
	raw, err := base64.StdEncoding.DecodeString(b.Data)
	if err != nil {
		return nil, fmt.Errorf("解析本地指标块失败: %w", err)
	}
	b.RawData = raw
	return raw, nil
}

// EncodeBinaryReport 编码 XG06 + uint32LE JSON 长度 + JSON + 顺序指标块。
// metadata 只描述块边界，已压缩指标块不经过 base64 或第二层 gzip。
func EncodeBinaryReport(report *AgentReport) ([]byte, error) {
	if report == nil || report.ProtocolVersion != AgentReportProtocolVersion {
		return nil, fmt.Errorf("二进制信封只支持 v%d", AgentReportProtocolVersion)
	}
	metadata := *report
	metadata.Blocks = make([]*AgentReportBlock, len(report.Blocks))
	chunks := make([][]byte, len(report.Blocks))
	total := 8
	for i, block := range report.Blocks {
		if block == nil {
			return nil, fmt.Errorf("第 %d 个指标块为空", i)
		}
		raw, err := block.BlockBytes()
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("第 %d 个指标块没有数据", i)
		}
		copy := *block
		copy.Data = ""
		copy.ByteLength = len(raw)
		metadata.Blocks[i] = &copy
		chunks[i] = raw
		total += len(raw)
	}
	encoded, err := json.Marshal(&metadata)
	if err != nil {
		return nil, fmt.Errorf("序列化上报摘要失败: %w", err)
	}
	if uint64(len(encoded)) > math.MaxUint32 {
		return nil, fmt.Errorf("上报摘要过大")
	}
	payload := make([]byte, 8, total+len(encoded))
	copy(payload[:4], "XG06")
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(encoded)))
	payload = append(payload, encoded...)
	for _, raw := range chunks {
		payload = append(payload, raw...)
	}
	return payload, nil
}
