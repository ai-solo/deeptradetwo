package dataconv

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

// DataRangeChecker 数据范围检查器
// 用于检测字段值是否超过 Int32 范围
type DataRangeChecker struct {
	mu         sync.Mutex
	ranges     map[string]*FieldRange // 字段名 -> 值域统计
	violations []ViolationRecord
	stopOnError bool // 检测到超限是否立即停止
}

// FieldRange 字段值域统计
type FieldRange struct {
	Min  int64
	Max  int64
	Count int64
}

// ViolationRecord 超限记录
type ViolationRecord struct {
	Field      string
	Value      int64
	Limit      int64
	Code       string
	SampleData string
}

// NewDataRangeChecker 创建数据范围检查器
func NewDataRangeChecker(stopOnError bool) *DataRangeChecker {
	return &DataRangeChecker{
		ranges:     make(map[string]*FieldRange),
		stopOnError: stopOnError,
	}
}

// Int32Limit Int32 的最大值
const Int32Limit = 2147483647

// CheckInt32 检查值是否在 Int32 范围内
// field: 字段名
// value: 要检查的值
// code: 证券代码（用于日志）
// sample: 样本数据（用于日志）
func (drc *DataRangeChecker) CheckInt32(field string, value int64, code string, sample interface{}) error {
	drc.mu.Lock()
	defer drc.mu.Unlock()

	// 更新统计范围
	if _, ok := drc.ranges[field]; !ok {
		drc.ranges[field] = &FieldRange{Min: value, Max: value, Count: 0}
	}
	r := drc.ranges[field]
	if value < r.Min {
		r.Min = value
	}
	if value > r.Max {
		r.Max = value
	}
	r.Count++

	// 检查是否超限
	if value > Int32Limit {
		v := ViolationRecord{
			Field: field,
			Value: value,
			Limit: Int32Limit,
			Code:  code,
		}

		// 尝试格式化样本数据
		if sample != nil {
			v.SampleData = fmt.Sprintf("%+v", sample)
		}

		drc.violations = append(drc.violations, v)

		// 记录日志
		drc.logViolation(v)

		if drc.stopOnError {
			return fmt.Errorf("%s 超过 Int32 范围", field)
		}
	}

	return nil
}

// CheckUpdateTimeOffset 检查 UpdateTime 偏移是否超限
// offset: 微秒偏移量
func (drc *DataRangeChecker) CheckUpdateTimeOffset(field string, offset int64, code string, sample interface{}) error {
	drc.mu.Lock()
	defer drc.mu.Unlock()

	// 更新统计范围（支持负值）
	if _, ok := drc.ranges[field]; !ok {
		drc.ranges[field] = &FieldRange{Min: offset, Max: offset, Count: 0}
	}
	r := drc.ranges[field]
	if offset < r.Min {
		r.Min = offset
	}
	if offset > r.Max {
		r.Max = offset
	}
	r.Count++

	// 检查绝对值是否超限
	if abs(offset) > Int32Limit {
		v := ViolationRecord{
			Field: field,
			Value: offset,
			Limit: Int32Limit,
			Code:  code,
		}

		if sample != nil {
			v.SampleData = fmt.Sprintf("%+v", sample)
		}

		drc.violations = append(drc.violations, v)
		drc.logViolation(v)

		if drc.stopOnError {
			return fmt.Errorf("%s 偏移超过 Int32 范围", field)
		}
	}

	return nil
}

// logViolation 记录超限日志
func (drc *DataRangeChecker) logViolation(v ViolationRecord) {
	log.Printf("========================================")
	log.Printf("[错误] %s 超过 Int32 范围", v.Field)
	log.Printf("  字段: %s", v.Field)
	log.Printf("  当前值: %d", v.Value)
	log.Printf("  阈值: %d", v.Limit)
	log.Printf("  超出: %d", v.Value-v.Limit)

	// 根据字段类型添加具体说明
	switch v.Field {
	case "UpdateTime偏移":
		minutes := float64(abs(v.Value)) / 1000000 / 60
		log.Printf("  说明: 时间偏移超过 %.2f 分钟 (阈值约 35 分钟)", minutes)
	case "Price":
		price := float64(v.Value) / 100
		log.Printf("  说明: 价格 %.2f 元超过阈值 %.2f 元", price, float64(v.Limit)/100)
	case "Volume":
		volume := v.Value
		log.Printf("  说明: 成交量 %d 手超过阈值 %d 手", volume, v.Limit)
	}

	if v.Code != "" {
		log.Printf("  代码: %s", v.Code)
	}
	if v.SampleData != "" {
		// 截断过长的样本数据
		sample := v.SampleData
		if len(sample) > 500 {
			sample = sample[:500] + "..."
		}
		log.Printf("  样本数据: %s", sample)
	}
	log.Printf("========================================")
}

// HasViolations 是否有超限记录
func (drc *DataRangeChecker) HasViolations() bool {
	drc.mu.Lock()
	defer drc.mu.Unlock()
	return len(drc.violations) > 0
}

// GetViolations 获取所有超限记录
func (drc *DataRangeChecker) GetViolations() []ViolationRecord {
	drc.mu.Lock()
	defer drc.mu.Unlock()
	result := make([]ViolationRecord, len(drc.violations))
	copy(result, drc.violations)
	return result
}

// GetFieldRange 获取字段的值域统计
func (drc *DataRangeChecker) GetFieldRange(field string) *FieldRange {
	drc.mu.Lock()
	defer drc.mu.Unlock()
	return drc.ranges[field]
}

// Report 生成检测报告
func (drc *DataRangeChecker) Report() string {
	drc.mu.Lock()
	defer drc.mu.Unlock()

	var sb strings.Builder

	sb.WriteString("========================================\n")
	sb.WriteString("[检测] 数据范围检测报告\n")
	sb.WriteString("========================================\n\n")

	// 输出各字段的值域统计
	for field, r := range drc.ranges {
		sb.WriteString(fmt.Sprintf("[字段] %s\n", field))
		sb.WriteString(fmt.Sprintf("  最小值: %d, 最大值: %d, 行数: %d\n", r.Min, r.Max, r.Count))

		// 判断是否超限
		if r.Max > Int32Limit {
			sb.WriteString(fmt.Sprintf("  ⚠️  超限! 最大值 %d > %d\n", r.Max, Int32Limit))
		} else if field == "UpdateTime偏移" && abs(r.Min) > Int32Limit {
			sb.WriteString(fmt.Sprintf("  ⚠️  超限! 偏移 %d > %d\n", r.Min, Int32Limit))
		} else {
			sb.WriteString(fmt.Sprintf("  ✅ 未超限\n"))
		}
		sb.WriteString("\n")
	}

	// 输出超限汇总
	if len(drc.violations) > 0 {
		sb.WriteString("========================================\n")
		sb.WriteString(fmt.Sprintf("[警告] 检测到 %d 个字段超限\n", len(drc.violations)))
		sb.WriteString("[致命错误] 检测到字段超限，处理停止\n")
		sb.WriteString("[建议] 请修改代码将该字段回退到 Int64\n")
		sb.WriteString("========================================\n")
	} else {
		sb.WriteString("========================================\n")
		sb.WriteString("[检测完成] 所有字段未超限\n")
		sb.WriteString("========================================\n")
	}

	return sb.String()
}

// abs 返回绝对值
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
