package dataconv

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"deeptrade/storage"
	"gorm.io/gorm"
)

// LimitPriceRepository 涨跌停价格查询
type LimitPriceRepository struct {
	db *gorm.DB
}

// NewLimitPriceRepository 创建涨跌停价格查询器
func NewLimitPriceRepository() (*LimitPriceRepository, error) {
	db, err := storage.GetMySQLClient()
	if err != nil {
		return nil, fmt.Errorf("连接MySQL失败: %w", err)
	}
	return &LimitPriceRepository{db: db}, nil
}

// LimitPriceRecord 涨跌停价格记录
type LimitPriceRecord struct {
	Code       string    `gorm:"column:code"`
	TradeDate  time.Time `gorm:"column:trade_date"`
	HighLimit  float64   `gorm:"column:high_limit"`
	LowLimit   float64   `gorm:"column:low_limit"`
	PreClose   float64   `gorm:"column:pre_close"`
}

// TableName 指定表名
func (LimitPriceRecord) TableName() string {
	return "mkt_limit"
}

// GetLimitPrices 批量获取涨跌停价格
func (r *LimitPriceRepository) GetLimitPrices(tradeDate time.Time) (map[string]LimitPriceRecord, error) {
	var records []LimitPriceRecord

	result := r.db.Where("trade_date = ?", tradeDate.Format("2006-01-02")).Find(&records)
	if result.Error != nil {
		// 如果表不存在或查询失败，返回空map而不是错误（与Python逻辑一致）
		if strings.Contains(result.Error.Error(), "doesn't exist") || strings.Contains(result.Error.Error(), "Table") {
			log.Printf("[警告] mkt_limit 表不存在，涨跌停价格将设为0")
			return make(map[string]LimitPriceRecord), nil
		}
		// 其他错误也返回空map，继续处理
		log.Printf("[警告] 查询涨跌停价格失败: %v，将使用默认值0", result.Error)
		return make(map[string]LimitPriceRecord), nil
	}

	priceMap := make(map[string]LimitPriceRecord)
	for _, record := range records {
		priceMap[record.Code] = record
	}

	log.Printf("[数据] 获取涨跌停价格: %d 条", len(records))
	return priceMap, nil
}

// GetLimitPrice 获取单只股票的涨跌停价格
func (r *LimitPriceRepository) GetLimitPrice(code string, tradeDate time.Time) (*LimitPriceRecord, error) {
	var record LimitPriceRecord

	result := r.db.Where("code = ? AND trade_date = ?", code, tradeDate.Format("2006-01-02")).First(&record)
	if result.Error != nil {
		return nil, result.Error
	}

	return &record, nil
}

// SecurityRepository 股票信息查询
type SecurityRepository struct {
	db *gorm.DB
}

// NewSecurityRepository 创建股票信息查询器
func NewSecurityRepository() (*SecurityRepository, error) {
	db, err := storage.GetMySQLClient()
	if err != nil {
		return nil, fmt.Errorf("连接MySQL失败: %w", err)
	}
	return &SecurityRepository{db: db}, nil
}

// SecurityInfo 股票信息
type SecurityInfo struct {
	Code         string    `gorm:"column:code"`
	Name         string    `gorm:"column:name"`
	ListDate     time.Time `gorm:"column:list_date"`
	DelistDate   time.Time `gorm:"column:delist_date"`
}

// TableName 指定表名
func (SecurityInfo) TableName() string {
	return "stock_info" // 根据实际表名修改
}

// GetAllSecurities 获取指定日期的所有股票代码
func (r *SecurityRepository) GetAllSecurities(tradeDate time.Time) ([]string, error) {
	var codes []string

	dateStr := tradeDate.Format("2006-01-02")

	result := r.db.Model(&SecurityInfo{}).
		Where("list_date <= ? AND (delist_date IS NULL OR delist_date >= ?)", dateStr, dateStr).
		Pluck("code", &codes)

	if result.Error != nil {
		return nil, fmt.Errorf("查询股票列表失败: %w", result.Error)
	}

	return codes, nil
}

// ========== 缓存层 ==========

// PriceCache 价格缓存
type PriceCache struct {
	mu     sync.RWMutex
	prices map[string]LimitPriceRecord
	date   time.Time
}

// NewPriceCache 创建价格缓存
func NewPriceCache() *PriceCache {
	return &PriceCache{
		prices: make(map[string]LimitPriceRecord),
	}
}

// Load 加载指定日期的价格数据
func (c *PriceCache) Load(repo *LimitPriceRepository, tradeDate time.Time) error {
	prices, err := repo.GetLimitPrices(tradeDate)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.prices = prices
	c.date = tradeDate

	return nil
}

// Get 获取指定股票的涨跌停价格
func (c *PriceCache) Get(code string) (LimitPriceRecord, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	record, ok := c.prices[code]
	return record, ok
}

// GetOrCompute 获取或计算涨跌停价格
func (c *PriceCache) GetOrCompute(code string) (highLimit, lowLimit float64) {
	record, ok := c.Get(code)
	if ok && record.HighLimit > 0 {
		return record.HighLimit, record.LowLimit
	}

	// 如果没有涨跌停价，根据前收盘价计算 (±10%)
	if record.PreClose > 0 {
		highLimit = round2(record.PreClose * 1.10)
		lowLimit = round2(record.PreClose * 0.90)
	}

	return highLimit, lowLimit
}

func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}
