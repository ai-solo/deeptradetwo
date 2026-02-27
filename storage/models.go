package storage

import (
	"time"

	"gorm.io/gorm"
)

// MarketSnapshot 通联行情快照数据
type MarketSnapshot struct {
	ID          uint    `gorm:"primaryKey" json:"id"`
	SecurityID  string  `gorm:"size:20;not null;index:idx_security_time,priority:1" json:"security_id"`
	SecurityName string  `gorm:"size:100" json:"security_name"`
	Sid         int     `gorm:"not null;index" json:"sid"`
	Mid         int     `gorm:"not null;index" json:"mid"`
	LastPrice   float64 `gorm:"type:decimal(12,4)" json:"last_price"`
	Volume      int64   `json:"volume"`
	Turnover    float64 `gorm:"type:decimal(20,2)" json:"turnover"`
	UpdateTime  int64   `gorm:"not null;index:idx_security_time,priority:2" json:"update_time"`
	LocalTime   int64   `gorm:"not null;uniqueIndex:uk_snapshot,priority:1" json:"local_time"`

	// Level 2 fields (10档行情) - JSON type for array storage
	AskPrices  string `gorm:"type:json" json:"ask_prices,omitempty"`   // 卖价 1-10 档
	BidPrices  string `gorm:"type:json" json:"bid_prices,omitempty"`   // 买价 1-10 档
	AskVolumes string `gorm:"type:json" json:"ask_volumes,omitempty"`  // 卖量 1-10 档
	BidVolumes string `gorm:"type:json" json:"bid_volumes,omitempty"`  // 买量 1-10 档

	CreatedAt   time.Time `json:"created_at"`
}

// TableName specifies the table name for MarketSnapshot
func (MarketSnapshot) TableName() string {
	return "tonglian_market_snapshots"
}

// Kline K线数据
type Kline struct {
	ID         uint      `gorm:"primaryKey" json:"id"`
	SecurityID string    `gorm:"size:20;not null;index:idx_security_timeframe_time,priority:1" json:"security_id"`
	Timeframe  string    `gorm:"size:10;not null;index:idx_security_timeframe_time,priority:2" json:"timeframe"`
	OpenTime   int64     `gorm:"not null;index:idx_security_timeframe_time,priority:3;uniqueIndex:uk_kline,priority:1" json:"open_time"`
	CloseTime  int64     `gorm:"not null" json:"close_time"`
	OpenPrice  float64   `gorm:"type:decimal(12,4)" json:"open_price"`
	HighPrice  float64   `gorm:"type:decimal(12,4)" json:"high_price"`
	LowPrice   float64   `gorm:"type:decimal(12,4)" json:"low_price"`
	ClosePrice float64   `gorm:"type:decimal(12,4)" json:"close_price"`
	Volume     int64     `json:"volume"`
	Turnover   float64   `gorm:"type:decimal(20,2)" json:"turnover"`
	TradeCount int       `json:"trade_count"`
	CreatedAt  time.Time `json:"created_at"`
}

// TableName specifies the table name for Kline
func (Kline) TableName() string {
	return "tonglian_klines"
}

// Subscription 订阅管理
type Subscription struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	CategoryID   string    `gorm:"size:50;not null;uniqueIndex" json:"category_id"`
	SecurityID   string    `gorm:"size:20;not null;index" json:"security_id"`
	SecurityName string    `gorm:"size:100" json:"security_name"`
	Sid          int       `gorm:"not null;index" json:"sid"`
	Mid          int       `gorm:"not null;index" json:"mid"`
	IsActive     bool      `gorm:"index:idx_active" json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// TableName specifies the table name for Subscription
func (Subscription) TableName() string {
	return "tonglian_subscriptions"
}

// ConnectionStatus 连接状态日志
type ConnectionStatus struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	EventType string    `gorm:"size:50;not null" json:"event_type"`
	Message   string    `gorm:"type:text" json:"message"`
	CreatedAt time.Time `gorm:"index:idx_created_at" json:"created_at"`
}

// TableName specifies the table name for ConnectionStatus
func (ConnectionStatus) TableName() string {
	return "tonglian_connection_status"
}

// AutoMigrate runs auto migration for all models
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&MarketSnapshot{},
		&Kline{},
		&Subscription{},
		&ConnectionStatus{},
	)
}
