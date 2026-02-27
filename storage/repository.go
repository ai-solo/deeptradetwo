package storage

import (
	"fmt"
	"log"
	"time"

	"gorm.io/gorm/clause"
)

// BatchInsertSnapshots inserts market snapshots in batch
func BatchInsertSnapshots(snapshots []MarketSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	// Use ON DUPLICATE KEY UPDATE to handle duplicates
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "security_id"}, {Name: "local_time"}},
		DoNothing: true,
	}).Create(&snapshots)

	if result.Error != nil {
		return fmt.Errorf("failed to batch insert snapshots: %w", result.Error)
	}

	log.Printf("[存储] 批量插入快照数据: %d 条", len(snapshots))
	return nil
}

// BatchInsertKlines inserts kline data in batch
func BatchInsertKlines(klines []Kline) error {
	if len(klines) == 0 {
		return nil
	}

	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	// Use ON DUPLICATE KEY UPDATE to handle duplicates
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "security_id"}, {Name: "timeframe"}, {Name: "open_time"}},
		DoNothing: true,
	}).Create(&klines)

	if result.Error != nil {
		return fmt.Errorf("failed to batch insert klines: %w", result.Error)
	}

	log.Printf("[存储] 批量插入K线数据: %d 条", len(klines))
	return nil
}

// InsertConnectionStatus logs a connection event
func InsertConnectionStatus(eventType, message string) error {
	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	status := ConnectionStatus{
		EventType: eventType,
		Message:   message,
		CreatedAt: time.Now(),
	}

	if err := db.Create(&status).Error; err != nil {
		return fmt.Errorf("failed to insert connection status: %w", err)
	}

	return nil
}

// GetActiveSubscriptions retrieves all active subscriptions from database
func GetActiveSubscriptions() ([]Subscription, error) {
	db, err := GetMySQLClient()
	if err != nil {
		return nil, err
	}

	var subscriptions []Subscription
	result := db.Where("is_active = ?", true).Find(&subscriptions)
	if result.Error != nil {
		return nil, fmt.Errorf("failed to get active subscriptions: %w", result.Error)
	}

	return subscriptions, nil
}

// AddSubscription adds a new subscription
func AddSubscription(categoryID, securityID, securityName string, sid, mid int) error {
	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	subscription := Subscription{
		CategoryID:   categoryID,
		SecurityID:   securityID,
		SecurityName: securityName,
		Sid:          sid,
		Mid:          mid,
		IsActive:     true,
	}

	// Use ON DUPLICATE KEY UPDATE to handle existing subscriptions
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "category_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"is_active", "updated_at"}),
	}).Create(&subscription)

	if result.Error != nil {
		return fmt.Errorf("failed to add subscription: %w", result.Error)
	}

	return nil
}

// RemoveSubscription deactivates a subscription
func RemoveSubscription(categoryID string) error {
	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	result := db.Model(&Subscription{}).
		Where("category_id = ?", categoryID).
		Update("is_active", false)

	if result.Error != nil {
		return fmt.Errorf("failed to remove subscription: %w", result.Error)
	}

	return nil
}

// GetSnapshotsByTimeRange retrieves snapshots within a time range
func GetSnapshotsByTimeRange(securityID string, startTime, endTime int64) ([]MarketSnapshot, error) {
	db, err := GetMySQLClient()
	if err != nil {
		return nil, err
	}

	var snapshots []MarketSnapshot
	result := db.Where("security_id = ? AND update_time >= ? AND update_time <= ?",
		securityID, startTime, endTime).
		Order("update_time ASC").
		Find(&snapshots)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get snapshots by time range: %w", result.Error)
	}

	return snapshots, nil
}

// GetKlinesByTimeframe retrieves klines for a security and timeframe
func GetKlinesByTimeframe(securityID, timeframe string, limit int) ([]Kline, error) {
	db, err := GetMySQLClient()
	if err != nil {
		return nil, err
	}

	if limit == 0 {
		limit = 100
	}

	var klines []Kline
	result := db.Where("security_id = ? AND timeframe = ?", securityID, timeframe).
		Order("open_time DESC").
		Limit(limit).
		Find(&klines)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get klines by timeframe: %w", result.Error)
	}

	return klines, nil
}
