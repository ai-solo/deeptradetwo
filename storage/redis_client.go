package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"deeptrade/conf"

	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client
var redisCtx = context.Background()

// GetRedisClient returns the Redis client instance
func GetRedisClient() (*redis.Client, error) {
	if redisClient != nil {
		return redisClient, nil
	}

	cfg := conf.Get().Storage.Redis

	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = 10
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: poolSize,
	})

	// Test connection
	if err := redisClient.Ping(redisCtx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("[存储] Redis连接成功: %s:%d DB:%d", cfg.Host, cfg.Port, cfg.DB)
	return redisClient, nil
}

// InitRedis initializes Redis connection
func InitRedis() error {
	_, err := GetRedisClient()
	return err
}

// CloseRedis closes the Redis connection
func CloseRedis() error {
	if redisClient == nil {
		return nil
	}

	return redisClient.Close()
}

// CacheMarketSnapshot caches a market snapshot in Redis ZSET
func CacheMarketSnapshot(securityID string, data interface{}, timestamp int64) error {
	client, err := GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("tonglian:%s:snapshot", securityID)
	member, err := encodeJSON(data)
	if err != nil {
		return err
	}

	// Add to sorted set with timestamp as score
	if err := client.ZAdd(redisCtx, key, &redis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Err(); err != nil {
		return fmt.Errorf("failed to cache snapshot: %w", err)
	}

	// Set TTL based on retention policy
	retention := time.Duration(conf.Get().Storage.Retention.SnapshotMinutes) * time.Minute
	if retention > 0 {
		client.Expire(redisCtx, key, retention)
	}

	return nil
}

// CacheKline caches a kline in Redis ZSET
func CacheKline(securityID, timeframe string, kline *Kline) error {
	client, err := GetRedisClient()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("tonglian:%s:kline_%s", securityID, timeframe)
	member, err := encodeJSON(kline)
	if err != nil {
		return err
	}

	// Add to sorted set with open_time as score
	if err := client.ZAdd(redisCtx, key, &redis.Z{
		Score:  float64(kline.OpenTime),
		Member: member,
	}).Err(); err != nil {
		return fmt.Errorf("failed to cache kline: %w", err)
	}

	// Set TTL based on retention policy
	var retention time.Duration
	switch timeframe {
	case "1m":
		retention = time.Duration(conf.Get().Storage.Retention.Kline1mHours) * time.Hour
	case "5m":
		retention = time.Duration(conf.Get().Storage.Retention.Kline5mDays) * 24 * time.Hour
	case "1h":
		retention = time.Duration(conf.Get().Storage.Retention.Kline1hDays) * 24 * time.Hour
	default:
		retention = 24 * time.Hour // default 1 day
	}

	if retention > 0 {
		client.Expire(redisCtx, key, retention)
	}

	return nil
}

// GetSnapshotsFromRedis retrieves snapshots from Redis within a time range
func GetSnapshotsFromRedis(securityID string, start, end int64) ([]string, error) {
	client, err := GetRedisClient()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("tonglian:%s:snapshot", securityID)

	// Get members with scores in the range [start, end]
	members, err := client.ZRangeByScore(redisCtx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(start, 10),
		Max: strconv.FormatInt(end, 10),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get snapshots by time range: %w", err)
	}

	return members, nil
}

// GetLatestKline retrieves the latest kline for a security and timeframe
func GetLatestKline(securityID, timeframe string) (*Kline, error) {
	client, err := GetRedisClient()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("tonglian:%s:kline_%s", securityID, timeframe)

	// Get the last element (highest score)
	members, err := client.ZRevRange(redisCtx, key, 0, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest kline: %w", err)
	}

	if len(members) == 0 {
		return nil, nil
	}

	kline := &Kline{}
	if err := decodeJSON(members[0], kline); err != nil {
		return nil, err
	}

	return kline, nil
}

// CleanupOldData removes old data from Redis based on retention policy
func CleanupOldData(securityID, dataType string, cutoffTime int64) error {
	client, err := GetRedisClient()
	if err != nil {
		return err
	}

	var key string
	switch dataType {
	case "snapshot":
		key = fmt.Sprintf("tonglian:%s:snapshot", securityID)
	case "kline_1m", "kline_5m", "kline_1h":
		tf := dataType[6:] // extract timeframe
		key = fmt.Sprintf("tonglian:%s:kline_%s", securityID, tf)
	default:
		return fmt.Errorf("unknown data type: %s", dataType)
	}

	// Remove members with score < cutoffTime
	removed, err := client.ZRemRangeByScore(redisCtx, key, "0", strconv.FormatInt(cutoffTime, 10)).Result()
	if err != nil {
		return fmt.Errorf("failed to cleanup old data: %w", err)
	}

	if removed > 0 {
		log.Printf("[存储] 清理过期数据: %s, 移除 %d 条", key, removed)
	}

	return nil
}

// AddSubscriptionToRedis adds a subscription to Redis set
func AddSubscriptionToRedis(categoryID string) error {
	client, err := GetRedisClient()
	if err != nil {
		return err
	}

	if err := client.SAdd(redisCtx, "tonglian:subscriptions", categoryID).Err(); err != nil {
		return fmt.Errorf("failed to add subscription: %w", err)
	}

	return nil
}

// RemoveSubscriptionFromRedis removes a subscription from Redis set
func RemoveSubscriptionFromRedis(categoryID string) error {
	client, err := GetRedisClient()
	if err != nil {
		return err
	}

	if err := client.SRem(redisCtx, "tonglian:subscriptions", categoryID).Err(); err != nil {
		return fmt.Errorf("failed to remove subscription: %w", err)
	}

	return nil
}

// GetAllSubscriptions retrieves all subscriptions from Redis
func GetAllSubscriptions() ([]string, error) {
	client, err := GetRedisClient()
	if err != nil {
		return nil, err
	}

	members, err := client.SMembers(redisCtx, "tonglian:subscriptions").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get all subscriptions: %w", err)
	}

	return members, nil
}

// encodeJSON encodes data to JSON string
func encodeJSON(data interface{}) (string, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// decodeJSON decodes JSON string to data structure
func decodeJSON(jsonStr string, data interface{}) error {
	return json.Unmarshal([]byte(jsonStr), data)
}
