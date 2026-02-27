## 1. Project Setup and Dependencies

- [x] 1.1 Add GORM MySQL driver to go.mod (`gorm.io/gorm`, `gorm.io/driver/mysql`)
- [x] 1.2 Update go.mod and run `go mod tidy`
- [x] 1.3 Verify Redis dependency is available (already in go.mod as `github.com/go-redis/redis`)
- [x] 1.4 Verify gorilla/websocket dependency is available (already in go.mod)

## 2. Configuration Management

- [x] 2.1 Add `[tonglian]` section to `conf/config.go` struct
  - ClientAddress (default: "localhost:9020")
  - DataFormat (default: "json")
  - MaxRetries (default: 10)
  - BackoffMaxMs (default: 30000)
  - SubscriptionLimit (default: 300)
- [x] 2.2 Add `[storage]` section to `conf/config.go` struct
  - MySQL: Host, Port, User, Password, Database
  - Redis: Host, Port, Password, DB
  - Retention policies for snapshots, klines
  - Batch sizes (MySQL: 100, Channel buffer: 1000)
- [x] 2.3 Update `conf/config.toml` with TongLian and storage configuration
- [x] 2.4 Add configuration validation logic in `conf/config.go`
- [ ] 2.5 Test configuration loading with sample config file

## 3. Database Schema and Models

- [x] 3.1 Create MySQL table migration scripts
  - `tonglian_market_snapshots`
  - `tonglian_klines`
  - `tonglian_subscriptions`
  - `tonglian_connection_status`
- [ ] 3.2 Execute migration scripts on hermes_trade_db at 39.105.45.63
- [x] 3.3 Create GORM model structs in `storage/models.go`
  - MarketSnapshot model
  - Kline model
  - Subscription model
  - ConnectionStatus model
- [x] 3.4 Add GORM tags for table names, indexes, and constraints
- [ ] 3.5 Test AutoMigrate creates tables correctly
- [ ] 3.6 Verify indexes are created properly

## 4. Redis Storage Layer

- [x] 4.1 Create `storage/redis_client.go` with Redis client initialization
- [x] 4.2 Implement connection pooling and configuration
- [x] 4.3 Create `storage/cache.go` with caching functions
  - CacheMarketSnapshot(securityID string, data interface{}) error
  - CacheKline(securityID, timeframe string, kline Kline) error
  - GetSnapshotsByTimeRange(securityID string, start, end int64) ([]Snapshot, error)
  - CleanupOldData(securityID, dataType string, retention time.Duration) error
- [x] 4.4 Implement ZSET operations with timestamp as score
- [x] 4.5 Add TTL-based cleanup logic using ZREMRANGEBYSCORE
- [x] 4.6 Implement connection failure handling and retry logic
- [ ] 4.7 Add unit tests for Redis operations

## 5. MySQL Storage Layer

- [x] 5.1 Create `storage/mysql_client.go` with GORM initialization
- [x] 5.2 Configure connection pool (max open connections, max idle, etc.)
- [x] 5.3 Implement batch insert functions in `storage/repository.go`
  - BatchInsertSnapshots([]MarketSnapshot) error
  - BatchInsertKlines([]Kline) error
- [x] 5.4 Implement duplicate key handling (ON DUPLICATE KEY UPDATE)
- [x] 5.5 Add connection health check and auto-reconnect
- [x] 5.6 Implement transaction support for batch operations
- [ ] 5.7 Add unit tests for MySQL operations (with test database)

## 6. TongLian WebSocket Client

- [x] 6.1 Create `tonglian/client.go` with WebSocket client struct
- [x] 6.2 Implement connection logic using gorilla/websocket
- [x] 6.3 Add connection state management (connecting, connected, disconnected)
- [x] 6.4 Implement exponential backoff reconnection logic
- [x] 6.5 Add connection timeout configuration
- [x] 6.6 Implement graceful connection close on shutdown
- [x] 6.7 Add connection event logging to database (tonglian_connection_status)
- [ ] 6.8 Write unit tests for connection logic (with mock WebSocket server)

## 7. TongLian Data Parsers

- [x] 7.1 Create `tonglian/parser.go` with data parsing functions
- [x] 7.2 Implement JSON parser for market data format
  - ParseMessage(json string) (*MarketData, error)
  - Extract SecurityID, SecurityName, LastPrice, Volume, etc.
- [x] 7.3 Implement CSV parser for market data format
  - ParseCSVMessage(csvString string) (*MarketData, error)
  - Handle field mapping by position
- [x] 7.4 Add timestamp parsing for lt (local time) and UpdateTime
- [x] 7.5 Implement error handling for malformed data
- [x] 7.6 Add metrics for parsing success/failure rates
- [ ] 7.7 Write unit tests for JSON and CSV parsers with sample data

## 8. Subscription Management

- [x] 8.1 Create `tonglian/subscription.go` with subscription manager
- [x] 8.2 Implement Subscribe(categories []string) error function
  - Construct JSON request: `{"format":"json","subscribe":[...]}`
  - Send to WebSocket
  - Parse response and verify success
- [x] 8.3 Implement Unsubscribe(categories []string) error function
- [x] 8.4 Add subscription tracking in memory map
- [x] 8.5 Implement LoadSubscriptionsFromDB() function
- [x] 8.6 Add subscription limit enforcement (max 300)
- [x] 8.7 Implement dynamic add/remove subscription APIs
- [x] 8.8 Add database operations for subscription CRUD
- [ ] 8.9 Test subscription with actual TongLian client

## 9. Async Processing Pipeline

- [x] 9.1 Create `tonglian/pipeline.go` with pipeline manager
- [x] 9.2 Define data flow channels
  - RawMessageChan (string) - WebSocket → Parser
  - ParsedDataChan (*MarketData) - Parser → Storage
- [x] 9.3 Implement WebSocket receiver goroutine
  - Read messages from WebSocket
  - Send to RawMessageChan
  - Handle backpressure
- [x] 9.4 Implement parser goroutine
  - Read from RawMessageChan
  - Parse messages (JSON/CSV)
  - Send valid data to ParsedDataChan
  - Log parsing errors
- [x] 9.5 Implement storage writer goroutine
  - Read from ParsedDataChan
  - Write to Redis (async, non-blocking)
  - Batch write to MySQL (every 100 records or 1 second)
  - Handle storage failures
- [x] 9.6 Add graceful shutdown logic
  - Stop accepting new messages
  - Drain channels before exit
  - Close connections cleanly
- [x] 9.7 Add channel depth monitoring and alerts
- [ ] 9.8 Test pipeline with high message load (1000 msg/sec)

## 10. K-line Aggregation

- [x] 10.1 Create `tonglian/aggregator.go` for K-line calculation
- [x] 10.2 Implement time-bucketing logic for 1m, 5m, 15m, 1h, 1d
- [x] 10.3 Aggregate OHLCV from market snapshots
  - Open = first price in bucket
  - High = max price in bucket
  - Low = min price in bucket
  - Close = last price in bucket
  - Volume = sum of volumes
  - Turnover = sum of turnovers
- [x] 10.4 Handle bucket transitions (detect new time window)
- [x] 10.5 Store aggregated K-lines to Redis and MySQL
- [ ] 10.6 Test aggregation accuracy with historical data
- [ ] 10.7 Verify K-line completeness (no missing buckets)

## 11. Integration and Main Loop

- [x] 11.1 Create `tonglian/service.go` as main service coordinator
- [x] 11.2 Implement service lifecycle: Start(), Stop(), Restart()
- [x] 11.3 Initialize all components in correct order
  1. Load configuration
  2. Connect to Redis
  3. Connect to MySQL
  4. Connect to TongLian WebSocket
  5. Load subscriptions
  6. Start pipeline goroutines
- [x] 11.4 Add signal handling (SIGTERM, SIGINT)
- [ ] 11.5 Implement health check endpoint (optional)
- [x] 11.6 Add startup logging with all configuration values
- [ ] 11.7 Test full service startup and shutdown cycle

## 12. Monitoring and Logging

- [x] 12.1 Add structured logging throughout all components
- [x] 12.2 Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)
- [x] 12.3 Implement metrics collection
  - Messages received/processed per second
  - Parse error rate
  - Redis write latency
  - MySQL batch insert duration
  - Channel depth utilization
  - Connection status
- [x] 12.4 Add periodic metric logging (every 60 seconds)
- [x] 12.5 Implement connection event logging to database
- [ ] 12.6 Add error rate alerts (>1% threshold)
- [ ] 12.7 Create dashboard queries (optional)

## 13. Testing

- [ ] 13.1 Write unit tests for parsers (JSON/CSV)
- [ ] 13.2 Write unit tests for Redis operations (with mock Redis)
- [ ] 13.3 Write unit tests for MySQL repository (with test DB)
- [ ] 13.4 Create integration test with mock WebSocket server
- [ ] 13.5 Test reconnection scenarios (disconnect during operation)
- [ ] 13.6 Test subscription limit enforcement
- [ ] 13.7 Load test with 1000 messages/second
- [ ] 13.8 Test graceful shutdown with queued data
- [ ] 13.9 Test with real TongLian client (dev environment)
- [ ] 13.10 Verify data accuracy (compare Redis vs MySQL)

## 14. Documentation

- [ ] 14.1 Write README.md for tonglian package
  - Architecture overview
  - Configuration guide
  - Usage examples
- [x] 14.2 Document API functions in GoDoc comments
- [ ] 14.3 Create troubleshooting guide
  - Common connection issues
  - Data format problems
  - Performance tuning
- [ ] 14.4 Document database schema with ER diagram
- [ ] 14.5 Add deployment guide
  - Dependencies (TongLian client, Redis, MySQL)
  - Configuration steps
  - Startup/shutdown procedures

## 15. Production Readiness

- [x] 15.1 Add security measures
  - Sanitize database credentials in logs
  - Validate all user inputs
  - Use prepared statements (GORM handles this)
- [ ] 15.2 Add performance optimization
  - Profile critical paths
  - Optimize hot spots
  - Tune batch sizes and buffer sizes
- [x] 15.3 Add error recovery mechanisms
  - Redis connection retry
  - MySQL connection retry
  - WebSocket reconnection
- [ ] 15.4 Configure system monitoring alerts
  - Connection down > 1 minute
  - Error rate > 1%
  - Channel buffer > 90%
  - MySQL write latency > 1 second
- [ ] 15.5 Run full end-to-end test in staging environment
- [ ] 15.6 Prepare rollback plan
