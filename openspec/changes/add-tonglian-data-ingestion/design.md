## Context

DeepTrade currently operates as a cryptocurrency futures trading system using Binance APIs. The project aims to expand into A-share (Chinese stock market) quantitative trading. TongLian (通联) is a financial data provider that offers real-time market data through a WebSocket client running on port 9020.

**Key Requirements:**
- Real-time data ingestion from TongLian WebSocket (port 9020)
- Support both CSV and JSON data formats from TongLian
- Minute-level data caching in Redis for fast access
- Long-term persistence in MySQL database (hermes_trade_db at 39.105.45.63)
- Handle subscription-based market data (format: `<MajorID>.<MinorID>.<SecurityCode>`)
- Independent system from existing Binance trading

**Constraints:**
- TongLian client limits subscriptions to 300 items by default
- WebSocket connection must handle reconnection and heartbeat
- Data format: `{"sid":5, "mid":2, "lt":153303662, "data":"..."}`
- Market data includes: SecurityID, SecurityName, LastPrice, Volume, Turnover, UpdateTime, etc.

## Goals / Non-Goals

**Goals:**
- Create a reliable WebSocket client for TongLian data ingestion
- Implement efficient Redis caching for minute-level data retention
- Design MySQL schema for historical A-share market data
- Support subscription management for multiple A-share stocks
- Handle connection failures with automatic reconnection
- Parse both CSV and JSON data formats from TongLian

**Non-Goals:**
- Trading execution (not in this change)
- Technical analysis of A-share data (future change)
- Integration with existing Binance trading system
- Real-time charting or UI (backend only)
- Backtesting framework (future change)

## Decisions

### 1. Database Driver: GORM vs go-sql-driver

**Decision**: Use GORM for MySQL integration

**Rationale**:
- Provides ORM capabilities for easier data modeling
- Built-in connection pooling, logging, and hooks
- Auto-migration support for schema management
- Type-safe query building
- Familiar pattern if team has used ORMs before

**Alternatives considered**:
- `go-sql-driver/mysql`: Raw SQL driver, more control but more boilerplate
- `sqlx`: Middle ground, but GORM offers better migration support

### 2. Redis Data Structure: Hash vs Sorted Set

**Decision**: Use Redis Sorted Sets (ZSET) for time-series data

**Rationale**:
- Sorted sets naturally order data by timestamp (score)
- Efficient range queries for time-based data
- Easy to expire old data (ZREMRANGEBYSCORE)
- Supports minute-level retention policies
- Fast queries for recent N records

**Data format**: `tonglian:<security_id>:<data_type>` → ZSET with timestamp as score

**Alternatives considered**:
- Hash: Simple key-value, but no built-in time ordering
- String: Requires manual key management (e.g., `data:600000:202502031530`)
- Redis TimeSeries: Perfect fit but requires Redis module, may not be available

### 3. TongLian Data Format: JSON vs CSV

**Decision**: Support both, default to JSON

**Rationale**:
- JSON is self-describing and easier to parse
- CSV may be more efficient for high-volume data
- System should handle both for flexibility
- JSON format provides field names, more resilient to format changes

**Implementation**: Accept both in subscription requests, parse accordingly

### 4. Connection Management: Single vs Connection Pool

**Decision**: Single persistent WebSocket connection with reconnection logic

**Rationale**:
- TongLian WebSocket is designed for long-lived connections
- Single connection simplifies subscription management
- Reconnection logic handles network failures
- Connection pooling unnecessary for WebSocket (unlike HTTP)

**Reconnection strategy**:
- Exponential backoff: 1s, 2s, 4s, 8s, max 30s
- Resend subscriptions after reconnection
- Log all reconnection events

### 5. Data Processing: Sync vs Async

**Decision**: Async processing with goroutines and channels

**Rationale**:
- WebSocket receives data continuously
- Decouple receiving from processing
- Prevent blocking on slow Redis/MySQL writes
- Use buffered channels as backpressure mechanism

**Architecture**:
```
WebSocket → Channel(Rx) → Parser → Channel(Processed) → Redis/MySQL Writers
```

## Data Model

### MySQL Schema (hermes_trade_db)

```sql
-- Market data snapshots (tick data)
CREATE TABLE tonglian_market_snapshots (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    security_id VARCHAR(20) NOT NULL,
    security_name VARCHAR(100),
    sid INT NOT NULL COMMENT 'Message major category ID',
    mid INT NOT NULL COMMENT 'Message minor category ID',
    last_price DECIMAL(12, 4),
    volume BIGINT,
    turnover DECIMAL(20, 2),
    update_time BIGINT NOT NULL COMMENT 'Timestamp from TongLian',
    local_time BIGINT NOT NULL COMMENT 'Local time at receiver',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_security_time (security_id, update_time),
    INDEX idx_created_at (created_at),
    UNIQUE KEY uk_snapshot (security_id, local_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- K-line data (aggregated from ticks)
CREATE TABLE tonglian_klines (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    security_id VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL COMMENT '1m, 5m, 15m, 1h, 1d',
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    volume BIGINT,
    turnover DECIMAL(20, 2),
    trade_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_security_timeframe_time (security_id, timeframe, open_time),
    UNIQUE KEY uk_kline (security_id, timeframe, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Subscription management
CREATE TABLE tonglian_subscriptions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    category_id VARCHAR(50) NOT NULL UNIQUE COMMENT 'e.g., 3.8.600000',
    security_id VARCHAR(20) NOT NULL,
    security_name VARCHAR(100),
    sid INT NOT NULL,
    mid INT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Connection status
CREATE TABLE tonglian_connection_status (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_type VARCHAR(50) NOT NULL COMMENT 'connected, disconnected, error',
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### Redis Data Structure

```
# Key pattern: tonglian:<security_id>:<data_type>
# Data type: snapshot, kline_1m, kline_5m, etc.

# Example keys:
tonglian:600000:snapshot              # Latest tick data (ZSET, score=timestamp)
tonglian:600000:kline_1m              # 1-minute K-lines (ZSET, score=open_time)
tonglian:600000:kline_5m              # 5-minute K-lines (ZSET, score=open_time)
tonglian:subscriptions                 # Set of active subscription IDs

# Retention policy (example):
- Snapshots: Keep last 60 minutes
- K-line 1m: Keep last 24 hours
- K-line 5m: Keep last 7 days
- K-line 1h: Keep last 30 days
```

## Risks / Trade-offs

### Risk 1: TongLian Client Dependency
**Risk**: System depends on external TongLian client running on port 9020
**Mitigation**:
- Add health check endpoint to verify TongLian client availability
- Implement graceful degradation when client is unavailable
- Log connection failures prominently

### Risk 2: Data Loss During Network Issues
**Risk**: WebSocket disconnection may cause data loss
**Mitigation**:
- Implement buffering with channels (size ~1000 messages)
- Persist to Redis before acknowledging
- Log gaps in data for potential reconciliation

### Risk 3: MySQL Write Performance
**Risk**: High-frequency data may overwhelm MySQL writes
**Mitigation**:
- Batch inserts (100 records per batch)
- Use connection pooling (GORM default)
- Consider async writes with error handling
- Monitor write latency and queue depth

### Risk 4: Redis Memory Usage
**Risk**: Unlimited data growth in Redis
**Mitigation**:
- Implement TTL policies for all keys
- Use ZREMRANGEBYSCORE to trim old data
- Monitor Redis memory usage
- Set maxmemory policy in Redis config

### Risk 5: Subscription Limit (300 items)
**Risk**: TongLian limits subscriptions to 300 items
**Mitigation**:
- Track subscription count in code
- Warn when approaching limit
- Implement subscription priority or rotation
- Log when subscriptions are truncated

## Migration Plan

### Phase 1: Setup and Dependencies
1. Add MySQL driver (GORM) to go.mod
2. Create database schema in hermes_trade_db
3. Verify Redis server availability
4. Test TongLian client connectivity on port 9020

### Phase 2: Core Implementation
1. Implement WebSocket client with reconnection
2. Implement data parsers (JSON/CSV)
3. Create Redis storage layer
4. Create MySQL models and GORM integration
5. Implement subscription management

### Phase 3: Data Pipeline
1. Build async processing pipeline
2. Add batch writing to MySQL
3. Implement Redis TTL and cleanup
4. Add logging and metrics

### Phase 4: Testing
1. Unit tests for parsers
2. Integration tests with mock WebSocket server
3. Load tests with high-frequency data
4. Test reconnection scenarios

### Rollback Plan
- If MySQL writes are too slow: Increase batch size or disable temporarily
- If Redis memory grows too fast: Reduce retention time
- If connection is unstable: Fall back to polling mode (if available)
- All changes are additive; existing Binance system unaffected

## Open Questions

1. **Subscription Management**: Should we pre-define subscriptions in config or load from database?
   - *Recommendation*: Load from database for flexibility, seed from config on first run

2. **Data Validation**: How to handle malformed data from TongLian?
   - *Recommendation*: Log errors, skip record, continue processing; add metrics for monitoring

3. **K-line Aggregation**: Should we aggregate K-lines in real-time or post-process?
   - *Recommendation*: Real-time aggregation in Redis, periodic flush to MySQL

4. **Historical Data Backfill**: Do we need to backfill historical data on startup?
   - *Recommendation*: Out of scope for this change; add later if needed

5. **Monitoring**: What metrics should we expose?
   - *Recommendation*: Connection status, messages/sec, Redis/MySQL write latency, queue depth
