## ADDED Requirements

### Requirement: WebSocket Client Connection

The system SHALL establish and maintain a persistent WebSocket connection to the TongLian client on port 9020 to receive real-time A-share market data.

#### Scenario: Successful connection establishment
- **GIVEN** the TongLian client is running on port 9020
- **WHEN** the system starts
- **THEN** a WebSocket connection is established using standard RFC 6455 protocol
- **AND** the connection is ready to receive market data

#### Scenario: Connection failure handling
- **GIVEN** the TongLian client is unavailable or port 9020 is blocked
- **WHEN** the system attempts to connect
- **THEN** the system logs the connection error
- **AND** initiates reconnection with exponential backoff (1s, 2s, 4s, max 30s)
- **AND** retries until connection succeeds

#### Scenario: Automatic reconnection after disconnect
- **GIVEN** an active WebSocket connection to TongLian
- **WHEN** the connection is interrupted (network failure, server restart, etc.)
- **THEN** the system detects the disconnection
- **AND** logs the disconnection event
- **AND** automatically attempts to reconnect using exponential backoff
- **AND** resubscribes to all previously subscribed market data after reconnection

---

### Requirement: Market Data Subscription

The system SHALL support subscribing to TongLian market data using the category format `<MajorID>.<MinorID>.<SecurityCode>` and handle subscription responses.

#### Scenario: Subscribe to single security
- **GIVEN** an active WebSocket connection to TongLian
- **WHEN** the system sends a subscription request for category "3.8.600000" (Shanghai stock)
- **THEN** the request is sent in JSON format: `{"format":"json","subscribe":["3.8.600000"]}`
- **AND** the system receives a success response: `{"result":"success","subscribed_messages":"3.8.600000"}`
- **AND** the subscription is tracked in the system

#### Scenario: Subscribe to multiple securities
- **GIVEN** an active WebSocket connection to TongLian
- **WHEN** the system subscribes to multiple categories (e.g., "3.8.600000,5.2.000001")
- **THEN** the request includes all categories in the subscribe array
- **AND** the system verifies all categories are included in the response
- **AND** logs warning if any subscriptions are truncated (TongLian limits to 300)

#### Scenario: Subscribe with CSV format
- **GIVEN** an active WebSocket connection to TongLian
- **WHEN** the system sends a subscription request with format "csv"
- **THEN** the request includes `"format":"csv"`
- **AND** subsequent market data is received in CSV format in the data field

#### Scenario: Subscription failure handling
- **GIVEN** an active WebSocket connection to TongLian
- **WHEN** a subscription request fails with response `{"result":"failed","reason":"invalid token"}`
- **THEN** the system logs the failure reason
- **AND** marks the subscription as failed
- **AND** does not retry the same subscription without manual intervention

---

### Requirement: Real-time Data Parsing

The system SHALL parse incoming TongLian market data messages in both JSON and CSV formats, extracting security information, prices, volume, and timestamps.

#### Scenario: Parse JSON format market data
- **GIVEN** the system has subscribed with JSON format
- **WHEN** a market data message is received: `{"sid":5,"mid":2,"lt":153303662,"data":{"SecurityID":"000001","SecurityName":"平安银行","LastPrice":12.59,"Volume":44621440,"UpdateTime":153703000}}`
- **THEN** the system extracts SecurityID as "000001"
- **AND** extracts SecurityName as "平安银行"
- **AND** extracts LastPrice as 12.59
- **AND** extracts Volume as 44621440
- **AND** parses lt (153303662) as local time (15:33:03.662)
- **AND** parses UpdateTime as Unix timestamp

#### Scenario: Parse CSV format market data
- **GIVEN** the system has subscribed with CSV format
- **WHEN** a market data message is received: `{"sid":5,"mid":2,"lt":153303662,"data":"000001,平安银行,15:33:03.000,2,12.790,12.670,12.710,12.470,..."}`
- **THEN** the system splits the CSV string by comma
- **AND** maps fields to data structure based on position (0=SecurityID, 1=SecurityName, 2=Time, etc.)
- **AND** converts numeric strings to appropriate types (Decimal, Int, etc.)
- **AND** handles missing or malformed fields gracefully

#### Scenario: Handle malformed data messages
- **GIVEN** the system is receiving market data
- **WHEN** a message is received with invalid JSON structure or non-numeric price values
- **THEN** the system logs the parsing error with the raw message
- **AND** skips processing the malformed message
- **AND** continues processing subsequent messages
- **AND** increments an error counter for monitoring

---

### Requirement: Redis Caching Layer

The system SHALL cache processed market data in Redis using sorted sets (ZSET) for efficient time-based queries and implement automatic TTL-based cleanup.

#### Scenario: Cache market snapshot in Redis
- **GIVEN** parsed market data for security "600000"
- **WHEN** the data is cached in Redis
- **THEN** the system stores data in key `tonglian:600000:snapshot` as a ZSET
- **AND** uses the message timestamp (lt) as the score
- **AND** stores the full data as JSON in the ZSET member
- **AND** the data is retrievable by time range queries

#### Scenario: Cache aggregated K-line data
- **GIVEN** multiple market snapshots for security "600000" within a 1-minute interval
- **WHEN** the system aggregates the data into a 1-minute K-line
- **THEN** the system stores the K-line in key `tonglian:600000:kline_1m` as a ZSET
- **AND** uses the K-line open_time as the score
- **AND** stores OHLCV (open, high, low, close, volume) data
- **AND** calculates turnover and trade count

#### Scenario: Automatic TTL-based cleanup
- **GIVEN** Redis keys with accumulated time-series data
- **WHEN** the retention time expires (e.g., 60 minutes for snapshots)
- **THEN** the system removes old entries using ZREMRANGEBYSCORE
- **AND** keeps only data within the retention window
- **AND** logs the number of entries removed

#### Scenario: Redis connection failure handling
- **GIVEN** the system is actively caching data to Redis
- **WHEN** Redis becomes unavailable
- **THEN** the system logs the connection error
- **AND** buffers data in memory (max 1000 messages)
- **AND** continues processing WebSocket messages
- **AND** flushes buffered data when Redis reconnects

---

### Requirement: MySQL Data Persistence

The system SHALL persist market data and K-line aggregates to MySQL database using GORM with batch inserts and connection pooling.

#### Scenario: Persist market snapshot to MySQL
- **GIVEN** a parsed market snapshot for security "000001" at timestamp 153703000
- **WHEN** the snapshot is written to MySQL
- **THEN** the system inserts a record into `tonglian_market_snapshots` table
- **AND** includes security_id, security_name, last_price, volume, turnover, update_time, local_time
- **AND** sets created_at to current timestamp
- **AND** respects the unique constraint on (security_id, local_time)

#### Scenario: Batch insert K-lines to MySQL
- **GIVEN** 100 aggregated 1-minute K-lines ready for persistence
- **WHEN** the batch is written to MySQL
- **THEN** the system uses GORM's CreateInBatches with batch size 100
- **AND** all records are inserted in a single transaction
- **AND** duplicates are skipped (on duplicate key update or ignore)
- **AND** the insert operation completes within 1 second

#### Scenario: Handle MySQL connection loss
- **GIVEN** the system is actively writing to MySQL
- **WHEN** the MySQL connection is lost
- **THEN** GORM's connection pool detects the failure
- **AND** the system logs the connection error
- **AND** queues writes in memory (max 5000 records)
- **AND** automatically reconnects when MySQL is available
- **AND** flushes queued writes after reconnection

#### Scenario: Auto-migration on startup
- **GIVEN** the system starts with a new or updated database schema
- **WHEN** the system initializes
- **THEN** GORM AutoMigrate is called for all models
- **AND** tables are created if they don't exist
- **AND** indexes are created as defined in the model tags
- **AND** existing tables are updated without data loss (if possible)

---

### Requirement: Subscription Management

The system SHALL manage TongLian market data subscriptions through database-backed configuration, support dynamic add/remove operations, and enforce the 300-subscription limit.

#### Scenario: Load subscriptions from database
- **GIVEN** the system starts with existing subscriptions in `tonglian_subscriptions` table
- **WHEN** the system initializes
- **THEN** all active subscriptions (is_active=true) are loaded from database
- **AND** each subscription is sent to TongLian WebSocket
- **AND** the system verifies all subscriptions succeed
- **AND** logs the number of active subscriptions

#### Scenario: Add new subscription dynamically
- **GIVEN** the system is running with active subscriptions
- **WHEN** a new subscription is added to the database
- **THEN** the system detects the new subscription
- **AND** sends a subscribe request to TongLian WebSocket
- **AND** updates the subscription record if successful
- **AND** logs error if subscription fails or limit exceeded

#### Scenario: Enforce 300-subscription limit
- **GIVEN** the system has 299 active subscriptions
- **WHEN** a request to add 2 more subscriptions is made
- **THEN** the system checks the current subscription count
- **AND** warns that adding 2 subscriptions would exceed the 300 limit
- **AND** adds only 1 subscription to stay within limit
- **AND** logs which subscriptions were truncated

#### Scenario: Deactivate subscription
- **GIVEN** an active subscription for "3.8.600000"
- **WHEN** the subscription is marked as is_active=false in database
- **THEN** the system sends a new subscribe request without this category
- **AND** verifies the subscription is no longer in TongLian response
- **AND** stops processing data for this security

---

### Requirement: Async Data Processing Pipeline

The system SHALL implement an asynchronous pipeline using goroutines and channels to decouple WebSocket message reception from data storage, with backpressure handling.

#### Scenario: Receive and process messages asynchronously
- **GIVEN** an active WebSocket connection receiving 1000 messages/second
- **WHEN** messages are received
- **THEN** the WebSocket goroutine sends parsed data to a buffered channel (capacity 1000)
- **AND** a separate processor goroutine reads from the channel
- **AND** the processor writes to Redis and MySQL
- **AND** the WebSocket goroutine never blocks on storage operations

#### Scenario: Backpressure when storage is slow
- **GIVEN** the processing channel has 900 messages buffered
- **WHEN** Redis or MySQL write latency increases
- **THEN** the channel approaches capacity (1000)
- **AND** the system logs a warning when buffer exceeds 90% capacity
- **AND** WebSocket continues receiving but doesn't process new messages until buffer has space
- **AND** no messages are dropped (flow control)

#### Scenario: Graceful shutdown
- **GIVEN** the system is running with 500 messages in the processing channel
- **WHEN** a shutdown signal is received (SIGTERM)
- **THEN** the system stops accepting new WebSocket messages
- **AND** processes all remaining messages in the channel
- **AND** closes Redis and MySQL connections after channel is empty
- **AND** exits cleanly

---

### Requirement: Configuration Management

The system SHALL support configuration through TOML config files for TongLian connection, Redis settings, and MySQL database credentials.

#### Scenario: Load TongLian configuration
- **GIVEN** a config.toml file with `[tonglian]` section
- **WHEN** the system starts
- **THEN** the system reads TongLian client address (default: localhost:9020)
- **AND** reads default data format (json or csv)
- **AND** reads reconnection settings (max_retries, backoff_max_ms)
- **AND** validates required configuration fields

#### Scenario: Load storage configuration
- **GIVEN** a config.toml file with `[storage]` section
- **WHEN** the system starts
- **THEN** the system reads MySQL connection details (host, port, user, password, database)
- **AND** reads Redis connection details (host, port, password, db)
- **AND** reads retention policies for different data types
- **AND** reads batch sizes for MySQL inserts

#### Scenario: Validate configuration on startup
- **GIVEN** a config.toml file with missing or invalid values
- **WHEN** the system starts
- **THEN** the system validates all required fields
- **AND** logs error and exits if MySQL host is missing
- **AND** logs error and exits if Redis host is missing
- **AND** uses default values for optional fields (e.g., TongLian port 9020)
- **AND** logs all loaded configuration values at DEBUG level

---

### Requirement: Monitoring and Logging

The system SHALL provide comprehensive logging of connection events, data flow metrics, and error tracking for operational visibility.

#### Scenario: Log connection lifecycle events
- **GIVEN** the system manages a WebSocket connection
- **WHEN** connection state changes
- **THEN** connection attempts are logged with timestamp and target address
- **AND** successful connections are logged at INFO level
- **AND** disconnections are logged at WARNING level with error details
- **AND** reconnection attempts are logged with backoff duration

#### Scenario: Track data flow metrics
- **GIVEN** the system is processing market data
- **WHEN** data flows through the pipeline
- **THEN** messages received per second are logged every 60 seconds
- **AND** messages successfully parsed are counted
- **AND** Redis write operations are counted with latency percentiles
- **AND** MySQL batch insert metrics are tracked (count, duration)
- **AND** processing channel depth is logged periodically

#### Scenario: Error tracking and alerting
- **GIVEN** the system encounters various error conditions
- **WHEN** errors occur
- **THEN** all parsing errors are logged with raw message snippets
- **AND** connection errors increment error counters
- **AND** storage errors (Redis/MySQL) are logged with stack traces
- **AND** subscription failures are logged with category IDs
- **AND** error rates are tracked and logged if exceeding threshold (e.g., >1% of messages)
