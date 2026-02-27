# Change: Add TongLian Data Ingestion System

## Why

The current DeepTrade system only supports cryptocurrency futures trading via Binance. To expand into A-share quantitative trading, we need to integrate TongLian (通联) financial data service as a separate data source. TongLian provides real-time market data for Chinese stocks through a WebSocket-based long connection that requires efficient data ingestion, caching, and persistence.

## What Changes

- **NEW**: TongLian WebSocket client integration for real-time A-share market data
- **NEW**: Redis caching layer for minute-level data retention
- **NEW**: MySQL persistence for long-term historical data storage
- **NEW**: Data transformation pipeline supporting both CSV and JSON formats from TongLian
- **NEW**: Market category management for subscription handling (e.g., 3.8.600000 for Shanghai stocks)

## Impact

- **Affected specs**: New capability - `tonglian-ingestion`
- **Affected code**:
  - New package: `tonglian/` for WebSocket client and data handling
  - New package: `storage/` for Redis and MySQL integration
  - New config section: `[tonglian]` and `[storage]` in `conf/config.toml`
  - Dependencies: Add MySQL driver (go-sql-driver/mysql or GORM)
- **Dependencies**:
  - TongLian client running on port 9020 (localhost or remote)
  - Redis server for caching
  - MySQL database at 39.105.45.63 (hermes_trade_db)
- **Non-breaking**: This is a standalone system; existing Binance trading logic remains unchanged

## Integration Approach

This system will operate **independently** from the existing Binance trading system, creating a parallel data pipeline specifically for A-share market data. Both systems can run concurrently without interference.
