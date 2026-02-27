# 通联数据采集系统

## 📖 系统概述

通联数据采集系统是一个独立于现有 Binance 交易系统的 A股市场数据采集服务，通过通联金融数据服务的 WebSocket 接口实时获取行情数据，并存储到 Redis 和 MySQL 数据库中。

### 系统架构

```
通联客户端 (feeder_client:9020)
    ↓ WebSocket
数据采集服务 (tonglian-ingestion)
    ├─→ 解析器 (JSON/CSV)
    ├─→ 异步处理管道
    ├─→ Redis 缓存 (ZSET)
    └─→ MySQL 持久化
```

---

## 🚀 快速开始

### 前置要求

1. **通联客户端 (feeder_client)**
   - 运行在端口 9020
   - 已配置正确的 Token
   - 参考 [通联客户端使用说明](#通联客户端配置)

2. **MySQL 数据库**
   - 版本: 5.7+
   - 数据库: hermes_trade_db
   - 自动创建表结构

3. **Redis 服务**
   - 版本: 5.0+
   - 用于实时数据缓存

### 一键部署

```bash
# 1. 确保通联客户端正在运行
netstat -tlnp | grep 9020

# 2. 运行部署脚本
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

部署脚本会自动：
- ✓ 检查通联客户端连接
- ✓ 检查 MySQL 连接
- ✓ 检查 Redis 连接
- ✓ 编译程序
- ✓ 创建 systemd 服务
- ✓ 启动服务

---

## ⚙️ 配置说明

### 1. 通联客户端配置

**文件位置**: `/opt/feeder_client/feeder_client.cfg`

**关键配置项**:
```json
{
  "AutoConfig": {
    "URL": "https://mdl01.datayes.com:19000/subscribe",
    "Token": "YOUR_TOKEN_HERE",  // ⚠️ 必须修改
    "Options": {
      "UseCDN": false
    }
  },
  "Publishers": {
    "websocket_server": {
      "Port": 9020
    }
  }
}
```

**启动通联客户端**:
```bash
# 开发环境
cd /opt/feeder_client
./feeder_client -d

# 生产环境 (systemd 服务)
sudo systemctl start mdl-forward.service
```

### 2. 数据采集服务配置

**文件位置**: `conf/config.toml`

```toml
[tonglian]
client_address = "localhost:9020"  # 通联客户端地址
data_format = "json"               # 数据格式: json 或 csv
subscription_limit = 300           # 订阅数量限制

[storage.mysql]
host = "39.105.45.63"
port = 3306
user = "wangmingjie"
password = "JK500Eyyds!"
database = "hermes_trade_db"

[storage.redis]
host = "localhost"
port = 6379
```

---

## 📊 订阅管理

### 订阅类别格式

```
<ServiceID>.<MessageID>.<SecurityCode>
```

- **ServiceID**: 3=上交所, 5=深交所
- **MessageID**: 8=股票快照
- **SecurityCode**: 证券代码

**示例**:
- `3.8.600000` - 浦发银行 (上交所)
- `5.8.000001` - 平安银行 (深交所)

### 使用管理脚本

```bash
# 查看所有订阅
./scripts/manage_subscriptions.sh list

# 添加单个订阅
./scripts/manage_subscriptions.sh add 3.8.600000 浦发银行

# 删除订阅
./scripts/manage_subscriptions.sh remove 3.8.600000

# 批量导入
cat > subscriptions.txt << EOF
3.8.600000,浦发银行
5.8.000001,平安银行
3.8.600019,宝钢股份
EOF
./scripts/manage_subscriptions.sh import subscriptions.txt

# 导出订阅
./scripts/manage_subscriptions.sh export subscriptions.txt
```

### 数据库直接操作

```sql
-- 查看活动订阅
SELECT * FROM tonglian_subscriptions WHERE is_active = 1;

-- 添加订阅
INSERT INTO tonglian_subscriptions (category_id, security_id, security_name, sid, mid, is_active)
VALUES ('3.8.600000', '600000', '浦发银行', 3, 8, 1);

-- 停用订阅
UPDATE tonglian_subscriptions SET is_active = 0 WHERE category_id = '3.8.600000';
```

---

## 🔍 数据查询

### Redis 查询

```bash
# 连接 Redis
redis-cli

# 查看某个股票的最新快照
ZRANGE tonglian:600000:snapshot -1 -1 WITHSCORES

# 查看 1 分钟 K 线
ZRANGE tonglian:600000:kline_1m -10 -1 WITHSCORES

# 查看所有订阅
SMEMBERS tonglian:subscriptions
```

### MySQL 查询

```sql
-- 查询最新快照
SELECT * FROM tonglian_market_snapshots
WHERE security_id = '600000'
ORDER BY update_time DESC
LIMIT 10;

-- 查询 K 线数据
SELECT * FROM tonglian_klines
WHERE security_id = '600000' AND timeframe = '1m'
ORDER BY open_time DESC
LIMIT 100;

-- 查询连接日志
SELECT * FROM tonglian_connection_status
ORDER BY created_at DESC
LIMIT 20;
```

---

## 🛠️ 服务管理

### systemd 服务命令

```bash
# 启动服务
sudo systemctl start tonglian-ingestion.service

# 停止服务
sudo systemctl stop tonglian-ingestion.service

# 重启服务
sudo systemctl restart tonglian-ingestion.service

# 查看状态
sudo systemctl status tonglian-ingestion.service

# 查看日志 (实时)
sudo journalctl -u tonglian-ingestion.service -f

# 查看最近日志
sudo journalctl -u tonglian-ingestion.service -n 100
```

### 直接运行 (开发调试)

```bash
# 编译
go build -o tonglian-ingestion cmd/tonglian-ingestion/main.go

# 运行
./tonglian-ingestion

# 或使用环境变量指定配置
export deeptrade_conf=./conf/config.toml
./tonglian-ingestion
```

---

## 📈 监控指标

系统每 60 秒输出一次运行指标：

```
[管道] 指标: 接收=1000 解析=1000 解析错误=0 Redis写入=1000 Redis错误=0 MySQL插入=1000 MySQL错误=0 通道深度=5
```

- **接收**: 从 WebSocket 接收的消息总数
- **解析**: 成功解析的消息数
- **解析错误**: 解析失败的消息数
- **Redis写入**: 成功写入 Redis 的消息数
- **Redis错误**: Redis 写入错误数
- **MySQL插入**: 成功插入 MySQL 的记录数
- **MySQL错误**: MySQL 插入错误数
- **通道深度**: 当前处理队列深度

---

## 🗂️ 数据结构

### Redis 数据结构

```
tonglian:600000:snapshot    # ZSET, 最新快照, score=timestamp
tonglian:600000:kline_1m   # ZSET, 1分钟K线, score=open_time
tonglian:600000:kline_5m   # ZSET, 5分钟K线
tonglian:subscriptions     # SET, 所有订阅ID
```

### MySQL 表结构

```sql
-- 市场快照
tonglian_market_snapshots
  ├─ id                  # 主键
  ├─ security_id         # 证券代码
  ├─ security_name       # 证券名称
  ├─ last_price          # 最新价
  ├─ volume              # 成交量
  ├─ turnover            # 成交额
  ├─ update_time         # 更新时间戳
  └─ created_at          # 创建时间

-- K线数据
tonglian_klines
  ├─ id
  ├─ security_id         # 证券代码
  ├─ timeframe           # K线类型 (1m, 5m, 15m, 1h, 1d)
  ├─ open_price          # 开盘价
  ├─ high_price          # 最高价
  ├─ low_price           # 最低价
  ├─ close_price         # 收盘价
  ├─ volume              # 成交量
  └─ trade_count         # 成交笔数

-- 订阅管理
tonglian_subscriptions
  ├─ category_id         # 订阅ID (3.8.600000)
  ├─ security_id         # 证券代码
  ├─ is_active           # 是否活跃
  └─ updated_at          # 更新时间

-- 连接状态
tonglian_connection_status
  ├─ event_type          # 事件类型 (connected, disconnected, error)
  ├─ message             # 消息内容
  └─ created_at          # 创建时间
```

---

## ⚠️ 故障排查

### 1. 无法连接通联客户端

**症状**: 日志显示 "连接失败"

**解决方法**:
```bash
# 检查通联客户端是否运行
netstat -tlnp | grep 9020

# 检查通联客户端日志
tail -f /opt/feeder_client/feeder_client.log

# 测试 WebSocket 连接
wscat -c ws://localhost:9020
```

### 2. MySQL 连接失败

**症状**: 日志显示 "MySQL连接失败"

**解决方法**:
```bash
# 测试 MySQL 连接
mysql -h 39.105.45.63 -P 3306 -u wangmingjie -p hermes_trade_db

# 检查防火墙
telnet 39.105.45.63 3306
```

### 3. Redis 连接失败

**症状**: 日志显示 "Redis连接失败"

**解决方法**:
```bash
# 测试 Redis 连接
redis-cli -h localhost -p 6379 ping

# 检查 Redis 服务
systemctl status redis
```

### 4. 订阅不生效

**症状**: 添加订阅后没有收到数据

**解决方法**:
```bash
# 1. 重启数据采集服务
sudo systemctl restart tonglian-ingestion.service

# 2. 检查订阅数量
./scripts/manage_subscriptions.sh list

# 3. 查看日志确认订阅已发送
sudo journalctl -u tonglian-ingestion.service -f | grep 订阅
```

### 5. 数据解析错误

**症状**: 日志显示大量 "解析错误"

**解决方法**:
```bash
# 检查数据格式配置
grep "data_format" conf/config.toml

# 确认与通联客户端配置一致
# 如果客户端返回 CSV，这里也应该是 csv
```

---

## 📝 附录

### 通联消息类别 ID

| ServiceID | 市场类型 | MessageID | 消息类型 |
|-----------|---------|-----------|---------|
| 3 | 上交所 | 8 | 股票快照 |
| 5 | 深交所 | 8 | 股票快照 |

### WebSocket 消息格式

**订阅请求**:
```json
{
  "format": "json",
  "subscribe": ["3.8.600000", "5.8.000001"]
}
```

**数据响应 (JSON)**:
```json
{
  "sid": 5,
  "mid": 2,
  "lt": 153703671,
  "data": {
    "SecurityID": "000001",
    "SecurityName": "平安银行",
    "LastPrice": 12.59,
    "Volume": 44621440,
    "Turnover": 562008050.35,
    "UpdateTime": 153703000
  }
}
```

**数据响应 (CSV)**:
```json
{
  "sid": 5,
  "mid": 2,
  "lt": 153303662,
  "data": "000001,平安银行,15:33:03.000,2,12.790,12.670,12.710,12.470,..."
}
```

---

## 📞 技术支持

- **项目文档**: `openspec/changes/add-tonglian-data-ingestion/`
- **设计文档**: `openspec/changes/add-tonglian-data-ingestion/design.md`
- **任务清单**: `openspec/changes/add-tonglian-data-ingestion/tasks.md`

---

## 📄 许可证

本项目遵循项目根目录的 LICENSE 文件。
