# OSS 上传配置说明

## 概述

数据转换器现已支持将处理后的数据自动上传到阿里云 OSS，替代原来的 ClickHouse 存储方式。

## Python 配置

### 1. 安装依赖

```bash
pip install oss2
```

### 2. 环境变量配置

在运行前设置以下环境变量：

```bash
export OSS_ACCESS_KEY_ID="your_access_key_id"
export OSS_ACCESS_KEY_SECRET="your_access_key_secret"
export OSS_ENDPOINT="oss-cn-shanghai.aliyuncs.com"  # 可选，默认为上海区域
export OSS_BUCKET_NAME="stock-data"  # 可选，默认为 stock-data
```

### 3. OSS 存储结构

```
market_data/
├── order/
│   ├── 2024/
│   │   ├── 01/
│   │   │   ├── order_000001.XSHE_20240101.parquet
│   │   │   └── order_600000.XSHG_20240101.parquet
│   │   └── 02/
│   └── 2025/
├── deal/
│   ├── 2024/
│   └── 2025/
├── tick/
│   ├── 2024/
│   └── 2025/
└── logs/
    ├── convert_log_20240101_120000.csv
    └── convert_log_20240102_093000.csv
```

## Go 配置

### 1. 安装依赖

```bash
cd /Users/zhangyang/Desktop/freedom/deeptrade
go get github.com/aliyun/aliyun-oss-go-sdk/oss
```

### 2. 环境变量配置

与 Python 相同，设置以下环境变量：

```bash
export OSS_ACCESS_KEY_ID="your_access_key_id"
export OSS_ACCESS_KEY_SECRET="your_access_key_secret"
export OSS_ENDPOINT="oss-cn-shanghai.aliyuncs.com"
export OSS_BUCKET_NAME="stock-data"
```

### 3. 使用示例

```go
package main

import (
    "log"
    "time"
    "your-project/dataconv"
)

func main() {
    cfg := dataconv.ProcessorConfig{
        TradingDay:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local),
        OutputDir:   "/tmp/stock_data",
        Workers:     16,
        ZipPassword: "your_password",
    }

    processor, err := dataconv.NewProcessor(cfg)
    if err != nil {
        log.Fatal(err)
    }

    // 处理深交所委托数据
    result, err := processor.ProcessSZOrder("path/to/data.zip")
    if err != nil {
        log.Printf("处理失败: %v", err)
    }
    log.Printf("处理完成: %+v", result)
}
```

## 功能特性

### Python 版本

1. ✅ 数据保存为 Parquet 格式（压缩存储）
2. ✅ 自动上传到 OSS
3. ✅ 上传成功后删除本地临时文件
4. ✅ 转换日志保存为 CSV 并上传
5. ✅ 完整的错误处理和日志记录

### Go 版本

1. ✅ 数据保存为 CSV 格式
2. ✅ 自动上传到 OSS
3. ✅ 上传成功后删除本地临时文件
4. ✅ 支持批量上传
5. ✅ 完整的错误处理和日志记录
6. ✅ 与 Python 逻辑完全一致的数据验证

## 数据格式

### Order (委托数据)

| 字段 | 类型 | 说明 |
|------|------|------|
| TradingDay | Date | 交易日期 |
| Code | String | 股票代码 (如 600000.XSHG) |
| Time | Timestamp | 委托时间 |
| UpdateTime | Timestamp | 更新时间 |
| OrderID | Int64 | 委托编号 |
| Side | Int16 | 买卖方向: 0=买, 1=卖 |
| Price | Float64 | 委托价格 |
| Volume | Float64 | 委托数量 |
| OrderType | Int16 | 订单类型 |
| Channel | Int64 | 通道号 |
| SeqNum | Int64 | 序列号 |

### Deal (成交数据)

| 字段 | 类型 | 说明 |
|------|------|------|
| TradingDay | Date | 交易日期 |
| Code | String | 股票代码 |
| Time | Timestamp | 成交时间 |
| UpdateTime | Timestamp | 更新时间 |
| SaleOrderID | Int64 | 卖方委托编号 |
| BuyOrderID | Int64 | 买方委托编号 |
| Side | Int16 | 买卖方向: 0=买, 1=卖, 4=撤单, 10=未知 |
| Price | Float64 | 成交价格 |
| Volume | Float64 | 成交数量 |
| Money | Float64 | 成交金额 |
| Channel | Int64 | 通道号 |
| SeqNum | Int64 | 序列号 |

### Tick (快照数据)

包含完整的 10 档行情数据，字段较多，请参考代码中的 `Tick` 结构体定义。

## 数据验证

两个版本都实现了完整的数据验证逻辑：

1. ✅ 股票代码格式验证
2. ✅ 价格、数量有限值检查（非 NaN/Inf）
3. ✅ 买卖方向枚举值验证
4. ✅ 订单类型验证（深市 1/2/3，沪市 2/5）
5. ✅ 快照 10 档数据完整性验证

## 从 ClickHouse 迁移到 OSS

### 主要变更

1. **存储方式**：从数据库存储改为对象存储
2. **数据格式**：
   - Python: Parquet 格式（高效压缩）
   - Go: CSV 格式（通用性好）
3. **日志记录**：从数据库表改为 CSV 文件
4. **性能优化**：移除了数据库连接和表优化操作

### 优势

- ✅ 存储成本更低
- ✅ 扩展性更好
- ✅ 易于备份和迁移
- ✅ 支持版本管理
- ✅ 与数据湖/数据分析平台集成更容易

## 故障排除

### Python

如果遇到 OSS 上传失败：

1. 检查环境变量是否正确设置
2. 检查网络连接
3. 检查 OSS Bucket 权限
4. 查看日志文件获取详细错误信息

### Go

如果遇到编译错误：

```bash
# 清理并重新获取依赖
go mod tidy
go get github.com/aliyun/aliyun-oss-go-sdk/oss
```

如果 OSS 上传失败，数据仍会保存在本地 `OutputDir` 目录中。

## 性能建议

1. **并发数**: 建议设置为 CPU 核心数的 2 倍
   - Python: `convert_data(trading_day, num_workers=16)`
   - Go: `Workers: 16` in `ProcessorConfig`

2. **本地存储**: 使用 SSD 以提高文件读写速度

3. **网络带宽**: OSS 上传速度受网络带宽限制，建议使用与 OSS 同地域的服务器

4. **批处理**: 处理多个交易日时，建议逐日处理并监控资源使用
