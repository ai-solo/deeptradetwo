# data-converter 性能优化

## 优化背景
在 2H2G 机器上，处理一天的数据需要 4 小时，性能太慢。

## 性能瓶颈分析
1. **文件串行处理**：6个数据文件（上交所委托+成交、快照；深交所委托、成交、快照）逐个处理
2. **过度 GC**：每个 chunk 处理后都强制 GC，增加 CPU 开销
3. **内存配置不当**：chunk 大小和 buffer 配置未针对低内存环境优化
4. **并发利用不足**：虽然 chunk 内部有并发，但文件间没有并发

## 优化措施

### 1. 文件级并发处理 (main.go)
**优化前**：
```go
// 逐个处理文件
for idx, task := range tasks {
    processFile(task)
}
```

**优化后**：
```go
// 并发处理多个文件
maxConcurrent := 2  // 2核机器同时处理2个文件
if workers >= 8 {
    maxConcurrent = 3  // 更多worker时可以处理3个
}
// 使用 goroutine + semaphore 控制并发
```

**预期效果**：文件处理时间减少 50%（2个文件并发）

### 2. 减少 GC 频率 (processor.go)
**优化前**：
```go
// 每个 chunk 后都强制 GC
runtime.GC()
```

**优化后**：
```go
// 只在每10个chunk或内存超过1GB时才GC
if chunkNum % 10 == 0 {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    if m.Alloc > 1GB {
        runtime.GC()
    }
}
```

**预期效果**：减少 GC 暂停时间，提升 20-30% 性能

### 3. 优化数据分块大小 (reader.go)
**优化前**：
```go
chunkSizeTick = 8_000
chunkSizeNormal = 30_000
```

**优化后**：
```go
chunkSizeTick = 5_000    // 降低内存峰值
chunkSizeNormal = 20_000  // 加快处理速度
```

**预期效果**：
- 更小的 chunk 减少内存峰值
- 更频繁的并发处理提升吞吐量

### 4. 优化 Parquet 写入 Buffer (opt_parquet_writer.go)
**优化后**：
```go
maxOrderBuffer: 10000  // 增加buffer，减少写入频率
maxDealBuffer:  10000
maxTickBuffer:  5000
```

**预期效果**：减少磁盘 I/O 次数，提升写入性能

## 总体性能预估

| 优化项 | 性能提升 |
|--------|----------|
| 文件级并发 | 40-50% |
| 减少 GC 频率 | 20-30% |
| 优化 chunk 和 buffer | 10-15% |
| **综合提升** | **60-70%** |

**预期处理时间**：从 4 小时降低到 **1.5-2 小时**

## 使用建议

### 1. 启用优化模式
```bash
./bin/data-converter \
  -optimize \
  -workers 8 \
  ...
```

### 2. 调整并发数
- 2核机器：`-workers 8`（已默认）
- 4核机器：`-workers 12`
- 更多核心：`-workers $(nproc)*3`

### 3. 监控内存使用
处理过程中会输出内存使用情况：
```
[内存] 当前使用: 512.34 MB (无需GC)
[内存] GC后使用: 892.45 MB
```

如果频繁触发 GC，可以：
- 增加内存限制阈值（修改代码中的 1024MB）
- 减少 workers 数量
- 降低 chunk 大小

## 进一步优化建议

如果性能仍不满足需求，可以考虑：

1. **Pipeline 模式**：读取下一个 chunk 的同时处理当前 chunk
2. **压缩优化**：使用更快的压缩算法（如 Snappy 代替 Zstd）
3. **数据库优化**：批量查询涨跌停价格，减少数据库调用
4. **SSD 存储**：使用 SSD 存储临时文件和输出文件
5. **增加内存**：4GB 内存可以使用更大的 buffer 和 chunk

## 回滚方案

如果优化后出现问题，可以通过 git 回滚：
```bash
git diff HEAD  # 查看改动
git checkout cmd/data-converter/main.go  # 回滚单个文件
```

## 测试建议

1. **小数据测试**：先用 `-limit 100000` 测试少量数据
2. **单日测试**：用 `-date 20250101` 测试单天数据
3. **对比测试**：记录优化前后的处理时间和内存使用

---

优化完成时间：2026-03-01
