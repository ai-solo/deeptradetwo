package dataconv

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// OptParquetWriter 优化后的 Parquet 文件写入器
// 特点：使用 Int32 替代 String/Float64/Int64，Zstd 压缩，删除冗余字段
type OptParquetWriter struct {
	outputDir      string
	mu             sync.Mutex
	securityCache  *SecurityCache   // 证券代码缓存
	rangeChecker   *DataRangeChecker // 数据范围检查器
	forceInt32     bool             // 强制使用 Int32（不检测）
	ossUploader    *OSSUploader     // OSS 上传器（可选）
	enableOSS      bool             // 是否启用 OSS 上传

	// 按数据类型缓存（不区分市场）
	orders []Order
	deals  []Deal
	ticks  []Tick

	tradingDay time.Time

	// 自动 Flush 阈值
	maxOrderBuffer int
	maxDealBuffer  int
	maxTickBuffer  int

	// 临时文件计数器
	orderSeq int
	dealSeq  int
	tickSeq  int
}

// NewOptParquetWriter 创建优化后的 Parquet 写入器
func NewOptParquetWriter(outputDir string, tradingDay time.Time, securityCache *SecurityCache, rangeChecker *DataRangeChecker, forceInt32 bool, ossConfig *OSSConfig) *OptParquetWriter {
	writer := &OptParquetWriter{
		outputDir:      outputDir,
		securityCache:  securityCache,
		rangeChecker:   rangeChecker,
		forceInt32:     forceInt32,
		tradingDay:     tradingDay,
		orders:         make([]Order, 0, 1000),
		deals:          make([]Deal, 0, 1000),
		ticks:          make([]Tick, 0, 500),
		maxOrderBuffer: 10000,
		maxDealBuffer:  10000,
		maxTickBuffer:  5000,
	}

	// 如果配置了 OSS，初始化上传器
	if ossConfig != nil && (ossConfig.AccessKeyID != "" || ossConfig.Endpoint != "") {
		uploader, err := NewOSSUploader(*ossConfig)
		if err != nil {
			log.Printf("[警告] OSS 初始化失败: %v", err)
		} else {
			writer.ossUploader = uploader
			writer.enableOSS = true
			log.Println("[OSS] OSS 上传已启用")
		}
	}

	return writer
}

// WriteOrders 缓存委托数据
func (w *OptParquetWriter) WriteOrders(orders []Order) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.orders = append(w.orders, orders...)
	if len(w.orders) >= w.maxOrderBuffer {
		if err := w.flushOrders(); err != nil {
			return err
		}
	}
	return nil
}

// WriteDeals 缓存成交数据
func (w *OptParquetWriter) WriteDeals(deals []Deal) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.deals = append(w.deals, deals...)
	if len(w.deals) >= w.maxDealBuffer {
		if err := w.flushDeals(); err != nil {
			return err
		}
	}
	return nil
}

// WriteTicks 缓存快照数据
func (w *OptParquetWriter) WriteTicks(ticks []Tick) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.ticks = append(w.ticks, ticks...)
	if len(w.ticks) >= w.maxTickBuffer {
		if err := w.flushTicks(); err != nil {
			return err
		}
	}
	return nil
}

// flushOrders 写入委托数据
func (w *OptParquetWriter) flushOrders() error {
	if len(w.orders) == 0 {
		return nil
	}
	dateStr := w.tradingDay.Format("20060102")
	w.orderSeq++
	filename := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_order_%03d.parquet", dateStr, w.orderSeq))
	if err := w.writeOrdersToParquet(w.orders, filename); err != nil {
		return fmt.Errorf("写入委托失败: %w", err)
	}
	fmt.Printf("[自动Flush] 委托: %d 条 -> %s\n", len(w.orders), filename)
	w.orders = w.orders[:0]
	return nil
}

// flushDeals 写入成交数据
func (w *OptParquetWriter) flushDeals() error {
	if len(w.deals) == 0 {
		return nil
	}
	dateStr := w.tradingDay.Format("20060102")
	w.dealSeq++
	filename := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_deal_%03d.parquet", dateStr, w.dealSeq))
	if err := w.writeDealsToParquet(w.deals, filename); err != nil {
		return fmt.Errorf("写入成交失败: %w", err)
	}
	fmt.Printf("[自动Flush] 成交: %d 条 -> %s\n", len(w.deals), filename)
	w.deals = w.deals[:0]
	return nil
}

// flushTicks 写入快照数据
func (w *OptParquetWriter) flushTicks() error {
	if len(w.ticks) == 0 {
		return nil
	}
	dateStr := w.tradingDay.Format("20060102")
	w.tickSeq++
	filename := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_tick_%03d.parquet", dateStr, w.tickSeq))
	if err := w.writeTicksToParquet(w.ticks, filename); err != nil {
		return fmt.Errorf("写入快照失败: %w", err)
	}
	fmt.Printf("[自动Flush] 快照: %d 条 -> %s\n", len(w.ticks), filename)
	w.ticks = w.ticks[:0]
	return nil
}

// Flush 将所有剩余缓存数据写入 Parquet 文件
func (w *OptParquetWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 输出检测报告
	if w.rangeChecker != nil {
		fmt.Print(w.rangeChecker.Report())
	}

	// 写入剩余数据
	if err := w.flushOrders(); err != nil {
		return err
	}
	if err := w.flushDeals(); err != nil {
		return err
	}
	if err := w.flushTicks(); err != nil {
		return err
	}

	dateStr := w.tradingDay.Format("20060102")

	fmt.Println("========================================")
	fmt.Println("[合并] 开始合并临时文件...")
	fmt.Println("========================================")

	// 合并委托数据
	orderTmpPattern := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_order_*.parquet", dateStr))
	orderFinal := filepath.Join(w.outputDir, fmt.Sprintf("%s_order.parquet", dateStr))
	if err := w.mergeParquetFilesSimple(orderTmpPattern, orderFinal, "委托"); err != nil {
		return fmt.Errorf("合并委托文件失败: %w", err)
	}

	// 合并成交数据
	dealTmpPattern := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_deal_*.parquet", dateStr))
	dealFinal := filepath.Join(w.outputDir, fmt.Sprintf("%s_deal.parquet", dateStr))
	if err := w.mergeParquetFilesSimple(dealTmpPattern, dealFinal, "成交"); err != nil {
		return fmt.Errorf("合并成交文件失败: %w", err)
	}

	// 合并快照数据
	tickTmpPattern := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_tick_*.parquet", dateStr))
	tickFinal := filepath.Join(w.outputDir, fmt.Sprintf("%s_tick.parquet", dateStr))
	if err := w.mergeParquetFilesSimple(tickTmpPattern, tickFinal, "快照"); err != nil {
		return fmt.Errorf("合并快照文件失败: %w", err)
	}

	fmt.Println("========================================")
	fmt.Println("[完成] 所有数据已合并为3个Parquet文件")
	fmt.Println("========================================")

	// OSS 上传
	if w.enableOSS && w.ossUploader != nil {
		fmt.Println("========================================")
		fmt.Println("[OSS] 开始上传到阿里云 OSS...")
		fmt.Println("========================================")

		// 上传委托数据
		if err := w.ossUploader.UploadFile(orderFinal, DataTypeOrder, w.tradingDay, ""); err != nil {
			log.Printf("[错误] 上传委托数据失败: %v", err)
		}

		// 上传成交数据
		if err := w.ossUploader.UploadFile(dealFinal, DataTypeDeal, w.tradingDay, ""); err != nil {
			log.Printf("[错误] 上传成交数据失败: %v", err)
		}

		// 上传快照数据
		if err := w.ossUploader.UploadFile(tickFinal, DataTypeTick, w.tradingDay, ""); err != nil {
			log.Printf("[错误] 上传快照数据失败: %v", err)
		}

		fmt.Println("========================================")
		fmt.Println("[OSS] 上传完成")
		fmt.Println("========================================")
	}

	return nil
}

// writeOrdersToParquet 写入委托数据（优化版）
func (w *OptParquetWriter) writeOrdersToParquet(orders []Order, filename string) error {
	if len(orders) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

	// 优化后的 Schema：删除 TradingDay, Channel；Code, Price, Volume, 等改为 Int32
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Code", Type: arrow.PrimitiveTypes.Int32},       // SECURITY_ID
			{Name: "Time", Type: arrow.PrimitiveTypes.Int64},       // UnixMicro
			{Name: "UpdateTime", Type: arrow.PrimitiveTypes.Int32}, // 微秒偏移
			{Name: "OrderID", Type: arrow.PrimitiveTypes.Int32},    // Int32
			{Name: "Side", Type: arrow.PrimitiveTypes.Int8},        // Int8
			{Name: "Price", Type: arrow.PrimitiveTypes.Int32},      // ×100
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},     // ÷100
			{Name: "OrderType", Type: arrow.PrimitiveTypes.Int8},   // Int8
			{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int32},    // Int32
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	skippedCount := 0
	for _, order := range orders {
		if err := w.checkAndConvertOrder(order, builder); err != nil {
			skippedCount++
			if skippedCount <= 3 {
				log.Printf("[警告] 跳过委托记录 [%s]: %v", order.Code, err)
			}
			continue
		}
	}

	if skippedCount > 3 {
		log.Printf("[警告] 共跳过 %d 条委托记录（数据库中不存在）", skippedCount)
	}

	record := builder.NewRecord()
	defer record.Release()

	// 写入文件（使用 Zstd 压缩）
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	return writer.Write(record)
}

// checkAndConvertOrder 检测并转换 Order 数据
func (w *OptParquetWriter) checkAndConvertOrder(order Order, builder *array.RecordBuilder) error {
	idx := 0

	// Code: String -> Int32 (SECURITY_ID)
	codeID, err := w.securityCache.GetID(order.Code)
	if err != nil {
		return fmt.Errorf("获取 SECURITY_ID 失败 [%s]: %w", order.Code, err)
	}
	builder.Field(idx).(*array.Int32Builder).Append(codeID)
	idx++

	// Time: Time -> Int64 (UnixMicro)
	builder.Field(idx).(*array.Int64Builder).Append(order.Time.UnixMicro())
	idx++

	// UpdateTime: Time -> Int32 (微秒偏移)
	offset := order.UpdateTime.Sub(order.Time).Microseconds()
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckUpdateTimeOffset("UpdateTime偏移", offset, order.Code, order); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(int32(offset))
	idx++

	// OrderID: Int64 -> Int32
	orderID := int32(order.OrderID)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("OrderID", int64(orderID), order.Code, order); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(orderID)
	idx++

	// Side: Int16 -> Int8
	builder.Field(idx).(*array.Int8Builder).Append(int8(order.Side))
	idx++

	// Price: Float64 -> Int32 (×100)
	priceInt := int32(order.Price * 100)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("Price", int64(priceInt), order.Code, order); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(priceInt)
	idx++

	// Volume: Float64 -> Int32 (÷100)
	volumeInt := int32(order.Volume / 100)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("Volume", int64(volumeInt), order.Code, order); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(volumeInt)
	idx++

	// OrderType: Int16 -> Int8
	builder.Field(idx).(*array.Int8Builder).Append(int8(order.OrderType))
	idx++

	// SeqNum: Int64 -> Int32
	seqNum := int32(order.SeqNum)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("SeqNum", int64(seqNum), order.Code, order); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(seqNum)
	idx++

	return nil
}

// writeDealsToParquet 写入成交数据（优化版）
func (w *OptParquetWriter) writeDealsToParquet(deals []Deal, filename string) error {
	if len(deals) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

	// 优化后的 Schema：删除 TradingDay, Money, Channel
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Code", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "UpdateTime", Type: arrow.PrimitiveTypes.Int32},
			{Name: "SaleOrderID", Type: arrow.PrimitiveTypes.Int32},
			{Name: "BuyOrderID", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Side", Type: arrow.PrimitiveTypes.Int8},
			{Name: "Price", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},
			{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	skippedCount := 0
	for _, deal := range deals {
		if err := w.checkAndConvertDeal(deal, builder); err != nil {
			skippedCount++
			if skippedCount <= 3 {
				log.Printf("[警告] 跳过成交记录 [%s]: %v", deal.Code, err)
			}
			continue
		}
	}

	if skippedCount > 3 {
		log.Printf("[警告] 共跳过 %d 条成交记录（数据库中不存在）", skippedCount)
	}

	record := builder.NewRecord()
	defer record.Release()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	return writer.Write(record)
}

// checkAndConvertDeal 检测并转换 Deal 数据
func (w *OptParquetWriter) checkAndConvertDeal(deal Deal, builder *array.RecordBuilder) error {
	idx := 0

	// Code: String -> Int32
	codeID, err := w.securityCache.GetID(deal.Code)
	if err != nil {
		return fmt.Errorf("获取 SECURITY_ID 失败 [%s]: %w", deal.Code, err)
	}
	builder.Field(idx).(*array.Int32Builder).Append(codeID)
	idx++

	// Time
	builder.Field(idx).(*array.Int64Builder).Append(deal.Time.UnixMicro())
	idx++

	// UpdateTime
	offset := deal.UpdateTime.Sub(deal.Time).Microseconds()
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckUpdateTimeOffset("UpdateTime偏移", offset, deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(int32(offset))
	idx++

	// SaleOrderID: Int64 -> Int32
	saleOrderID := int32(deal.SaleOrderID)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("SaleOrderID", int64(saleOrderID), deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(saleOrderID)
	idx++

	// BuyOrderID: Int64 -> Int32
	buyOrderID := int32(deal.BuyOrderID)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("BuyOrderID", int64(buyOrderID), deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(buyOrderID)
	idx++

	// Side: Int16 -> Int8
	builder.Field(idx).(*array.Int8Builder).Append(int8(deal.Side))
	idx++

	// Price: Float64 -> Int32
	priceInt := int32(deal.Price * 100)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("Price", int64(priceInt), deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(priceInt)
	idx++

	// Volume: Float64 -> Int32
	volumeInt := int32(deal.Volume / 100)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("Volume", int64(volumeInt), deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(volumeInt)
	idx++

	// SeqNum: Int64 -> Int32
	seqNum := int32(deal.SeqNum)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("SeqNum", int64(seqNum), deal.Code, deal); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(seqNum)
	idx++

	return nil
}

// writeTicksToParquet 写入快照数据（优化版）
func (w *OptParquetWriter) writeTicksToParquet(ticks []Tick, filename string) error {
	if len(ticks) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

	// 优化后的 Schema：删除 TradingDay, TotalMoney, Channel
	// 所有 Price, Volume, Num 改为 Int32
	fields := []arrow.Field{
		{Name: "Code", Type: arrow.PrimitiveTypes.Int32},
		{Name: "Time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "UpdateTime", Type: arrow.PrimitiveTypes.Int32},
		{Name: "CurrentPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "TotalVolume", Type: arrow.PrimitiveTypes.Int32},
		{Name: "PreClosePrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "OpenPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "HighestPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "LowestPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "HighLimitPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "LowLimitPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "IOPV", Type: arrow.PrimitiveTypes.Int32},
		{Name: "TradeNum", Type: arrow.PrimitiveTypes.Int32},
		{Name: "TotalBidVolume", Type: arrow.PrimitiveTypes.Int32},
		{Name: "TotalAskVolume", Type: arrow.PrimitiveTypes.Int32},
		{Name: "AvgBidPrice", Type: arrow.PrimitiveTypes.Int32},
		{Name: "AvgAskPrice", Type: arrow.PrimitiveTypes.Int32},
	}

	// 10档买卖价量
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskPrice%d", i), Type: arrow.PrimitiveTypes.Int32})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskVolume%d", i), Type: arrow.PrimitiveTypes.Int32})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskNum%d", i), Type: arrow.PrimitiveTypes.Int32})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidPrice%d", i), Type: arrow.PrimitiveTypes.Int32})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidVolume%d", i), Type: arrow.PrimitiveTypes.Int32})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidNum%d", i), Type: arrow.PrimitiveTypes.Int32})
	}

	fields = append(fields, arrow.Field{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int32})

	schema := arrow.NewSchema(fields, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	skippedCount := 0
	for _, tick := range ticks {
		if err := w.checkAndConvertTick(tick, builder); err != nil {
			skippedCount++
			if skippedCount <= 3 {
				log.Printf("[警告] 跳过快照记录 [%s]: %v", tick.Code, err)
			}
			continue
		}
	}

	if skippedCount > 3 {
		log.Printf("[警告] 共跳过 %d 条快照记录（数据库中不存在）", skippedCount)
	}

	record := builder.NewRecord()
	defer record.Release()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	return writer.Write(record)
}

// checkAndConvertTick 检测并转换 Tick 数据
func (w *OptParquetWriter) checkAndConvertTick(tick Tick, builder *array.RecordBuilder) error {
	idx := 0

	// Code: String -> Int32
	codeID, err := w.securityCache.GetID(tick.Code)
	if err != nil {
		return fmt.Errorf("获取 SECURITY_ID 失败 [%s]: %w", tick.Code, err)
	}
	builder.Field(idx).(*array.Int32Builder).Append(codeID)
	idx++

	// Time
	builder.Field(idx).(*array.Int64Builder).Append(tick.Time.UnixMicro())
	idx++

	// UpdateTime
	offset := tick.UpdateTime.Sub(tick.Time).Microseconds()
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckUpdateTimeOffset("UpdateTime偏移", offset, tick.Code, tick); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(int32(offset))
	idx++

	// 价格字段 (×100)
	priceFields := []float64{
		tick.CurrentPrice, tick.PreClosePrice, tick.OpenPrice,
		tick.HighestPrice, tick.LowestPrice, tick.HighLimitPrice,
		tick.LowLimitPrice, tick.IOPV, tick.AvgBidPrice, tick.AvgAskPrice,
		tick.AskPrice1, tick.AskPrice2, tick.AskPrice3, tick.AskPrice4, tick.AskPrice5,
		tick.AskPrice6, tick.AskPrice7, tick.AskPrice8, tick.AskPrice9, tick.AskPrice10,
		tick.BidPrice1, tick.BidPrice2, tick.BidPrice3, tick.BidPrice4, tick.BidPrice5,
		tick.BidPrice6, tick.BidPrice7, tick.BidPrice8, tick.BidPrice9, tick.BidPrice10,
	}
	for _, price := range priceFields {
		priceInt := int32(price * 100)
		builder.Field(idx).(*array.Int32Builder).Append(priceInt)
		idx++
	}

	// 数量字段 (÷100)
	volumeFields := []float64{
		tick.TotalVolume, tick.TotalBidVolume, tick.TotalAskVolume,
		tick.AskVolume1, tick.AskVolume2, tick.AskVolume3, tick.AskVolume4, tick.AskVolume5,
		tick.AskVolume6, tick.AskVolume7, tick.AskVolume8, tick.AskVolume9, tick.AskVolume10,
		tick.BidVolume1, tick.BidVolume2, tick.BidVolume3, tick.BidVolume4, tick.BidVolume5,
		tick.BidVolume6, tick.BidVolume7, tick.BidVolume8, tick.BidVolume9, tick.BidVolume10,
	}
	for _, volume := range volumeFields {
		volumeInt := int32(volume / 100)
		if !w.forceInt32 && w.rangeChecker != nil {
			if err := w.rangeChecker.CheckInt32("Volume", int64(volumeInt), tick.Code, tick); err != nil {
				return err
			}
		}
		builder.Field(idx).(*array.Int32Builder).Append(volumeInt)
		idx++
	}

	// Num 字段
	numFields := []float64{
		tick.TradeNum,
		tick.AskNum1, tick.AskNum2, tick.AskNum3, tick.AskNum4, tick.AskNum5,
		tick.AskNum6, tick.AskNum7, tick.AskNum8, tick.AskNum9, tick.AskNum10,
		tick.BidNum1, tick.BidNum2, tick.BidNum3, tick.BidNum4, tick.BidNum5,
		tick.BidNum6, tick.BidNum7, tick.BidNum8, tick.BidNum9, tick.BidNum10,
	}
	for _, num := range numFields {
		numInt := int32(num)
		if !w.forceInt32 && w.rangeChecker != nil {
			if err := w.rangeChecker.CheckInt32("Num", int64(numInt), tick.Code, tick); err != nil {
				return err
			}
		}
		builder.Field(idx).(*array.Int32Builder).Append(numInt)
		idx++
	}

	// SeqNum: Int64 -> Int32
	seqNum := int32(tick.SeqNum)
	if !w.forceInt32 && w.rangeChecker != nil {
		if err := w.rangeChecker.CheckInt32("SeqNum", int64(seqNum), tick.Code, tick); err != nil {
			return err
		}
	}
	builder.Field(idx).(*array.Int32Builder).Append(seqNum)
	idx++

	return nil
}

// mergeParquetFilesSimple 合并临时 Parquet 文件
func (w *OptParquetWriter) mergeParquetFilesSimple(pattern, outputFile, dataType string) error {
	tmpFiles, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(tmpFiles) == 0 {
		fmt.Printf("[跳过] %s: 没有临时文件\n", dataType)
		return nil
	}

	fmt.Printf("[合并] %s: %d 个临时文件 -> %s\n", dataType, len(tmpFiles), outputFile)

	if len(tmpFiles) == 1 {
		return os.Rename(tmpFiles[0], outputFile)
	}

	mem := memory.NewGoAllocator()

	// 读取第一个文件获取 Schema
	firstFH, err := os.Open(tmpFiles[0])
	if err != nil {
		return fmt.Errorf("打开第一个文件失败: %w", err)
	}

	firstFile, err := file.NewParquetReader(firstFH, file.WithReadProps(&parquet.ReaderProperties{}))
	if err != nil {
		firstFH.Close()
		return fmt.Errorf("创建 parquet reader 失败: %w", err)
	}

	pqReader, err := pqarrow.NewFileReader(firstFile, pqarrow.ArrowReadProperties{BatchSize: 128}, mem)
	if err != nil {
		firstFH.Close()
		return fmt.Errorf("创建 arrow reader 失败: %w", err)
	}

	schema, err := pqReader.Schema()
	if err != nil {
		firstFH.Close()
		return err
	}
	firstFH.Close()

	// 创建输出文件
	outFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// 创建 Writer（使用 Zstd）
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, outFile, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	// 逐个读取并写入
	totalRows := int64(0)
	for idx, tmpPath := range tmpFiles {
		fh, err := os.Open(tmpPath)
		if err != nil {
			return fmt.Errorf("打开文件 %s 失败: %w", tmpPath, err)
		}

		pqFile, err := file.NewParquetReader(fh, file.WithReadProps(&parquet.ReaderProperties{}))
		if err != nil {
			fh.Close()
			return fmt.Errorf("创建 parquet reader 失败: %w", err)
		}

		reader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{Parallel: true}, mem)
		if err != nil {
			fh.Close()
			return fmt.Errorf("创建 arrow reader 失败: %w", err)
		}

		table, err := reader.ReadTable(context.Background())
		if err != nil {
			table.Release()
			fh.Close()
			return fmt.Errorf("读取表失败: %w", err)
		}

		if err := writer.WriteTable(table, table.NumRows()); err != nil {
			table.Release()
			fh.Close()
			return fmt.Errorf("写入表失败: %w", err)
		}

		totalRows += table.NumRows()
		table.Release()
		fh.Close()

		os.Remove(tmpPath)

		if (idx+1)%50 == 0 {
			fmt.Printf("[进度] %s: 已合并 %d/%d 文件\n", dataType, idx+1, len(tmpFiles))
		}
	}

	fmt.Printf("[完成] %s: %d 条记录\n", dataType, totalRows)
	return nil
}
