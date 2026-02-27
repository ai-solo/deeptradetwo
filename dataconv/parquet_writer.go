package dataconv

import (
	"context"
	"fmt"
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

// ParquetWriter Parquet文件写入器（流式写入，限制内存，合并市场）
type ParquetWriter struct {
	outputDir string
	mu        sync.Mutex
	
	// 按数据类型缓存（不区分市场）
	orders []Order
	deals  []Deal
	ticks  []Tick
	
	tradingDay time.Time
	
	// 自动Flush阈值（避免内存累积）
	maxOrderBuffer int
	maxDealBuffer  int
	maxTickBuffer  int
	
	// 临时文件计数器
	orderSeq int
	dealSeq  int
	tickSeq  int
}

// NewParquetWriter 创建Parquet写入器（流式写入，限制内存，合并市场）
func NewParquetWriter(outputDir string, tradingDay time.Time) *ParquetWriter {
	return &ParquetWriter{
		outputDir:  outputDir,
		tradingDay: tradingDay,
		orders:     make([]Order, 0, 5000),
		deals:      make([]Deal, 0, 5000),
		ticks:      make([]Tick, 0, 2000),
		// 自动Flush阈值（降低到5万/1万，避免2GB内存OOM）
		maxOrderBuffer: 50000,
		maxDealBuffer:  50000,
		maxTickBuffer:  10000,
	}
}

// WriteOrders 缓存委托数据（自动flush避免OOM，不区分市场）
func (w *ParquetWriter) WriteOrders(orders []Order) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.orders = append(w.orders, orders...)
	// 达到阈值，立即写入并清空
	if len(w.orders) >= w.maxOrderBuffer {
		if err := w.flushOrders(); err != nil {
			return err
		}
	}
	return nil
}

// WriteDeals 缓存成交数据（自动flush避免OOM，不区分市场）
func (w *ParquetWriter) WriteDeals(deals []Deal) error {
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

// WriteTicks 缓存快照数据（自动flush避免OOM，不区分市场）
func (w *ParquetWriter) WriteTicks(ticks []Tick) error {
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

// flushOrders 写入委托并清空buffer（使用临时文件）
func (w *ParquetWriter) flushOrders() error {
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
	w.orders = w.orders[:0] // 清空但保留容量
	return nil
}

func (w *ParquetWriter) flushDeals() error {
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

func (w *ParquetWriter) flushTicks() error {
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

// Flush 将所有剩余缓存数据写入Parquet文件，并重命名为最终文件
func (w *ParquetWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// 写入剩余数据（如果有）
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
	
	return nil
}

// mergeParquetFiles 合并临时Parquet文件（使用泛型接口）
func (w *ParquetWriter) mergeParquetFiles(dateStr, market, dataType string, readFunc interface{}, writeFunc interface{}) error {
	// 查找所有临时文件
	pattern := filepath.Join(w.outputDir, fmt.Sprintf(".tmp_%s_%s_%s_*.parquet", dateStr, market, dataType))
	tmpFiles, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	
	if len(tmpFiles) == 0 {
		return nil // 没有临时文件，跳过
	}
	
	finalFile := filepath.Join(w.outputDir, fmt.Sprintf("%s_%s_%s.parquet", dateStr, market, dataType))
	
	// 根据数据类型选择合并逻辑
	switch dataType {
	case "order":
		return w.mergeOrderFiles(tmpFiles, finalFile)
	case "deal":
		return w.mergeDealFiles(tmpFiles, finalFile)
	case "tick":
		return w.mergeTickFiles(tmpFiles, finalFile)
	}
	
	return fmt.Errorf("未知数据类型: %s", dataType)
}

func (w *ParquetWriter) mergeOrderFiles(tmpFiles []string, finalFile string) error {
	var allOrders []Order
	totalCount := 0
	
	for _, tmpFile := range tmpFiles {
		orders, err := w.readOrdersFromParquet(tmpFile)
		if err != nil {
			return fmt.Errorf("读取临时文件 %s 失败: %w", tmpFile, err)
		}
		allOrders = append(allOrders, orders...)
		totalCount += len(orders)
		os.Remove(tmpFile) // 删除临时文件
	}
	
	if err := w.writeOrdersToParquet(allOrders, finalFile); err != nil {
		return err
	}
	
	fmt.Printf("[输出] %s (%d 条)\n", finalFile, totalCount)
	return nil
}

func (w *ParquetWriter) mergeDealFiles(tmpFiles []string, finalFile string) error {
	var allDeals []Deal
	totalCount := 0
	
	for _, tmpFile := range tmpFiles {
		deals, err := w.readDealsFromParquet(tmpFile)
		if err != nil {
			return fmt.Errorf("读取临时文件 %s 失败: %w", tmpFile, err)
		}
		allDeals = append(allDeals, deals...)
		totalCount += len(deals)
		os.Remove(tmpFile)
	}
	
	if err := w.writeDealsToParquet(allDeals, finalFile); err != nil {
		return err
	}
	
	fmt.Printf("[输出] %s (%d 条)\n", finalFile, totalCount)
	return nil
}

func (w *ParquetWriter) mergeTickFiles(tmpFiles []string, finalFile string) error {
	var allTicks []Tick
	totalCount := 0
	
	for _, tmpFile := range tmpFiles {
		ticks, err := w.readTicksFromParquet(tmpFile)
		if err != nil {
			return fmt.Errorf("读取临时文件 %s 失败: %w", tmpFile, err)
		}
		allTicks = append(allTicks, ticks...)
		totalCount += len(ticks)
		os.Remove(tmpFile)
	}
	
	if err := w.writeTicksToParquet(allTicks, finalFile); err != nil {
		return err
	}
	
	fmt.Printf("[输出] %s (%d 条)\n", finalFile, totalCount)
	return nil
}

// writeOrdersToParquet 写入委托数据到Parquet文件
func (w *ParquetWriter) writeOrdersToParquet(orders []Order, filename string) error {
	if len(orders) == 0 {
		return nil
	}
	
	mem := memory.NewGoAllocator()
	
	// 定义Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "TradingDay", Type: arrow.BinaryTypes.String},
			{Name: "Code", Type: arrow.BinaryTypes.String},
			{Name: "Time", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "UpdateTime", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "OrderID", Type: arrow.PrimitiveTypes.Int64},
			{Name: "Side", Type: arrow.PrimitiveTypes.Int16},
			{Name: "Price", Type: arrow.PrimitiveTypes.Float64},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Float64},
			{Name: "OrderType", Type: arrow.PrimitiveTypes.Int16},
			{Name: "Channel", Type: arrow.PrimitiveTypes.Int64},
			{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	
	// 构建数据
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	for _, order := range orders {
		builder.Field(0).(*array.StringBuilder).Append(order.TradingDay.Format("2006-01-02"))
		builder.Field(1).(*array.StringBuilder).Append(order.Code)
		builder.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(order.Time.UnixMicro()))
		builder.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(order.UpdateTime.UnixMicro()))
		builder.Field(4).(*array.Int64Builder).Append(order.OrderID)
		builder.Field(5).(*array.Int16Builder).Append(order.Side)
		builder.Field(6).(*array.Float64Builder).Append(order.Price)
		builder.Field(7).(*array.Float64Builder).Append(order.Volume)
		builder.Field(8).(*array.Int16Builder).Append(order.OrderType)
		builder.Field(9).(*array.Int64Builder).Append(order.Channel)
		builder.Field(10).(*array.Int64Builder).Append(order.SeqNum)
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	// 写入文件
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()
	
	return writer.Write(record)
}

// writeDealsToParquet 写入成交数据到Parquet文件
func (w *ParquetWriter) writeDealsToParquet(deals []Deal, filename string) error {
	if len(deals) == 0 {
		return nil
	}
	
	mem := memory.NewGoAllocator()
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "TradingDay", Type: arrow.BinaryTypes.String},
			{Name: "Code", Type: arrow.BinaryTypes.String},
			{Name: "Time", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "UpdateTime", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "SaleOrderID", Type: arrow.PrimitiveTypes.Int64},
			{Name: "BuyOrderID", Type: arrow.PrimitiveTypes.Int64},
			{Name: "Side", Type: arrow.PrimitiveTypes.Int16},
			{Name: "Price", Type: arrow.PrimitiveTypes.Float64},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Float64},
			{Name: "Money", Type: arrow.PrimitiveTypes.Float64},
			{Name: "Channel", Type: arrow.PrimitiveTypes.Int64},
			{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	for _, deal := range deals {
		builder.Field(0).(*array.StringBuilder).Append(deal.TradingDay.Format("2006-01-02"))
		builder.Field(1).(*array.StringBuilder).Append(deal.Code)
		builder.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(deal.Time.UnixMicro()))
		builder.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(deal.UpdateTime.UnixMicro()))
		builder.Field(4).(*array.Int64Builder).Append(deal.SaleOrderID)
		builder.Field(5).(*array.Int64Builder).Append(deal.BuyOrderID)
		builder.Field(6).(*array.Int16Builder).Append(deal.Side)
		builder.Field(7).(*array.Float64Builder).Append(deal.Price)
		builder.Field(8).(*array.Float64Builder).Append(deal.Volume)
		builder.Field(9).(*array.Float64Builder).Append(deal.Money)
		builder.Field(10).(*array.Int64Builder).Append(deal.Channel)
		builder.Field(11).(*array.Int64Builder).Append(deal.SeqNum)
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()
	
	return writer.Write(record)
}

// writeTicksToParquet 写入快照数据到Parquet文件
func (w *ParquetWriter) writeTicksToParquet(ticks []Tick, filename string) error {
	if len(ticks) == 0 {
		return nil
	}
	
	mem := memory.NewGoAllocator()
	
	// Tick的schema比较复杂，包含很多字段
	fields := []arrow.Field{
		{Name: "TradingDay", Type: arrow.BinaryTypes.String},
		{Name: "Code", Type: arrow.BinaryTypes.String},
		{Name: "Time", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "UpdateTime", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "CurrentPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "TotalVolume", Type: arrow.PrimitiveTypes.Float64},
		{Name: "TotalMoney", Type: arrow.PrimitiveTypes.Float64},
		{Name: "PreClosePrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "OpenPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "HighestPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LowestPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "HighLimitPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LowLimitPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "IOPV", Type: arrow.PrimitiveTypes.Float64},
		{Name: "TradeNum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "TotalBidVolume", Type: arrow.PrimitiveTypes.Float64},
		{Name: "TotalAskVolume", Type: arrow.PrimitiveTypes.Float64},
		{Name: "AvgBidPrice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "AvgAskPrice", Type: arrow.PrimitiveTypes.Float64},
	}
	
	// 添加10档买卖价量
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskPrice%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskVolume%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("AskNum%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidPrice%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidVolume%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	for i := 1; i <= 10; i++ {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("BidNum%d", i), Type: arrow.PrimitiveTypes.Float64})
	}
	
	fields = append(fields, arrow.Field{Name: "Channel", Type: arrow.PrimitiveTypes.Int64})
	fields = append(fields, arrow.Field{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int64})
	
	schema := arrow.NewSchema(fields, nil)
	
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	for _, tick := range ticks {
		idx := 0
		builder.Field(idx).(*array.StringBuilder).Append(tick.TradingDay.Format("2006-01-02")); idx++
		builder.Field(idx).(*array.StringBuilder).Append(tick.Code); idx++
		builder.Field(idx).(*array.TimestampBuilder).Append(arrow.Timestamp(tick.Time.UnixMicro())); idx++
		builder.Field(idx).(*array.TimestampBuilder).Append(arrow.Timestamp(tick.UpdateTime.UnixMicro())); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.CurrentPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.TotalVolume); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.TotalMoney); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.PreClosePrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.OpenPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.HighestPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.LowestPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.HighLimitPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.LowLimitPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.IOPV); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.TradeNum); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.TotalBidVolume); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.TotalAskVolume); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AvgBidPrice); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AvgAskPrice); idx++
		
		// 10档卖价
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskPrice10); idx++
		
		// 10档卖量
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskVolume10); idx++
		
		// 10档卖单数
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.AskNum10); idx++
		
		// 10档买价
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidPrice10); idx++
		
		// 10档买量
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidVolume10); idx++
		
		// 10档买单数
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum1); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum2); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum3); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum4); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum5); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum6); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum7); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum8); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum9); idx++
		builder.Field(idx).(*array.Float64Builder).Append(tick.BidNum10); idx++
		
		builder.Field(idx).(*array.Int64Builder).Append(tick.Channel); idx++
		builder.Field(idx).(*array.Int64Builder).Append(tick.SeqNum); idx++
	}
	
	record := builder.NewRecord()
	defer record.Release()
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()
	
	return writer.Write(record)
}

// readOrdersFromParquet 从Parquet文件读取委托数据（用于合并临时文件）
func (w *ParquetWriter) readOrdersFromParquet(filename string) ([]Order, error) {
	// 简化版：不实际读取，因为我们改用不合并策略
	return nil, fmt.Errorf("readOrdersFromParquet 暂未实现，请使用单文件写入模式")
}

// readDealsFromParquet 从Parquet文件读取成交数据
func (w *ParquetWriter) readDealsFromParquet(filename string) ([]Deal, error) {
	return nil, fmt.Errorf("readDealsFromParquet 暂未实现，请使用单文件写入模式")
}

// readTicksFromParquet 从Parquet文件读取快照数据
func (w *ParquetWriter) readTicksFromParquet(filename string) ([]Tick, error) {
	return nil, fmt.Errorf("readTicksFromParquet 暂未实现，请使用单文件写入模式")
}

// mergeParquetFilesSimple 合并多个Parquet文件
func (w *ParquetWriter) mergeParquetFilesSimple(pattern, outputFile, dataType string) error {
	// 查找所有临时文件
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
		// 只有一个文件，直接重命名
		return os.Rename(tmpFiles[0], outputFile)
	}
	
	// 使用Arrow合并
	mem := memory.NewGoAllocator()
	
	// 读取第一个文件获取Schema
	firstFH, err := os.Open(tmpFiles[0])
	if err != nil {
		return fmt.Errorf("打开第一个文件失败: %w", err)
	}
	
	firstFile, err := file.NewParquetReader(firstFH, file.WithReadProps(&parquet.ReaderProperties{}))
	if err != nil {
		firstFH.Close()
		return fmt.Errorf("创建parquet reader失败: %w", err)
	}
	
	pqReader, err := pqarrow.NewFileReader(firstFile, pqarrow.ArrowReadProperties{BatchSize: 128}, mem)
	if err != nil {
		firstFH.Close()
		return fmt.Errorf("创建arrow reader失败: %w", err)
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
	
	// 创建Writer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
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
			return fmt.Errorf("创建parquet reader失败: %w", err)
		}
		
		reader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{Parallel: true}, mem)
		if err != nil {
			fh.Close()
			return fmt.Errorf("创建arrow reader失败: %w", err)
		}
		
		// 读取整个表
		table, err := reader.ReadTable(context.Background())
		if err != nil {
			fh.Close()
			return fmt.Errorf("读取表失败: %w", err)
		}
		
		// 写入（使用表的全部行数）
		if err := writer.WriteTable(table, table.NumRows()); err != nil {
			table.Release()
			fh.Close()
			return fmt.Errorf("写入表失败: %w", err)
		}
		
		totalRows += table.NumRows()
		table.Release()
		fh.Close()
		
		// 删除临时文件
		os.Remove(tmpPath)
		
		if (idx+1) % 50 == 0 {
			fmt.Printf("[进度] %s: 已合并 %d/%d 文件\n", dataType, idx+1, len(tmpFiles))
		}
	}
	
	fmt.Printf("[完成] %s: %d 条记录\n", dataType, totalRows)
	return nil
}
