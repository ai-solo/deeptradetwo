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

// WriteAllDirect 接收已排好序的全量数据，直接写出三个 parquet 文件，不走临时文件
// 调用前数据必须已按 Code+SeqNum 排好序
func (w *OptParquetWriter) WriteAllDirect(orders []Order, deals []Deal, ticks []Tick) error {
	dateStr := w.tradingDay.Format("20060102")

	orderFile := filepath.Join(w.outputDir, fmt.Sprintf("%s_order.parquet", dateStr))
	dealFile := filepath.Join(w.outputDir, fmt.Sprintf("%s_deal.parquet", dateStr))
	tickFile := filepath.Join(w.outputDir, fmt.Sprintf("%s_tick.parquet", dateStr))

	// 并发写出三个 parquet 文件
	type writeTask struct {
		label string
		fn    func() error
	}
	tasks := []writeTask{
		{"委托", func() error {
			log.Printf("[直接写入] 委托 %d 条 -> %s", len(orders), orderFile)
			return w.writeOrdersByCodeRowGroups(orders, orderFile)
		}},
		{"成交", func() error {
			log.Printf("[直接写入] 成交 %d 条 -> %s", len(deals), dealFile)
			return w.writeDealsByCodeRowGroups(deals, dealFile)
		}},
		{"快照", func() error {
			log.Printf("[直接写入] 快照 %d 条 -> %s", len(ticks), tickFile)
			return w.writeTicksByCodeRowGroups(ticks, tickFile)
		}},
	}

	writeErrs := make([]error, len(tasks))
	var wg sync.WaitGroup
	for i, t := range tasks {
		i, t := i, t
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := t.fn(); err != nil {
				writeErrs[i] = fmt.Errorf("写入%s失败: %w", t.label, err)
			}
		}()
	}
	wg.Wait()
	for _, err := range writeErrs {
		if err != nil {
			return err
		}
	}

	fmt.Printf("[输出] %s (%d 条)\n", orderFile, len(orders))
	fmt.Printf("[输出] %s (%d 条)\n", dealFile, len(deals))
	fmt.Printf("[输出] %s (%d 条)\n", tickFile, len(ticks))

	// OSS 并发上传
	if w.enableOSS && w.ossUploader != nil {
		ossFiles := []struct {
			path string
			dt   DataType
		}{
			{orderFile, DataTypeOrder},
			{dealFile, DataTypeDeal},
			{tickFile, DataTypeTick},
		}
		ossErrs := make([]error, len(ossFiles))
		var ossWg sync.WaitGroup
		for i, f := range ossFiles {
			i, f := i, f
			ossWg.Add(1)
			go func() {
				defer ossWg.Done()
				log.Printf("[OSS] 上传 %s...", f.path)
				if err := w.ossUploader.UploadFile(f.path, f.dt, w.tradingDay, ""); err != nil {
					ossErrs[i] = fmt.Errorf("OSS上传失败 [%s]: %w", f.path, err)
				}
			}()
		}
		ossWg.Wait()
		for _, err := range ossErrs {
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ========== Parquet 读取（用于 OSS 重排序路径）==========

// ReadOrdersFromParquet 从 parquet 文件读取委托数据
func ReadOrdersFromParquet(filename string, tradingDay time.Time, secCache *SecurityCache) ([]Order, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer fh.Close()

	pqReader, err := file.NewParquetReader(fh)
	if err != nil {
		return nil, fmt.Errorf("创建 parquet reader 失败: %w", err)
	}

	mem := memory.NewGoAllocator()
	arReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 65536}, mem)
	if err != nil {
		return nil, fmt.Errorf("创建 arrow reader 失败: %w", err)
	}

	tbl, err := arReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("读取 table 失败: %w", err)
	}
	defer tbl.Release()

	nRows := int(tbl.NumRows())
	orders := make([]Order, 0, nRows)

	// 列索引按 schema 顺序: Code(0) Time(1) UpdateTime(2) OrderID(3) Side(4) Price(5) Volume(6) OrderType(7) SeqNum(8)
	getCol := func(name string) arrow.Array {
		for i := 0; i < int(tbl.NumCols()); i++ {
			if tbl.Schema().Field(i).Name == name {
				return tbl.Column(i).Data().Chunk(0)
			}
		}
		return nil
	}

	codeCol := getCol("Code").(*array.Int32)
	timeCol := getCol("Time").(*array.Int64)
	updateCol := getCol("UpdateTime").(*array.Int32)
	orderIDCol := getCol("OrderID").(*array.Int32)
	sideCol := getCol("Side").(*array.Int8)
	priceCol := getCol("Price").(*array.Int32)
	volumeCol := getCol("Volume").(*array.Int32)
	orderTypeCol := getCol("OrderType").(*array.Int8)
	seqNumCol := getCol("SeqNum").(*array.Int32)

	for i := 0; i < nRows; i++ {
		codeID := codeCol.Value(i)
		code := ""
		if secCache != nil {
			code, _ = secCache.GetCode(codeID)
		} else {
			code = fmt.Sprintf("%06d", codeID)
		}
		tUnix := timeCol.Value(i)
		t := time.UnixMicro(tUnix).In(time.Local)
		offsetUS := int64(updateCol.Value(i))
		updateTime := t.Add(time.Duration(offsetUS) * time.Microsecond)

		orders = append(orders, Order{
			TradingDay: tradingDay,
			Code:       code,
			Time:       t,
			UpdateTime: updateTime,
			OrderID:    int64(orderIDCol.Value(i)),
			Side:       int16(sideCol.Value(i)),
			Price:      float64(priceCol.Value(i)) / 100,
			Volume:     float64(volumeCol.Value(i)) * 100,
			OrderType:  int16(orderTypeCol.Value(i)),
			SeqNum:     int64(seqNumCol.Value(i)),
		})
	}

	return orders, nil
}

// ReadDealsFromParquet 从 parquet 文件读取成交数据
func ReadDealsFromParquet(filename string, tradingDay time.Time, secCache *SecurityCache) ([]Deal, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer fh.Close()

	pqReader, err := file.NewParquetReader(fh)
	if err != nil {
		return nil, fmt.Errorf("创建 parquet reader 失败: %w", err)
	}

	mem := memory.NewGoAllocator()
	arReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 65536}, mem)
	if err != nil {
		return nil, fmt.Errorf("创建 arrow reader 失败: %w", err)
	}

	tbl, err := arReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("读取 table 失败: %w", err)
	}
	defer tbl.Release()

	nRows := int(tbl.NumRows())
	deals := make([]Deal, 0, nRows)

	getCol := func(name string) arrow.Array {
		for i := 0; i < int(tbl.NumCols()); i++ {
			if tbl.Schema().Field(i).Name == name {
				return tbl.Column(i).Data().Chunk(0)
			}
		}
		return nil
	}

	codeCol := getCol("Code").(*array.Int32)
	timeCol := getCol("Time").(*array.Int64)
	updateCol := getCol("UpdateTime").(*array.Int32)
	saleCol := getCol("SaleOrderID").(*array.Int32)
	buyCol := getCol("BuyOrderID").(*array.Int32)
	sideCol := getCol("Side").(*array.Int8)
	priceCol := getCol("Price").(*array.Int32)
	volumeCol := getCol("Volume").(*array.Int32)
	seqNumCol := getCol("SeqNum").(*array.Int32)

	for i := 0; i < nRows; i++ {
		codeID := codeCol.Value(i)
		code := ""
		if secCache != nil {
			code, _ = secCache.GetCode(codeID)
		} else {
			code = fmt.Sprintf("%06d", codeID)
		}
		tUnix := timeCol.Value(i)
		t := time.UnixMicro(tUnix).In(time.Local)
		offsetUS := int64(updateCol.Value(i))
		updateTime := t.Add(time.Duration(offsetUS) * time.Microsecond)

		deals = append(deals, Deal{
			TradingDay:  tradingDay,
			Code:        code,
			Time:        t,
			UpdateTime:  updateTime,
			SaleOrderID: int64(saleCol.Value(i)),
			BuyOrderID:  int64(buyCol.Value(i)),
			Side:        int16(sideCol.Value(i)),
			Price:       float64(priceCol.Value(i)) / 100,
			Volume:      float64(volumeCol.Value(i)) * 100,
			SeqNum:      int64(seqNumCol.Value(i)),
		})
	}

	return deals, nil
}

// ReadTicksFromParquet 从 parquet 文件读取快照数据（字段较多，按名称查找）
func ReadTicksFromParquet(filename string, tradingDay time.Time, secCache *SecurityCache) ([]Tick, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer fh.Close()

	pqReader, err := file.NewParquetReader(fh)
	if err != nil {
		return nil, fmt.Errorf("创建 parquet reader 失败: %w", err)
	}

	mem := memory.NewGoAllocator()
	arReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 65536}, mem)
	if err != nil {
		return nil, fmt.Errorf("创建 arrow reader 失败: %w", err)
	}

	tbl, err := arReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("读取 table 失败: %w", err)
	}
	defer tbl.Release()

	nRows := int(tbl.NumRows())
	ticks := make([]Tick, 0, nRows)

	// 建立列名->索引映射
	colMap := make(map[string]int, tbl.NumCols())
	for i := 0; i < int(tbl.NumCols()); i++ {
		colMap[tbl.Schema().Field(i).Name] = i
	}

	getInt32Col := func(name string) *array.Int32 {
		i, ok := colMap[name]
		if !ok {
			return nil
		}
		c := tbl.Column(i).Data().Chunk(0)
		if c == nil {
			return nil
		}
		return c.(*array.Int32)
	}
	getInt64Col := func(name string) *array.Int64 {
		i, ok := colMap[name]
		if !ok {
			return nil
		}
		c := tbl.Column(i).Data().Chunk(0)
		if c == nil {
			return nil
		}
		return c.(*array.Int64)
	}

	codeCol := getInt32Col("Code")
	timeCol := getInt64Col("Time")
	updateCol := getInt32Col("UpdateTime")
	seqNumCol := getInt32Col("SeqNum")

	currentPriceCol := getInt32Col("CurrentPrice")
	totalVolumeCol := getInt32Col("TotalVolume")
	totalMoneyCol := getInt32Col("TotalMoney")
	preCloseCol := getInt32Col("PreClosePrice")
	openCol := getInt32Col("OpenPrice")
	highestCol := getInt32Col("HighestPrice")
	lowestCol := getInt32Col("LowestPrice")
	highLimitCol := getInt32Col("HighLimitPrice")
	lowLimitCol := getInt32Col("LowLimitPrice")
	iopvCol := getInt32Col("IOPV")
	tradeNumCol := getInt32Col("TradeNum")
	totalBidVolCol := getInt32Col("TotalBidVolume")
	totalAskVolCol := getInt32Col("TotalAskVolume")
	avgBidCol := getInt32Col("AvgBidPrice")
	avgAskCol := getInt32Col("AvgAskPrice")

	getI32 := func(col *array.Int32, i int) int32 {
		if col == nil {
			return 0
		}
		return col.Value(i)
	}

	for i := 0; i < nRows; i++ {
		codeID := codeCol.Value(i)
		code := ""
		if secCache != nil {
			code, _ = secCache.GetCode(codeID)
		} else {
			code = fmt.Sprintf("%06d", codeID)
		}
		tUnix := timeCol.Value(i)
		t := time.UnixMicro(tUnix).In(time.Local)
		offsetUS := int64(updateCol.Value(i))
		updateTime := t.Add(time.Duration(offsetUS) * time.Microsecond)

		tick := Tick{
			TradingDay:     tradingDay,
			Code:           code,
			Time:           t,
			UpdateTime:     updateTime,
			CurrentPrice:   float64(getI32(currentPriceCol, i)) / 100,
			TotalVolume:    float64(getI32(totalVolumeCol, i)) * 100,
			TotalMoney:     float64(getI32(totalMoneyCol, i)),
			PreClosePrice:  float64(getI32(preCloseCol, i)) / 100,
			OpenPrice:      float64(getI32(openCol, i)) / 100,
			HighestPrice:   float64(getI32(highestCol, i)) / 100,
			LowestPrice:    float64(getI32(lowestCol, i)) / 100,
			HighLimitPrice: float64(getI32(highLimitCol, i)) / 100,
			LowLimitPrice:  float64(getI32(lowLimitCol, i)) / 100,
			IOPV:           float64(getI32(iopvCol, i)) / 100,
			TradeNum:       float64(getI32(tradeNumCol, i)),
			TotalBidVolume: float64(getI32(totalBidVolCol, i)) * 100,
			TotalAskVolume: float64(getI32(totalAskVolCol, i)) * 100,
			AvgBidPrice:    float64(getI32(avgBidCol, i)) / 100,
			AvgAskPrice:    float64(getI32(avgAskCol, i)) / 100,
			SeqNum:         int64(getI32(seqNumCol, i)),
		}

		// 10档价量
		for lv := 1; lv <= 10; lv++ {
			setTickLevel(&tick, lv,
				float64(getI32(getInt32Col(fmt.Sprintf("BidPrice%d", lv)), i))/100,
				float64(getI32(getInt32Col(fmt.Sprintf("BidVolume%d", lv)), i))*100,
				float64(getI32(getInt32Col(fmt.Sprintf("BidNum%d", lv)), i)),
				float64(getI32(getInt32Col(fmt.Sprintf("AskPrice%d", lv)), i))/100,
				float64(getI32(getInt32Col(fmt.Sprintf("AskVolume%d", lv)), i))*100,
				float64(getI32(getInt32Col(fmt.Sprintf("AskNum%d", lv)), i)),
			)
		}

		ticks = append(ticks, tick)
	}

	return ticks, nil
}

// rowGroupSize 每个 RowGroup 的目标行数。同一个 Code 不会跨 RowGroup，
// 因此实际大小可能略超此值，但 DuckDB 的 min/max pruning 仍然有效。
const rowGroupSize = 50_000

// ========== 按 Code 分 RowGroup 写入 ==========

// writeOrdersByCodeRowGroups 将已按 Code 排好序的委托数据写入 parquet。
// 每个 RowGroup 包含约 rowGroupSize 条记录，同一 Code 不跨 RowGroup。
func (w *OptParquetWriter) writeOrdersByCodeRowGroups(orders []Order, filename string) error {
	if len(orders) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Code", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "UpdateTime", Type: arrow.PrimitiveTypes.Int32},
			{Name: "OrderID", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Side", Type: arrow.PrimitiveTypes.Int8},
			{Name: "Price", Type: arrow.PrimitiveTypes.Int32},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},
			{Name: "OrderType", Type: arrow.PrimitiveTypes.Int8},
			{Name: "SeqNum", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	skippedTotal := 0
	i := 0
	for i < len(orders) {
		// 积累多个 Code，直到凑满 rowGroupSize 条（但同一 Code 不跨 RowGroup）
		builder := array.NewRecordBuilder(mem, schema)
		groupRows := 0
		for i < len(orders) {
			curCode := orders[i].Code
			j := i
			for j < len(orders) && orders[j].Code == curCode {
				j++
			}
			skipped := 0
			for _, o := range orders[i:j] {
				if err := w.checkAndConvertOrder(o, builder); err != nil {
					skipped++
					if skipped <= 3 {
						log.Printf("[警告] 跳过委托记录 [%s]: %v", o.Code, err)
					}
				}
			}
			skippedTotal += skipped
			groupRows += j - i
			i = j
			if groupRows >= rowGroupSize {
				break
			}
		}
		record := builder.NewRecord()
		if record.NumRows() > 0 {
			if err := writer.Write(record); err != nil {
				record.Release()
				builder.Release()
				return fmt.Errorf("写入委托 RowGroup 失败: %w", err)
			}
		}
		record.Release()
		builder.Release()
	}

	if skippedTotal > 3 {
		log.Printf("[警告] 共跳过 %d 条委托记录", skippedTotal)
	}
	return nil
}

// writeDealsByCodeRowGroups 将已按 Code 排好序的成交数据写入 parquet。
// 每个 RowGroup 包含约 rowGroupSize 条记录，同一 Code 不跨 RowGroup。
func (w *OptParquetWriter) writeDealsByCodeRowGroups(deals []Deal, filename string) error {
	if len(deals) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

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

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	skippedTotal := 0
	i := 0
	for i < len(deals) {
		builder := array.NewRecordBuilder(mem, schema)
		groupRows := 0
		for i < len(deals) {
			curCode := deals[i].Code
			j := i
			for j < len(deals) && deals[j].Code == curCode {
				j++
			}
			skipped := 0
			for _, d := range deals[i:j] {
				if err := w.checkAndConvertDeal(d, builder); err != nil {
					skipped++
					if skipped <= 3 {
						log.Printf("[警告] 跳过成交记录 [%s]: %v", d.Code, err)
					}
				}
			}
			skippedTotal += skipped
			groupRows += j - i
			i = j
			if groupRows >= rowGroupSize {
				break
			}
		}
		record := builder.NewRecord()
		if record.NumRows() > 0 {
			if err := writer.Write(record); err != nil {
				record.Release()
				builder.Release()
				return fmt.Errorf("写入成交 RowGroup 失败: %w", err)
			}
		}
		record.Release()
		builder.Release()
	}

	if skippedTotal > 3 {
		log.Printf("[警告] 共跳过 %d 条成交记录", skippedTotal)
	}
	return nil
}

// writeTicksByCodeRowGroups 将已按 Code 排好序的快照数据写入 parquet。
// 每个 RowGroup 包含约 rowGroupSize 条记录，同一 Code 不跨 RowGroup。
func (w *OptParquetWriter) writeTicksByCodeRowGroups(ticks []Tick, filename string) error {
	if len(ticks) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()

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

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	writer, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	skippedTotal := 0
	i := 0
	for i < len(ticks) {
		builder := array.NewRecordBuilder(mem, schema)
		groupRows := 0
		for i < len(ticks) {
			curCode := ticks[i].Code
			j := i
			for j < len(ticks) && ticks[j].Code == curCode {
				j++
			}
			skipped := 0
			for _, t := range ticks[i:j] {
				if err := w.checkAndConvertTick(t, builder); err != nil {
					skipped++
					if skipped <= 3 {
						log.Printf("[警告] 跳过快照记录 [%s]: %v", t.Code, err)
					}
				}
			}
			skippedTotal += skipped
			groupRows += j - i
			i = j
			if groupRows >= rowGroupSize {
				break
			}
		}
		record := builder.NewRecord()
		if record.NumRows() > 0 {
			if err := writer.Write(record); err != nil {
				record.Release()
				builder.Release()
				return fmt.Errorf("写入快照 RowGroup 失败: %w", err)
			}
		}
		record.Release()
		builder.Release()
	}

	if skippedTotal > 3 {
		log.Printf("[警告] 共跳过 %d 条快照记录", skippedTotal)
	}
	return nil
}
