package dataconv

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Processor 数据处理器
type Processor struct {
	converter     *Converter
	reader        *ZipReader
	parquetWriter *ParquetWriter
	priceRepo     *LimitPriceRepository
	priceCache    *PriceCache
	workers       int
	rowLimit      int // 限制处理行数
}

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	TradingDay  time.Time
	OutputDir   string
	Workers     int
	ZipPassword string
	RowLimit    int // 限制处理行数，0表示不限制
}

// NewProcessor 创建处理器
func NewProcessor(cfg ProcessorConfig) (*Processor, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}

	// 初始化MySQL连接
	priceRepo, err := NewLimitPriceRepository()
	if err != nil {
		log.Printf("[警告] 无法连接MySQL，涨跌停价格将为0: %v", err)
	}

	// 加载价格缓存
	priceCache := NewPriceCache()
	if priceRepo != nil {
		if err := priceCache.Load(priceRepo, cfg.TradingDay); err != nil {
			log.Printf("[警告] 加载涨跌停价格失败: %v", err)
		}
	}

	// 创建Parquet Writer（合并写入）
	parquetWriter := NewParquetWriter(cfg.OutputDir, cfg.TradingDay)

	return &Processor{
		converter:     NewConverter(cfg.TradingDay),
		reader:        NewZipReader(cfg.ZipPassword),
		parquetWriter: parquetWriter,
		priceRepo:     priceRepo,
		priceCache:    priceCache,
		workers:       cfg.Workers,
		rowLimit:      cfg.RowLimit,
	}, nil
}

// ProcessResult 处理结果
type ProcessResult struct {
	DataType   DataType
	TotalRows  int64
	ValidRows  int64
	ErrorCount int64
	Duration   time.Duration
}

// ProcessSHOrderDeal 处理上交所委托+成交数据 (mdl_4_24_0) - 流式处理
func (p *Processor) ProcessSHOrderDeal(zipPath string) (*ProcessResult, error) {
	start := time.Now()
	result := &ProcessResult{DataType: "order_deal"}

	log.Printf("[处理] 开始处理上交所委托+成交: %s", zipPath)

	opts := ReadOptions{}
	if p.rowLimit > 0 {
		opts.MaxRows = p.rowLimit
		log.Printf("[测试模式] 限制读取 %d 行数据", p.rowLimit)
	}
	iter, err := p.reader.ReadZipFile(zipPath, opts)
	if err != nil {
		return nil, fmt.Errorf("读取ZIP失败: %w", err)
	}
	defer iter.Close()

	header, err := iter.Header()
	if err != nil {
		return nil, err
	}

	var orderCount, dealCount int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.workers)

	// 流式处理：读取一批，处理一批
	chunkNum := 0
	for {
		chunk, err := iter.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		chunkNum++
		log.Printf("[进度] 正在处理第 %d 批数据 (%d 行)...", chunkNum, len(chunk))

		// 本批数据按股票分组
		codeGroups := make(map[string][]map[string]string)
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 处理本批数据
		for securityID, records := range codeGroups {
			wg.Add(1)
			sem <- struct{}{}

			go func(sid string, recs []map[string]string) {
				defer wg.Done()
				defer func() { <-sem }()

				orders, deals, err := p.converter.ConvertSHOrderDeal(recs)
				if err != nil {
					atomic.AddInt64(&result.ErrorCount, 1)
					return
				}

				// 验证和统计
				validOrders := 0
				for _, o := range orders {
					if ValidateOrder(o) {
						validOrders++
					}
				}

				validDeals := 0
				for _, d := range deals {
					if ValidateDeal(d) {
						validDeals++
					}
				}

			atomic.AddInt64(&orderCount, int64(validOrders))
			atomic.AddInt64(&dealCount, int64(validDeals))

			// 写入缓存
			if len(orders) > 0 {
				if err := p.parquetWriter.WriteOrders(orders); err != nil {
					log.Printf("[错误] 写入委托失败: %v", err)
				}
			}
			if len(deals) > 0 {
				if err := p.parquetWriter.WriteDeals(deals); err != nil {
					log.Printf("[错误] 写入成交失败: %v", err)
				}
			}
			}(securityID, records)
		}

		// 等待本批数据处理完成
		wg.Wait()
		
		// 清空map，释放内存
		codeGroups = nil
		
		// 强制GC，对低内存环境很重要
		runtime.GC()
		
		// 记录内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[内存] 当前使用: %.2f MB, 系统总分配: %.2f MB", 
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)
	}

	result.ValidRows = orderCount + dealCount
	result.Duration = time.Since(start)

	log.Printf("[完成] 委托: %d, 成交: %d, 耗时: %v", orderCount, dealCount, result.Duration)
	return result, nil
}

// ProcessSHTick 处理上交所快照数据 (MarketData) - 流式处理
func (p *Processor) ProcessSHTick(zipPath string) (*ProcessResult, error) {
	start := time.Now()
	result := &ProcessResult{DataType: DataTypeTick}

	log.Printf("[处理] 开始处理上交所快照: %s", zipPath)

	opts := ReadOptions{ChunkSize: chunkSizeTick}
	if p.rowLimit > 0 {
		opts.MaxRows = p.rowLimit
		log.Printf("[测试模式] 限制读取 %d 行数据", p.rowLimit)
	}
	iter, err := p.reader.ReadZipFile(zipPath, opts)
	if err != nil {
		return nil, fmt.Errorf("读取ZIP失败: %w", err)
	}
	defer iter.Close()

	header, err := iter.Header()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.workers)

	// 流式处理：读取一批，处理一批
	chunkNum := 0
	for {
		chunk, err := iter.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		chunkNum++
		log.Printf("[进度] 正在处理第 %d 批快照数据 (%d 行)...", chunkNum, len(chunk))

		// 本批数据按股票分组
		codeGroups := make(map[string][]map[string]string)
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 处理本批数据
		for securityID, records := range codeGroups {
			wg.Add(1)
			sem <- struct{}{}

			go func(sid string, recs []map[string]string) {
				defer wg.Done()
				defer func() { <-sem }()

				code := FormatCode(int(parseInt64(sid)))

				// 获取涨跌停价格
				highLimit, lowLimit := p.priceCache.GetOrCompute(code)

				ticks, err := p.converter.ConvertSHTick(recs, highLimit, lowLimit)
				if err != nil {
					atomic.AddInt64(&result.ErrorCount, 1)
					return
				}

				// 验证数据
				validTicks := make([]Tick, 0, len(ticks))
				for _, tick := range ticks {
					if ValidateTick(tick) {
						validTicks = append(validTicks, tick)
					}
				}

				atomic.AddInt64(&result.ValidRows, int64(len(validTicks)))

				// 按SeqNum排序
				sort.Slice(validTicks, func(i, j int) bool {
					return validTicks[i].SeqNum < validTicks[j].SeqNum
				})

			// 写入缓存
			if len(validTicks) > 0 {
				if err := p.parquetWriter.WriteTicks(validTicks); err != nil {
					log.Printf("[错误] 写入快照失败: %v", err)
				}
			}
			}(securityID, records)
		}

		// 等待本批数据处理完成
		wg.Wait()
		
		// 清空map，释放内存
		codeGroups = nil
		
		// 强制GC，对低内存环境很重要
		runtime.GC()
		
		// 记录内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[内存] 当前使用: %.2f MB, 系统总分配: %.2f MB", 
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)
	}

	result.Duration = time.Since(start)
	log.Printf("[完成] 快照: %d 条, 耗时: %v", result.ValidRows, result.Duration)
	return result, nil
}

// ProcessSZOrder 处理深交所委托数据 (mdl_6_33_0) - 流式处理
func (p *Processor) ProcessSZOrder(zipPath string) (*ProcessResult, error) {
	start := time.Now()
	result := &ProcessResult{DataType: DataTypeOrder}

	log.Printf("[处理] 开始处理深交所委托: %s", zipPath)

	opts := ReadOptions{}
	if p.rowLimit > 0 {
		opts.MaxRows = p.rowLimit
		log.Printf("[测试模式] 限制读取 %d 行数据", p.rowLimit)
	}
	iter, err := p.reader.ReadZipFile(zipPath, opts)
	if err != nil {
		return nil, fmt.Errorf("读取ZIP失败: %w", err)
	}
	defer iter.Close()

	header, err := iter.Header()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.workers)

	// 流式处理：读取一批，处理一批
	chunkNum := 0
	for {
		chunk, err := iter.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		chunkNum++
		log.Printf("[进度] 正在处理第 %d 批深交所委托数据 (%d 行)...", chunkNum, len(chunk))

		// 本批数据按股票分组
		codeGroups := make(map[string][]map[string]string)
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 处理本批数据
		for securityID, records := range codeGroups {
			wg.Add(1)
			sem <- struct{}{}

			go func(sid string, recs []map[string]string) {
				defer wg.Done()
				defer func() { <-sem }()

				orders, err := p.converter.ConvertSZOrder(recs)
				if err != nil {
					atomic.AddInt64(&result.ErrorCount, 1)
					return
				}

				validCount := 0
				for _, o := range orders {
					if ValidateOrder(o) {
						validCount++
					}
				}
			atomic.AddInt64(&result.ValidRows, int64(validCount))

			if len(orders) > 0 {
				if err := p.parquetWriter.WriteOrders(orders); err != nil {
					log.Printf("[错误] 写入委托失败: %v", err)
				}
			}
			}(securityID, records)
		}

		// 等待本批数据处理完成
		wg.Wait()
		
		// 清空map，释放内存
		codeGroups = nil
		
		// 强制GC，对低内存环境很重要
		runtime.GC()
		
		// 记录内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[内存] 当前使用: %.2f MB, 系统总分配: %.2f MB", 
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)
	}

	result.Duration = time.Since(start)
	log.Printf("[完成] 深交所委托: %d 条, 耗时: %v", result.ValidRows, result.Duration)
	return result, nil
}

// ProcessSZDeal 处理深交所成交数据 (mdl_6_36_0) - 流式处理
func (p *Processor) ProcessSZDeal(zipPath string) (*ProcessResult, error) {
	start := time.Now()
	result := &ProcessResult{DataType: DataTypeDeal}

	log.Printf("[处理] 开始处理深交所成交: %s", zipPath)

	opts := ReadOptions{}
	if p.rowLimit > 0 {
		opts.MaxRows = p.rowLimit
		log.Printf("[测试模式] 限制读取 %d 行数据", p.rowLimit)
	}
	iter, err := p.reader.ReadZipFile(zipPath, opts)
	if err != nil {
		return nil, fmt.Errorf("读取ZIP失败: %w", err)
	}
	defer iter.Close()

	header, err := iter.Header()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.workers)

	// 流式处理：读取一批，处理一批
	chunkNum := 0
	for {
		chunk, err := iter.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		chunkNum++
		log.Printf("[进度] 正在处理第 %d 批深交所成交数据 (%d 行)...", chunkNum, len(chunk))

		// 本批数据按股票分组
		codeGroups := make(map[string][]map[string]string)
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 处理本批数据
		for securityID, records := range codeGroups {
			wg.Add(1)
			sem <- struct{}{}

			go func(sid string, recs []map[string]string) {
				defer wg.Done()
				defer func() { <-sem }()

				deals, err := p.converter.ConvertSZDeal(recs)
				if err != nil {
					atomic.AddInt64(&result.ErrorCount, 1)
					return
				}

				validCount := 0
				for _, d := range deals {
					if ValidateDeal(d) {
						validCount++
					}
				}
			atomic.AddInt64(&result.ValidRows, int64(validCount))

			if len(deals) > 0 {
				if err := p.parquetWriter.WriteDeals(deals); err != nil {
					log.Printf("[错误] 写入成交失败: %v", err)
				}
			}
			}(securityID, records)
		}

		// 等待本批数据处理完成
		wg.Wait()
		
		// 清空map，释放内存
		codeGroups = nil
		
		// 强制GC，对低内存环境很重要
		runtime.GC()
		
		// 记录内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[内存] 当前使用: %.2f MB, 系统总分配: %.2f MB", 
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)
	}

	result.Duration = time.Since(start)
	log.Printf("[完成] 深交所成交: %d 条, 耗时: %v", result.ValidRows, result.Duration)
	return result, nil
}

// ProcessSZTick 处理深交所快照数据 (mdl_6_28_0) - 流式处理
func (p *Processor) ProcessSZTick(zipPath string) (*ProcessResult, error) {
	start := time.Now()
	result := &ProcessResult{DataType: DataTypeTick}

	log.Printf("[处理] 开始处理深交所快照: %s", zipPath)

	opts := ReadOptions{ChunkSize: chunkSizeTick}
	if p.rowLimit > 0 {
		opts.MaxRows = p.rowLimit
		log.Printf("[测试模式] 限制读取 %d 行数据", p.rowLimit)
	}
	iter, err := p.reader.ReadZipFile(zipPath, opts)
	if err != nil {
		return nil, fmt.Errorf("读取ZIP失败: %w", err)
	}
	defer iter.Close()

	header, err := iter.Header()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.workers)

	// 流式处理：读取一批，处理一批
	chunkNum := 0
	for {
		chunk, err := iter.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		chunkNum++
		log.Printf("[进度] 正在处理第 %d 批深交所快照数据 (%d 行)...", chunkNum, len(chunk))

		// 本批数据按股票分组
		codeGroups := make(map[string][]map[string]string)
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 处理本批数据
		for securityID, records := range codeGroups {
			wg.Add(1)
			sem <- struct{}{}

			go func(sid string, recs []map[string]string) {
				defer wg.Done()
				defer func() { <-sem }()

				code := FormatCode(int(parseInt64(sid)))

				// 获取涨跌停价格（从MySQL缓存）
				highLimit, lowLimit := p.priceCache.GetOrCompute(code)

				ticks, err := p.converter.ConvertSZTick(recs, highLimit, lowLimit)
				if err != nil {
					atomic.AddInt64(&result.ErrorCount, 1)
					return
				}

				// 验证数据
				validTicks := make([]Tick, 0, len(ticks))
				for _, tick := range ticks {
					if ValidateTick(tick) {
						validTicks = append(validTicks, tick)
					}
				}

				atomic.AddInt64(&result.ValidRows, int64(len(validTicks)))

				// 按SeqNum排序
				sort.Slice(validTicks, func(i, j int) bool {
					return validTicks[i].SeqNum < validTicks[j].SeqNum
				})

			if len(validTicks) > 0 {
				if err := p.parquetWriter.WriteTicks(validTicks); err != nil {
					log.Printf("[错误] 写入快照失败: %v", err)
				}
			}
			}(securityID, records)
		}

		// 等待本批数据处理完成
		wg.Wait()
		
		// 清空map，释放内存
		codeGroups = nil
		
		// 强制GC，对低内存环境很重要
		runtime.GC()
		
		// 记录内存使用情况
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("[内存] 当前使用: %.2f MB, 系统总分配: %.2f MB", 
			float64(m.Alloc)/1024/1024, float64(m.TotalAlloc)/1024/1024)
	}

	result.Duration = time.Since(start)
	log.Printf("[完成] 深交所快照: %d 条, 耗时: %v", result.ValidRows, result.Duration)
	return result, nil
}

// Flush 将所有缓存数据写入Parquet文件
func (p *Processor) Flush() error {
	return p.parquetWriter.Flush()
}
