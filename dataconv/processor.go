package dataconv

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"deeptrade/storage"
)

// Processor 数据处理器
type Processor struct {
	converter        *Converter
	reader           *ZipReader
	parquetWriter    *ParquetWriter
	optParquetWriter *OptParquetWriter // 优化模式写入器
	priceRepo        *LimitPriceRepository
	priceCache       *PriceCache
	securityCache    *SecurityCache // 证券代码缓存
	rangeChecker     *DataRangeChecker // 数据范围检查器
	optMode          bool            // 是否启用优化模式
	forceInt32       bool            // 强制使用 Int32（不检测）
	workers          int
	rowLimit         int // 限制处理行数
}

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	TradingDay  time.Time
	OutputDir   string
	Workers     int
	ZipPassword string
	RowLimit    int // 限制处理行数，0表示不限制
	Optimize    bool // 是否启用优化模式
	ForceInt32  bool // 强制使用 Int32（不检测，有溢出风险）
	OSSConfig   *OSSConfig // OSS 配置（可选）
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

	// 创建普通 Parquet Writer
	parquetWriter := NewParquetWriter(cfg.OutputDir, cfg.TradingDay)

	// 优化模式相关组件
	var securityCache *SecurityCache
	var rangeChecker *DataRangeChecker
	var optParquetWriter *OptParquetWriter

	if cfg.Optimize {
		log.Println("========================================")
		log.Println("[优化模式] 已启用")
		log.Println("  - Code 使用 SECURITY_ID (Int32)")
		log.Println("  - Price ×100 (Int32)")
		log.Println("  - Volume ÷100 (Int32)")
		log.Println("  - 删除冗余字段")
		log.Println("  - 使用 Zstd 压缩")
		if cfg.ForceInt32 {
			log.Println("  - 强制 Int32 模式（不检测）")
		} else {
			log.Println("  - 实时检测超限")
		}
		log.Println("========================================")

		// 获取 MySQL 客户端
		gormDB, err := storage.GetMySQLClient()
		if err != nil {
			log.Printf("[错误] 优化模式需要数据库连接: %v", err)
			return nil, fmt.Errorf("无法获取数据库连接: %w", err)
		}
		db, err := gormDB.DB()
		if err != nil {
			return nil, fmt.Errorf("获取 sql.DB 失败: %w", err)
		}

		// 创建证券代码缓存
		securityCache = NewSecurityCache(db)

		// 创建数据范围检查器
		stopOnError := !cfg.ForceInt32 // 强制模式下不因超限停止
		rangeChecker = NewDataRangeChecker(stopOnError)

		// 创建优化后的写入器
		optParquetWriter = NewOptParquetWriter(cfg.OutputDir, cfg.TradingDay, securityCache, rangeChecker, cfg.ForceInt32, cfg.OSSConfig)
	}

	return &Processor{
		converter:        NewConverter(cfg.TradingDay),
		reader:           NewZipReader(cfg.ZipPassword),
		parquetWriter:    parquetWriter,
		optParquetWriter: optParquetWriter,
		priceRepo:        priceRepo,
		priceCache:       priceCache,
		securityCache:    securityCache,
		rangeChecker:     rangeChecker,
		optMode:          cfg.Optimize,
		forceInt32:       cfg.ForceInt32,
		workers:          cfg.Workers,
		rowLimit:         cfg.RowLimit,
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
		uniqueSecurityIDs := make(map[string]struct{})
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			uniqueSecurityIDs[securityID] = struct{}{}
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 优化模式：批量预加载本批数据的 SECURITY_ID
		if p.optMode && p.securityCache != nil && len(uniqueSecurityIDs) > 0 {
			codes := make([]string, 0, len(uniqueSecurityIDs))
			for sid := range uniqueSecurityIDs {
				code := FormatCode(int(parseInt64(sid)))
				codes = append(codes, code)
			}
			if err := p.securityCache.BatchLoad(codes); err != nil {
				log.Printf("[警告] 批量预加载本批 %d 个证券失败: %v", len(codes), err)
			}
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

			// 验证和过滤
			validOrders := make([]Order, 0, len(orders))
			for _, o := range orders {
				if ValidateOrder(o) {
					validOrders = append(validOrders, o)
				}
			}

			validDeals := make([]Deal, 0, len(deals))
			for _, d := range deals {
				if ValidateDeal(d) {
					validDeals = append(validDeals, d)
				}
			}

			atomic.AddInt64(&orderCount, int64(len(validOrders)))
			atomic.AddInt64(&dealCount, int64(len(validDeals)))

			// 写入缓存（根据模式选择不同的 Writer）
			if len(validOrders) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteOrders(validOrders)
				} else {
					err = p.parquetWriter.WriteOrders(validOrders)
				}
				if err != nil {
					log.Printf("[错误] 写入委托失败: %v", err)
				}
			}
			if len(validDeals) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteDeals(validDeals)
				} else {
					err = p.parquetWriter.WriteDeals(validDeals)
				}
				if err != nil {
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
		uniqueSecurityIDs := make(map[string]struct{})
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			uniqueSecurityIDs[securityID] = struct{}{}
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 优化模式：批量预加载本批数据的 SECURITY_ID
		if p.optMode && p.securityCache != nil && len(uniqueSecurityIDs) > 0 {
			codes := make([]string, 0, len(uniqueSecurityIDs))
			for sid := range uniqueSecurityIDs {
				code := FormatCode(int(parseInt64(sid)))
				codes = append(codes, code)
			}
			if err := p.securityCache.BatchLoad(codes); err != nil {
				log.Printf("[警告] 批量预加载本批 %d 个证券失败: %v", len(codes), err)
			}
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

			// 写入缓存（根据模式选择不同的 Writer）
			if len(validTicks) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteTicks(validTicks)
				} else {
					err = p.parquetWriter.WriteTicks(validTicks)
				}
				if err != nil {
					log.Printf("[错误] 写入快照失败: %v", err)
					return
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
		uniqueSecurityIDs := make(map[string]struct{})
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			uniqueSecurityIDs[securityID] = struct{}{}
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 优化模式：批量预加载本批数据的 SECURITY_ID
		if p.optMode && p.securityCache != nil && len(uniqueSecurityIDs) > 0 {
			codes := make([]string, 0, len(uniqueSecurityIDs))
			for sid := range uniqueSecurityIDs {
				code := FormatCode(int(parseInt64(sid)))
				codes = append(codes, code)
			}
			if err := p.securityCache.BatchLoad(codes); err != nil {
				log.Printf("[警告] 批量预加载本批 %d 个证券失败: %v", len(codes), err)
			}
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

			// 验证和过滤
			validOrders := make([]Order, 0, len(orders))
			for _, o := range orders {
				if ValidateOrder(o) {
					validOrders = append(validOrders, o)
				}
			}
			atomic.AddInt64(&result.ValidRows, int64(len(validOrders)))

			// 写入缓存（根据模式选择不同的 Writer）
			if len(validOrders) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteOrders(validOrders)
				} else {
					err = p.parquetWriter.WriteOrders(validOrders)
				}
				if err != nil {
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
		uniqueSecurityIDs := make(map[string]struct{})
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			uniqueSecurityIDs[securityID] = struct{}{}
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 优化模式：批量预加载本批数据的 SECURITY_ID
		if p.optMode && p.securityCache != nil && len(uniqueSecurityIDs) > 0 {
			codes := make([]string, 0, len(uniqueSecurityIDs))
			for sid := range uniqueSecurityIDs {
				code := FormatCode(int(parseInt64(sid)))
				codes = append(codes, code)
			}
			if err := p.securityCache.BatchLoad(codes); err != nil {
				log.Printf("[警告] 批量预加载本批 %d 个证券失败: %v", len(codes), err)
			}
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

			// 验证和过滤
			validDeals := make([]Deal, 0, len(deals))
			for _, d := range deals {
				if ValidateDeal(d) {
					validDeals = append(validDeals, d)
				}
			}
			atomic.AddInt64(&result.ValidRows, int64(len(validDeals)))

			// 写入缓存（根据模式选择不同的 Writer）
			if len(validDeals) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteDeals(validDeals)
				} else {
					err = p.parquetWriter.WriteDeals(validDeals)
				}
				if err != nil {
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
		uniqueSecurityIDs := make(map[string]struct{})
		
		for _, record := range chunk {
			row := make(map[string]string)
			for i, h := range header {
				if i < len(record) {
					row[h] = record[i]
				}
			}

			securityID := row["SecurityID"]
			codeGroups[securityID] = append(codeGroups[securityID], row)
			uniqueSecurityIDs[securityID] = struct{}{}
			atomic.AddInt64(&result.TotalRows, 1)
		}

		// 优化模式：批量预加载本批数据的 SECURITY_ID
		if p.optMode && p.securityCache != nil && len(uniqueSecurityIDs) > 0 {
			codes := make([]string, 0, len(uniqueSecurityIDs))
			for sid := range uniqueSecurityIDs {
				code := FormatCode(int(parseInt64(sid)))
				codes = append(codes, code)
			}
			if err := p.securityCache.BatchLoad(codes); err != nil {
				log.Printf("[警告] 批量预加载本批 %d 个证券失败: %v", len(codes), err)
			}
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

			// 写入缓存（根据模式选择不同的 Writer）
			if len(validTicks) > 0 {
				var err error
				if p.optMode {
					err = p.optParquetWriter.WriteTicks(validTicks)
				} else {
					err = p.parquetWriter.WriteTicks(validTicks)
				}
				if err != nil {
					log.Printf("[错误] 写入快照失败: %v", err)
					return
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
	if p.optMode {
		return p.optParquetWriter.Flush()
	}
	return p.parquetWriter.Flush()
}
