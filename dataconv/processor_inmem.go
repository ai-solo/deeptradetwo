package dataconv

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"time"
)

// InMemResult 全量内存处理结果
type InMemResult struct {
	Orders   []Order
	Deals    []Deal
	Ticks    []Tick
	Duration time.Duration
}

// ProcessAllInMemory 全量内存处理一天的所有数据文件，排序后一次写出
// zipFiles: map[dataFileType]zipPath，key 为 "sz_order"/"sz_deal"/"sz_tick"/"sh_orderdeal"/"sh_tick"
func (p *Processor) ProcessAllInMemory(zipFiles map[string][]string) error {
	start := time.Now()

	readOpts := ReadOptions{MaxRows: p.rowLimit}

	// ---- 并发读取+转换所有文件类型 ----
	type readResult struct {
		orders []Order
		deals  []Deal
		ticks  []Tick
		err    error
		label  string
	}

	tasks := []func() readResult{}

	if paths, ok := zipFiles["sz_order"]; ok {
		for _, path := range paths {
			path := path
			tasks = append(tasks, func() readResult {
				t0 := time.Now()
				log.Printf("[内存处理] 读取深交所委托: %s", path)
				header, rows, err := p.reader.ReadAll(path, readOpts)
				if err != nil {
					return readResult{err: fmt.Errorf("读取深交所委托失败: %w", err), label: path}
				}
				readDur := time.Since(t0)
				log.Printf("[计时] 深交所委托读取 %d 行耗时 %v", len(rows), readDur)
				t1 := time.Now()
				orders, err := p.converter.ConvertSZOrderParallel(header, rows)
				runtime.GC()
				log.Printf("[计时] 深交所委托转换 %d 条耗时 %v", len(orders), time.Since(t1))
				return readResult{orders: orders, err: err, label: path}
			})
		}
	}

	if paths, ok := zipFiles["sz_deal"]; ok {
		for _, path := range paths {
			path := path
			tasks = append(tasks, func() readResult {
				t0 := time.Now()
				log.Printf("[内存处理] 读取深交所成交: %s", path)
				header, rows, err := p.reader.ReadAll(path, readOpts)
				if err != nil {
					return readResult{err: fmt.Errorf("读取深交所成交失败: %w", err), label: path}
				}
				log.Printf("[计时] 深交所成交读取 %d 行耗时 %v", len(rows), time.Since(t0))
				t1 := time.Now()
				deals, err := p.converter.ConvertSZDealParallel(header, rows)
				runtime.GC()
				log.Printf("[计时] 深交所成交转换 %d 条耗时 %v", len(deals), time.Since(t1))
				return readResult{deals: deals, err: err, label: path}
			})
		}
	}

	if paths, ok := zipFiles["sz_tick"]; ok {
		for _, path := range paths {
			path := path
			tasks = append(tasks, func() readResult {
				t0 := time.Now()
				log.Printf("[内存处理] 读取深交所快照: %s", path)
				header, rows, err := p.reader.ReadAll(path, readOpts)
				if err != nil {
					return readResult{err: fmt.Errorf("读取深交所快照失败: %w", err), label: path}
				}
				log.Printf("[计时] 深交所快照读取 %d 行耗时 %v", len(rows), time.Since(t0))
				t1 := time.Now()
				ticks, err := p.converter.ConvertSZTickParallel(header, rows, p.priceCache)
				runtime.GC()
				log.Printf("[计时] 深交所快照转换 %d 条耗时 %v", len(ticks), time.Since(t1))
				return readResult{ticks: ticks, err: err, label: path}
			})
		}
	}

	if paths, ok := zipFiles["sh_orderdeal"]; ok {
		for _, path := range paths {
			path := path
			tasks = append(tasks, func() readResult {
				t0 := time.Now()
				log.Printf("[内存处理] 读取上交所委托+成交: %s", path)
				header, rows, err := p.reader.ReadAll(path, readOpts)
				if err != nil {
					return readResult{err: fmt.Errorf("读取上交所委托+成交失败: %w", err), label: path}
				}
				log.Printf("[计时] 上交所委托+成交读取 %d 行耗时 %v", len(rows), time.Since(t0))
				t1 := time.Now()
				orders, deals, err := p.converter.ConvertSHOrderDealParallel(header, rows)
				runtime.GC()
				log.Printf("[计时] 上交所委托+成交转换 orders=%d deals=%d 耗时 %v", len(orders), len(deals), time.Since(t1))
				return readResult{orders: orders, deals: deals, err: err, label: path}
			})
		}
	}

	if paths, ok := zipFiles["sh_tick"]; ok {
		for _, path := range paths {
			path := path
			tasks = append(tasks, func() readResult {
				t0 := time.Now()
				log.Printf("[内存处理] 读取上交所快照: %s", path)
				header, rows, err := p.reader.ReadAll(path, readOpts)
				if err != nil {
					return readResult{err: fmt.Errorf("读取上交所快照失败: %w", err), label: path}
				}
				log.Printf("[计时] 上交所快照读取 %d 行耗时 %v", len(rows), time.Since(t0))
				t1 := time.Now()
				ticks, err := p.converter.ConvertSHTickParallel(header, rows, p.priceCache)
				runtime.GC()
				log.Printf("[计时] 上交所快照转换 %d 条耗时 %v", len(ticks), time.Since(t1))
				return readResult{ticks: ticks, err: err, label: path}
			})
		}
	}

	// 串行执行所有任务（避免并发读取导致内存峰值过高）
	var allOrders []Order
	var allDeals []Deal
	var allTicks []Tick
	for _, task := range tasks {
		r := task()
		if r.err != nil {
			return r.err
		}
		allOrders = append(allOrders, r.orders...)
		allDeals = append(allDeals, r.deals...)
		allTicks = append(allTicks, r.ticks...)
		runtime.GC()
	}

	// ---- 并发排序 Code + SeqNum ----
	log.Printf("[排序] 开始排序 orders=%d deals=%d ticks=%d", len(allOrders), len(allDeals), len(allTicks))
	var sortWg sync.WaitGroup
	sortWg.Add(3)
	go func() {
		defer sortWg.Done()
		sort.Slice(allOrders, func(i, j int) bool {
			if allOrders[i].Code != allOrders[j].Code {
				return allOrders[i].Code < allOrders[j].Code
			}
			return allOrders[i].SeqNum < allOrders[j].SeqNum
		})
	}()
	go func() {
		defer sortWg.Done()
		sort.Slice(allDeals, func(i, j int) bool {
			if allDeals[i].Code != allDeals[j].Code {
				return allDeals[i].Code < allDeals[j].Code
			}
			return allDeals[i].SeqNum < allDeals[j].SeqNum
		})
	}()
	go func() {
		defer sortWg.Done()
		sort.Slice(allTicks, func(i, j int) bool {
			if allTicks[i].Code != allTicks[j].Code {
				return allTicks[i].Code < allTicks[j].Code
			}
			return allTicks[i].SeqNum < allTicks[j].SeqNum
		})
	}()
	sortWg.Wait()
	log.Printf("[排序] 完成")

	// ---- 批量预加载 SECURITY_ID（避免写入时逐条查数据库）----
	if p.optMode && p.securityCache != nil {
		allCodes := make(map[string]struct{}, len(allOrders)+len(allDeals)+len(allTicks))
		for _, o := range allOrders {
			allCodes[o.Code] = struct{}{}
		}
		for _, d := range allDeals {
			allCodes[d.Code] = struct{}{}
		}
		for _, t := range allTicks {
			allCodes[t.Code] = struct{}{}
		}
		codes := make([]string, 0, len(allCodes))
		for c := range allCodes {
			codes = append(codes, c)
		}
		log.Printf("[内存处理] 批量预加载 %d 个证券代码...", len(codes))
		if err := p.securityCache.BatchLoad(codes, p.tradingDay); err != nil {
			log.Printf("[警告] 批量预加载 SECURITY_ID 失败: %v", err)
		}
	}

	// ---- 写出 ----
	if p.optParquetWriter == nil {
		return fmt.Errorf("需要 -optimize 模式才能使用内存处理")
	}

	if err := p.optParquetWriter.WriteAllDirect(allOrders, allDeals, allTicks); err != nil {
		return fmt.Errorf("写出 parquet 失败: %w", err)
	}

	log.Printf("[内存处理] 全部完成，耗时 %v", time.Since(start))
	return nil
}

// ResortFromParquet 对已有的 parquet 文件重新排序（Code+SeqNum）后覆盖写入
func (p *Processor) ResortFromParquet(orderPath, dealPath, tickPath string) error {
	start := time.Now()
	log.Printf("[重排序] 开始读取 parquet 文件...")

	w := p.optParquetWriter
	if w == nil {
		return fmt.Errorf("需要 -optimize 模式")
	}

	tradingDay := p.tradingDay
	secCache := p.securityCache

	// 读取
	allOrders, err := ReadOrdersFromParquet(orderPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 order parquet 失败: %w", err)
	}
	log.Printf("[重排序] 读取 orders=%d", len(allOrders))

	allDeals, err := ReadDealsFromParquet(dealPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 deal parquet 失败: %w", err)
	}
	log.Printf("[重排序] 读取 deals=%d", len(allDeals))

	allTicks, err := ReadTicksFromParquet(tickPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 tick parquet 失败: %w", err)
	}
	log.Printf("[重排序] 读取 ticks=%d", len(allTicks))

	// 排序
	sort.Slice(allOrders, func(i, j int) bool {
		if allOrders[i].Code != allOrders[j].Code {
			return allOrders[i].Code < allOrders[j].Code
		}
		return allOrders[i].SeqNum < allOrders[j].SeqNum
	})
	sort.Slice(allDeals, func(i, j int) bool {
		if allDeals[i].Code != allDeals[j].Code {
			return allDeals[i].Code < allDeals[j].Code
		}
		return allDeals[i].SeqNum < allDeals[j].SeqNum
	})
	sort.Slice(allTicks, func(i, j int) bool {
		if allTicks[i].Code != allTicks[j].Code {
			return allTicks[i].Code < allTicks[j].Code
		}
		return allTicks[i].SeqNum < allTicks[j].SeqNum
	})
	log.Printf("[重排序] 排序完成")

	// 覆盖写出
	if err := w.writeOrdersToParquet(allOrders, orderPath); err != nil {
		return fmt.Errorf("写出 order 失败: %w", err)
	}
	if err := w.writeDealsToParquet(allDeals, dealPath); err != nil {
		return fmt.Errorf("写出 deal 失败: %w", err)
	}
	if err := w.writeTicksToParquet(allTicks, tickPath); err != nil {
		return fmt.Errorf("写出 tick 失败: %w", err)
	}

	log.Printf("[重排序] 完成，耗时 %v", time.Since(start))
	return nil
}
