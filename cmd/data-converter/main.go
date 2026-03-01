package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"deeptrade/dataconv"
	"deeptrade/pkg/progress"
	"deeptrade/storage"
)

var (
	// 基础参数
	flagTradingDay = flag.String("date", "", "交易日期 (格式: 2024-01-02 或 20240102)")
	flagDataDir    = flag.String("dir", "", "数据目录")
	flagOutputDir  = flag.String("output", "./output", "输出目录")
	flagDataType   = flag.String("type", "all", "数据类型: order, deal, tick, all")
	flagMarket     = flag.String("market", "all", "市场: sh, sz, all")
	flagWorkers    = flag.Int("workers", 0, "并发数 (默认CPU核心数)")
	flagPassword   = flag.String("password", "DataYes", "ZIP密码")
	flagDataPrefix = flag.String("prefix", "", "数据文件路径前缀 (可选)")
	flagNoMySQL    = flag.Bool("no-mysql", false, "不使用MySQL (涨跌停价格将为0)")
	flagLimit      = flag.Int("limit", 0, "限制处理行数 (0表示不限制，用于测试)")
	flagOptimize   = flag.Bool("optimize", false, "启用优化模式 (Int32+Zstd压缩，需MySQL)")
	flagForceInt32 = flag.Bool("force-int32", false, "强制Int32模式 (不检测超限，有溢出风险)")
	
	// OSS 相关参数
	flagOSS          = flag.Bool("oss", false, "启用阿里云 OSS 上传")
	flagOSSAccessKey = flag.String("oss-access-key", "", "OSS AccessKey ID (默认从环境变量读取)")
	flagOSSSecretKey = flag.String("oss-secret-key", "", "OSS AccessKey Secret (默认从环境变量读取)")
	flagOSSEndpoint  = flag.String("oss-endpoint", "", "OSS Endpoint (默认: oss-cn-shanghai.aliyuncs.com)")
	flagOSSBucket    = flag.String("oss-bucket", "", "OSS Bucket 名称 (默认: stock-data)")
	flagOSSBasePath  = flag.String("oss-path", "", "OSS 存储路径前缀 (默认: market_data)")
	flagOSSCleanLocal = flag.Bool("oss-clean-local", false, "OSS上传后删除本地Parquet文件")
	
	// 自动下载参数
	flagAutoDownload     = flag.Bool("auto-download", false, "启用自动下载模式")
	flagAria2URL         = flag.String("aria2-url", "http://121.43.209.82:6800/jsonrpc", "Aria2 JSON-RPC地址")
	flagAria2Token       = flag.String("aria2-token", "", "Aria2 认证token")
	flagFileServerURL    = flag.String("fileserver-url", "http://121.43.209.82:5244", "文件服务器地址")
	flagFileServerAuth   = flag.String("fileserver-auth", "", "文件服务器Authorization token")
	flagDownloadDir      = flag.String("download-dir", "/data/raw", "下载目录")
	flagKeepRaw          = flag.Bool("keep-raw", false, "处理完成后保留原始ZIP文件")
	flagDownloadTimeout  = flag.Int("download-timeout", 1800, "单个文件下载超时时间(秒)")
	
	// 批量处理参数
	flagYear      = flag.Int("year", 0, "处理整年数据 (例如: 2025)")
	flagMonth     = flag.Int("month", 0, "处理指定月份 (1-12, 配合-year使用)")
	flagStartDate = flag.String("start-date", "", "开始日期 (格式: 20250101)")
	flagEndDate   = flag.String("end-date", "", "结束日期 (格式: 20251231)")
	flagResume    = flag.Bool("resume", false, "断点续传模式")
	flagFlushDaily = flag.Bool("flush-daily", true, "每天处理完立即写入Parquet")
)

func main() {
	flag.Parse()

	log.Println("========================================")
	if *flagAutoDownload {
		log.Println("数据清洗工具 (Go版本) - 自动下载模式")
	} else {
		log.Println("数据清洗工具 (Go版本)")
	}
	log.Println("========================================")

	// 优化GC策略，对低内存环境更激进
	debug.SetGCPercent(50)
	log.Println("[优化] GC百分比设置为50% (更频繁回收)")

	// 判断是批量处理还是单日处理
	if *flagAutoDownload && (*flagYear > 0 || *flagStartDate != "") {
		runBatchMode()
	} else {
		runSingleDayMode()
	}
}

func runBatchMode() {
	log.Println("[模式] 批量自动下载处理")
	
	// 生成日期列表
	dates, err := generateDateRange()
	if err != nil {
		log.Fatalf("生成日期范围失败: %v", err)
	}
	
	log.Printf("[配置] 日期范围: %s ~ %s (共 %d 天)", 
		dates[0].Format("2006-01-02"),
		dates[len(dates)-1].Format("2006-01-02"),
		len(dates))
	log.Printf("[配置] 市场: %s, 类型: %s", *flagMarket, *flagDataType)
	log.Printf("[配置] Aria2: %s", *flagAria2URL)
	log.Printf("[配置] 文件服务器: %s", *flagFileServerURL)
	log.Printf("[配置] 下载目录: %s", *flagDownloadDir)
	log.Printf("[配置] 输出目录: %s", *flagOutputDir)
	if *flagOSS {
		log.Printf("[配置] OSS上传: 启用")
		if *flagOSSCleanLocal {
			log.Printf("[配置] OSS上传后清理本地: 是")
		}
	}
	log.Println("========================================")
	
	// 初始化进度跟踪器
	var tracker *progress.Tracker
	progressFile := filepath.Join(*flagOutputDir, ".progress.json")
	if *flagResume {
		tracker, err = progress.LoadTracker(progressFile)
		if err != nil {
			log.Fatalf("加载进度文件失败: %v", err)
		}
		if tracker != nil {
			processed, _, failed := tracker.GetStats()
			log.Printf("[断点续传] 已处理 %d 天，失败 %d 天", processed, failed)
		}
	}
	if tracker == nil {
		year := *flagYear
		if year == 0 && len(dates) > 0 {
			year = dates[0].Year()
		}
		tracker = progress.NewTracker(year, progressFile)
	}
	
	// 初始化下载器
	downloader := dataconv.NewDownloader(
		*flagAria2URL,
		*flagAria2Token,
		*flagFileServerURL,
		*flagFileServerAuth,
		*flagDownloadDir,
		*flagKeepRaw,
		time.Duration(*flagDownloadTimeout)*time.Second,
	)
	
	// 初始化MySQL (可选)
	if !*flagNoMySQL {
		if _, err := storage.GetMySQLClient(); err != nil {
			log.Printf("[警告] MySQL连接失败，涨跌停价格将为0: %v", err)
		} else {
			defer storage.CloseMySQL()
		}
	}
	
	// 统计信息
	var totalDays, tradingDays, successDays, skipDays, failedDays int
	totalStart := time.Now()
	
	// 逐天处理
	for i, date := range dates {
		totalDays++
		dateStr := date.Format("20060102")
		
		log.Printf("\n[%d/%d] %s", i+1, len(dates), date.Format("2006-01-02"))
		
		// 检查是否已处理
		if *flagResume && tracker.IsProcessed(dateStr) {
			log.Printf("[跳过] 已处理")
			skipDays++
			continue
		}
		
		// 处理单日数据
		err := processSingleDay(date, downloader)
		if err != nil {
			if strings.Contains(err.Error(), "非交易日") {
				log.Printf("[跳过] %v", err)
				skipDays++
			} else {
				log.Printf("[失败] %v", err)
				failedDays++
				tracker.MarkFailed(dateStr, err.Error())
			}
			continue
		}
		
		// 标记为已处理
		tradingDays++
		successDays++
		if err := tracker.MarkProcessed(dateStr); err != nil {
			log.Printf("[警告] 保存进度失败: %v", err)
		}
		
		log.Printf("[完成] %s", date.Format("2006-01-02"))
	}
	
	// 输出总结
	log.Println("\n========================================")
	log.Println("[总结] 批量处理完成")
	log.Println("========================================")
	log.Printf("  - 总天数: %d", totalDays)
	log.Printf("  - 交易日: %d", tradingDays)
	log.Printf("  - 成功处理: %d", successDays)
	log.Printf("  - 跳过: %d", skipDays)
	log.Printf("  - 失败: %d", failedDays)
	log.Printf("  - 总耗时: %v", time.Since(totalStart))
	if successDays > 0 {
		avgTime := time.Since(totalStart) / time.Duration(successDays)
		log.Printf("  - 平均每个交易日: %v", avgTime)
	}
	log.Println("========================================")
}

func processSingleDay(date time.Time, downloader *dataconv.Downloader) error {
	dayStart := time.Now()
	
	// 1. 获取文件列表
	log.Printf("[检查] 查询文件列表...")
	files, err := downloader.ListDayFiles(date)
	if err != nil {
		return fmt.Errorf("查询文件列表失败: %w", err)
	}
	
	if len(files) == 0 {
		return fmt.Errorf("非交易日或无数据")
	}
	
	log.Printf("[开始] 交易日，找到 %d 个文件", len(files))
	
	// 2. 筛选和排序文件
	tasks := downloader.FilterAndSortFiles(files, date, *flagMarket, *flagDataType)
	if len(tasks) == 0 {
		return fmt.Errorf("没有需要处理的文件")
	}
	
	log.Printf("[筛选] 需要处理 %d 个文件", len(tasks))
	for _, task := range tasks {
		sizeMB := float64(task.File.Size) / 1024 / 1024
		log.Printf("  ✓ %s (%.1fMB)", task.File.Name, sizeMB)
	}
	
	// 3. 创建处理器
	workers := getWorkers()
	ossConfig := getOSSConfig()
	
	processor, err := dataconv.NewProcessor(dataconv.ProcessorConfig{
		TradingDay:  date,
		OutputDir:   *flagOutputDir,
		Workers:     workers,
		ZipPassword: *flagPassword,
		RowLimit:    *flagLimit,
		Optimize:    *flagOptimize,
		ForceInt32:  *flagForceInt32,
		OSSConfig:   ossConfig,
	})
	if err != nil {
		return fmt.Errorf("创建处理器失败: %w", err)
	}
	
	// 4. 逐个下载和处理文件
	for idx, task := range tasks {
		log.Println("----------------------------------------")
		log.Printf("[%d/%d] %s", idx+1, len(tasks), task.File.Name)
		
		// 下载文件
		if err := downloader.DownloadFile(task); err != nil {
			log.Printf("[错误] 下载失败: %v", err)
			continue
		}
		
		// 处理文件
		if err := processFile(task.LocalPath, date, processor); err != nil {
			log.Printf("[错误] 处理失败: %v", err)
			// 继续处理其他文件
		}
		
		// 清理原始文件
		if err := downloader.CleanupFile(task.LocalPath); err != nil {
			log.Printf("[警告] 清理文件失败: %v", err)
		}
	}
	
	// 5. 写入 Parquet
	if *flagFlushDaily {
		log.Println("========================================")
		log.Printf("[写入] 将 %s 的数据写入Parquet文件...", date.Format("2006-01-02"))
		if err := processor.Flush(); err != nil {
			return fmt.Errorf("写入Parquet失败: %w", err)
		}
		log.Printf("[写入] 完成 ✓")
		
		// 6. OSS上传并清理
		if *flagOSS && *flagOSSCleanLocal {
			log.Printf("[清理] 删除本地Parquet文件...")
			cleanLocalParquetFiles(*flagOutputDir, date)
		}
	}
	
	log.Printf("[完成] %s - 耗时: %v", date.Format("2006-01-02"), time.Since(dayStart))
	return nil
}

func runSingleDayMode() {
	log.Println("[模式] 单日处理")
	
	// 解析交易日期
	tradingDay, err := parseTradingDay(*flagTradingDay)
	if err != nil {
		log.Fatalf("解析交易日期失败: %v", err)
	}
	log.Printf("[配置] 交易日期: %s", tradingDay.Format("2006-01-02"))

	// 设置并发数
	workers := getWorkers()
	log.Printf("[配置] 并发数: %d (CPU核心数: %d)", workers, runtime.NumCPU())

	// 确定数据目录
	dataDir := *flagDataDir
	if dataDir == "" {
		dataDir = "."
	}
	log.Printf("[配置] 数据目录: %s", dataDir)

	// 确定输出目录
	outputDir := *flagOutputDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}
	log.Printf("[配置] 输出目录: %s", outputDir)

	// 初始化MySQL (可选)
	if !*flagNoMySQL {
		if _, err := storage.GetMySQLClient(); err != nil {
			log.Printf("[警告] MySQL连接失败，涨跌停价格将为0: %v", err)
		} else {
			defer storage.CloseMySQL()
		}
	}

	// 配置 OSS
	ossConfig := getOSSConfig()
	if ossConfig != nil {
		log.Println("========================================")
		log.Println("[OSS] OSS 上传已启用")
		log.Println("========================================")
	}

	// 创建处理器
	processor, err := dataconv.NewProcessor(dataconv.ProcessorConfig{
		TradingDay:  tradingDay,
		OutputDir:   outputDir,
		Workers:     workers,
		ZipPassword: *flagPassword,
		RowLimit:    *flagLimit,
		Optimize:    *flagOptimize,
		ForceInt32:  *flagForceInt32,
		OSSConfig:   ossConfig,
	})
	if err != nil {
		log.Fatalf("创建处理器失败: %v", err)
	}

	// 根据日期确定数据文件
	datePath := tradingDay.Format("2006/2006.01/20060102")
	datePrefix := tradingDay.Format("20060102")

	// 处理数据
	totalStart := time.Now()

	market := strings.ToLower(*flagMarket)
	dataType := strings.ToLower(*flagDataType)

	// 上交所数据
	if market == "all" || market == "sh" {
		// 2023-12-22 之后使用 mdl_4_24_0 (委托+成交合并)
		if tradingDay.After(time.Date(2023, 12, 21, 0, 0, 0, 0, time.Local)) {
			if dataType == "all" || dataType == "order" || dataType == "deal" {
				orderDealPath := findDataFile(dataDir, datePath, datePrefix+"_mdl_4_24_0.csv.zip")
				if orderDealPath != "" {
					if _, err := processor.ProcessSHOrderDeal(orderDealPath); err != nil {
						log.Printf("[错误] 处理上交所委托+成交失败: %v", err)
					}
				} else {
					log.Printf("[警告] 找不到上交所委托+成交文件")
				}
			}
		} else {
			// 旧格式
			if dataType == "all" || dataType == "order" {
				orderPath := findDataFile(dataDir, datePath, datePrefix+"_mdl_4_19_0.csv.zip")
				if orderPath != "" {
					if _, err := processor.ProcessSHOrderDeal(orderPath); err != nil {
						log.Printf("[错误] 处理上交所委托失败: %v", err)
					}
				}
			}
			if dataType == "all" || dataType == "deal" {
				dealPath := findDataFile(dataDir, datePath, datePrefix+"_Transaction.csv.zip")
				if dealPath != "" {
					// 需要单独的成交处理函数
					log.Printf("[提示] 上交所成交文件: %s (需要单独处理)", dealPath)
				}
			}
		}

		if dataType == "all" || dataType == "tick" {
			tickPath := findDataFile(dataDir, datePath, datePrefix+"_MarketData.csv.zip")
			if tickPath != "" {
				if _, err := processor.ProcessSHTick(tickPath); err != nil {
					log.Printf("[错误] 处理上交所快照失败: %v", err)
				}
			} else {
				log.Printf("[警告] 找不到上交所快照文件")
			}
		}
	}

	// 深交所数据
	if market == "all" || market == "sz" {
		if dataType == "all" || dataType == "order" {
			orderPath := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_33_0.csv.zip")
			if orderPath != "" {
				if _, err := processor.ProcessSZOrder(orderPath); err != nil {
					log.Printf("[错误] 处理深交所委托失败: %v", err)
				}
			} else {
				log.Printf("[警告] 找不到深交所委托文件")
			}
		}

		if dataType == "all" || dataType == "deal" {
			dealPath := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_36_0.csv.zip")
			if dealPath != "" {
				if _, err := processor.ProcessSZDeal(dealPath); err != nil {
					log.Printf("[错误] 处理深交所成交失败: %v", err)
				}
			} else {
				log.Printf("[警告] 找不到深交所成交文件")
			}
		}

		if dataType == "all" || dataType == "tick" {
			tickPath := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_28_0.csv.zip")
			if tickPath != "" {
				if _, err := processor.ProcessSZTick(tickPath); err != nil {
					log.Printf("[错误] 处理深交所快照失败: %v", err)
				}
			} else {
				log.Printf("[警告] 找不到深交所快照文件")
			}
		}
	}

	// 写入所有Parquet文件
	log.Println("========================================")
	log.Println("[写入] 正在将数据写入Parquet文件...")
	if err := processor.Flush(); err != nil {
		log.Fatalf("写入Parquet文件失败: %v", err)
	}

	log.Println("========================================")
	log.Printf("[完成] 总耗时: %v", time.Since(totalStart))
}

// generateDateRange 生成日期范围
func generateDateRange() ([]time.Time, error) {
	var dates []time.Time
	
	// 优先使用 start-date 和 end-date
	if *flagStartDate != "" && *flagEndDate != "" {
		start, err := time.Parse("20060102", *flagStartDate)
		if err != nil {
			return nil, fmt.Errorf("解析开始日期失败: %w", err)
		}
		end, err := time.Parse("20060102", *flagEndDate)
		if err != nil {
			return nil, fmt.Errorf("解析结束日期失败: %w", err)
		}
		
		for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
			dates = append(dates, d)
		}
		return dates, nil
	}
	
	// 使用 year 和可选的 month
	if *flagYear == 0 {
		return nil, fmt.Errorf("请指定 -year 或 -start-date/-end-date")
	}
	
	startDate := time.Date(*flagYear, 1, 1, 0, 0, 0, 0, time.Local)
	endDate := time.Date(*flagYear, 12, 31, 0, 0, 0, 0, time.Local)
	
	if *flagMonth > 0 {
		if *flagMonth < 1 || *flagMonth > 12 {
			return nil, fmt.Errorf("月份必须在 1-12 之间")
		}
		startDate = time.Date(*flagYear, time.Month(*flagMonth), 1, 0, 0, 0, 0, time.Local)
		endDate = startDate.AddDate(0, 1, -1) // 月末
	}
	
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d)
	}
	
	return dates, nil
}

// getWorkers 获取并发数
func getWorkers() int {
	workers := *flagWorkers
	if workers <= 0 {
		cpuCount := runtime.NumCPU()
		if cpuCount <= 2 {
			workers = 1
		} else if cpuCount <= 4 {
			workers = 2
		} else {
			workers = cpuCount / 3
		}
	}
	return workers
}

// getOSSConfig 获取OSS配置
func getOSSConfig() *dataconv.OSSConfig {
	if !*flagOSS && *flagOSSAccessKey == "" {
		return nil
	}
	
	return &dataconv.OSSConfig{
		AccessKeyID:     *flagOSSAccessKey,
		AccessKeySecret: *flagOSSSecretKey,
		Endpoint:        *flagOSSEndpoint,
		BucketName:      *flagOSSBucket,
		BasePath:        *flagOSSBasePath,
	}
}

// processFile 处理单个文件
func processFile(localPath string, date time.Time, processor *dataconv.Processor) error {
	filename := filepath.Base(localPath)
	log.Printf("[清洗] 处理 %s...", filename)
	
	start := time.Now()
	var err error
	
	// 根据文件名判断类型
	if strings.HasSuffix(filename, "_mdl_4_24_0.csv.zip") {
		_, err = processor.ProcessSHOrderDeal(localPath)
		log.Printf("[清洗] 上交所委托+成交 - 完成 (耗时: %v)", time.Since(start))
	} else if strings.HasSuffix(filename, "_mdl_4_19_0.csv.zip") {
		_, err = processor.ProcessSHOrderDeal(localPath)
		log.Printf("[清洗] 上交所委托 - 完成 (耗时: %v)", time.Since(start))
	} else if strings.HasSuffix(filename, "_MarketData.csv.zip") {
		_, err = processor.ProcessSHTick(localPath)
		log.Printf("[清洗] 上交所快照 - 完成 (耗时: %v)", time.Since(start))
	} else if strings.HasSuffix(filename, "_mdl_6_33_0.csv.zip") {
		_, err = processor.ProcessSZOrder(localPath)
		log.Printf("[清洗] 深交所委托 - 完成 (耗时: %v)", time.Since(start))
	} else if strings.HasSuffix(filename, "_mdl_6_36_0.csv.zip") {
		_, err = processor.ProcessSZDeal(localPath)
		log.Printf("[清洗] 深交所成交 - 完成 (耗时: %v)", time.Since(start))
	} else if strings.HasSuffix(filename, "_mdl_6_28_0.csv.zip") {
		_, err = processor.ProcessSZTick(localPath)
		log.Printf("[清洗] 深交所快照 - 完成 (耗时: %v)", time.Since(start))
	} else {
		log.Printf("[跳过] 未知文件类型: %s", filename)
		return nil
	}
	
	return err
}

// cleanLocalParquetFiles 清理本地Parquet文件
func cleanLocalParquetFiles(outputDir string, date time.Time) {
	dateStr := date.Format("20060102")
	pattern := filepath.Join(outputDir, "*"+dateStr+"*.parquet")
	
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("[警告] 查找Parquet文件失败: %v", err)
		return
	}
	
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Printf("[警告] 删除文件失败 %s: %v", filepath.Base(file), err)
		} else {
			log.Printf("[清理] 删除 %s", filepath.Base(file))
		}
	}
}

// parseTradingDay 解析交易日期
func parseTradingDay(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("请指定交易日期")
	}

	formats := []string{
		"2006-01-02",
		"20060102",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("无法解析日期: %s (支持格式: 2024-01-02 或 20240102)", s)
}

// findDataFile 查找数据文件
func findDataFile(baseDir, datePath, filename string) string {
	datePrefix := strings.Split(filename, "_")[0]
	
	paths := []string{
		filepath.Join(baseDir, datePrefix, filename),
		filepath.Join(baseDir, datePath, filename),
		filepath.Join(baseDir, filename),
		filepath.Join(baseDir, "datayes", datePath, filename),
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	matches, _ := filepath.Glob(filepath.Join(baseDir, "**", filename))
	if len(matches) > 0 {
		return matches[0]
	}

	return ""
}
