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
	"sync"
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
	flagDataPrefix   = flag.String("prefix", "", "数据文件路径前缀 (可选)")
	flagNoMySQL      = flag.Bool("no-mysql", false, "不使用MySQL (涨跌停价格将为0)")
	flagLimit        = flag.Int("limit", 0, "限制处理行数 (0表示不限制，用于测试)")
	flagOptimize     = flag.Bool("optimize", false, "启用优化模式 (Int32+Zstd压缩，需MySQL)")
	flagForceInt32   = flag.Bool("force-int32", false, "强制Int32模式 (不检测超限，有溢出风险)")
	
	// OSS 相关参数
	flagOSS          = flag.Bool("oss", false, "启用阿里云 OSS 上传")
	flagOSSAccessKey = flag.String("oss-access-key", "", "OSS AccessKey ID (默认从环境变量读取)")
	flagOSSSecretKey = flag.String("oss-secret-key", "", "OSS AccessKey Secret (默认从环境变量读取)")
	flagOSSEndpoint  = flag.String("oss-endpoint", "", "OSS Endpoint (默认: oss-cn-shanghai.aliyuncs.com)")
	flagOSSBucket    = flag.String("oss-bucket", "", "OSS Bucket 名称 (默认: stock-data)")
	flagOSSBasePath  = flag.String("oss-path", "", "OSS 存储路径前缀 (默认: market_data)")
	flagOSSCleanLocal = flag.Bool("oss-clean-local", false, "OSS上传后删除本地Parquet文件")
	
	// 重排序参数
	flagResort = flag.Bool("resort", false, "OSS已有文件时，下载→按Code+SeqNum重排序→重新上传（替代直接跳过）")

	// 自动下载参数
	flagAutoDownload     = flag.Bool("auto-download", false, "启用自动下载模式")
	flagAria2URL         = flag.String("aria2-url", "", "Aria2 JSON-RPC地址")
	flagAria2Token       = flag.String("aria2-token", "", "Aria2 认证token")
	flagFileServerURL      = flag.String("fileserver-url", "", "文件服务器地址")
	flagFileServerAuth     = flag.String("fileserver-auth", "", "文件服务器Authorization token (可选，过期时自动重新登录)")
	flagFileServerUser     = flag.String("fileserver-username", "admin", "文件服务器登录用户名 (用于token过期时自动重新登录)")
	flagFileServerPassHash = flag.String("fileserver-password", "", "文件服务器登录密码SHA256哈希值 (用于token过期时自动重新登录)")
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

	// daily basic data 参数
	flagDailyBasic        = flag.Bool("daily-basic", false, "生成每日基础数据 Parquet (equity/exposure/mkt_idx/daily_basic_data)")
	flagDailyBasicSkipOSS = flag.Bool("daily-basic-skip-oss-check", false, "跳过 OSS 存在检查，强制重新生成 daily basic data")
	flagDailyBasicForce   = flag.Bool("daily-basic-force", false, "强制删除并重新生成 daily_basic_data.parquet (添加 SECURITY_ID 字段)")
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

	// 判断运行模式
	switch {
	case *flagAutoDownload && *flagYear > 0 && *flagMonth > 0:
		// 自动下载 + 整月流水线处理
		runMonthMode()
	case *flagAutoDownload && (*flagYear > 0 || *flagStartDate != ""):
		// 自动下载 + 批量处理（原有逻辑）
		runBatchMode()
	case *flagDailyBasic && (*flagYear > 0 || *flagStartDate != ""):
		// 仅生成 daily basic data（不需要下载原始数据）
		runDailyBasicMonthMode()
	default:
		runSingleDayMode()
	}
}

// runMonthMode 整月流水线模式：一次提交整月所有文件到 aria2，逐天串行处理
// 下载和处理并行：处理第N天时，第N+1天的文件已在后台下载
func runMonthMode() {
	log.Printf("[模式] 整月流水线处理 %d-%02d", *flagYear, *flagMonth)

	// 生成本月所有日期
	dates, err := generateDateRange()
	if err != nil {
		log.Fatalf("生成日期范围失败: %v", err)
	}

	ossConfig := getOSSConfig()
	workers := getWorkers()
	totalStart := time.Now()

	// 初始化 MySQL
	if !*flagNoMySQL {
		if _, err := storage.GetMySQLClient(); err != nil {
			log.Printf("[警告] MySQL连接失败，涨跌停价格将为0: %v", err)
		} else {
			defer storage.CloseMySQL()
		}
	}

	// 初始化下载器
	downloader := dataconv.NewDownloader(
		*flagAria2URL,
		*flagAria2Token,
		*flagFileServerURL,
		*flagFileServerAuth,
		*flagFileServerUser,
		*flagFileServerPassHash,
		*flagDownloadDir,
		*flagKeepRaw,
		time.Duration(*flagDownloadTimeout)*time.Minute,
	)

	if err := os.MkdirAll(*flagOutputDir, 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}

	log.Println("========================================")
	log.Printf("[月模式] 共 %d 天: %s ~ %s",
		len(dates), dates[0].Format("2006-01-02"), dates[len(dates)-1].Format("2006-01-02"))

	// ---- 阶段1: OSS 预检，分类每天的处理策略 ----
	type dayPlan struct {
		date          time.Time
		needProcess   bool   // 需要下载+完整处理
		needDailyOnly bool   // 只需补 daily_basic
	}

	plans := make([]dayPlan, 0, len(dates))
	var uploader *dataconv.OSSUploader
	if *flagOSS && ossConfig != nil {
		uploader, _ = dataconv.NewOSSUploader(*ossConfig)
	}

	for _, date := range dates {
		if uploader != nil && !*flagDailyBasicSkipOSS {
			tradeExist := uploader.TradeDataFilesExist(date)
			dailyBasicExist := !*flagDailyBasic || uploader.ObjectExists(
				uploader.BuildFilePath(date, date.Format("20060102")+"_daily_basic_data.parquet"),
			)
			if tradeExist && dailyBasicExist {
				log.Printf("[跳过] %s 所有文件已在 OSS", date.Format("2006-01-02"))
				continue
			}
			if tradeExist && !dailyBasicExist {
				log.Printf("[补充] %s 仅补 daily_basic", date.Format("2006-01-02"))
				plans = append(plans, dayPlan{date: date, needDailyOnly: true})
				continue
			}
		}
		plans = append(plans, dayPlan{date: date, needProcess: true})
	}

	log.Printf("[月模式] 需完整处理: %d 天，仅补daily_basic: %d 天",
		func() int { n := 0; for _, p := range plans { if p.needProcess { n++ } }; return n }(),
		func() int { n := 0; for _, p := range plans { if p.needDailyOnly { n++ } }; return n }(),
	)

	// ---- 阶段2: 先补 daily_basic（不需要下载）----
	for _, plan := range plans {
		if !plan.needDailyOnly {
			continue
		}
		log.Printf("[daily_basic] 补生成 %s...", plan.date.Format("2006-01-02"))
		n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
			TradingDay: plan.date,
			OutputDir:  *flagOutputDir,
			OSSConfig:  ossConfig,
			ForceRegen: *flagDailyBasicForce,
		})
		if err != nil {
			log.Printf("[daily_basic] %s 失败: %v", plan.date.Format("2006-01-02"), err)
		} else {
			log.Printf("[daily_basic] %s 成功生成 %d 个文件", plan.date.Format("2006-01-02"), n)
		}
	}

	// ---- 阶段3: 对需要完整处理的天，提交所有文件到 aria2 ----
	type dayJobs struct {
		date time.Time
		jobs []struct {
			task          dataconv.DownloadTask
			gid           string
			alreadyExists bool
		}
	}

	allDayJobs := make([]dayJobs, 0)

	for _, plan := range plans {
		if !plan.needProcess {
			continue
		}
		date := plan.date

		// 列出该天文件
		files, err := downloader.ListDayFiles(date)
		if err != nil {
			log.Printf("[错误] %s 列出文件失败: %v", date.Format("2006-01-02"), err)
			continue
		}
		tasks := downloader.FilterAndSortFiles(files, date, *flagMarket, *flagDataType)
		if len(tasks) == 0 {
			log.Printf("[警告] %s 没有需要处理的文件", date.Format("2006-01-02"))
			continue
		}

		dj := dayJobs{date: date}
		for _, task := range tasks {
			// 已存在则不重复下载
			if stat, err := os.Stat(task.LocalPath); err == nil && stat.Size() > 0 {
				log.Printf("[已存在] %s %s (%.1fMB)",
					date.Format("20060102"), task.File.Name, float64(stat.Size())/1024/1024)
				dj.jobs = append(dj.jobs, struct {
					task          dataconv.DownloadTask
					gid           string
					alreadyExists bool
				}{task, "exists", true})
				continue
			}
			gid, _, err := downloader.SubmitDownload(task)
			if err != nil {
				log.Printf("[错误] %s 提交下载失败 %s: %v", date.Format("20060102"), task.File.Name, err)
				continue
			}
			log.Printf("[下载] %s 已提交 %s (gid=%s)", date.Format("20060102"), task.File.Name, gid)
			dj.jobs = append(dj.jobs, struct {
				task          dataconv.DownloadTask
				gid           string
				alreadyExists bool
			}{task, gid, false})
		}
		if len(dj.jobs) > 0 {
			allDayJobs = append(allDayJobs, dj)
		}
	}

	log.Printf("[月模式] 所有下载任务已提交，开始逐天处理...")
	log.Println("========================================")

	// ---- 阶段4: 逐天等待下载完成 → 处理 → 清理 ----
	var successDays, failedDays int
	for _, dj := range allDayJobs {
		date := dj.date
		dayStart := time.Now()
		log.Println("========================================")
		log.Printf("[处理] %s 等待文件下载完成...", date.Format("2006-01-02"))

		// 等待该天所有文件下载完成
		hasError := false
		for _, job := range dj.jobs {
			if job.alreadyExists {
				continue
			}
			if err := downloader.WaitForDownload(job.gid, job.task.LocalPath); err != nil {
				log.Printf("[错误] %s 下载失败 %s: %v", date.Format("20060102"), job.task.File.Name, err)
				hasError = true
			}
		}
		if hasError {
			log.Printf("[错误] %s 部分文件下载失败，跳过处理", date.Format("2006-01-02"))
			failedDays++
			continue
		}

		// 构建 zipFiles map
		zipFiles := make(map[string][]string)
		for _, job := range dj.jobs {
			name := job.task.File.Name
			switch {
			case strings.HasSuffix(name, "_mdl_4_24_0.csv.zip"):
				zipFiles["sh_orderdeal"] = append(zipFiles["sh_orderdeal"], job.task.LocalPath)
			case strings.HasSuffix(name, "_MarketData.csv.zip"):
				zipFiles["sh_tick"] = append(zipFiles["sh_tick"], job.task.LocalPath)
			case strings.Contains(name, "_mdl_6_33_") && strings.HasSuffix(name, ".csv.zip"):
				zipFiles["sz_order"] = append(zipFiles["sz_order"], job.task.LocalPath)
			case strings.Contains(name, "_mdl_6_36_") && strings.HasSuffix(name, ".csv.zip"):
				zipFiles["sz_deal"] = append(zipFiles["sz_deal"], job.task.LocalPath)
			case strings.HasSuffix(name, "_mdl_6_28_0.csv.zip"):
				zipFiles["sz_tick"] = append(zipFiles["sz_tick"], job.task.LocalPath)
			}
		}

		// 创建处理器
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
			log.Printf("[错误] %s 创建处理器失败: %v", date.Format("2006-01-02"), err)
			failedDays++
			continue
		}

		// 处理
		if err := processor.ProcessAllInMemory(zipFiles); err != nil {
			log.Printf("[错误] %s 处理失败: %v", date.Format("2006-01-02"), err)
			failedDays++
			continue
		}

		// 生成 daily basic data
		if *flagDailyBasic {
			log.Printf("[daily_basic] 生成 %s...", date.Format("2006-01-02"))
			n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
				TradingDay: date,
				OutputDir:  *flagOutputDir,
				OSSConfig:  ossConfig,
				ForceRegen: *flagDailyBasicForce,
			})
			if err != nil {
				log.Printf("[daily_basic] %s 失败: %v", date.Format("2006-01-02"), err)
			} else {
				log.Printf("[daily_basic] %s 成功生成 %d 个文件", date.Format("2006-01-02"), n)
			}
		}

		// 清理原始 ZIP
		if !*flagKeepRaw {
			for _, job := range dj.jobs {
				_ = downloader.CleanupFile(job.task.LocalPath)
			}
		}

		// 清理本地 Parquet
		if *flagOSS && *flagOSSCleanLocal {
			cleanLocalParquetFiles(*flagOutputDir, date)
		}

		successDays++
		log.Printf("[完成] %s 耗时 %v", date.Format("2006-01-02"), time.Since(dayStart))
	}

	log.Println("========================================")
	log.Printf("[月模式] 完成 %d-%02d: 成功 %d 天，失败 %d 天，总耗时 %v",
		*flagYear, *flagMonth, successDays, failedDays, time.Since(totalStart))
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
	
	// 创建输出目录
	if err := os.MkdirAll(*flagOutputDir, 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}
	
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
		*flagFileServerUser,
		*flagFileServerPassHash,
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
			// 若开启了 -daily-basic，还需确认 daily_basic_data 已在 OSS，
			// 否则即使 trade 数据已处理，也要补跑 SQL 生成第4份文件。
			if *flagDailyBasic && *flagOSS && !*flagDailyBasicSkipOSS {
				if ossConfig := getOSSConfig(); ossConfig != nil {
					if uploader, err := dataconv.NewOSSUploader(*ossConfig); err == nil {
						dailyBasicKey := uploader.BuildFilePath(date, dateStr+"_daily_basic_data.parquet")
						if !uploader.ObjectExists(dailyBasicKey) {
							log.Printf("[补充] %s 已处理，但 daily_basic_data 缺失，补生成...", dateStr)
							n, genErr := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
								TradingDay: date,
								OutputDir:  *flagOutputDir,
								OSSConfig:  ossConfig,
								ForceRegen: *flagDailyBasicForce,
							})
							if genErr != nil {
								log.Printf("[daily_basic] 生成失败: %v", genErr)
							} else {
								log.Printf("[daily_basic] 成功生成 %d 个文件 ✓", n)
							}
						}
					}
				}
			}
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

	// 0. OSS 预检：根据结果文件是否存在决定跳过策略
	//
	//   情况A：4个文件全在 OSS                          → 跳过（-resort 时执行重排序）
	//   情况B：order/deal/tick 全在，仅 daily_basic 缺失 → 跳过下载，只补 daily_basic
	//   情况C：order/deal/tick 有缺失                    → 走完整下载+处理流程
	if *flagOSS && !*flagDailyBasicSkipOSS {
		ossConfig := getOSSConfig()
		if ossConfig != nil {
			if uploader, err := dataconv.NewOSSUploader(*ossConfig); err == nil {
				tradeExist := uploader.TradeDataFilesExist(date)
				dailyBasicExist := !*flagDailyBasic || uploader.ObjectExists(
					uploader.BuildFilePath(date, date.Format("20060102")+"_daily_basic_data.parquet"),
				)

				if tradeExist && dailyBasicExist {
					// 情况A：全部存在
					if *flagResort && *flagOptimize {
						log.Println("========================================")
						log.Printf("[重排序] %s 所有文件已在 OSS，执行重排序...", date.Format("2006-01-02"))
						log.Println("========================================")
						workers := getWorkers()
						processor, perr := dataconv.NewProcessor(dataconv.ProcessorConfig{
							TradingDay:  date,
							OutputDir:   *flagOutputDir,
							Workers:     workers,
							ZipPassword: *flagPassword,
							Optimize:    true,
							OSSConfig:   ossConfig,
						})
						if perr != nil {
							return fmt.Errorf("创建处理器失败: %w", perr)
						}
						tmpDir := filepath.Join(*flagOutputDir, ".resort_tmp", date.Format("20060102"))
						if err := os.MkdirAll(tmpDir, 0755); err != nil {
							return fmt.Errorf("创建临时目录失败: %w", err)
						}
						defer os.RemoveAll(tmpDir)
						if err := processor.ResortFromOSS(date, tmpDir); err != nil {
							return fmt.Errorf("重排序失败: %w", err)
						}
						log.Printf("[完成] %s 重排序完成，耗时: %v", date.Format("2006-01-02"), time.Since(dayStart))
					} else {
						log.Printf("[跳过] %s 所有结果文件已在 OSS，跳过", date.Format("2006-01-02"))
					}
					return nil
				}

				if tradeExist && !dailyBasicExist {
					// 情况B：tick/order/deal 已有，只需补 daily_basic_data
					log.Printf("[补充] %s order/deal/tick 已在 OSS，仅补生成 daily_basic_data",
						date.Format("2006-01-02"))
					n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
						TradingDay: date,
						OutputDir:  *flagOutputDir,
						OSSConfig:  ossConfig,
						ForceRegen: *flagDailyBasicForce,
					})
					if err != nil {
						log.Printf("[daily_basic] 生成失败: %v", err)
					} else {
						log.Printf("[daily_basic] 成功生成 %d 个文件 ✓", n)
					}
					return nil
				}

				// 情况C：trade 文件缺失，打印缺失列表后继续走下载流程
				log.Printf("[缺失] %s 部分文件需要重新处理:", date.Format("2006-01-02"))
				for _, name := range dataconv.DailyParquetFileNames(date, *flagDailyBasic) {
					key := uploader.BuildFilePath(date, name)
					if uploader.ObjectExists(key) {
						log.Printf("  [已存在] %s", name)
					} else {
						log.Printf("  [缺失]   %s", name)
					}
				}
			}
		}
	}

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
	
	// 4. 检测文件状态，分类处理
	log.Println("========================================")
	log.Printf("[检测] 检查 %d 个文件状态...", len(tasks))
	
	type downloadJob struct {
		Task     dataconv.DownloadTask
		GID      string
		Idx      int
		AlreadyExists bool
	}
	
	jobs := make([]downloadJob, 0, len(tasks))
	var existsCount, needDownloadCount int
	
	// 检测文件并提交下载任务
	for idx, task := range tasks {
		// 检查文件是否已存在
		if stat, err := os.Stat(task.LocalPath); err == nil && stat.Size() > 0 {
			log.Printf("[已存在] [%d/%d] %s (%.1fMB, 直接处理)", 
				idx+1, len(tasks), task.File.Name, float64(stat.Size())/1024/1024)
			
			jobs = append(jobs, downloadJob{
				Task:          task,
				GID:           "exists",
				Idx:           idx,
				AlreadyExists: true,
			})
			existsCount++
			continue
		}
		
		// 文件不存在，提交下载
		gid, _, err := downloader.SubmitDownload(task)
		if err != nil {
			log.Printf("[错误] 提交下载失败 [%d/%d] %s: %v", idx+1, len(tasks), task.File.Name, err)
			continue
		}
		
		log.Printf("[提交下载] [%d/%d] %s (GID: %s)", idx+1, len(tasks), task.File.Name, gid[:8])
		
		jobs = append(jobs, downloadJob{
			Task:          task,
			GID:           gid,
			Idx:           idx,
			AlreadyExists: false,
		})
		needDownloadCount++
	}
	
	log.Println("========================================")
	log.Printf("[统计] 已存在: %d 个, 需下载: %d 个", existsCount, needDownloadCount)
	if needDownloadCount > 0 {
		log.Printf("[下载] aria2 开始并发下载 %d 个文件...", needDownloadCount)
	}
	log.Println("========================================")
	
	// 5. 并发等待各个文件下载完成并处理
	var wgFiles sync.WaitGroup
	var processErrors []error
	var errMutex sync.Mutex
	
	for _, job := range jobs {
		wgFiles.Add(1)
		
		go func(j downloadJob) {
			defer wgFiles.Done()
			
			// 如果文件已存在，直接处理
			if j.AlreadyExists {
				log.Printf("[处理] [%d/%d] %s 开始处理（文件已存在）...", j.Idx+1, len(tasks), j.Task.File.Name)
			} else {
				log.Printf("[等待] [%d/%d] %s 下载中...", j.Idx+1, len(tasks), j.Task.File.Name)
				
				// 等待这个文件下载完成
				if err := downloader.WaitForDownload(j.GID, j.Task.LocalPath); err != nil {
					log.Printf("[错误] 下载失败 [%d/%d] %s: %v", j.Idx+1, len(tasks), j.Task.File.Name, err)
					errMutex.Lock()
					processErrors = append(processErrors, err)
					errMutex.Unlock()
					return
				}
				
				log.Printf("[完成] [%d/%d] %s 下载完成 ✓", j.Idx+1, len(tasks), j.Task.File.Name)
			}
			
			// 处理文件
			if err := processFile(j.Task.LocalPath, date, processor); err != nil {
				log.Printf("[错误] 处理失败 [%d/%d] %s: %v", j.Idx+1, len(tasks), j.Task.File.Name, err)
				errMutex.Lock()
				processErrors = append(processErrors, err)
				errMutex.Unlock()
				return
			}
			
			// 清理原始文件
			if err := downloader.CleanupFile(j.Task.LocalPath); err != nil {
				log.Printf("[警告] 清理文件失败: %v", err)
			}
		}(job)
	}
	
	// 等待所有文件处理完成
	wgFiles.Wait()
	
	if len(processErrors) > 0 {
		log.Printf("[警告] %d 个文件处理失败", len(processErrors))
	}
	
	// 5. 写入 Parquet
	if *flagFlushDaily {
		log.Println("========================================")
		log.Printf("[写入] 将 %s 的数据写入Parquet文件...", date.Format("2006-01-02"))
		if err := processor.Flush(); err != nil {
			return fmt.Errorf("写入Parquet失败: %w", err)
		}
		log.Printf("[写入] 完成 ✓")

		// 6. 生成 daily basic data（equity / exposure / mkt_idx / daily_basic_data）
		if *flagDailyBasic {
			log.Printf("[daily_basic] 开始生成 %s 的每日基础数据...", date.Format("2006-01-02"))
			n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
				TradingDay: date,
				OutputDir:  *flagOutputDir,
				OSSConfig:  getOSSConfig(),
				ForceRegen: *flagDailyBasicForce,
			})
			if err != nil {
				log.Printf("[daily_basic] 生成失败: %v", err)
			} else {
				log.Printf("[daily_basic] 成功生成 %d 个文件 ✓", n)
			}
		}

		// 7. OSS上传后清理本地 Parquet
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

	// OSS 前置检查：三个文件都存在时走重排序路径
	if *flagOSS && *flagOptimize {
		ossConfig := getOSSConfig()
		if ossConfig != nil {
			uploader, uperr := dataconv.NewOSSUploader(*ossConfig)
			if uperr == nil && uploader.TradeDataFilesExist(tradingDay) {
				if *flagResort {
					log.Println("========================================")
					log.Printf("[OSS] %s 三个文件已存在，执行重排序...", tradingDay.Format("20060102"))
					log.Println("========================================")
					tmpDir := filepath.Join(outputDir, ".resort_tmp")
					if err := os.MkdirAll(tmpDir, 0755); err != nil {
						log.Fatalf("创建临时目录失败: %v", err)
					}
					defer os.RemoveAll(tmpDir)
					if err := processor.ResortFromOSS(tradingDay, tmpDir); err != nil {
						log.Fatalf("重排序失败: %v", err)
					}
					log.Printf("[完成] 重排序完成")
				} else {
					log.Printf("[跳过] %s OSS已有三个数据文件，跳过处理（用 -resort 强制重排序）", tradingDay.Format("20060102"))
				}
				return
			}
		}
	}

	// 根据日期确定数据文件
	datePath := tradingDay.Format("2006/2006.01/20060102")
	datePrefix := tradingDay.Format("20060102")

	// 处理数据
	totalStart := time.Now()

	market := strings.ToLower(*flagMarket)
	dataType := strings.ToLower(*flagDataType)

	// 构建文件映射，走全量内存处理路径（-optimize 模式）
	if *flagOptimize {
		zipFiles := map[string][]string{}

		if market == "all" || market == "sh" {
			if dataType == "all" || dataType == "order" || dataType == "deal" {
				if tradingDay.After(time.Date(2023, 12, 21, 0, 0, 0, 0, time.Local)) {
					if p := findDataFile(dataDir, datePath, datePrefix+"_mdl_4_24_0.csv.zip"); p != "" {
						zipFiles["sh_orderdeal"] = append(zipFiles["sh_orderdeal"], p)
					} else {
						log.Printf("[警告] 找不到上交所委托+成交文件")
					}
				} else {
					if p := findDataFile(dataDir, datePath, datePrefix+"_mdl_4_19_0.csv.zip"); p != "" {
						zipFiles["sh_orderdeal"] = append(zipFiles["sh_orderdeal"], p)
					}
				}
			}
			if dataType == "all" || dataType == "tick" {
				if p := findDataFile(dataDir, datePath, datePrefix+"_MarketData.csv.zip"); p != "" {
					zipFiles["sh_tick"] = append(zipFiles["sh_tick"], p)
				} else {
					log.Printf("[警告] 找不到上交所快照文件")
				}
			}
		}

		if market == "all" || market == "sz" {
			if dataType == "all" || dataType == "order" {
				if p := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_33_0.csv.zip"); p != "" {
					zipFiles["sz_order"] = append(zipFiles["sz_order"], p)
				} else {
					log.Printf("[警告] 找不到深交所委托文件")
				}
			}
			if dataType == "all" || dataType == "deal" {
				if p := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_36_0.csv.zip"); p != "" {
					zipFiles["sz_deal"] = append(zipFiles["sz_deal"], p)
				} else {
					log.Printf("[警告] 找不到深交所成交文件")
				}
			}
			if dataType == "all" || dataType == "tick" {
				// 只取 _mdl_6_28_0，_mdl_6_28_1/_2 是 OrderQueue 档位数据，格式不同
				if p := findDataFile(dataDir, datePath, datePrefix+"_mdl_6_28_0.csv.zip"); p != "" {
					zipFiles["sz_tick"] = append(zipFiles["sz_tick"], p)
				} else {
					log.Printf("[警告] 找不到深交所快照文件")
				}
			}
		}

		if err := processor.ProcessAllInMemory(zipFiles); err != nil {
			log.Fatalf("全量内存处理失败: %v", err)
		}

		// 生成 daily basic data
		if *flagDailyBasic {
			log.Printf("[daily_basic] 开始生成 %s 的每日基础数据...", tradingDay.Format("2006-01-02"))
			n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
				TradingDay: tradingDay,
				OutputDir:  outputDir,
				OSSConfig:  ossConfig,
				ForceRegen: *flagDailyBasicForce,
			})
			if err != nil {
				log.Printf("[daily_basic] 生成失败: %v", err)
			} else {
				log.Printf("[daily_basic] 成功生成 %d 个文件 ✓", n)
			}
		}

		// OSS上传后清理本地 Parquet
		if *flagOSS && *flagOSSCleanLocal {
			log.Printf("[清理] 删除本地Parquet文件...")
			cleanLocalParquetFiles(outputDir, tradingDay)
		}

		log.Println("========================================")
		log.Printf("[完成] 总耗时: %v", time.Since(totalStart))
		return
	}

	// 非 optimize 模式：原有逐文件处理路径
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
// 数据处理是IO密集型任务，可以使用更多goroutine
func getWorkers() int {
	workers := *flagWorkers
	if workers <= 0 {
		cpuCount := runtime.NumCPU()
		// 优化：IO密集型任务，即使2核也可以用更多goroutine
		if cpuCount <= 2 {
			workers = 8  // 2核机器使用8个worker（原来是1）
		} else if cpuCount <= 4 {
			workers = 12 // 4核机器使用12个worker（原来是2）
		} else {
			workers = cpuCount * 3 // 更多核心按3倍设置
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

// runDailyBasicMonthMode 仅生成 daily basic data，不需要下载原始数据。
// 支持 -year/-month 或 -start-date/-end-date 指定日期范围。
// 每天处理前先检查 OSS，四个文件均已存在则跳过（除非 -daily-basic-skip-oss-check）。
func runDailyBasicMonthMode() {
	log.Println("[模式] Daily Basic Data 月份生成")

	dates, err := generateDateRange()
	if err != nil {
		log.Fatalf("生成日期范围失败: %v", err)
	}

	log.Printf("[配置] 日期范围: %s ~ %s (共 %d 天)",
		dates[0].Format("2006-01-02"),
		dates[len(dates)-1].Format("2006-01-02"),
		len(dates))
	log.Printf("[配置] 输出目录: %s", *flagOutputDir)
	if *flagOSS {
		log.Printf("[配置] OSS上传: 启用")
	}
	log.Println("========================================")

	if err := os.MkdirAll(*flagOutputDir, 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}

	ossConfig := getOSSConfig()
	var uploader *dataconv.OSSUploader
	if ossConfig != nil {
		uploader, err = dataconv.NewOSSUploader(*ossConfig)
		if err != nil {
			log.Printf("[警告] OSS初始化失败，将跳过存在检查和上传: %v", err)
			uploader = nil
		}
	}

	// 初始化MySQL（daily basic data 必须用 MySQL）
	if _, err := storage.GetMySQLClient(); err != nil {
		log.Fatalf("MySQL连接失败，daily basic data 无法生成: %v", err)
	}
	defer storage.CloseMySQL()

	totalStart := time.Now()
	var successDays, skipDays, failDays int

	for i, date := range dates {
		dateStr := date.Format("20060102")
		log.Printf("\n[%d/%d] %s", i+1, len(dates), date.Format("2006-01-02"))

		// 检查 OSS：四个文件都存在则跳过（ForceRegen=true 时不跳过）
		if uploader != nil && !*flagDailyBasicSkipOSS && !*flagDailyBasicForce {
			if uploader.AllDayFilesExist(date) {
				log.Printf("[跳过] OSS 中已存在四个 Parquet 文件")
				skipDays++
				continue
			}
			// 输出缺失的文件名
			for _, name := range dataconv.DailyParquetFileNames(date, true) {
				ossKey := uploader.BuildFilePath(date, name)
				if !uploader.ObjectExists(ossKey) {
					log.Printf("[缺失] %s", name)
				}
			}
		}

		n, err := dataconv.GenerateDailyBasicData(dataconv.DailyBasicConfig{
			TradingDay: date,
			OutputDir:  *flagOutputDir,
			OSSConfig:  ossConfig,
			ForceRegen: *flagDailyBasicForce,
		})
		if err != nil {
			log.Printf("[失败] %s: %v", dateStr, err)
			failDays++
			continue
		}
		log.Printf("[完成] %s — 生成 %d 个文件", dateStr, n)
		successDays++
	}

	log.Println("\n========================================")
	log.Println("[总结] Daily Basic Data 生成完成")
	log.Printf("  - 成功: %d 天", successDays)
	log.Printf("  - 跳过: %d 天 (OSS已存在)", skipDays)
	log.Printf("  - 失败: %d 天", failDays)
	log.Printf("  - 总耗时: %v", time.Since(totalStart))
	log.Println("========================================")
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
