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
	"deeptrade/storage"
)

var (
	// 命令行参数
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
	flagOSS            = flag.Bool("oss", false, "启用阿里云 OSS 上传")
	flagOSSAccessKey   = flag.String("oss-access-key", "", "OSS AccessKey ID (默认从环境变量读取)")
	flagOSSSecretKey   = flag.String("oss-secret-key", "", "OSS AccessKey Secret (默认从环境变量读取)")
	flagOSSEndpoint   = flag.String("oss-endpoint", "", "OSS Endpoint (默认: oss-cn-shanghai.aliyuncs.com)")
	flagOSSBucket     = flag.String("oss-bucket", "", "OSS Bucket 名称 (默认: stock-data)")
	flagOSSBasePath   = flag.String("oss-path", "", "OSS 存储路径前缀 (默认: market_data)")
)

func main() {
	flag.Parse()

	log.Println("========================================")
	log.Println("数据清洗工具 (Go版本)")
	log.Println("========================================")

	// 优化GC策略，对低内存环境更激进
	debug.SetGCPercent(50) // 默认100，设置为50让GC更频繁
	log.Println("[优化] GC百分比设置为50% (更频繁回收)")

	// 解析交易日期
	tradingDay, err := parseTradingDay(*flagTradingDay)
	if err != nil {
		log.Fatalf("解析交易日期失败: %v", err)
	}
	log.Printf("[配置] 交易日期: %s", tradingDay.Format("2006-01-02"))

	// 设置并发数（优化为低内存环境）
	workers := *flagWorkers
	if workers <= 0 {
		// 对于低内存环境，限制并发数
		cpuCount := runtime.NumCPU()
		if cpuCount <= 2 {
			workers = 1 // 2核或以下使用单线程，避免OOM
		} else if cpuCount <= 4 {
			workers = 2 // 4核以下使用2线程
		} else {
			workers = cpuCount / 3 // 多核环境更保守的并发数
		}
	}
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

	// 初始化MySQL (可选) - 只连接不创建表
	if !*flagNoMySQL {
		if _, err := storage.GetMySQLClient(); err != nil {
			log.Printf("[警告] MySQL连接失败，涨跌停价格将为0: %v", err)
		} else {
			defer storage.CloseMySQL()
		}
	}

	// 配置 OSS（如果启用）
	var ossConfig *dataconv.OSSConfig
	if *flagOSS || *flagOSSAccessKey != "" {
		ossConfig = &dataconv.OSSConfig{
			AccessKeyID:     *flagOSSAccessKey,
			AccessKeySecret: *flagOSSSecretKey,
			Endpoint:        *flagOSSEndpoint,
			BucketName:      *flagOSSBucket,
			BasePath:        *flagOSSBasePath,
		}
		if *flagOSS {
			log.Println("========================================")
			log.Println("[OSS] OSS 上传已启用")
			if *flagOSSAccessKey != "" {
				accessKeyLen := len(*flagOSSAccessKey)
				if accessKeyLen > 8 {
					log.Printf("  - AccessKey: %s***", (*flagOSSAccessKey)[:8])
				} else {
					log.Printf("  - AccessKey: %s***", *flagOSSAccessKey)
				}
			} else {
				log.Println("  - AccessKey: 从环境变量读取")
			}
			if *flagOSSEndpoint != "" {
				log.Printf("  - Endpoint: %s", *flagOSSEndpoint)
			}
			if *flagOSSBucket != "" {
				log.Printf("  - Bucket: %s", *flagOSSBucket)
			}
			if *flagOSSBasePath != "" {
				log.Printf("  - BasePath: %s", *flagOSSBasePath)
			}
			log.Println("========================================")
		}
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

// parseTradingDay 解析交易日期
func parseTradingDay(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("请指定交易日期")
	}

	// 尝试多种格式
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
	// 尝试多种路径
	paths := []string{
		filepath.Join(baseDir, datePath, filename),
		filepath.Join(baseDir, filename),
		filepath.Join(baseDir, "datayes", datePath, filename),
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// 在目录中搜索
	matches, _ := filepath.Glob(filepath.Join(baseDir, "**", filename))
	if len(matches) > 0 {
		return matches[0]
	}

	return ""
}
