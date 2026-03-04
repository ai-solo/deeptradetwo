package dataconv

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// OSSUploader 阿里云 OSS 上传器
type OSSUploader struct {
	bucket   *oss.Bucket
	basePath string
}

// OSSConfig OSS 配置
type OSSConfig struct {
	AccessKeyID     string
	AccessKeySecret string
	Endpoint        string
	BucketName      string
	BasePath        string // OSS 存储路径前缀 (如 "market_data")
}

// NewOSSUploader 创建 OSS 上传器
func NewOSSUploader(cfg OSSConfig) (*OSSUploader, error) {
	// 如果没有配置，从环境变量读取
	if cfg.AccessKeyID == "" {
		cfg.AccessKeyID = os.Getenv("OSS_ACCESS_KEY_ID")
	}
	if cfg.AccessKeySecret == "" {
		cfg.AccessKeySecret = os.Getenv("OSS_ACCESS_KEY_SECRET")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = os.Getenv("OSS_ENDPOINT")
		if cfg.Endpoint == "" {
			cfg.Endpoint = "oss-cn-shanghai.aliyuncs.com"
		}
	}
	if cfg.BucketName == "" {
		cfg.BucketName = os.Getenv("OSS_BUCKET_NAME")
		if cfg.BucketName == "" {
			cfg.BucketName = "stock-data"
		}
	}
	if cfg.BasePath == "" {
		cfg.BasePath = "market_data"
	}

	// 检查必需配置
	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		return nil, fmt.Errorf("OSS credentials not configured")
	}

	// 创建 OSS 客户端
	client, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("创建 OSS 客户端失败: %w", err)
	}

	// 获取 Bucket
	bucket, err := client.Bucket(cfg.BucketName)
	if err != nil {
		return nil, fmt.Errorf("获取 OSS Bucket 失败: %w", err)
	}

	return &OSSUploader{
		bucket:   bucket,
		basePath: cfg.BasePath,
	}, nil
}

// UploadFile 上传文件到 OSS
// localPath: 本地文件路径
// dataType: 数据类型 (order/deal/tick)
// tradingDay: 交易日期
// code: 股票代码
func (u *OSSUploader) UploadFile(localPath string, dataType DataType, tradingDay time.Time, code string) error {
	// 构建 OSS 路径: {year}/{yearmonth}/{yearmonthday}/{filename}
	// 例如: 2025/202512/20251231/20251231_deal.parquet
	year := tradingDay.Format("2006")       // 2025
	yearMonth := tradingDay.Format("200601") // 202512
	yearMonthDay := tradingDay.Format("20060102") // 20251231
	filename := filepath.Base(localPath)
	
	var ossPath string
	if u.basePath != "" && u.basePath != "market_data" {
		// 如果有自定义 basePath，则添加前缀
		ossPath = fmt.Sprintf("%s/%s/%s/%s/%s",
			u.basePath,
			year,
			yearMonth,
			yearMonthDay,
			filename,
		)
	} else {
		// 否则直接使用日期路径
		ossPath = fmt.Sprintf("%s/%s/%s/%s",
			year,
			yearMonth,
			yearMonthDay,
			filename,
		)
	}

	// 上传文件
	err := u.bucket.PutObjectFromFile(ossPath, localPath)
	if err != nil {
		return fmt.Errorf("上传文件到 OSS 失败 [%s]: %w", ossPath, err)
	}

	log.Printf("[OSS] 成功上传: %s", ossPath)
	return nil
}

// UploadLog 上传日志文件到 OSS
func (u *OSSUploader) UploadLog(localPath string) error {
	filename := filepath.Base(localPath)
	ossPath := fmt.Sprintf("%s/logs/%s", u.basePath, filename)

	err := u.bucket.PutObjectFromFile(ossPath, localPath)
	if err != nil {
		return fmt.Errorf("上传日志到 OSS 失败: %w", err)
	}

	log.Printf("[OSS] 日志已上传: %s", ossPath)
	return nil
}

// BatchUpload 批量上传文件
func (u *OSSUploader) BatchUpload(files []string, dataType DataType, tradingDay time.Time, code string) []error {
	var errors []error
	
	for _, file := range files {
		if err := u.UploadFile(file, dataType, tradingDay, code); err != nil {
			errors = append(errors, err)
		}
	}
	
	return errors
}

// DeleteLocalFile 删除本地文件
func DeleteLocalFile(path string) error {
	return os.Remove(path)
}

// BuildFilePath 根据交易日和文件名构建 OSS 对象 Key，与 UploadFile 的路径规则保持一致
func (u *OSSUploader) BuildFilePath(tradingDay time.Time, filename string) string {
	year        := tradingDay.Format("2006")
	yearMonth   := tradingDay.Format("200601")
	yearMonthDay := tradingDay.Format("20060102")
	if u.basePath != "" && u.basePath != "market_data" {
		return fmt.Sprintf("%s/%s/%s/%s/%s", u.basePath, year, yearMonth, yearMonthDay, filename)
	}
	return fmt.Sprintf("%s/%s/%s/%s", year, yearMonth, yearMonthDay, filename)
}

// ObjectExists 检查 OSS 中指定 key 的对象是否存在
func (u *OSSUploader) ObjectExists(ossKey string) bool {
	exist, err := u.bucket.IsObjectExist(ossKey)
	if err != nil {
		log.Printf("[OSS] IsObjectExist 错误 key=%s: %v", ossKey, err)
		return false
	}
	return exist
}

// DailyParquetFileNames 返回某交易日在 OSS 中应有的 Parquet 文件名列表。
// withDailyBasic=true 时包含 daily_basic_data（共4个），否则只含 order/deal/tick（共3个）。
func DailyParquetFileNames(tradingDay time.Time, withDailyBasic bool) []string {
	d := tradingDay.Format("20060102")
	names := []string{
		d + "_order.parquet",
		d + "_deal.parquet",
		d + "_tick.parquet",
	}
	if withDailyBasic {
		names = append(names, d+"_daily_basic_data.parquet")
	}
	return names
}

// TradeDataFilesExist 检查某交易日的 order/deal/tick 三个 Parquet 是否全部已在 OSS 中
func (u *OSSUploader) TradeDataFilesExist(tradingDay time.Time) bool {
	for _, name := range DailyParquetFileNames(tradingDay, false) {
		if !u.ObjectExists(u.BuildFilePath(tradingDay, name)) {
			return false
		}
	}
	return true
}

// AllDayFilesExist 检查某交易日的四个 Parquet 文件（含 daily_basic_data）是否全部已在 OSS 中
func (u *OSSUploader) AllDayFilesExist(tradingDay time.Time) bool {
	for _, name := range DailyParquetFileNames(tradingDay, true) {
		if !u.ObjectExists(u.BuildFilePath(tradingDay, name)) {
			return false
		}
	}
	return true
}

// UploadLocalFile 将本地文件直接按指定 OSS key 上传
func (u *OSSUploader) UploadLocalFile(localPath, ossKey string) error {
	if err := u.bucket.PutObjectFromFile(ossKey, localPath); err != nil {
		return fmt.Errorf("OSS上传失败 [%s]: %w", ossKey, err)
	}
	log.Printf("[OSS] 上传成功: %s", ossKey)
	return nil
}
