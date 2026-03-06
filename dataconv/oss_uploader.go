package dataconv

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

const (
	// 超过此大小（字节）使用分片断点续传，否则用简单上传
	multipartThreshold = 32 * 1024 * 1024 // 32 MB
	// 分片大小：3GB 文件切出约 60 片，避免碎片过多
	multipartPartSize = 50 * 1024 * 1024 // 50 MB
	// 并发上传分片数：提升大文件吞吐
	multipartRoutines = 5
	// 最大重试次数
	ossMaxRetries = 5
	// 重试基础等待时间
	ossRetryBaseDelay = 2 * time.Second
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

	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		return nil, fmt.Errorf("OSS credentials not configured")
	}

	client, err := oss.New(cfg.Endpoint, cfg.AccessKeyID, cfg.AccessKeySecret,
		oss.Timeout(30, 1800), // 连接超时 30s，读写超时 1800s（单片 50MB，慢线路需要足够时间）
	)
	if err != nil {
		return nil, fmt.Errorf("创建 OSS 客户端失败: %w", err)
	}

	bucket, err := client.Bucket(cfg.BucketName)
	if err != nil {
		return nil, fmt.Errorf("获取 OSS Bucket 失败: %w", err)
	}

	return &OSSUploader{
		bucket:   bucket,
		basePath: cfg.BasePath,
	}, nil
}

// uploadWithRetry 带指数退避重试的上传，fn 是实际上传动作
func uploadWithRetry(ossPath string, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < ossMaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(math.Pow(2, float64(attempt-1))) * ossRetryBaseDelay
			log.Printf("[OSS] 第 %d 次重试 (等待 %v): %s", attempt, delay, ossPath)
			time.Sleep(delay)
		}
		if err := fn(); err != nil {
			lastErr = err
			log.Printf("[OSS] 上传失败 (attempt %d/%d) [%s]: %v", attempt+1, ossMaxRetries, ossPath, err)
			continue
		}
		return nil
	}
	return fmt.Errorf("OSS 上传失败，已重试 %d 次 [%s]: %w", ossMaxRetries, ossPath, lastErr)
}

// putFile 根据文件大小自动选择简单上传或分片断点续传
func (u *OSSUploader) putFile(ossPath, localPath string) error {
	info, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("无法获取文件信息 [%s]: %w", localPath, err)
	}

	if info.Size() >= multipartThreshold {
		// 分片断点续传，checkpoint 文件放在同目录
		cpFile := localPath + ".cp"
		defer os.Remove(cpFile)
		return u.bucket.UploadFile(ossPath, localPath, multipartPartSize,
			oss.Checkpoint(true, cpFile),
			oss.Routines(multipartRoutines),
		)
	}
	return u.bucket.PutObjectFromFile(ossPath, localPath)
}

// UploadFile 上传文件到 OSS，支持重试与分片断点续传
// localPath: 本地文件路径
// dataType: 数据类型 (order/deal/tick)
// tradingDay: 交易日期
// code: 股票代码
func (u *OSSUploader) UploadFile(localPath string, dataType DataType, tradingDay time.Time, code string) error {
	year         := tradingDay.Format("2006")
	yearMonth    := tradingDay.Format("200601")
	yearMonthDay := tradingDay.Format("20060102")
	filename     := filepath.Base(localPath)

	var ossPath string
	if u.basePath != "" && u.basePath != "market_data" {
		ossPath = fmt.Sprintf("%s/%s/%s/%s/%s", u.basePath, year, yearMonth, yearMonthDay, filename)
	} else {
		ossPath = fmt.Sprintf("%s/%s/%s/%s", year, yearMonth, yearMonthDay, filename)
	}

	if err := uploadWithRetry(ossPath, func() error {
		return u.putFile(ossPath, localPath)
	}); err != nil {
		return err
	}

	log.Printf("[OSS] 成功上传: %s", ossPath)
	return nil
}

// UploadLog 上传日志文件到 OSS，支持重试
func (u *OSSUploader) UploadLog(localPath string) error {
	filename := filepath.Base(localPath)
	ossPath := fmt.Sprintf("%s/logs/%s", u.basePath, filename)

	if err := uploadWithRetry(ossPath, func() error {
		return u.bucket.PutObjectFromFile(ossPath, localPath)
	}); err != nil {
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

// UploadLocalFile 将本地文件直接按指定 OSS key 上传，支持重试与分片断点续传
func (u *OSSUploader) UploadLocalFile(localPath, ossKey string) error {
	if err := uploadWithRetry(ossKey, func() error {
		return u.putFile(ossKey, localPath)
	}); err != nil {
		return fmt.Errorf("OSS上传失败 [%s]: %w", ossKey, err)
	}
	log.Printf("[OSS] 上传成功: %s", ossKey)
	return nil
}

// DeleteFile 删除 OSS 中的指定文件
func (u *OSSUploader) DeleteFile(ossKey string) error {
	if err := u.bucket.DeleteObject(ossKey); err != nil {
		return fmt.Errorf("OSS删除失败 [%s]: %w", ossKey, err)
	}
	log.Printf("[OSS] 删除成功: %s", ossKey)
	return nil
}

// DeleteDailyBasicFile 删除指定交易日的 daily_basic_data.parquet 文件
func (u *OSSUploader) DeleteDailyBasicFile(tradingDay time.Time) error {
	filename := tradingDay.Format("20060102") + "_daily_basic_data.parquet"
	ossKey := u.BuildFilePath(tradingDay, filename)
	return u.DeleteFile(ossKey)
}
