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
