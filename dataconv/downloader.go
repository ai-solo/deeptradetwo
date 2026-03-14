package dataconv

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"deeptrade/pkg/aria2"
	"deeptrade/pkg/fileserver"
)

type Downloader struct {
	aria2Client      *aria2.Client
	fileServerClient *fileserver.Client
	downloadDir      string
	keepRaw          bool
	downloadTimeout  time.Duration
}

type DownloadTask struct {
	File         fileserver.FileInfo
	DownloadURL  string
	LocalPath    string
	Priority     int // 数字越小优先级越高
}

func NewDownloader(aria2URL, aria2Token, fileServerURL, fileServerAuth, fileServerUser, fileServerPassHash, downloadDir string, keepRaw bool, timeout time.Duration) *Downloader {
	fsClient := fileserver.NewClient(fileServerURL, fileServerAuth)
	if fileServerUser != "" && fileServerPassHash != "" {
		fsClient.SetCredentials(fileServerUser, fileServerPassHash)
	}
	return &Downloader{
		aria2Client:      aria2.NewClient(aria2URL, aria2Token),
		fileServerClient: fsClient,
		downloadDir:      downloadDir,
		keepRaw:          keepRaw,
		downloadTimeout:  timeout,
	}
}

func (d *Downloader) ListDayFiles(date time.Time) ([]fileserver.FileInfo, error) {
	path := BuildFileServerPath(date)
	return d.fileServerClient.ListFiles(path)
}

func (d *Downloader) FilterAndSortFiles(files []fileserver.FileInfo, date time.Time, market, dataType string) []DownloadTask {
	var tasks []DownloadTask
	dateStr := date.Format("20060102")

	market = strings.ToLower(market)
	dataType = strings.ToLower(dataType)

	// 是否是新格式（2023-12-22之后）
	isNewFormat := date.After(time.Date(2023, 12, 21, 0, 0, 0, 0, time.Local))

	for _, file := range files {
		if file.IsDir {
			continue
		}

		name := file.Name
		task := DownloadTask{
			File:        file,
			DownloadURL: d.fileServerClient.BuildDownloadURL(file.Path, file.Sign),
			LocalPath:   filepath.Join(d.downloadDir, date.Format("2006.01"), dateStr, name),
		}

		// Index 文件（最高优先级）
		if strings.HasSuffix(name, "_Index.csv.zip") {
			task.Priority = 1
			tasks = append(tasks, task)
			continue
		}

		// 上交所数据
		if market == "all" || market == "sh" {
			if isNewFormat {
				// 新格式：委托+成交合并
				if (dataType == "all" || dataType == "order" || dataType == "deal") &&
					strings.HasSuffix(name, "_mdl_4_24_0.csv.zip") {
					task.Priority = 50
					tasks = append(tasks, task)
					continue
				}
			} else {
				// 旧格式
				if (dataType == "all" || dataType == "order") &&
					strings.HasSuffix(name, "_mdl_4_19_0.csv.zip") {
					task.Priority = 50
					tasks = append(tasks, task)
					continue
				}
				if (dataType == "all" || dataType == "deal") &&
					strings.HasSuffix(name, "_Transaction.csv.zip") {
					task.Priority = 51
					tasks = append(tasks, task)
					continue
				}
			}

			// 上交所快照
			if (dataType == "all" || dataType == "tick") &&
				strings.HasSuffix(name, "_MarketData.csv.zip") {
				task.Priority = 100
				tasks = append(tasks, task)
				continue
			}
		}

		// 深交所数据
		if market == "all" || market == "sz" {
			if (dataType == "all" || dataType == "order") &&
				strings.Contains(name, "_mdl_6_33_") && strings.HasSuffix(name, ".csv.zip") {
				task.Priority = 20
				tasks = append(tasks, task)
				continue
			}

			if (dataType == "all" || dataType == "deal") &&
				strings.Contains(name, "_mdl_6_36_") && strings.HasSuffix(name, ".csv.zip") {
				task.Priority = 30
				tasks = append(tasks, task)
				continue
			}

			if (dataType == "all" || dataType == "tick") &&
				strings.HasSuffix(name, "_mdl_6_28_0.csv.zip") {
				task.Priority = 40
				tasks = append(tasks, task)
				continue
			}
		}
	}

	// 按优先级排序
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Priority < tasks[j].Priority
	})

	return tasks
}

// SubmitDownload 提交下载任务（不等待完成）
func (d *Downloader) SubmitDownload(task DownloadTask) (gid string, localPath string, err error) {
	// 创建目录
	dir := filepath.Dir(task.LocalPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", "", fmt.Errorf("create directory: %w", err)
	}

	// 检查文件是否已存在
	if _, err := os.Stat(task.LocalPath); err == nil {
		log.Printf("[跳过] 文件已存在: %s", task.File.Name)
		return "exists", task.LocalPath, nil
	}

	options := aria2.DownloadOptions{
		Out:              task.File.Name,
		Dir:              dir,
		CheckCertificate: "false",
	}

	gid, err = d.aria2Client.AddDownload(task.DownloadURL, options)
	if err != nil {
		return "", "", fmt.Errorf("submit download: %w", err)
	}

	return gid, task.LocalPath, nil
}

// WaitForDownload 等待指定 GID 的下载完成
func (d *Downloader) WaitForDownload(gid, localPath string) error {
	// 如果文件已存在（跳过的文件）
	if gid == "exists" {
		return nil
	}

	// 等待下载完成
	if err := d.aria2Client.WaitForCompletion(gid, d.downloadTimeout); err != nil {
		return fmt.Errorf("wait completion: %w", err)
	}

	// 清理 aria2 任务记录
	if err := d.aria2Client.RemoveDownloadResult(gid); err != nil {
		log.Printf("[警告] 清理任务记录失败: %v", err)
	}

	// 验证文件
	if _, err := os.Stat(localPath); err != nil {
		return fmt.Errorf("file not found after download: %w", err)
	}

	return nil
}

// DownloadFile 下载文件（保留向后兼容，单日处理模式使用）
func (d *Downloader) DownloadFile(task DownloadTask) error {
	log.Printf("[下载] 提交任务: %s (%.1fMB)", task.File.Name, float64(task.File.Size)/1024/1024)
	
	gid, localPath, err := d.SubmitDownload(task)
	if err != nil {
		return err
	}
	
	log.Printf("[下载] 任务GID: %s", gid)
	
	if err := d.WaitForDownload(gid, localPath); err != nil {
		return err
	}
	
	fmt.Println() // 换行（因为进度条在同一行）
	log.Printf("[下载] 完成 ✓")
	
	return nil
}

func (d *Downloader) CleanupFile(localPath string) error {
	if d.keepRaw {
		return nil
	}

	if err := os.Remove(localPath); err != nil {
		return fmt.Errorf("remove file: %w", err)
	}

	log.Printf("[清理] 删除原始文件: %s", filepath.Base(localPath))
	return nil
}

func BuildFileServerPath(date time.Time) string {
	// /baidupan/量化/2025/2025.01/20250102
	return fmt.Sprintf("/baidupan/量化/%d/%d.%02d/%s",
		date.Year(),
		date.Year(),
		date.Month(),
		date.Format("20060102"))
}
