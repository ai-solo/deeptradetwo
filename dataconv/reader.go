package dataconv

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	// 快照数据使用较小的分块大小（优化为10万条，适合2GB内存）
	chunkSizeTick = 100_000
	// 其他数据使用中等分块大小（优化为50万条，避免OOM）
	chunkSizeNormal = 500_000
)

// ZipReader ZIP文件读取器
type ZipReader struct {
	password    string
	tmpDir      string
	useSystemUnzip bool  // 是否使用系统unzip命令
}

// NewZipReader 创建ZIP读取器
func NewZipReader(password string) *ZipReader {
	tmpDir := os.TempDir()

	// 检查系统是否有unzip命令
	_, err := exec.LookPath("unzip")
	useSystemUnzip := err == nil

	return &ZipReader{
		password:    password,
		tmpDir:      tmpDir,
		useSystemUnzip: useSystemUnzip,
	}
}

// ReadOptions 读取选项
type ReadOptions struct {
	ChunkSize int // 分块大小，0表示自动
	MaxRows   int // 最大读取行数，0表示不限制
}

// ReadZipFile 读取ZIP文件，返回CSV行迭代器（流式解压，节省内存）
func (r *ZipReader) ReadZipFile(zipPath string, opts ...ReadOptions) (*CSVIterator, error) {
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("文件不存在: %s", zipPath)
	}

	// 确定分块大小
	chunkSize := chunkSizeNormal
	if len(opts) > 0 && opts[0].ChunkSize > 0 {
		chunkSize = opts[0].ChunkSize
	} else {
		// 根据文件名自动选择分块大小
		if strings.Contains(zipPath, "6_28_0") || strings.Contains(zipPath, "MarketData") {
			chunkSize = chunkSizeTick
		}
	}

	maxRows := 0
	if len(opts) > 0 {
		maxRows = opts[0].MaxRows
	}

	// 检查是否加密
	isEncrypted, err := r.isEncrypted(zipPath)
	if err != nil {
		return nil, fmt.Errorf("检查加密状态失败: %w", err)
	}

	// 如果加密，使用传统解压方式
	if isEncrypted {
		log.Printf("[警告] ZIP加密，需要完整解压（占用磁盘空间）")
		return r.readZipExtract(zipPath, isEncrypted, chunkSize, maxRows)
	}

	// 非加密ZIP使用流式读取（不解压到磁盘，节省内存和磁盘）
	log.Printf("[优化] 使用流式解压（不占用磁盘空间）")
	return r.readZipStreaming(zipPath, chunkSize, maxRows)
}

// readZipStreaming 流式读取ZIP（不解压到磁盘）
func (r *ZipReader) readZipStreaming(zipPath string, chunkSize int, maxRows int) (*CSVIterator, error) {
	// 打开ZIP
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("打开ZIP失败: %w", err)
	}

	if len(zipReader.File) == 0 {
		zipReader.Close()
		return nil, fmt.Errorf("ZIP为空")
	}

	// 打开ZIP内第一个文件
	zipFile := zipReader.File[0]
	rc, err := zipFile.Open()
	if err != nil {
		zipReader.Close()
		return nil, fmt.Errorf("打开ZIP内文件失败: %w", err)
	}

	// 创建CSV读取器
	csvReader := csv.NewReader(bufio.NewReaderSize(rc, 2*1024*1024)) // 2MB缓冲
	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	return &CSVIterator{
		file:       nil,
		reader:     csvReader,
		chunkSize:  chunkSize,
		extractDir: "",
		header:     nil,
		maxRows:    maxRows,
		readRows:   0,
		zipReader:  zipReader,
		zipRC:      rc,
		streaming:  true,
	}, nil
}

// readZipExtract 传统方式：解压到临时目录
func (r *ZipReader) readZipExtract(zipPath string, isEncrypted bool, chunkSize int, maxRows int) (*CSVIterator, error) {
	// 解压到临时目录
	extractDir, fileName, err := r.extractZip(zipPath, isEncrypted)
	if err != nil {
		return nil, fmt.Errorf("解压失败: %w", err)
	}

	csvPath := filepath.Join(extractDir, fileName)

	// 打开CSV文件
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("打开CSV失败: %w", err)
	}

	csvReader := csv.NewReader(bufio.NewReaderSize(file, 1024*1024))
	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	return &CSVIterator{
		file:       file,
		reader:     csvReader,
		chunkSize:  chunkSize,
		extractDir: extractDir,
		header:     nil,
		maxRows:    maxRows,
		readRows:   0,
		zipReader:  nil,
		zipRC:      nil,
		streaming:  false,
	}, nil
}

// isEncrypted 检查ZIP是否加密
func (r *ZipReader) isEncrypted(zipPath string) (bool, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	for _, f := range reader.File {
		if f.Flags&0x1 != 0 {
			return true, nil
		}
	}
	return false, nil
}

// extractZip 解压ZIP文件
func (r *ZipReader) extractZip(zipPath string, isEncrypted bool) (extractDir string, fileName string, err error) {
	// 创建临时目录
	baseName := strings.TrimSuffix(filepath.Base(zipPath), ".zip")
	extractDir = filepath.Join(r.tmpDir, "dataconv_"+baseName)

	// 清理已存在的目录
	os.RemoveAll(extractDir)
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		return "", "", fmt.Errorf("创建临时目录失败: %w", err)
	}

	// 获取ZIP内的文件名
	{
		reader, err := zip.OpenReader(zipPath)
		if err != nil {
			return "", "", err
		}
		if len(reader.File) == 0 {
			reader.Close()
			return "", "", fmt.Errorf("ZIP文件为空")
		}
		fileName = reader.File[0].Name
		reader.Close()
	}

	// 使用系统unzip命令（更快）
	if r.useSystemUnzip {
		var cmd *exec.Cmd
		if isEncrypted {
			cmd = exec.Command("unzip", "-q", "-P", r.password, zipPath, "-d", extractDir)
		} else {
			cmd = exec.Command("unzip", "-q", zipPath, "-d", extractDir)
		}
		if err := cmd.Run(); err != nil {
			// 如果系统unzip失败，回退到Go实现
			log.Printf("[警告] 系统unzip失败，回退到Go实现: %v", err)
			return r.extractZipNative(zipPath, extractDir, isEncrypted)
		}
		return extractDir, fileName, nil
	}

	// 使用Go原生实现
	return r.extractZipNative(zipPath, extractDir, isEncrypted)
}

// extractZipNative 使用Go原生方法解压 (不支持加密，需要使用系统unzip)
func (r *ZipReader) extractZipNative(zipPath, extractDir string, isEncrypted bool) (string, string, error) {
	if isEncrypted {
		return "", "", fmt.Errorf("加密ZIP需要使用系统unzip命令")
	}

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", "", err
	}
	defer reader.Close()

	var fileName string

	for _, f := range reader.File {
		fileName = f.Name

		// 打开ZIP内的文件
		rc, err := f.Open()
		if err != nil {
			return "", "", fmt.Errorf("打开ZIP内文件失败: %w", err)
		}

		// 创建目标文件
		dstPath := filepath.Join(extractDir, f.Name)
		dst, err := os.Create(dstPath)
		if err != nil {
			rc.Close()
			return "", "", fmt.Errorf("创建目标文件失败: %w", err)
		}

		// 复制数据
		_, err = io.Copy(dst, rc)
		dst.Close()
		rc.Close()

		if err != nil {
			return "", "", fmt.Errorf("解压文件失败: %w", err)
		}

		break // 只处理第一个文件
	}

	return extractDir, fileName, nil
}

// CSVIterator CSV迭代器
type CSVIterator struct {
	file       *os.File
	reader     *csv.Reader
	chunkSize  int
	extractDir string
	header     []string
	maxRows    int // 最大读取行数
	readRows   int // 已读取行数
	
	// 流式读取ZIP
	zipReader  *zip.ReadCloser // ZIP读取器
	zipRC      io.ReadCloser   // ZIP内文件读取器
	streaming  bool            // 是否流式读取
}

// Header 获取CSV表头
func (it *CSVIterator) Header() ([]string, error) {
	if it.header != nil {
		return it.header, nil
	}

	record, err := it.reader.Read()
	if err != nil {
		return nil, fmt.Errorf("读取表头失败: %w", err)
	}
	it.header = record
	return it.header, nil
}

// ReadChunk 读取一块数据
func (it *CSVIterator) ReadChunk() ([][]string, error) {
	var records [][]string

	for i := 0; i < it.chunkSize; i++ {
		// 检查是否已达到最大行数限制
		if it.maxRows > 0 && it.readRows >= it.maxRows {
			break
		}
		
		record, err := it.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取CSV行失败: %w", err)
		}
		records = append(records, record)
		it.readRows++
	}

	return records, nil
}

// ReadAll 读取所有数据
func (it *CSVIterator) ReadAll() ([][]string, error) {
	header, err := it.Header()
	if err != nil {
		return nil, err
	}

	var allRecords [][]string
	allRecords = append(allRecords, header)

	for {
		chunk, err := it.ReadChunk()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}
		allRecords = append(allRecords, chunk...)
	}

	return allRecords, nil
}

// Close 关闭迭代器并清理临时文件
func (it *CSVIterator) Close() error {
	// 流式读取：关闭ZIP资源
	if it.streaming {
		if it.zipRC != nil {
			it.zipRC.Close()
		}
		if it.zipReader != nil {
			it.zipReader.Close()
		}
		return nil
	}
	
	// 传统方式：关闭文件并删除临时目录
	if it.file != nil {
		it.file.Close()
	}
	if it.extractDir != "" {
		os.RemoveAll(it.extractDir)
	}
	return nil
}

// Cleanup 清理所有临时文件
func (r *ZipReader) Cleanup() {
	// 清理 dataconv_ 开头的临时目录
	entries, _ := os.ReadDir(r.tmpDir)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "dataconv_") {
			os.RemoveAll(filepath.Join(r.tmpDir, entry.Name()))
		}
	}
}
