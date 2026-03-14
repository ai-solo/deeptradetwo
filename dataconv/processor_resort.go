package dataconv

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// ResortFromOSS 从 OSS 下载已有的 order/deal/tick parquet，按 Code+SeqNum 重排序后重新上传，删除旧文件
// 用于：OSS 已有文件但未排序的场景
func (p *Processor) ResortFromOSS(tradingDay time.Time, tmpDir string) error {
	if p.optParquetWriter == nil {
		return fmt.Errorf("ResortFromOSS 需要 optimize 模式（optParquetWriter）")
	}
	uploader := p.optParquetWriter.ossUploader
	if uploader == nil {
		return fmt.Errorf("ResortFromOSS 需要配置 OSS")
	}
	secCache := p.securityCache

	// 1. 下载三个文件
	log.Printf("[重排序] 从 OSS 下载 %s 的数据文件...", tradingDay.Format("20060102"))
	orderPath, dealPath, tickPath, err := uploader.DownloadTradeDataFiles(tradingDay, tmpDir)
	if err != nil {
		return fmt.Errorf("下载失败: %w", err)
	}
	defer func() {
		os.Remove(orderPath)
		os.Remove(dealPath)
		os.Remove(tickPath)
	}()

	// 2. 读取全量数据到内存
	log.Printf("[重排序] 读取 order parquet...")
	orders, err := ReadOrdersFromParquet(orderPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 order 失败: %w", err)
	}
	log.Printf("[重排序] 读取 deal parquet...")
	deals, err := ReadDealsFromParquet(dealPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 deal 失败: %w", err)
	}
	log.Printf("[重排序] 读取 tick parquet...")
	ticks, err := ReadTicksFromParquet(tickPath, tradingDay, secCache)
	if err != nil {
		return fmt.Errorf("读取 tick 失败: %w", err)
	}

	// 3. 并发排序
	log.Printf("[重排序] 排序 %d orders, %d deals, %d ticks...", len(orders), len(deals), len(ticks))
	var sortWg sync.WaitGroup
	sortWg.Add(3)
	go func() {
		defer sortWg.Done()
		sort.Slice(orders, func(i, j int) bool {
			if orders[i].Code != orders[j].Code {
				return orders[i].Code < orders[j].Code
			}
			return orders[i].SeqNum < orders[j].SeqNum
		})
	}()
	go func() {
		defer sortWg.Done()
		sort.Slice(deals, func(i, j int) bool {
			if deals[i].Code != deals[j].Code {
				return deals[i].Code < deals[j].Code
			}
			return deals[i].SeqNum < deals[j].SeqNum
		})
	}()
	go func() {
		defer sortWg.Done()
		sort.Slice(ticks, func(i, j int) bool {
			if ticks[i].Code != ticks[j].Code {
				return ticks[i].Code < ticks[j].Code
			}
			return ticks[i].SeqNum < ticks[j].SeqNum
		})
	}()
	sortWg.Wait()

	// 4. 写出到本地（写入 outputDir），临时禁用 OSS 避免重复上传
	log.Printf("[重排序] 写出排序后文件...")
	savedEnableOSS := p.optParquetWriter.enableOSS
	p.optParquetWriter.enableOSS = false
	writeErr := p.optParquetWriter.WriteAllDirect(orders, deals, ticks)
	p.optParquetWriter.enableOSS = savedEnableOSS
	if writeErr != nil {
		return fmt.Errorf("写出失败: %w", writeErr)
	}

	// 5. 删除 OSS 旧文件
	log.Printf("[重排序] 删除 OSS 旧文件...")
	if err := uploader.DeleteTradeDataFiles(tradingDay); err != nil {
		return fmt.Errorf("删除旧文件失败: %w", err)
	}

	// 6. 上传新文件
	log.Printf("[重排序] 上传排序后文件...")
	dateStr := tradingDay.Format("20060102")
	outputDir := p.optParquetWriter.outputDir
	files := []string{
		fmt.Sprintf("%s/%s_order.parquet", outputDir, dateStr),
		fmt.Sprintf("%s/%s_deal.parquet", outputDir, dateStr),
		fmt.Sprintf("%s/%s_tick.parquet", outputDir, dateStr),
	}
	for _, f := range files {
		ossKey := uploader.BuildFilePath(tradingDay, f[len(outputDir)+1:])
		if err := uploader.UploadLocalFile(f, ossKey); err != nil {
			return fmt.Errorf("上传失败 [%s]: %w", f, err)
		}
	}

	log.Printf("[重排序] 完成 %s", tradingDay.Format("20060102"))
	return nil
}
