package dataconv

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Writer 数据写入器
type Writer struct {
	outputDir   string
	ossUploader *OSSUploader
	uploadToOSS bool
}

// NewWriter 创建写入器
func NewWriter(outputDir string) *Writer {
	return &Writer{
		outputDir:   outputDir,
		uploadToOSS: false,
	}
}

// NewWriterWithOSS 创建带 OSS 上传的写入器
func NewWriterWithOSS(outputDir string, ossConfig OSSConfig) (*Writer, error) {
	uploader, err := NewOSSUploader(ossConfig)
	if err != nil {
		log.Printf("[警告] OSS 初始化失败，将只保存到本地: %v", err)
		return &Writer{
			outputDir:   outputDir,
			uploadToOSS: false,
		}, nil
	}

	return &Writer{
		outputDir:   outputDir,
		ossUploader: uploader,
		uploadToOSS: true,
	}, nil
}

// WriteOrders 写入委托数据到CSV
func (w *Writer) WriteOrders(orders []Order, filename string) error {
	if len(orders) == 0 {
		return nil
	}

	path := filepath.Join(w.outputDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	header := []string{
		"TradingDay", "Code", "Time", "UpdateTime", "OrderID",
		"Side", "Price", "Volume", "OrderType", "Channel", "SeqNum",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// 写入数据
	for _, order := range orders {
		record := []string{
			order.TradingDay.Format("2006-01-02"),
			order.Code,
			order.Time.Format("2006-01-02 15:04:05.000"),
			order.UpdateTime.Format("2006-01-02 15:04:05.000"),
			strconv.FormatInt(order.OrderID, 10),
			strconv.FormatInt(int64(order.Side), 10),
			strconv.FormatFloat(order.Price, 'f', 2, 64),
			strconv.FormatFloat(order.Volume, 'f', 0, 64),
			strconv.FormatInt(int64(order.OrderType), 10),
			strconv.FormatInt(order.Channel, 10),
			strconv.FormatInt(order.SeqNum, 10),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()
	file.Close()

	// 上传到 OSS
	if w.uploadToOSS && w.ossUploader != nil {
		if err := w.ossUploader.UploadFile(path, DataTypeOrder, orders[0].TradingDay, orders[0].Code); err != nil {
			log.Printf("[警告] OSS 上传失败: %v", err)
		} else {
			// 上传成功后删除本地文件
			os.Remove(path)
		}
	}

	return nil
}

// WriteDeals 写入成交数据到CSV
func (w *Writer) WriteDeals(deals []Deal, filename string) error {
	if len(deals) == 0 {
		return nil
	}

	path := filepath.Join(w.outputDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	header := []string{
		"TradingDay", "Code", "Time", "UpdateTime", "SaleOrderID",
		"BuyOrderID", "Side", "Price", "Volume", "Money", "Channel", "SeqNum",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// 写入数据
	for _, deal := range deals {
		record := []string{
			deal.TradingDay.Format("2006-01-02"),
			deal.Code,
			deal.Time.Format("2006-01-02 15:04:05.000"),
			deal.UpdateTime.Format("2006-01-02 15:04:05.000"),
			strconv.FormatInt(deal.SaleOrderID, 10),
			strconv.FormatInt(deal.BuyOrderID, 10),
			strconv.FormatInt(int64(deal.Side), 10),
			strconv.FormatFloat(deal.Price, 'f', 2, 64),
			strconv.FormatFloat(deal.Volume, 'f', 0, 64),
			strconv.FormatFloat(deal.Money, 'f', 2, 64),
			strconv.FormatInt(deal.Channel, 10),
			strconv.FormatInt(deal.SeqNum, 10),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()
	file.Close()

	// 上传到 OSS
	if w.uploadToOSS && w.ossUploader != nil {
		if err := w.ossUploader.UploadFile(path, DataTypeDeal, deals[0].TradingDay, deals[0].Code); err != nil {
			log.Printf("[警告] OSS 上传失败: %v", err)
		} else {
			// 上传成功后删除本地文件
			os.Remove(path)
		}
	}

	return nil
}

// WriteTicks 写入快照数据到CSV
func (w *Writer) WriteTicks(ticks []Tick, filename string) error {
	if len(ticks) == 0 {
		return nil
	}

	path := filepath.Join(w.outputDir, filename)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	header := []string{
		"TradingDay", "Code", "Time", "UpdateTime", "CurrentPrice", "TotalVolume", "TotalMoney",
		"PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice", "HighLimitPrice", "LowLimitPrice",
		"IOPV", "TradeNum", "TotalBidVolume", "TotalAskVolume", "AvgBidPrice", "AvgAskPrice",
		"AskPrice1", "AskPrice2", "AskPrice3", "AskPrice4", "AskPrice5",
		"AskPrice6", "AskPrice7", "AskPrice8", "AskPrice9", "AskPrice10",
		"AskVolume1", "AskVolume2", "AskVolume3", "AskVolume4", "AskVolume5",
		"AskVolume6", "AskVolume7", "AskVolume8", "AskVolume9", "AskVolume10",
		"AskNum1", "AskNum2", "AskNum3", "AskNum4", "AskNum5",
		"AskNum6", "AskNum7", "AskNum8", "AskNum9", "AskNum10",
		"BidPrice1", "BidPrice2", "BidPrice3", "BidPrice4", "BidPrice5",
		"BidPrice6", "BidPrice7", "BidPrice8", "BidPrice9", "BidPrice10",
		"BidVolume1", "BidVolume2", "BidVolume3", "BidVolume4", "BidVolume5",
		"BidVolume6", "BidVolume7", "BidVolume8", "BidVolume9", "BidVolume10",
		"BidNum1", "BidNum2", "BidNum3", "BidNum4", "BidNum5",
		"BidNum6", "BidNum7", "BidNum8", "BidNum9", "BidNum10",
		"Channel", "SeqNum",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// 写入数据
	for _, tick := range ticks {
		record := []string{
			tick.TradingDay.Format("2006-01-02"),
			tick.Code,
			tick.Time.Format("2006-01-02 15:04:05.000"),
			tick.UpdateTime.Format("2006-01-02 15:04:05.000"),
			formatFloat(tick.CurrentPrice),
			formatFloat(tick.TotalVolume),
			formatFloat(tick.TotalMoney),
			formatFloat(tick.PreClosePrice),
			formatFloat(tick.OpenPrice),
			formatFloat(tick.HighestPrice),
			formatFloat(tick.LowestPrice),
			formatFloat(tick.HighLimitPrice),
			formatFloat(tick.LowLimitPrice),
			formatFloat(tick.IOPV),
			formatFloat(tick.TradeNum),
			formatFloat(tick.TotalBidVolume),
			formatFloat(tick.TotalAskVolume),
			formatFloat(tick.AvgBidPrice),
			formatFloat(tick.AvgAskPrice),
			// Ask
			formatFloat(tick.AskPrice1), formatFloat(tick.AskPrice2), formatFloat(tick.AskPrice3),
			formatFloat(tick.AskPrice4), formatFloat(tick.AskPrice5), formatFloat(tick.AskPrice6),
			formatFloat(tick.AskPrice7), formatFloat(tick.AskPrice8), formatFloat(tick.AskPrice9),
			formatFloat(tick.AskPrice10),
			formatFloat(tick.AskVolume1), formatFloat(tick.AskVolume2), formatFloat(tick.AskVolume3),
			formatFloat(tick.AskVolume4), formatFloat(tick.AskVolume5), formatFloat(tick.AskVolume6),
			formatFloat(tick.AskVolume7), formatFloat(tick.AskVolume8), formatFloat(tick.AskVolume9),
			formatFloat(tick.AskVolume10),
			formatFloat(tick.AskNum1), formatFloat(tick.AskNum2), formatFloat(tick.AskNum3),
			formatFloat(tick.AskNum4), formatFloat(tick.AskNum5), formatFloat(tick.AskNum6),
			formatFloat(tick.AskNum7), formatFloat(tick.AskNum8), formatFloat(tick.AskNum9),
			formatFloat(tick.AskNum10),
			// Bid
			formatFloat(tick.BidPrice1), formatFloat(tick.BidPrice2), formatFloat(tick.BidPrice3),
			formatFloat(tick.BidPrice4), formatFloat(tick.BidPrice5), formatFloat(tick.BidPrice6),
			formatFloat(tick.BidPrice7), formatFloat(tick.BidPrice8), formatFloat(tick.BidPrice9),
			formatFloat(tick.BidPrice10),
			formatFloat(tick.BidVolume1), formatFloat(tick.BidVolume2), formatFloat(tick.BidVolume3),
			formatFloat(tick.BidVolume4), formatFloat(tick.BidVolume5), formatFloat(tick.BidVolume6),
			formatFloat(tick.BidVolume7), formatFloat(tick.BidVolume8), formatFloat(tick.BidVolume9),
			formatFloat(tick.BidVolume10),
			formatFloat(tick.BidNum1), formatFloat(tick.BidNum2), formatFloat(tick.BidNum3),
			formatFloat(tick.BidNum4), formatFloat(tick.BidNum5), formatFloat(tick.BidNum6),
			formatFloat(tick.BidNum7), formatFloat(tick.BidNum8), formatFloat(tick.BidNum9),
			formatFloat(tick.BidNum10),
			// Channel & SeqNum
			strconv.FormatInt(tick.Channel, 10),
			strconv.FormatInt(tick.SeqNum, 10),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()
	file.Close()

	// 上传到 OSS
	if w.uploadToOSS && w.ossUploader != nil {
		if err := w.ossUploader.UploadFile(path, DataTypeTick, ticks[0].TradingDay, ticks[0].Code); err != nil {
			log.Printf("[警告] OSS 上传失败: %v", err)
		} else {
			// 上传成功后删除本地文件
			os.Remove(path)
		}
	}

	return nil
}

func formatFloat(v float64) string {
	if v == 0 {
		return "0"
	}
	s := strconv.FormatFloat(v, 'f', 2, 64)
	return strings.TrimRight(strings.TrimRight(s, "0"), ".")
}

// EnsureDir 确保目录存在
func EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// GenerateFilename 生成输出文件名
func GenerateFilename(dataType DataType, market Market, tradingDay time.Time, code string) string {
	dateStr := tradingDay.Format("20060102")
	switch dataType {
	case DataTypeOrder:
		return fmt.Sprintf("%s_%s_order_%s.csv", dateStr, market, code)
	case DataTypeDeal:
		return fmt.Sprintf("%s_%s_deal_%s.csv", dateStr, market, code)
	case DataTypeTick:
		return fmt.Sprintf("%s_%s_tick_%s.csv", dateStr, market, code)
	default:
		return fmt.Sprintf("%s_%s_%s.csv", dateStr, market, code)
	}
}
