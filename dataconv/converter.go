package dataconv

import (
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Converter 数据转换器
type Converter struct {
	tradingDay time.Time
}

// NewConverter 创建转换器
func NewConverter(tradingDay time.Time) *Converter {
	return &Converter{tradingDay: tradingDay}
}

// GetMarket 根据股票代码判断市场
func GetMarket(code string) Market {
	if len(code) == 0 {
		return ""
	}
	firstChar := code[0]
	switch firstChar {
	case '0', '1', '3', '4': // 深交所
		return MarketSZ
	case '2': // 深交所B股/中小板
		return MarketSZ
	case '5', '6': // 上交所
		return MarketSH
	case '7': // 上交所新股申购/B股
		return MarketSH
	case '8': // 北交所
		return MarketSH
	case '9': // 科创板/上交所
		return MarketSH
	default:
		return ""
	}
}

// FormatCode 格式化股票代码 (添加市场后缀)
func FormatCode(securityID int) string {
	code := fmt.Sprintf("%06d", securityID)
	market := GetMarket(code)
	return code + "." + string(market)
}

// ParseTime 解析时间字符串
func ParseTime(timeStr string, tradingDay time.Time) (time.Time, error) {
	// 尝试多种格式
	formats := []string{
		"20060102150405.000", // 上交所历史格式
		"150405.000",         // 时间戳格式
		"15:04:05.000",       // 冒号分隔
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			if format == "150405.000" || format == "15:04:05.000" {
				return time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(),
					t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local), nil
			}
			return t, nil
		}
	}

	// 处理纯数字格式 (如 92500000 表示 09:25:00.000)
	if len(timeStr) == 8 || len(timeStr) == 9 {
		hour, _ := strconv.Atoi(timeStr[0:2])
		minute, _ := strconv.Atoi(timeStr[2:4])
		second, _ := strconv.Atoi(timeStr[4:6])
		nano, _ := strconv.Atoi(timeStr[6:])
		return time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(),
			hour, minute, second, nano*1000000, time.Local), nil
	}

	return time.Time{}, fmt.Errorf("无法解析时间: %s", timeStr)
}

// ========== 上交所转换函数 ==========

// ConvertSHOrderOld 上交所逐笔委托转换 (2021-06-21 ~ 2023-12-21)
// 数据格式: mdl_4_19_0
func (c *Converter) ConvertSHOrderOld(records []map[string]string) ([]Order, error) {
	var orders []Order

	for _, r := range records {
		securityID, _ := strconv.ParseInt(r["SecurityID"], 10, 64)
		code := FormatCode(int(securityID))

		orderTime, err := ParseTime(r["OrderTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		side := int16(0)
		if r["OrderBSFlag"] == "S" {
			side = 1
		}

		orderType := int16(2)
		if r["OrderType"] == "D" {
			orderType = 5
		}

		order := Order{
			TradingDay: c.tradingDay,
			Code:       code,
			Time:       orderTime,
			UpdateTime: localTime,
			OrderID:    parseInt64(r["OrderNO"]),
			Side:       side,
			Price:      parseFloat64(r["OrderPrice"]),
			Volume:     parseFloat64(r["Balance"]),
			OrderType:  orderType,
			Channel:    parseInt64(r["OrderChannel"]),
			SeqNum:     parseInt64(r["BizIndex"]),
		}
		orders = append(orders, order)
	}

	return orders, nil
}

// ConvertSHDealOld 上交所逐笔成交转换 (2021-06-21 ~ )
// 数据格式: Transaction.csv
func (c *Converter) ConvertSHDealOld(records []map[string]string) ([]Deal, error) {
	var deals []Deal

	for _, r := range records {
		securityID, _ := strconv.ParseInt(r["SecurityID"], 10, 64)
		code := FormatCode(int(securityID))

		tradeTime, err := ParseTime(r["TradTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		side := int16(10) // 未知
		switch r["TradeBSFlag"] {
		case "B":
			side = 0
		case "S":
			side = 1
		}

		price := parseFloat64(r["TradPrice"])
		volume := parseFloat64(r["TradVolume"])

		deal := Deal{
			TradingDay:  c.tradingDay,
			Code:        code,
			Time:        tradeTime,
			UpdateTime:  localTime,
			SaleOrderID: parseInt64(r["TradeSellNo"]),
			BuyOrderID:  parseInt64(r["TradeBuyNo"]),
			Side:        side,
			Price:       price,
			Volume:      volume,
			Money:       parseFloat64(r["TradeMoney"]),
			Channel:     parseInt64(r["TradeChan"]),
			SeqNum:      parseInt64(r["BizIndex"]),
		}

		// 2021-04-26之前使用TradeIndex
		if c.tradingDay.Before(time.Date(2021, 4, 26, 0, 0, 0, 0, time.Local)) {
			deal.SeqNum = parseInt64(r["TradeIndex"])
		}

		deals = append(deals, deal)
	}

	return deals, nil
}

// ConvertSHOrderDeal 上交所委托+成交转换 (2023-12-22 之后)
// 数据格式: mdl_4_24_0
func (c *Converter) ConvertSHOrderDeal(records []map[string]string) ([]Order, []Deal, error) {
	var orders []Order
	var deals []Deal

	for _, r := range records {
		securityID, _ := strconv.ParseInt(r["SecurityID"], 10, 64)
		code := FormatCode(int(securityID))

		tickTime, err := ParseTime(r["TickTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		dataType := r["Type"]

		if dataType == "A" || dataType == "D" {
			// 委托数据
			side := int16(0)
			if r["TickBSFlag"] == "S" {
				side = 1
			}

			orderType := int16(2)
			if dataType == "D" {
				orderType = 5
			}

			order := Order{
				TradingDay: c.tradingDay,
				Code:       code,
				Time:       tickTime,
				UpdateTime: localTime,
				OrderID:    parseInt64(r["BuyOrderNO"]) + parseInt64(r["SellOrderNO"]),
				Side:       side,
				Price:      parseFloat64(r["Price"]),
				Volume:     parseFloat64(r["Qty"]),
				OrderType:  orderType,
				Channel:    parseInt64(r["ChannelNo"]),
				SeqNum:     parseInt64(r["BizIndex"]),
			}
			orders = append(orders, order)

		} else if dataType == "T" {
			// 成交数据
			side := int16(10)
			switch r["TickBSFlag"] {
			case "B":
				side = 0
			case "S":
				side = 1
			}

			price := parseFloat64(r["Price"])
			volume := parseFloat64(r["Qty"])

			deal := Deal{
				TradingDay:  c.tradingDay,
				Code:        code,
				Time:        tickTime,
				UpdateTime:  localTime,
				SaleOrderID: parseInt64(r["SellOrderNO"]),
				BuyOrderID:  parseInt64(r["BuyOrderNO"]),
				Side:        side,
				Price:       price,
				Volume:      volume,
				Money:       price * volume,
				Channel:     parseInt64(r["ChannelNo"]),
				SeqNum:     parseInt64(r["BizIndex"]),
			}
			deals = append(deals, deal)
		}
	}

	return orders, deals, nil
}

// ConvertSHTick 上交所快照转换
// 数据格式: MarketData
func (c *Converter) ConvertSHTick(records []map[string]string, highLimit, lowLimit float64) ([]Tick, error) {
	var ticks []Tick
	skippedCount := 0

	for _, r := range records {
		securityID, err := strconv.ParseInt(r["SecurityID"], 10, 64)
		if err != nil || securityID <= 0 {
			skippedCount++
			if err != nil {
				// 记录原始数据格式问题（比如带小数点）
				if skippedCount <= 5 {
					log.Printf("[跳过] 无效 SecurityID: '%s' (错误: %v)", r["SecurityID"], err)
				}
			}
			continue
		}
		code := FormatCode(int(securityID))

		updateTime, err := ParseTime(r["UpdateTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		tick := Tick{
			TradingDay:     c.tradingDay,
			Code:           code,
			Time:           updateTime,
			UpdateTime:     localTime,
			CurrentPrice:   parseFloat64(r["LastPrice"]),
			TotalVolume:    parseFloat64(r["TradVolume"]),
			TotalMoney:     parseFloat64(r["Turnover"]),
			PreClosePrice:  parseFloat64(r["PreCloPrice"]),
			HighestPrice:   parseFloat64(r["HighPrice"]),
			LowestPrice:    parseFloat64(r["LowPrice"]),
			HighLimitPrice: highLimit,
			LowLimitPrice:  lowLimit,
			IOPV:           0,
			TradeNum:       parseFloat64(r["TradNumber"]),
			TotalBidVolume: parseFloat64(r["TotalBidVol"]),
			TotalAskVolume: parseFloat64(r["TotalAskVol"]),
			AvgBidPrice:    parseFloat64(r["WAvgBidPri"]),
			AvgAskPrice:    parseFloat64(r["WAvgAskPri"]),
			Channel:        0,
			SeqNum:         parseInt64(r["SeqNo"]),
		}

		// 解析10档买卖价量
		for i := 1; i <= 10; i++ {
			setTickLevel(&tick, i,
				parseFloat64(r[fmt.Sprintf("BidPrice%d", i)]),
				parseFloat64(r[fmt.Sprintf("BidVolume%d", i)]),
				parseFloat64(r[fmt.Sprintf("NumOrdersB%d", i)]),
				parseFloat64(r[fmt.Sprintf("AskPrice%d", i)]),
				parseFloat64(r[fmt.Sprintf("AskVolume%d", i)]),
				parseFloat64(r[fmt.Sprintf("NumOrdersS%d", i)]),
			)
		}

		ticks = append(ticks, tick)
	}

	if skippedCount > 0 {
		log.Printf("[统计] 上交所快照跳过 %d 条无效 SecurityID 记录", skippedCount)
	}

	return ticks, nil
}

// ========== 深交所转换函数 ==========

// ConvertSZOrder 深交所逐笔委托转换
// 数据格式: mdl_6_33_0
func (c *Converter) ConvertSZOrder(records []map[string]string) ([]Order, error) {
	var orders []Order
	skippedCount := 0

	for _, r := range records {
		securityID, err := strconv.ParseInt(r["SecurityID"], 10, 64)
		if err != nil || securityID <= 0 {
			skippedCount++
			if err != nil {
				if skippedCount <= 5 {
					log.Printf("[跳过] 无效 SecurityID: '%s' (错误: %v)", r["SecurityID"], err)
				}
			}
			continue
		}
		code := FormatCode(int(securityID))

		transactTime, err := ParseTime(r["TransactTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		// Side: 49='1'=买, 50='2'=卖
		side := int16(0)
		if r["Side"] == "50" || r["Side"] == "2" {
			side = 1
		}

		// OrdType: 49='1'=限价, 50='2'=市价, 85='U'=对手方最优
		orderType := int16(1)
		switch r["OrdType"] {
		case "50", "2":
			orderType = 2
		case "85", "U":
			orderType = 3
		}

		orderID := parseInt64(r["ApplSeqNum"])

		order := Order{
			TradingDay: c.tradingDay,
			Code:       code,
			Time:       transactTime,
			UpdateTime: localTime,
			OrderID:    orderID,
			Side:       side,
			Price:      parseFloat64(r["Price"]),
			Volume:     parseFloat64(r["OrderQty"]),
			OrderType:  orderType,
			Channel:    parseInt64(r["ChannelNo"]),
			SeqNum:     orderID,
		}
		orders = append(orders, order)
	}

	if skippedCount > 0 {
		log.Printf("[统计] 深交所委托跳过 %d 条无效 SecurityID 记录", skippedCount)
	}

	return orders, nil
}

// ConvertSZDeal 深交所逐笔成交转换
// 数据格式: mdl_6_36_0
func (c *Converter) ConvertSZDeal(records []map[string]string) ([]Deal, error) {
	var deals []Deal
	skippedCount := 0

	for _, r := range records {
		securityID, err := strconv.ParseInt(r["SecurityID"], 10, 64)
		if err != nil || securityID <= 0 {
			skippedCount++
			if err != nil {
				if skippedCount <= 5 {
					log.Printf("[跳过] 无效 SecurityID: '%s' (错误: %v)", r["SecurityID"], err)
				}
			}
			continue
		}
		code := FormatCode(int(securityID))

		transactTime, err := ParseTime(r["TransactTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		buyOrderID := parseInt64(r["BidApplSeqNum"])
		saleOrderID := parseInt64(r["OfferApplSeqNum"])

		// 根据买卖委托号判断方向
		side := int16(1) // 默认卖
		if buyOrderID > saleOrderID {
			side = 0
		}

		// ExecType == 52 表示撤单
		if r["ExecType"] == "52" {
			side = 4
		}

		price := parseFloat64(r["LastPx"])
		volume := parseFloat64(r["LastQty"])

		deal := Deal{
			TradingDay:  c.tradingDay,
			Code:        code,
			Time:        transactTime,
			UpdateTime:  localTime,
			SaleOrderID: saleOrderID,
			BuyOrderID:  buyOrderID,
			Side:        side,
			Price:       price,
			Volume:      volume,
			Money:       price * volume,
			Channel:     parseInt64(r["ChannelNo"]),
			SeqNum:      parseInt64(r["ApplSeqNum"]),
		}
		deals = append(deals, deal)
	}

	if skippedCount > 0 {
		log.Printf("[统计] 深交所成交跳过 %d 条无效 SecurityID 记录", skippedCount)
	}

	return deals, nil
}

// ConvertSZTick 深交所快照转换
// 数据格式: mdl_6_28_0
func (c *Converter) ConvertSZTick(records []map[string]string, highLimit, lowLimit float64) ([]Tick, error) {
	var ticks []Tick
	skippedCount := 0

	for _, r := range records {
		securityID, err := strconv.ParseInt(r["SecurityID"], 10, 64)
		if err != nil || securityID <= 0 {
			skippedCount++
			if err != nil {
				// 记录原始数据格式问题（比如带小数点）
				if skippedCount <= 5 {
					log.Printf("[跳过] 无效 SecurityID: '%s' (错误: %v)", r["SecurityID"], err)
				}
			}
			continue
		}
		code := FormatCode(int(securityID))

		updateTime, err := ParseTime(r["UpdateTime"], c.tradingDay)
		if err != nil {
			continue
		}

		localTime, _ := ParseTime(r["LocalTime"], c.tradingDay)

		tick := Tick{
			TradingDay:     c.tradingDay,
			Code:           code,
			Time:           updateTime,
			UpdateTime:     localTime,
			CurrentPrice:   parseFloat64(r["LastPrice"]),
			TotalVolume:    parseFloat64(r["Volume"]),
			TotalMoney:     parseFloat64(r["Turnover"]),
			PreClosePrice:  parseFloat64(r["PreCloPrice"]),
			HighestPrice:   parseFloat64(r["HighPrice"]),
			LowestPrice:    parseFloat64(r["LowPrice"]),
			HighLimitPrice: highLimit, // 从MySQL获取涨停价
			LowLimitPrice:  lowLimit,  // 从MySQL获取跌停价
			IOPV:           0,
			TradeNum:       parseFloat64(r["TurnNum"]),
			TotalBidVolume: parseFloat64(r["TotalBidQty"]),
			TotalAskVolume: parseFloat64(r["TotalOfferQty"]),
			AvgBidPrice:    parseFloat64(r["WeightedAvgBidPx"]),
			AvgAskPrice:    parseFloat64(r["WeightedAvgOfferPx"]),
			Channel:        0,
			SeqNum:         parseInt64(r["SeqNo"]),
		}

		// 解析10档买卖价量
		for i := 1; i <= 10; i++ {
			setTickLevel(&tick, i,
				parseFloat64(r[fmt.Sprintf("BidPrice%d", i)]),
				parseFloat64(r[fmt.Sprintf("BidVolume%d", i)]),
				parseFloat64(r[fmt.Sprintf("NumOrdersB%d", i)]),
				parseFloat64(r[fmt.Sprintf("AskPrice%d", i)]),
				parseFloat64(r[fmt.Sprintf("AskVolume%d", i)]),
				parseFloat64(r[fmt.Sprintf("NumOrdersS%d", i)]),
			)
		}

		ticks = append(ticks, tick)
	}

	if skippedCount > 0 {
		log.Printf("[统计] 深交所快照跳过 %d 条无效 SecurityID 记录", skippedCount)
	}

	return ticks, nil
}

// ========== 辅助函数 ==========

func setTickLevel(tick *Tick, level int, bidPrice, bidVolume, bidNum, askPrice, askVolume, askNum float64) {
	switch level {
	case 1:
		tick.BidPrice1, tick.BidVolume1, tick.BidNum1 = bidPrice, bidVolume, bidNum
		tick.AskPrice1, tick.AskVolume1, tick.AskNum1 = askPrice, askVolume, askNum
	case 2:
		tick.BidPrice2, tick.BidVolume2, tick.BidNum2 = bidPrice, bidVolume, bidNum
		tick.AskPrice2, tick.AskVolume2, tick.AskNum2 = askPrice, askVolume, askNum
	case 3:
		tick.BidPrice3, tick.BidVolume3, tick.BidNum3 = bidPrice, bidVolume, bidNum
		tick.AskPrice3, tick.AskVolume3, tick.AskNum3 = askPrice, askVolume, askNum
	case 4:
		tick.BidPrice4, tick.BidVolume4, tick.BidNum4 = bidPrice, bidVolume, bidNum
		tick.AskPrice4, tick.AskVolume4, tick.AskNum4 = askPrice, askVolume, askNum
	case 5:
		tick.BidPrice5, tick.BidVolume5, tick.BidNum5 = bidPrice, bidVolume, bidNum
		tick.AskPrice5, tick.AskVolume5, tick.AskNum5 = askPrice, askVolume, askNum
	case 6:
		tick.BidPrice6, tick.BidVolume6, tick.BidNum6 = bidPrice, bidVolume, bidNum
		tick.AskPrice6, tick.AskVolume6, tick.AskNum6 = askPrice, askVolume, askNum
	case 7:
		tick.BidPrice7, tick.BidVolume7, tick.BidNum7 = bidPrice, bidVolume, bidNum
		tick.AskPrice7, tick.AskVolume7, tick.AskNum7 = askPrice, askVolume, askNum
	case 8:
		tick.BidPrice8, tick.BidVolume8, tick.BidNum8 = bidPrice, bidVolume, bidNum
		tick.AskPrice8, tick.AskVolume8, tick.AskNum8 = askPrice, askVolume, askNum
	case 9:
		tick.BidPrice9, tick.BidVolume9, tick.BidNum9 = bidPrice, bidVolume, bidNum
		tick.AskPrice9, tick.AskVolume9, tick.AskNum9 = askPrice, askVolume, askNum
	case 10:
		tick.BidPrice10, tick.BidVolume10, tick.BidNum10 = bidPrice, bidVolume, bidNum
		tick.AskPrice10, tick.AskVolume10, tick.AskNum10 = askPrice, askVolume, askNum
	}
}

func parseInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	
	// 处理浮点数格式 (如 "200596." 或 "200596.0")
	if idx := strings.IndexByte(s, '.'); idx != -1 {
		s = s[:idx]
	}
	
	val, _ := strconv.ParseInt(s, 10, 64)
	return val
}

func parseFloat64(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	val, _ := strconv.ParseFloat(s, 64)
	return val
}

// ValidateCode 验证股票代码格式（仅股票，不含基金债券）
func ValidateCode(code string) bool {
	// 深交所股票：
	//   0 = 主板（000001-002999）
	//   2 = B股/中小板（200001-204999）
	//   3 = 创业板（300001-309999）
	patternSZ := regexp.MustCompile(`^[023]\d{5}\.XSHE$`)
	
	// 上交所股票：
	//   6 = 主板股票（600000-699999，含科创板688XXX）
	//   9 = B股（900XXX）
	patternSH := regexp.MustCompile(`^[69]\d{5}\.XSHG$`)
	
	return patternSZ.MatchString(code) || patternSH.MatchString(code)
}

// ValidateOrder 验证委托数据
func ValidateOrder(order Order) bool {
	if !ValidateCode(order.Code) {
		return false
	}
	if order.Side != 0 && order.Side != 1 {
		return false
	}
	
	// 验证价格和数量是否有限
	if !isFinite(order.Price) || !isFinite(order.Volume) {
		return false
	}

	// 深市订单类型: 1/2/3, 沪市: 2/5
	if strings.HasSuffix(order.Code, ".XSHE") {
		if order.OrderType != 1 && order.OrderType != 2 && order.OrderType != 3 {
			return false
		}
	} else {
		if order.OrderType != 2 && order.OrderType != 5 {
			return false
		}
	}

	return true
}

// ValidateDeal 验证成交数据
func ValidateDeal(deal Deal) bool {
	if !ValidateCode(deal.Code) {
		return false
	}
	
	// 验证价格、数量、金额是否有限
	if !isFinite(deal.Price) || !isFinite(deal.Volume) || !isFinite(deal.Money) {
		return false
	}

	if strings.HasSuffix(deal.Code, ".XSHE") {
		if deal.Side != 0 && deal.Side != 1 && deal.Side != 4 && deal.Side != 10 {
			return false
		}
	} else {
		if deal.Side != 0 && deal.Side != 1 && deal.Side != 10 {
			return false
		}
	}

	return true
}

// ValidateTick 验证快照数据
func ValidateTick(tick Tick) bool {
	if !ValidateCode(tick.Code) {
		return false
	}
	
	// 验证10档买卖价格、数量、订单数是否有限
	prices := []float64{
		tick.AskPrice1, tick.AskPrice2, tick.AskPrice3, tick.AskPrice4, tick.AskPrice5,
		tick.AskPrice6, tick.AskPrice7, tick.AskPrice8, tick.AskPrice9, tick.AskPrice10,
		tick.BidPrice1, tick.BidPrice2, tick.BidPrice3, tick.BidPrice4, tick.BidPrice5,
		tick.BidPrice6, tick.BidPrice7, tick.BidPrice8, tick.BidPrice9, tick.BidPrice10,
	}
	
	volumes := []float64{
		tick.AskVolume1, tick.AskVolume2, tick.AskVolume3, tick.AskVolume4, tick.AskVolume5,
		tick.AskVolume6, tick.AskVolume7, tick.AskVolume8, tick.AskVolume9, tick.AskVolume10,
		tick.BidVolume1, tick.BidVolume2, tick.BidVolume3, tick.BidVolume4, tick.BidVolume5,
		tick.BidVolume6, tick.BidVolume7, tick.BidVolume8, tick.BidVolume9, tick.BidVolume10,
	}
	
	nums := []float64{
		tick.AskNum1, tick.AskNum2, tick.AskNum3, tick.AskNum4, tick.AskNum5,
		tick.AskNum6, tick.AskNum7, tick.AskNum8, tick.AskNum9, tick.AskNum10,
		tick.BidNum1, tick.BidNum2, tick.BidNum3, tick.BidNum4, tick.BidNum5,
		tick.BidNum6, tick.BidNum7, tick.BidNum8, tick.BidNum9, tick.BidNum10,
	}
	
	for _, v := range prices {
		if !isFinite(v) {
			return false
		}
	}
	
	for _, v := range volumes {
		if !isFinite(v) {
			return false
		}
	}
	
	for _, v := range nums {
		if !isFinite(v) {
			return false
		}
	}
	
	return true
}

// isFinite 检查浮点数是否有限（不是 NaN 或 Inf）
func isFinite(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}
