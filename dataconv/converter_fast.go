package dataconv

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// colIndex 列名到索引的映射，一次建立，反复使用
type colIndex map[string]int

func buildColIndex(header []string) colIndex {
	idx := make(colIndex, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	return idx
}

func (idx colIndex) str(row []string, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func (idx colIndex) i64(row []string, col string) int64 {
	return parseInt64(idx.str(row, col))
}

func (idx colIndex) f64(row []string, col string) float64 {
	return parseFloat64(idx.str(row, col))
}

// ========== 深交所委托 mdl_6_33_0 ==========

// ConvertSZOrderFast 深交所逐笔委托快速转换（按索引，无 map）
func (c *Converter) ConvertSZOrderFast(header []string, rows [][]string) ([]Order, error) {
	idx := buildColIndex(header)
	orders := make([]Order, 0, len(rows))
	skipped := 0

	for _, row := range rows {
		secStr := idx.str(row, "SecurityID")
		secID, err := strconv.ParseInt(secStr, 10, 64)
		if err != nil || secID <= 0 {
			skipped++
			continue
		}
		code := FormatCode(int(secID))

		transactTime, err := ParseTime(idx.str(row, "TransactTime"), c.tradingDay)
		if err != nil {
			continue
		}
		localTime, _ := ParseTime(idx.str(row, "LocalTime"), c.tradingDay)

		sideStr := idx.str(row, "Side")
		side := int16(0)
		if sideStr == "50" || sideStr == "2" {
			side = 1
		}

		ordType := int16(1)
		switch idx.str(row, "OrdType") {
		case "50", "2":
			ordType = 2
		case "85", "U":
			ordType = 3
		}

		orderID := idx.i64(row, "ApplSeqNum")

		orders = append(orders, Order{
			TradingDay: c.tradingDay,
			Code:       code,
			Time:       transactTime,
			UpdateTime: localTime,
			OrderID:    orderID,
			Side:       side,
			Price:      idx.f64(row, "Price"),
			Volume:     idx.f64(row, "OrderQty"),
			OrderType:  ordType,
			Channel:    idx.i64(row, "ChannelNo"),
			SeqNum:     orderID,
		})
	}

	if skipped > 0 {
		fmt.Printf("[统计] 深交所委托跳过 %d 条无效 SecurityID\n", skipped)
	}
	return orders, nil
}

// ========== 深交所成交 mdl_6_36_0 ==========

// ConvertSZDealFast 深交所逐笔成交快速转换
func (c *Converter) ConvertSZDealFast(header []string, rows [][]string) ([]Deal, error) {
	idx := buildColIndex(header)
	deals := make([]Deal, 0, len(rows))
	skipped := 0

	for _, row := range rows {
		secStr := idx.str(row, "SecurityID")
		secID, err := strconv.ParseInt(secStr, 10, 64)
		if err != nil || secID <= 0 {
			skipped++
			continue
		}
		code := FormatCode(int(secID))

		transactTime, err := ParseTime(idx.str(row, "TransactTime"), c.tradingDay)
		if err != nil {
			continue
		}
		localTime, _ := ParseTime(idx.str(row, "LocalTime"), c.tradingDay)

		buyID := idx.i64(row, "BidApplSeqNum")
		saleID := idx.i64(row, "OfferApplSeqNum")

		side := int16(1)
		if buyID > saleID {
			side = 0
		}
		if idx.str(row, "ExecType") == "52" {
			side = 4
		}

		price := idx.f64(row, "LastPx")
		vol := idx.f64(row, "LastQty")

		deals = append(deals, Deal{
			TradingDay:  c.tradingDay,
			Code:        code,
			Time:        transactTime,
			UpdateTime:  localTime,
			SaleOrderID: saleID,
			BuyOrderID:  buyID,
			Side:        side,
			Price:       price,
			Volume:      vol,
			Money:       price * vol,
			Channel:     idx.i64(row, "ChannelNo"),
			SeqNum:      idx.i64(row, "ApplSeqNum"),
		})
	}

	if skipped > 0 {
		fmt.Printf("[统计] 深交所成交跳过 %d 条无效 SecurityID\n", skipped)
	}
	return deals, nil
}

// ========== 深交所快照 mdl_6_28_x ==========

// ConvertSZTickFast 深交所快照快速转换
func (c *Converter) ConvertSZTickFast(header []string, rows [][]string, priceCache *PriceCache) ([]Tick, error) {
	idx := buildColIndex(header)
	ticks := make([]Tick, 0, len(rows))
	skipped := 0

	for _, row := range rows {
		secStr := idx.str(row, "SecurityID")
		secID, err := strconv.ParseInt(secStr, 10, 64)
		if err != nil || secID <= 0 {
			skipped++
			continue
		}
		code := FormatCode(int(secID))

		updateTime, err := ParseTime(idx.str(row, "UpdateTime"), c.tradingDay)
		if err != nil {
			continue
		}
		localTime, _ := ParseTime(idx.str(row, "LocalTime"), c.tradingDay)

		var highLimit, lowLimit float64
		if priceCache != nil {
			highLimit, lowLimit = priceCache.GetOrCompute(code)
		}

		tick := Tick{
			TradingDay:     c.tradingDay,
			Code:           code,
			Time:           updateTime,
			UpdateTime:     localTime,
			CurrentPrice:   idx.f64(row, "LastPrice"),
			TotalVolume:    idx.f64(row, "Volume"),
			TotalMoney:     idx.f64(row, "Turnover"),
			PreClosePrice:  idx.f64(row, "PreCloPrice"),
			OpenPrice:      idx.f64(row, "OpenPrice"),
			HighestPrice:   idx.f64(row, "HighPrice"),
			LowestPrice:    idx.f64(row, "LowPrice"),
			HighLimitPrice: highLimit,
			LowLimitPrice:  lowLimit,
			IOPV:           idx.f64(row, "IOPV"),
			TradeNum:       idx.f64(row, "TurnNum"),
			TotalBidVolume: idx.f64(row, "TotalBidQty"),
			TotalAskVolume: idx.f64(row, "TotalOfferQty"),
			AvgBidPrice:    idx.f64(row, "WeightedAvgBidPx"),
			AvgAskPrice:    idx.f64(row, "WeightedAvgOfferPx"),
			Channel:        0,
			SeqNum:         idx.i64(row, "SeqNo"),
		}

		for i := 1; i <= 10; i++ {
			setTickLevel(&tick, i,
				idx.f64(row, fmt.Sprintf("BidPrice%d", i)),
				idx.f64(row, fmt.Sprintf("BidVolume%d", i)),
				idx.f64(row, fmt.Sprintf("NumOrdersB%d", i)),
				idx.f64(row, fmt.Sprintf("AskPrice%d", i)),
				idx.f64(row, fmt.Sprintf("AskVolume%d", i)),
				idx.f64(row, fmt.Sprintf("NumOrdersS%d", i)),
			)
		}

		ticks = append(ticks, tick)
	}

	if skipped > 0 {
		fmt.Printf("[统计] 深交所快照跳过 %d 条无效 SecurityID\n", skipped)
	}
	return ticks, nil
}

// ========== 上交所委托+成交 mdl_4_24_0 ==========

// ConvertSHOrderDealFast 上交所委托+成交快速转换
func (c *Converter) ConvertSHOrderDealFast(header []string, rows [][]string) ([]Order, []Deal, error) {
	idx := buildColIndex(header)
	orders := make([]Order, 0, len(rows)/2)
	deals := make([]Deal, 0, len(rows)/2)

	for _, row := range rows {
		secStr := idx.str(row, "SecurityID")
		secID, err := strconv.ParseInt(secStr, 10, 64)
		if err != nil || secID <= 0 {
			continue
		}
		code := FormatCode(int(secID))

		tickTime, err := ParseTime(idx.str(row, "TickTime"), c.tradingDay)
		if err != nil {
			continue
		}
		localTime, _ := ParseTime(idx.str(row, "LocalTime"), c.tradingDay)

		dataType := idx.str(row, "Type")
		seqNum := idx.i64(row, "BizIndex")
		channel := idx.i64(row, "ChannelNo")

		switch dataType {
		case "A", "D":
			side := int16(0)
			if idx.str(row, "TickBSFlag") == "S" {
				side = 1
			}
			ordType := int16(2)
			if dataType == "D" {
				ordType = 5
			}
			orders = append(orders, Order{
				TradingDay: c.tradingDay,
				Code:       code,
				Time:       tickTime,
				UpdateTime: localTime,
				OrderID:    idx.i64(row, "BuyOrderNO") + idx.i64(row, "SellOrderNO"),
				Side:       side,
				Price:      idx.f64(row, "Price"),
				Volume:     idx.f64(row, "Qty"),
				OrderType:  ordType,
				Channel:    channel,
				SeqNum:     seqNum,
			})
		case "T":
			side := int16(10)
			switch idx.str(row, "TickBSFlag") {
			case "B":
				side = 0
			case "S":
				side = 1
			}
			price := idx.f64(row, "Price")
			vol := idx.f64(row, "Qty")
			deals = append(deals, Deal{
				TradingDay:  c.tradingDay,
				Code:        code,
				Time:        tickTime,
				UpdateTime:  localTime,
				SaleOrderID: idx.i64(row, "SellOrderNO"),
				BuyOrderID:  idx.i64(row, "BuyOrderNO"),
				Side:        side,
				Price:       price,
				Volume:      vol,
				Money:       price * vol,
				Channel:     channel,
				SeqNum:      seqNum,
			})
		}
	}
	return orders, deals, nil
}

// ========== 上交所快照 MarketData ==========

// ConvertSHTickFast 上交所快照快速转换
func (c *Converter) ConvertSHTickFast(header []string, rows [][]string, priceCache *PriceCache) ([]Tick, error) {
	idx := buildColIndex(header)
	ticks := make([]Tick, 0, len(rows))
	skipped := 0

	for _, row := range rows {
		secStr := idx.str(row, "SecurityID")
		secID, err := strconv.ParseInt(secStr, 10, 64)
		if err != nil || secID <= 0 {
			skipped++
			continue
		}
		code := FormatCode(int(secID))

		updateTime, err := ParseTime(idx.str(row, "UpdateTime"), c.tradingDay)
		if err != nil {
			continue
		}
		localTime, _ := ParseTime(idx.str(row, "LocalTime"), c.tradingDay)

		var highLimit, lowLimit float64
		if priceCache != nil {
			highLimit, lowLimit = priceCache.GetOrCompute(code)
		}

		tick := Tick{
			TradingDay:     c.tradingDay,
			Code:           code,
			Time:           updateTime,
			UpdateTime:     localTime,
			CurrentPrice:   idx.f64(row, "LastPrice"),
			TotalVolume:    idx.f64(row, "TradVolume"),
			TotalMoney:     idx.f64(row, "Turnover"),
			PreClosePrice:  idx.f64(row, "PreCloPrice"),
			OpenPrice:      idx.f64(row, "OpenPrice"),
			HighestPrice:   idx.f64(row, "HighPrice"),
			LowestPrice:    idx.f64(row, "LowPrice"),
			HighLimitPrice: highLimit,
			LowLimitPrice:  lowLimit,
			IOPV:           0,
			TradeNum:       idx.f64(row, "TradNumber"),
			TotalBidVolume: idx.f64(row, "TotalBidVol"),
			TotalAskVolume: idx.f64(row, "TotalAskVol"),
			AvgBidPrice:    idx.f64(row, "WAvgBidPri"),
			AvgAskPrice:    idx.f64(row, "WAvgAskPri"),
			Channel:        0,
			SeqNum:         idx.i64(row, "SeqNo"),
		}

		for i := 1; i <= 10; i++ {
			setTickLevel(&tick, i,
				idx.f64(row, fmt.Sprintf("BidPrice%d", i)),
				idx.f64(row, fmt.Sprintf("BidVolume%d", i)),
				idx.f64(row, fmt.Sprintf("NumOrdersB%d", i)),
				idx.f64(row, fmt.Sprintf("AskPrice%d", i)),
				idx.f64(row, fmt.Sprintf("AskVolume%d", i)),
				idx.f64(row, fmt.Sprintf("NumOrdersS%d", i)),
			)
		}

		ticks = append(ticks, tick)
	}

	if skipped > 0 {
		fmt.Printf("[统计] 上交所快照跳过 %d 条无效 SecurityID\n", skipped)
	}
	return ticks, nil
}

// parseTimeQuick 快速解析时间（已知格式，避免循环尝试）
func parseTimeQuick(s string, tradingDay time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	// 优先尝试最常见格式
	if len(s) == 12 && s[2] == ':' {
		// 15:04:05.000
		t, err := time.Parse("15:04:05.000", s)
		if err == nil {
			return time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(),
				t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local), nil
		}
	}
	if len(s) == 10 && s[6] == '.' {
		// 150405.000
		t, err := time.Parse("150405.000", s)
		if err == nil {
			return time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(),
				t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local), nil
		}
	}
	return ParseTime(s, tradingDay)
}
