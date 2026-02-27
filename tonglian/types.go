package tonglian

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// MarketData represents parsed market data from TongLian
type MarketData struct {
	SecurityID   string
	SecurityName string
	Sid          int
	Mid          int
	LastPrice    float64
	Volume       int64
	Turnover     float64
	UpdateTime   int64
	LocalTime    time.Time
	RawData      string

	// Level 2 fields (10档行情)
	AskPrices []float64 `json:"AskPrices,omitempty"` // 卖价 1-10 档
	BidPrices []float64 `json:"BidPrices,omitempty"` // 买价 1-10 档
	AskVolumes []int64  `json:"AskVolumes,omitempty"` // 卖量 1-10 档
	BidVolumes []int64  `json:"BidVolumes,omitempty"` // 买量 1-10 档
}

// TongLianMessage represents a message from TongLian WebSocket
type TongLianMessage struct {
	Sid  int    `json:"sid"`
	Mid  int    `json:"mid"`
	Lt   int    `json:"lt"`
	Data string `json:"data"`
}

// TongLianJSONData represents JSON data field from TongLian
type TongLianJSONData struct {
	SecurityID   string  `json:"SecurityID"`
	SecurityName string  `json:"SecurityName"`
	LastPrice    float64 `json:"LastPrice"`
	Volume       int64   `json:"Volume"`
	Turnover     float64 `json:"Turnover"`
	UpdateTime   int64   `json:"UpdateTime"`
	TurnNum      int     `json:"TurnNum"`
}

// TongLianLevel2Data represents Level 2 market data with 10-level quotes
// Used for SH Messages 4, 6 and SZ Message 28
type TongLianLevel2Data struct {
	SecurityID   string    `json:"SecurityID"`
	SecurityName string    `json:"SecurityName"`
	LastPrice    float64   `json:"LastPrice"`
	Volume       int64     `json:"Volume"`
	Turnover     float64   `json:"Turnover"`
	UpdateTime   int64     `json:"UpdateTime"`
	AskPrices    []float64 `json:"AskPrices,omitempty"` // 卖价 1-10 档
	BidPrices    []float64 `json:"BidPrices,omitempty"` // 买价 1-10 档
	AskVolumes   []int64   `json:"AskVolumes,omitempty"` // 卖量 1-10 档
	BidVolumes   []int64   `json:"BidVolumes,omitempty"` // 买量 1-10 档
}

// SubscriptionRequest represents a subscription request to TongLian
type SubscriptionRequest struct {
	Format    string   `json:"format"`
	Subscribe []string `json:"subscribe"`
}

// SubscriptionResponse represents a subscription response from TongLian
type SubscriptionResponse struct {
	Result            string `json:"result"`
	SubscribedMessages string `json:"subscribed_messages"`
	Reason            string `json:"reason,omitempty"`
}

// ConnectionState represents the WebSocket connection state
type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
)

func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	default:
		return "unknown"
	}
}

// ParseLocalTime parses TongLian local time format (HHMMSSmmm) to Time
func ParseLocalTime(lt int) time.Time {
	hours := lt / 10000000
	minutes := (lt / 100000) % 100
	seconds := (lt / 1000) % 100
	millis := lt % 1000

	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(),
		hours, minutes, seconds, millis*1000000, now.Location())
}

// ParseJSONMessage parses a JSON format message from TongLian
func ParseJSONMessage(msg []byte) (*MarketData, error) {
	var tlMsg TongLianMessage
	if err := json.Unmarshal(msg, &tlMsg); err != nil {
		return nil, err
	}

	var jsonData TongLianJSONData
	if err := json.Unmarshal([]byte(tlMsg.Data), &jsonData); err != nil {
		return nil, err
	}

	return &MarketData{
		SecurityID:   jsonData.SecurityID,
		SecurityName: jsonData.SecurityName,
		Sid:          tlMsg.Sid,
		Mid:          tlMsg.Mid,
		LastPrice:    jsonData.LastPrice,
		Volume:       jsonData.Volume,
		Turnover:     jsonData.Turnover,
		UpdateTime:   jsonData.UpdateTime,
		LocalTime:    ParseLocalTime(tlMsg.Lt),
		RawData:      string(msg),
	}, nil
}

// ParseLevel2JSONMessage parses a Level 2 JSON message from TongLian
// Handles SH Messages 4, 6 and SZ Message 28 with 10-level quotes
func ParseLevel2JSONMessage(msg []byte) (*MarketData, error) {
	var tlMsg TongLianMessage
	if err := json.Unmarshal(msg, &tlMsg); err != nil {
		return nil, err
	}

	var level2Data TongLianLevel2Data
	if err := json.Unmarshal([]byte(tlMsg.Data), &level2Data); err != nil {
		return nil, err
	}

	return &MarketData{
		SecurityID:   level2Data.SecurityID,
		SecurityName: level2Data.SecurityName,
		Sid:          tlMsg.Sid,
		Mid:          tlMsg.Mid,
		LastPrice:    level2Data.LastPrice,
		Volume:       level2Data.Volume,
		Turnover:     level2Data.Turnover,
		UpdateTime:   level2Data.UpdateTime,
		LocalTime:    ParseLocalTime(tlMsg.Lt),
		RawData:      string(msg),
		AskPrices:    level2Data.AskPrices,
		BidPrices:    level2Data.BidPrices,
		AskVolumes:   level2Data.AskVolumes,
		BidVolumes:   level2Data.BidVolumes,
	}, nil
}

// IsLevel2Message checks if a message is a Level 2 market data message
func IsLevel2Message(sid, mid int) bool {
	// SH Level 2: ServiceID 3, Messages 4, 6, 16, 17, 20, 21, 22, 23
	if sid == 3 {
		switch mid {
		case 4, 6, 16, 17, 20, 21, 22, 23:
			return true
		}
	}

	// SZ Level 2: ServiceID 5, Messages 28, 29, 30, 31, 42, 43, 44, 45, 46, 47, 50, 51, 54
	if sid == 5 {
		switch mid {
		case 28, 29, 30, 31, 42, 43, 44, 45, 46, 47, 50, 51, 54:
			return true
		}
	}

	return false
}

// ParseCSVMessage parses a CSV format message from TongLian
func ParseCSVMessage(msg []byte) (*MarketData, error) {
	var tlMsg TongLianMessage
	if err := json.Unmarshal(msg, &tlMsg); err != nil {
		return nil, err
	}

	// CSV format for bond data (Message 23, 51, 54):
	// SecurityID,UpdateTime,SecurityName,ObjectID,MarketClass,AssetClass,SubAssetClass,
	// Currency,FaceValue,LastTradeDate,ListDate,SetID,AskUnit,BidUnit,LLimitNum,ULimitNum,
	// PreClosePrice,DeltaPriceUnit,LimitType,HighLimitPrice,LowLimitPrice,XRPercentage,
	// XDMoney,FType,SType,Status,LMktPNum,UMktNum,Note,LocalTime,SeqNo
	fields := strings.Split(tlMsg.Data, ",")
	if len(fields) < 30 {
		return nil, &ParseError{Message: "CSV has insufficient fields", Raw: tlMsg.Data}
	}

	securityID := fields[0]
	// UpdateTime is field 1, but format is "HH:MM:SS.mmm"
	securityName := fields[2]

	// Parse PreClosePrice (field 16) as LastPrice for bonds
	var lastPrice float64
	if len(fields) > 16 {
		lastPrice, _ = strconv.ParseFloat(fields[16], 64)
	}

	// Volume and Turnover are not always present in bond snapshots
	var volume int64
	var turnover float64

	return &MarketData{
		SecurityID:   securityID,
		SecurityName: securityName,
		Sid:          tlMsg.Sid,
		Mid:          tlMsg.Mid,
		LastPrice:    lastPrice,
		Volume:       volume,
		Turnover:     turnover,
		UpdateTime:   time.Now().Unix(),
		LocalTime:    ParseLocalTime(tlMsg.Lt),
		RawData:      string(msg),
	}, nil
}

// ParseError represents a parsing error
type ParseError struct {
	Message string
	Raw     string
}

func (e *ParseError) Error() string {
	return e.Message + ": " + e.Raw
}

// KlineBucket represents an aggregation bucket for K-line calculation
type KlineBucket struct {
	SecurityID  string
	Timeframe   string
	OpenTime    int64
	CloseTime   int64
	OpenPrice   float64
	HighPrice   float64
	LowPrice    float64
	ClosePrice  float64
	Volume      int64
	Turnover    float64
	TradeCount  int
	IsClosed    bool
}

// NewKlineBucket creates a new K-line bucket
func NewKlineBucket(securityID, timeframe string, openTime int64) *KlineBucket {
	return &KlineBucket{
		SecurityID: securityID,
		Timeframe:  timeframe,
		OpenTime:   openTime,
		HighPrice:  -1, // Will be updated on first tick
		LowPrice:   -1, // Will be updated on first tick
		TradeCount: 0,
		IsClosed:   false,
	}
}

// Update updates the bucket with a new tick
func (b *KlineBucket) Update(price float64, volume int64, turnover float64) {
	if b.TradeCount == 0 {
		b.OpenPrice = price
		b.HighPrice = price
		b.LowPrice = price
	} else {
		if price > b.HighPrice {
			b.HighPrice = price
		}
		if price < b.LowPrice {
			b.LowPrice = price
		}
	}

	b.ClosePrice = price
	b.Volume += volume
	b.Turnover += turnover
	b.TradeCount++
}

// GetBucketOpenTime calculates the bucket open time for a given timestamp
func GetBucketOpenTime(timestamp int64, timeframe string) int64 {
	t := time.Unix(timestamp, 0)

	switch timeframe {
	case "1m":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).Unix()
	case "5m":
		minute := (t.Minute() / 5) * 5
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, t.Location()).Unix()
	case "15m":
		minute := (t.Minute() / 15) * 15
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, t.Location()).Unix()
	case "1h":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Unix()
	case "1d":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
	default:
		return timestamp
	}
}
