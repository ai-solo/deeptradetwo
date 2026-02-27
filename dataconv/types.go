package dataconv

import "time"

// DataType 数据类型
type DataType string

const (
	DataTypeOrder DataType = "order" // 逐笔委托
	DataTypeDeal  DataType = "deal"  // 逐笔成交
	DataTypeTick  DataType = "tick"  // 快照
)

// Market 市场
type Market string

const (
	MarketSH Market = "XSHG" // 上交所
	MarketSZ Market = "XSHE" // 深交所
)

// Order 逐笔委托数据
type Order struct {
	TradingDay time.Time // 交易日期
	Code       string    // 股票代码 (如 600000.XSHG)
	Time       time.Time // 委托时间
	UpdateTime time.Time // 更新时间
	OrderID    int64     // 委托编号
	Side       int16     // 买卖方向: 0=买, 1=卖
	Price      float64   // 委托价格
	Volume     float64   // 委托数量
	OrderType  int16     // 订单类型: 深市1/2/3, 沪市2/5
	Channel    int64     // 通道号
	SeqNum     int64     // 序列号
}

// Deal 逐笔成交数据
type Deal struct {
	TradingDay  time.Time // 交易日期
	Code        string    // 股票代码
	Time        time.Time // 成交时间
	UpdateTime  time.Time // 更新时间
	SaleOrderID int64     // 卖方委托编号
	BuyOrderID  int64     // 买方委托编号
	Side        int16     // 买卖方向: 0=买, 1=卖, 4=撤单, 10=未知
	Price       float64   // 成交价格
	Volume      float64   // 成交数量
	Money       float64   // 成交金额
	Channel     int64     // 通道号
	SeqNum      int64     // 序列号
}

// Tick 快照数据 (10档行情)
type Tick struct {
	TradingDay     time.Time // 交易日期
	Code           string    // 股票代码
	Time           time.Time // 快照时间
	UpdateTime     time.Time // 更新时间
	CurrentPrice   float64   // 最新价
	TotalVolume    float64   // 累计成交量
	TotalMoney     float64   // 累计成交额
	PreClosePrice  float64   // 前收盘价
	OpenPrice      float64   // 开盘价
	HighestPrice   float64   // 最高价
	LowestPrice    float64   // 最低价
	HighLimitPrice float64   // 涨停价
	LowLimitPrice  float64   // 跌停价
	IOPV           float64   // IOPV净值估值
	TradeNum       float64   // 成交笔数
	TotalBidVolume float64   // 委托买入总量
	TotalAskVolume float64   // 委托卖出总量
	AvgBidPrice    float64   // 加权平均买价
	AvgAskPrice    float64   // 加权平均卖价

	// 10档卖价
	AskPrice1, AskPrice2, AskPrice3, AskPrice4, AskPrice5     float64
	AskPrice6, AskPrice7, AskPrice8, AskPrice9, AskPrice10    float64
	// 10档卖量
	AskVolume1, AskVolume2, AskVolume3, AskVolume4, AskVolume5     float64
	AskVolume6, AskVolume7, AskVolume8, AskVolume9, AskVolume10    float64
	// 10档卖订单数
	AskNum1, AskNum2, AskNum3, AskNum4, AskNum5     float64
	AskNum6, AskNum7, AskNum8, AskNum9, AskNum10    float64
	// 10档买价
	BidPrice1, BidPrice2, BidPrice3, BidPrice4, BidPrice5     float64
	BidPrice6, BidPrice7, BidPrice8, BidPrice9, BidPrice10    float64
	// 10档买量
	BidVolume1, BidVolume2, BidVolume3, BidVolume4, BidVolume5     float64
	BidVolume6, BidVolume7, BidVolume8, BidVolume9, BidVolume10    float64
	// 10档买订单数
	BidNum1, BidNum2, BidNum3, BidNum4, BidNum5     float64
	BidNum6, BidNum7, BidNum8, BidNum9, BidNum10    float64

	Channel int64 // 通道号
	SeqNum  int64 // 序列号
}

// LimitPrice 涨跌停价格
type LimitPrice struct {
	Code       string
	TradingDay time.Time
	HighLimit  float64
	LowLimit   float64
	PreClose   float64
}

// ConvertConfig 转换配置
type ConvertConfig struct {
	DataDir      string    // 数据目录
	TradingDay   time.Time // 交易日期
	Workers      int       // 并发数
	OutputDir    string    // 输出目录
	DataType     DataType  // 数据类型
	SaveToCSV    bool      // 是否保存为CSV
	SaveToParquet bool     // 是否保存为Parquet
}

// ConvertResult 转换结果
type ConvertResult struct {
	DataType     DataType
	Code         string
	RowCount     int
	ConvertError error
	InvalidData  bool
	SaveError    error
}
